//! etcd v3 gRPC adapter.
//!
//! This adapter exposes an etcd v3-compatible gRPC endpoint per §10.1.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                      etcd v3 gRPC                           │
//! ├─────────────┬─────────────┬─────────────┬───────────────────┤
//! │   KV API    │  Watch API  │  Lease API  │      Txn API      │
//! │  Range/Put  │   Create    │   Grant     │  Compare-And-Set  │
//! │   Delete    │   Cancel    │   Revoke    │    Atomic Ops     │
//! │             │   Progress  │  KeepAlive  │                   │
//! └─────────────┴─────────────┴─────────────┴───────────────────┘
//!                              │
//! ┌─────────────────────────────────────────────────────────────┐
//! │                     Service Layer                           │
//! │  - mTLS identity extraction                                 │
//! │  - Request context propagation                              │
//! │  - kv_epoch routing validation                              │
//! │  - LIN-BOUND enforcement                                    │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Key Behaviors
//!
//! - Linearizable reads require `can_linearize=true` (ReadIndex fence)
//! - `serializable=true` maps to snapshot-only semantics
//! - Transactions evaluated at single revision fence (CAS-FENCE)
//! - Watch requires persistence of `last_sent_revision`
//!
//! # Modules
//!
//! - [`service`] - gRPC service setup, listener, and mTLS
//! - [`kv`] - Range, Put, Delete request/response types
//! - [`watch`] - Watch stream types
//! - [`lease`] - Lease management types
//! - [`txn`] - Transaction types and compare evaluation
//! - [`auth`] - Authentication and identity verification
//! - [`errors`] - Error mapping to gRPC status codes

pub mod auth;
pub mod errors;
#[cfg(feature = "grpc")]
pub mod grpc;
pub mod kv;
pub mod lease;
#[cfg(feature = "grpc")]
pub mod proto;
pub mod service;
pub mod txn;
pub mod watch;

// Re-export commonly used types
pub use auth::{
    AuthCacheState, AuthService, AuthServiceConfig, AuthServiceMetrics, AuthenticateRequest,
    AuthenticateResponse, IdentityExtractor, IdentityVerifier, KeyPermission, Permission, Role,
    RoleAddRequest, RoleDeleteRequest, RoleGrantPermissionRequest, UserAddRequest,
    UserDeleteRequest, UserEntry, UserGrantRoleRequest,
};
pub use errors::{to_etcd_error, EtcdError, GrpcCode};
#[cfg(feature = "grpc")]
pub use grpc::{EtcdGrpcServer, KvService, LeaseService as GrpcLeaseService, SharedState};
pub use kv::{
    DeleteRangeRequest, DeleteRangeResponse, KeyValue, PutRequest, PutResponse, RangeRequest,
    RangeResponse, ResponseHeader,
};
pub use lease::{
    LeaseExpiration, LeaseGrantRequest, LeaseGrantResponse, LeaseKeepAliveRequest,
    LeaseKeepAliveResponse, LeaseLeasesRequest, LeaseLeasesResponse, LeaseRevokeRequest,
    LeaseRevokeResponse, LeaseService, LeaseServiceConfig, LeaseServiceMetrics, LeaseSnapshotEntry,
    LeaseStatus, LeaseTimeToLiveRequest, LeaseTimeToLiveResponse,
};
pub use service::{EtcdAdapter, EtcdServiceConfig, GrpcListener};
pub use txn::{
    Compare, RequestOp, ResponseOp, TxnCachedOutcome, TxnExecutor, TxnExecutorContext,
    TxnExecutorResult, TxnIdempotencyKey, TxnRequest, TxnResponse, TxnValidationConfig,
};
pub use watch::{
    Event, WatchCreateRequest, WatchCreateResult, WatchKeyRange, WatchRequest, WatchResponse,
    WatchSemantics, WatchStreamConfig, WatchStreamManager, WatchStreamMetrics, WatchStreamState,
    WatchableEvent,
};
