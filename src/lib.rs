//! Lattice - Multi-protocol key-value overlay with Raft-based replication.
//!
//! Lattice is a single-binary, multi-protocol distributed key-value store that layers
//! etcd v3, Redis, and Memcached client semantics on top of the Clustor Raft consensus
//! core. Each tenant is sharded into Key-Value Partition Groups (KPGs) that host the
//! same KV state machine, while ControlPlaneRaft (CP-Raft) stores tenant manifests,
//! routing epochs, adapter policies, quotas, and PKI material.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                        Client Protocols                         │
//! │    etcd v3 gRPC    │    Redis (future)    │  Memcached (future) │
//! └─────────────────────────────────────────────────────────────────┘
//!                                  │
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                      Protocol Adapters                          │
//! │              (identity, routing, quota enforcement)             │
//! └─────────────────────────────────────────────────────────────────┘
//!                                  │
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                      KPG State Machine                          │
//! │     KV Store │ Leases │ Watches │ MVCC │ Deterministic TTL      │
//! └─────────────────────────────────────────────────────────────────┘
//!                                  │
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                      Clustor Raft Core                          │
//! │              WAL │ Snapshots │ Replication │ Consensus          │
//! └─────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Module Organization
//!
//! ## Core
//! - [`core::config`] - Configuration parsing and validation
//! - [`core::runtime`] - Main runtime orchestration
//! - [`core::time`] - Deterministic time utilities
//! - [`core::error`] - Error types and mapping
//!
//! ## Control Plane
//! - [`control::cache`] - CP-Raft cache agent and freshness semantics
//! - [`control::routing`] - Routing metadata and epoch validation
//! - [`control::capabilities`] - Adapter capability registry
//! - [`control::placement`] - KPG placement management
//!
//! ## KPG (Key-Value Partition Group)
//! - [`kpg::state_machine`] - KV state machine core
//! - [`kpg::apply_loop`] - WAL entry processing and apply loop
//! - [`kpg::revision`] - Revision model and fencing
//! - [`kpg::lease`] - Lease management
//! - [`kpg::watch`] - Watch stream state
//! - [`kpg::idempotency`] - Retry deduplication
//!
//! ## Storage
//! - [`storage::wal`] - Write-ahead log and snapshot management
//! - [`storage::compaction`] - Log compaction and cleanup
//!
//! ## Adapters
//! - [`adapters::etcd`] - etcd v3 gRPC adapter
//!
//! ## Networking
//! - [`net::tls`] - TLS and mTLS configuration
//! - [`net::security`] - Security manager and credentials
//! - [`net::listeners`] - Protocol listeners
//!
//! ## Operations
//! - [`ops::observability`] - Metrics and health checks
//! - [`ops::telemetry`] - Telemetry collection
//! - [`ops::dr`] - Disaster recovery
//!
//! ## CLI
//! - [`cli::commands`] - CLI command implementations
//!
//! # Key Invariants
//!
//! - **LIN-BOUND**: Linearizable operations require Clustor ReadIndex eligibility
//! - **KVSOURCE**: All KV effects derive from WAL entries or signed snapshots
//! - **REV-MONOTONE**: Revision is strictly increasing per KPG
//! - **CAS-FENCE**: CAS operations evaluated at single revision fence
//! - **TTL-DETERMINISM**: Expiration via apply-loop ticks only

// Core infrastructure
pub mod core;

// Control plane integration
pub mod control;

// KPG runtime and state machine
pub mod kpg;

// Storage layer
pub mod storage;

// Protocol adapters
pub mod adapters;

// Networking
pub mod net;

// Operations and observability
pub mod ops;

// CLI
pub mod cli;

// Re-exports for convenience
pub use self::core::{config, error, runtime, time};
pub use adapters::etcd;
pub use control::{cache as cp_cache, capabilities, placement, routing};
pub use kpg::{apply_loop, idempotency, lease, lin_bound, revision, state_machine, ttl, watch};
pub use net::{listeners, security, tls};
pub use ops::{dr, observability, telemetry};
pub use storage::{compaction, wal};
