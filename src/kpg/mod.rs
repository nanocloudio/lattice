//! Key-Value Partition Group (KPG) runtime.
//!
//! A KPG is a tenant-scoped Clustor partition hosting a KV state machine.
//! This module contains:
//! - [`state_machine`] - KV state machine core
//! - [`apply_loop`] - WAL entry processing
//! - [`revision`] - Revision model and fencing
//! - [`lease`] - Lease management
//! - [`watch`] - Watch stream state
//! - [`idempotency`] - Retry deduplication
//! - [`lin_bound`] - LIN-BOUND linearizability gate
//! - [`ttl`] - Deterministic TTL and expiration handling

pub mod apply_loop;
pub mod idempotency;
pub mod lease;
pub mod lin_bound;
pub mod revision;
pub mod state_machine;
pub mod ttl;
pub mod watch;
