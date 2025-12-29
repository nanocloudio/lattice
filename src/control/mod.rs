//! Control plane integration.
//!
//! This module handles interaction with CP-Raft (ControlPlaneRaft):
//! - [`api`] - CP-Raft API types (TenantManifest, AdapterPolicy, QuotaConfig)
//! - [`cache`] - CP cache agent and freshness semantics
//! - [`routing`] - Routing metadata and epoch validation
//! - [`capabilities`] - Adapter capability registry
//! - [`placement`] - KPG placement management
//!
//! # Cache Freshness
//!
//! The cache agent maintains tenant configuration with Clustor's Fresh/Cached/Stale/Expired
//! state matrix (§12.2). When cache degrades to Stale/Expired:
//! - Linearizable operations fail with LIN-BOUND
//! - Follower reads are revoked
//! - Watch start linearization guarantees are revoked

pub mod api;
pub mod cache;
pub mod capabilities;
pub mod placement;
pub mod routing;
