//! Operations and observability.
//!
//! This module handles operational concerns:
//! - [`version`] - Version gates and compatibility checking
//! - [`observability`] - Metrics and health checks
//! - [`telemetry`] - Telemetry collection
//! - [`audit`] - Security audit logging
//! - [`dr`] - Disaster recovery

pub mod audit;
pub mod dr;
pub mod observability;
pub mod telemetry;
pub mod version;
