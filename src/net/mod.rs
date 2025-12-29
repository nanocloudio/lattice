//! Networking layer.
//!
//! This module handles network transport and security:
//! - [`tls`] - TLS and mTLS configuration
//! - [`security`] - Security manager and credentials
//! - [`listeners`] - Protocol listeners
//! - [`tcp`] - TCP listener abstraction for protocol adapters

pub mod listeners;
pub mod security;
pub mod tcp;
pub mod tls;
