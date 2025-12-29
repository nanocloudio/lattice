//! Core runtime infrastructure.
//!
//! This module contains the essential components for running Lattice:
//! - [`config`] - Configuration parsing and validation
//! - [`runtime`] - Main runtime orchestration
//! - [`time`] - Deterministic time utilities
//! - [`error`] - Error types and adapter-specific mapping

pub mod config;
pub mod error;
pub mod runtime;
pub mod time;
