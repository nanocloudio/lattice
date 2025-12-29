//! Command-line interface.
//!
//! Unified CLI for Lattice operations.

pub mod commands;

use clap::{Parser, Subcommand};

/// Lattice - Raft-backed multi-protocol key-value overlay.
#[derive(Parser, Debug)]
#[command(name = "lattice")]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    /// Configuration file path.
    #[arg(short, long, global = true)]
    pub config: Option<String>,

    /// Log level (trace, debug, info, warn, error).
    #[arg(long, global = true)]
    pub log_level: Option<String>,

    #[command(subcommand)]
    pub command: Commands,
}

/// Available commands.
#[derive(Subcommand, Debug)]
pub enum Commands {
    /// Start the Lattice server.
    Start(commands::StartArgs),
    /// Show server status.
    Status(commands::StatusArgs),
    /// Initialize storage and fixtures.
    Init(commands::InitArgs),
    /// Configuration operations.
    Config(commands::ConfigArgs),
    /// Cluster operations.
    Cluster(commands::ClusterArgs),
    /// Inspect WAL and state.
    Inspect(commands::InspectArgs),
    /// Snapshot operations.
    Snapshot(commands::SnapshotArgs),
    /// Telemetry operations.
    Telemetry(commands::TelemetryArgs),
}
