//! Lattice - unified CLI entrypoint.
//!
//! Usage:
//!   lattice start --config config/lattice.toml
//!   lattice status [--endpoint URL]
//!   lattice config validate --config config/lattice.toml
//!   lattice cluster status [--endpoint URL]
//!   lattice inspect wal <wal-segment-or-directory>...
//!   lattice snapshot inspect <target>

use anyhow::Result;
use clap::Parser;
use lattice::cli::commands::{
    run_cluster, run_config, run_init, run_inspect, run_snapshot, run_start_with_config,
    run_status, run_telemetry,
};
use lattice::cli::{Cli, Commands};
use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    // Determine config path - use global --config or default
    let config_path = cli
        .config
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("config/lattice.toml"));

    match cli.command {
        Commands::Start(_args) => run_start_with_config(&config_path).await,
        Commands::Status(args) => run_status(args),
        Commands::Init(args) => run_init(args),
        Commands::Config(args) => run_config(args),
        Commands::Cluster(args) => run_cluster(args),
        Commands::Inspect(args) => run_inspect(args),
        Commands::Snapshot(args) => run_snapshot(args),
        Commands::Telemetry(args) => run_telemetry(args),
    }
}
