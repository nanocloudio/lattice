//! Start command implementation.

use crate::core::config::Config;
use crate::core::runtime::Runtime;
use anyhow::{Context, Result};
use clap::Args;
use std::path::PathBuf;

/// Start the Lattice server.
#[derive(Args, Debug)]
pub struct StartArgs {
    // No additional arguments - config is handled globally
}

/// Initialize tracing subscriber if the telemetry feature is enabled.
#[cfg(feature = "telemetry")]
fn init_tracing() {
    use tracing_subscriber::{fmt, prelude::*, EnvFilter};

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));

    tracing_subscriber::registry()
        .with(fmt::layer().with_target(true))
        .with(filter)
        .init();
}

#[cfg(not(feature = "telemetry"))]
fn init_tracing() {}

/// Run the start command with the given config path.
pub async fn run_start_with_config(config_path: &PathBuf) -> Result<()> {
    init_tracing();

    let config = Config::from_file(config_path)
        .with_context(|| format!("failed to load config from {:?}", config_path))?;

    let mut runtime = Runtime::new(config)?;
    runtime.run().await
}

/// Run the start command (for backwards compatibility).
pub async fn run_start(_args: StartArgs) -> Result<()> {
    // This should not be called directly - use run_start_with_config
    let default_path = PathBuf::from("config/lattice.toml");
    run_start_with_config(&default_path).await
}
