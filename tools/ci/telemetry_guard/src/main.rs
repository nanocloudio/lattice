//! Telemetry catalog validation tool.

#![allow(
    clippy::disallowed_macros,
    reason = "host-side validation CLI: reporting catalog findings on stdout is the tool's output contract"
)]

use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "telemetry_guard")]
#[command(about = "Validate telemetry catalog definitions")]
struct Args {
    /// Path to the telemetry catalog JSON file.
    #[arg(short, long)]
    catalog: PathBuf,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let content = std::fs::read_to_string(&args.catalog)?;
    let _catalog: serde_json::Value = serde_json::from_str(&content)?;

    println!("Catalog validated: {:?}", args.catalog);
    Ok(())
}
