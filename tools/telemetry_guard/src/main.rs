//! Telemetry catalog validation tool.

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
