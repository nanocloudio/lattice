//! Wire protocol definition linting tool.

use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "wire_lint")]
#[command(about = "Lint wire protocol definitions")]
struct Args {
    /// Path to the wire definition JSON file.
    #[arg(short, long)]
    wire: PathBuf,
}

fn main() -> Result<()> {
    let args = Args::parse();

    let content = std::fs::read_to_string(&args.wire)?;
    let _wire: serde_json::Value = serde_json::from_str(&content)?;

    println!("Wire definition validated: {:?}", args.wire);
    Ok(())
}
