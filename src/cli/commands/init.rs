//! Init command implementation.

use anyhow::Result;
use clap::Args;
use std::path::PathBuf;

/// Initialize storage and fixtures.
#[derive(Args, Debug)]
pub struct InitArgs {
    /// Data directory.
    #[arg(short, long, default_value = "data")]
    pub data_dir: PathBuf,

    /// Generate development certificates.
    #[arg(long)]
    pub dev_certs: bool,
}

/// Run the init command.
pub fn run_init(args: InitArgs) -> Result<()> {
    std::fs::create_dir_all(&args.data_dir)?;
    println!("Initialized data directory: {:?}", args.data_dir);

    if args.dev_certs {
        let certs_dir = args.data_dir.join("certs");
        std::fs::create_dir_all(&certs_dir)?;
        println!("Created certs directory: {:?}", certs_dir);
        // TODO: Generate development certificates
    }

    Ok(())
}
