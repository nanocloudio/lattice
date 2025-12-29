//! Inspect command implementation.

use anyhow::Result;
use clap::{Args, Subcommand};
use std::path::PathBuf;

/// Inspect WAL and state.
#[derive(Args, Debug)]
pub struct InspectArgs {
    #[command(subcommand)]
    pub command: InspectCommand,
}

/// Inspect subcommands.
#[derive(Subcommand, Debug)]
pub enum InspectCommand {
    /// Inspect WAL segments.
    Wal {
        /// WAL segment paths.
        #[arg(required = true)]
        paths: Vec<PathBuf>,
    },
    /// Inspect KV state.
    State {
        /// Data directory.
        #[arg(short, long, default_value = "data")]
        data_dir: PathBuf,
    },
}

/// Run the inspect command.
pub fn run_inspect(args: InspectArgs) -> Result<()> {
    match args.command {
        InspectCommand::Wal { paths } => {
            for path in paths {
                println!("Inspecting WAL: {:?}", path);
                // TODO: Implement WAL inspection
            }
        }
        InspectCommand::State { data_dir } => {
            println!("Inspecting state in: {:?}", data_dir);
            // TODO: Implement state inspection
        }
    }
    Ok(())
}
