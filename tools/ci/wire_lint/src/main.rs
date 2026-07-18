//! Wire protocol definition linting tool.

#![allow(
    clippy::disallowed_macros,
    reason = "host-side lint CLI: reporting findings on stdout is the tool's output contract"
)]

use anyhow::{bail, Context, Result};
use clap::Parser;
use std::path::{Path, PathBuf};

#[derive(Parser)]
#[command(name = "wire_lint")]
#[command(about = "Lint wire protocol definitions")]
struct Args {
    /// Path to a single wire definition JSON file.
    #[arg(short, long)]
    wire: Option<PathBuf>,

    /// Directory of wire definitions; every `*.json` in it is validated.
    /// This is what `make lint` uses (all of `wire/`).
    #[arg(long)]
    wire_dir: Option<PathBuf>,
}

fn validate(path: &Path) -> Result<()> {
    let content =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    serde_json::from_str::<serde_json::Value>(&content)
        .with_context(|| format!("parsing {}", path.display()))?;
    println!("Wire definition validated: {}", path.display());
    Ok(())
}

fn main() -> Result<()> {
    let args = Args::parse();

    match (args.wire, args.wire_dir) {
        (Some(file), None) => validate(&file),
        (None, Some(dir)) => {
            // Deterministic order so CI output is stable.
            let mut files: Vec<PathBuf> = std::fs::read_dir(&dir)
                .with_context(|| format!("reading dir {}", dir.display()))?
                .filter_map(|e| e.ok().map(|e| e.path()))
                .filter(|p| p.extension().is_some_and(|ext| ext == "json"))
                .collect();
            files.sort();
            if files.is_empty() {
                bail!("no *.json wire definitions found in {}", dir.display());
            }
            for f in &files {
                validate(f)?;
            }
            println!(
                "{} wire definition(s) validated in {}",
                files.len(),
                dir.display()
            );
            Ok(())
        }
        (Some(_), Some(_)) => bail!("pass either --wire <file> or --wire-dir <dir>, not both"),
        (None, None) => bail!("pass --wire <file> or --wire-dir <dir>"),
    }
}
