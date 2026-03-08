mod config;
mod project;

use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};

/// Forward-only DuckDB migration CLI.
#[derive(Debug, Parser)]
#[command(
    name = "mallard",
    version,
    about = "Manage forward-only DuckDB schema migrations"
)]
struct Cli {
    /// Path to a Mallard config file.
    #[arg(long, global = true)]
    config: Option<PathBuf>,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Initialize a Mallard project.
    Init,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Init => {
            let result = project::init(cli.config.as_deref())?;

            if result.config_created {
                println!("Created {}", result.config_path.display());
            } else {
                println!("Using existing {}", result.config_path.display());
            }

            println!("Prepared {}", result.committed_dir.display());
            println!("Prepared {}", result.current_migration.display());
        }
    }

    Ok(())
}
