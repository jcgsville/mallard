use std::path::PathBuf;

use clap::{Parser, Subcommand};
use duckdb::Connection;

/// Forward-only DuckDB migration CLI.
#[derive(Debug, Parser)]
#[command(
    name = "mallard",
    version,
    about = "Manage forward-only DuckDB schema migrations"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Initialize Mallard against a DuckDB database.
    Init {
        /// Path to the DuckDB database file.
        #[arg(long)]
        db_path: PathBuf,
    },
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Init { db_path } => {
            let _connection = Connection::open(&db_path)?;
            println!("Connected to DuckDB at {}", db_path.display());
        }
    }

    Ok(())
}
