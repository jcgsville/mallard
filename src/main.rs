mod config;
mod migrate;
mod migration_files;
mod migration_hash;
mod migration_state;
mod project;
mod reset;
mod status;

use std::env;
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

    /// Apply committed migrations to the target database.
    Migrate,

    /// Report pending committed and current migration status.
    Status,

    /// Recreate the database and reapply committed migrations.
    Reset {
        /// Required confirmation for destructive database reset.
        #[arg(long)]
        force: bool,
    },
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
        Commands::Migrate => {
            let working_dir = env::current_dir()?;
            let config = config::Config::discover(&working_dir, cli.config.as_deref())?;
            let result = migrate::run(&config)?;

            println!(
                "Applied {} committed migration(s) to {}",
                result.applied_count,
                result.database_path.display()
            );
        }
        Commands::Status => {
            let working_dir = env::current_dir()?;
            let config = config::Config::discover(&working_dir, cli.config.as_deref())?;
            let result = status::run(&config)?;

            println!(
                "Pending committed migrations: {}",
                if result.pending_committed {
                    "yes"
                } else {
                    "no"
                }
            );
            println!(
                "Current migration has changes: {}",
                if result.current_migration_has_changes {
                    "yes"
                } else {
                    "no"
                }
            );
            std::process::exit(result.exit_code());
        }
        Commands::Reset { force } => {
            let working_dir = env::current_dir()?;
            let config = config::Config::discover(&working_dir, cli.config.as_deref())?;
            let result = reset::run(&config, force)?;

            println!(
                "Reset {} and applied {} committed migration(s)",
                result.database_path.display(),
                result.migrate_result.applied_count
            );
        }
    }

    Ok(())
}
