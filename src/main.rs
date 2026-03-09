mod commit;
mod compiler;
mod config;
mod current_migration;
mod migrate;
mod migration_files;
mod migration_hash;
mod migration_state;
mod project;
mod reset;
mod run_current;
mod status;
mod uncommit;
mod watch;

use std::env;
use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};

/// DuckDB schema migration CLI
#[derive(Debug, Parser)]
#[command(name = "mallard", version, about = "DuckDB schema migrations")]
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

    /// Commit the current migration into the committed sequence.
    Commit {
        /// Message stored in the committed migration header and filename slug.
        message: String,
    },

    /// Move the latest unapplied committed migration back into the current migration.
    Uncommit,

    /// Compile the current migration with includes and placeholders resolved.
    Compile {
        /// Optional output path. Prints to stdout when omitted.
        #[arg(long)]
        output: Option<PathBuf>,
    },

    /// Replay committed migrations and apply the current migration.
    Run {
        /// Target database to use for the run helper.
        #[arg(long, value_enum, default_value = "shadow")]
        target: run_current::RunTarget,
    },

    /// Watch migration inputs and rerun the current migration flow.
    Watch {
        /// Target database to use while watching.
        #[arg(long, value_enum, default_value = "shadow")]
        target: run_current::RunTarget,

        /// Run a single watch cycle and exit.
        #[arg(long)]
        once: bool,

        /// Polling interval in milliseconds.
        #[arg(long, default_value_t = 1000)]
        interval_ms: u64,
    },

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
        Commands::Commit { message } => {
            let working_dir = env::current_dir()?;
            let config = config::Config::discover(&working_dir, cli.config.as_deref())?;
            let result = commit::run(&config, &message)?;

            println!("Created {}", result.committed_path.display());
            println!("Reset {}", result.reset_target_path.display());
            println!(
                "Validated against shadow database {}",
                result.shadow_database_path.display()
            );
        }
        Commands::Uncommit => {
            let working_dir = env::current_dir()?;
            let config = config::Config::discover(&working_dir, cli.config.as_deref())?;
            let result = uncommit::run(&config)?;

            println!("Removed {}", result.removed_committed_path.display());
            println!(
                "Restored current migration at {}",
                result.restored_current_path.display()
            );
        }
        Commands::Compile { output } => {
            let working_dir = env::current_dir()?;
            let config = config::Config::discover(&working_dir, cli.config.as_deref())?;
            let compiled = compiler::compile_current(&config)?;

            if let Some(output) = output {
                std::fs::write(&output, &compiled)?;
                println!("Wrote {}", output.display());
            } else {
                print!("{compiled}");
            }
        }
        Commands::Run { target } => {
            let working_dir = env::current_dir()?;
            let config = config::Config::discover(&working_dir, cli.config.as_deref())?;
            let result = run_current::run(&config, target)?;

            println!(
                "Applied current migration to {}",
                result.database_path.display()
            );
        }
        Commands::Watch {
            target,
            once,
            interval_ms,
        } => {
            let working_dir = env::current_dir()?;
            let config = config::Config::discover(&working_dir, cli.config.as_deref())?;
            let result = watch::run(
                &config,
                target,
                once,
                std::time::Duration::from_millis(interval_ms),
            )?;

            println!(
                "Completed {} watch cycle(s) against {}",
                result.cycles,
                result.last_run.database_path.display()
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
