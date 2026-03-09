use std::fs;

use anyhow::{bail, Context, Result};

use crate::{config::Config, migrate, migrate::MigrateResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResetResult {
    pub database_path: std::path::PathBuf,
    pub migrate_result: MigrateResult,
}

pub fn run(config: &Config, force: bool) -> Result<ResetResult> {
    if !force {
        bail!(
            "reset is destructive and will replace {}. Re-run with --force to continue",
            config.database_path.display()
        );
    }

    remove_if_exists(&config.database_path)?;
    remove_if_exists(&config.database_path.with_extension("duckdb.wal"))?;

    let migrate_result = migrate::run(config)?;
    Ok(ResetResult {
        database_path: config.database_path.clone(),
        migrate_result,
    })
}

fn remove_if_exists(path: &std::path::Path) -> Result<()> {
    if path.exists() {
        fs::remove_file(path).with_context(|| format!("failed to remove {}", path.display()))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use duckdb::Connection;
    use tempfile::tempdir;

    use super::run;
    use crate::{config::Config, migrate, migration_hash};

    #[test]
    fn requires_force_for_destructive_reset() {
        let temp_dir = tempdir().unwrap();
        let config_path = temp_dir.path().join("mallard.toml");
        fs::write(&config_path, "version = 1").unwrap();
        fs::create_dir_all(temp_dir.path().join("migrations/committed")).unwrap();

        let config = Config::load(&config_path).unwrap();
        let error = run(&config, false).unwrap_err();

        assert!(error.to_string().contains("Re-run with --force"));
    }

    #[test]
    fn recreates_database_from_committed_migrations() {
        let temp_dir = tempdir().unwrap();
        let config_path = temp_dir.path().join("mallard.toml");
        fs::write(&config_path, "version = 1").unwrap();
        let committed_dir = temp_dir.path().join("migrations/committed");
        fs::create_dir_all(&committed_dir).unwrap();

        let body =
            "create table users (id integer primary key); insert into users (id) values (1);";
        let hash = migration_hash::calculate(None, body);
        fs::write(
            committed_dir.join("000001.sql"),
            format!("--! Previous: \n--! Hash: {hash}\n\n{body}\n"),
        )
        .unwrap();

        let config = Config::load(&config_path).unwrap();
        migrate::run(&config).unwrap();

        let connection = Connection::open(&config.database_path).unwrap();
        connection
            .execute("insert into users (id) values (?)", [2])
            .unwrap();

        let result = run(&config, true).unwrap();

        assert_eq!(result.migrate_result.applied_count, 1);

        let connection = Connection::open(&config.database_path).unwrap();
        let user_count: i64 = connection
            .query_row("select count(*) from users", [], |row| row.get(0))
            .unwrap();
        assert_eq!(user_count, 1);
    }
}
