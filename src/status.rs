use anyhow::{Context, Result};
use duckdb::Connection;

use crate::{
    config::Config,
    current_migration,
    migration_files::load_committed_migrations,
    migration_state::{
        load_applied_migrations_if_present, metadata_table_exists, verify_applied_history,
    },
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatusResult {
    pub pending_committed: bool,
    pub current_migration_has_changes: bool,
}

impl StatusResult {
    pub fn exit_code(&self) -> i32 {
        let mut code = 0;
        if self.pending_committed {
            code |= 1;
        }
        if self.current_migration_has_changes {
            code |= 2;
        }
        code
    }
}

pub fn run(config: &Config) -> Result<StatusResult> {
    let committed_dir = config.migrations_dir.join("committed");
    let committed = load_committed_migrations(&committed_dir)?;
    let current = current_migration::load(config)?;

    let applied = if config.database_path.exists() {
        let connection = Connection::open(&config.database_path)
            .with_context(|| format!("failed to open {}", config.database_path.display()))?;
        ensure_metadata_for_history(&connection, config)?;
        load_applied_migrations_if_present(&connection, &config.internal_schema)?
    } else {
        Vec::new()
    };

    verify_applied_history(&committed, &applied)?;

    Ok(StatusResult {
        pending_committed: committed.len() > applied.len(),
        current_migration_has_changes: !current.is_empty(),
    })
}

fn ensure_metadata_for_history(connection: &Connection, config: &Config) -> Result<()> {
    if config.manage_metadata || metadata_table_exists(connection, &config.internal_schema)? {
        Ok(())
    } else {
        anyhow::bail!(
            "metadata table {}.migrations does not exist and `manage_metadata` is false; cannot safely determine which migrations have been applied",
            config.internal_schema
        )
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use duckdb::Connection;
    use tempfile::tempdir;

    use super::run;
    use crate::{config::Config, migrate, migration_hash};

    #[test]
    fn reports_clean_status_when_nothing_is_pending() {
        let temp_dir = tempdir().unwrap();
        let config_path = temp_dir.path().join("mallard.toml");
        fs::write(&config_path, "version = 1").unwrap();
        fs::create_dir_all(temp_dir.path().join("migrations/committed")).unwrap();
        fs::write(temp_dir.path().join("migrations/current.sql"), "").unwrap();

        let config = Config::load(&config_path).unwrap();
        let status = run(&config).unwrap();

        assert!(!status.pending_committed);
        assert!(!status.current_migration_has_changes);
        assert_eq!(status.exit_code(), 0);
    }

    #[test]
    fn reports_pending_committed_and_current_changes() {
        let temp_dir = tempdir().unwrap();
        let config_path = temp_dir.path().join("mallard.toml");
        fs::write(&config_path, "version = 1").unwrap();
        let committed_dir = temp_dir.path().join("migrations/committed");
        fs::create_dir_all(&committed_dir).unwrap();
        fs::write(
            temp_dir.path().join("migrations/current.sql"),
            "create view v_users as select 1;\n",
        )
        .unwrap();

        let body = "create table users (id integer primary key);";
        let hash = migration_hash::calculate(None, body);
        fs::write(
            committed_dir.join("000001.sql"),
            format!("--! Previous: \n--! Hash: {hash}\n\n{body}\n"),
        )
        .unwrap();

        let config = Config::load(&config_path).unwrap();
        let status = run(&config).unwrap();

        assert!(status.pending_committed);
        assert!(status.current_migration_has_changes);
        assert_eq!(status.exit_code(), 3);
    }

    #[test]
    fn reports_pending_committed_after_already_applied_prefix() {
        let temp_dir = tempdir().unwrap();
        let config_path = temp_dir.path().join("mallard.toml");
        fs::write(&config_path, "version = 1").unwrap();
        let committed_dir = temp_dir.path().join("migrations/committed");
        fs::create_dir_all(&committed_dir).unwrap();
        fs::write(temp_dir.path().join("migrations/current.sql"), "").unwrap();

        let body = "create table users (id integer primary key);";
        let hash = migration_hash::calculate(None, body);
        fs::write(
            committed_dir.join("000001.sql"),
            format!("--! Previous: \n--! Hash: {hash}\n\n{body}\n"),
        )
        .unwrap();

        let config = Config::load(&config_path).unwrap();
        migrate::run(&config).unwrap();

        let second_body = "alter table users add column email text;";
        let second_hash = migration_hash::calculate(Some(&hash), second_body);
        fs::write(
            committed_dir.join("000002.sql"),
            format!("--! Previous: {hash}\n--! Hash: {second_hash}\n\n{second_body}\n"),
        )
        .unwrap();

        let status = run(&config).unwrap();

        assert!(status.pending_committed);
        assert_eq!(status.exit_code(), 1);
    }

    #[test]
    fn rejects_status_without_metadata_when_management_is_disabled() {
        let temp_dir = tempdir().unwrap();
        let config_path = temp_dir.path().join("mallard.toml");
        fs::write(&config_path, "version = 1\nmanage_metadata = false\n").unwrap();
        fs::create_dir_all(temp_dir.path().join("migrations/committed")).unwrap();
        fs::write(temp_dir.path().join("migrations/current.sql"), "").unwrap();

        let config = Config::load(&config_path).unwrap();
        Connection::open(&config.database_path).unwrap();
        let error = run(&config).unwrap_err();

        assert!(error
            .to_string()
            .contains("cannot safely determine which migrations have been applied"));
    }
}
