use anyhow::{Context, Result};
use duckdb::Connection;

use crate::{
    config::Config,
    current_migration,
    migration_files::load_committed_migrations,
    migration_state::{load_applied_migrations_if_present, verify_applied_history},
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

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use tempfile::tempdir;

    use super::run;
    use crate::{
        config::Config,
        migrate,
        migration_files::CommittedMigration,
        migration_hash,
        migration_state::{verify_applied_history, AppliedMigration},
    };

    fn committed_migration(
        version: u32,
        filename: &str,
        previous_hash: Option<&str>,
        hash: &str,
    ) -> CommittedMigration {
        CommittedMigration {
            version,
            filename: filename.to_string(),
            path: PathBuf::from(filename),
            previous_hash: previous_hash.map(str::to_string),
            hash: hash.to_string(),
            body: format!("-- body for {filename}"),
        }
    }

    fn applied_migration(
        filename: &str,
        previous_hash: Option<&str>,
        hash: &str,
    ) -> AppliedMigration {
        AppliedMigration {
            filename: filename.to_string(),
            previous_hash: previous_hash.map(str::to_string),
            hash: hash.to_string(),
        }
    }

    #[test]
    fn verify_applied_history_accepts_matching_applied_prefix() {
        let first_hash = "a".repeat(64);
        let second_hash = "b".repeat(64);
        let committed = vec![
            committed_migration(1, "000001.sql", None, &first_hash),
            committed_migration(2, "000002.sql", Some(&first_hash), &second_hash),
        ];
        let applied = vec![applied_migration("000001.sql", None, &first_hash)];

        verify_applied_history(&committed, &applied).unwrap();
    }

    #[test]
    fn verify_applied_history_rejects_extra_applied_migrations() {
        let first_hash = "a".repeat(64);
        let second_hash = "b".repeat(64);
        let committed = vec![committed_migration(1, "000001.sql", None, &first_hash)];
        let applied = vec![
            applied_migration("000001.sql", None, &first_hash),
            applied_migration("000002.sql", Some(&first_hash), &second_hash),
        ];

        let error = verify_applied_history(&committed, &applied).unwrap_err();

        assert!(error
            .to_string()
            .contains("database has 2 applied migrations but only 1 exist on disk"));
    }

    #[test]
    fn verify_applied_history_rejects_divergent_metadata() {
        let committed = vec![committed_migration(1, "000001.sql", None, &"a".repeat(64))];
        let applied = vec![applied_migration("000001.sql", None, &"b".repeat(64))];

        let error = verify_applied_history(&committed, &applied).unwrap_err();

        assert!(error
            .to_string()
            .contains("applied migration history diverges at 000001.sql"));
    }

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
}
