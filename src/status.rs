use std::fs;

use anyhow::{Context, Result, bail};
use duckdb::Connection;

use crate::{
    config::Config,
    migration_files::{CommittedMigration, load_committed_migrations},
    migration_state::{AppliedMigration, load_applied_migrations_if_present},
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
    let current_migration = config.migrations_dir.join("current.sql");
    let current_contents = fs::read_to_string(&current_migration)
        .with_context(|| format!("failed to read {}", current_migration.display()))?;

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
        current_migration_has_changes: !current_contents.trim().is_empty(),
    })
}

fn verify_applied_history(
    committed: &[CommittedMigration],
    applied: &[AppliedMigration],
) -> Result<()> {
    if applied.len() > committed.len() {
        bail!(
            "database has {} applied migrations but only {} exist on disk",
            applied.len(),
            committed.len()
        );
    }

    for (index, applied_migration) in applied.iter().enumerate() {
        let disk_migration = &committed[index];
        if applied_migration.filename != disk_migration.filename
            || applied_migration.hash != disk_migration.hash
            || applied_migration.previous_hash != disk_migration.previous_hash
        {
            bail!(
                "applied migration history diverges at {}",
                applied_migration.filename
            );
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;

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
            committed_dir.join("000001-init.sql"),
            format!("--! Previous: \n--! Hash: {hash}\n--! Message: init\n\n{body}\n"),
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
            committed_dir.join("000001-init.sql"),
            format!("--! Previous: \n--! Hash: {hash}\n--! Message: init\n\n{body}\n"),
        )
        .unwrap();

        let config = Config::load(&config_path).unwrap();
        migrate::run(&config).unwrap();

        let second_body = "alter table users add column email text;";
        let second_hash = migration_hash::calculate(Some(&hash), second_body);
        fs::write(
            committed_dir.join("000002-add-email.sql"),
            format!(
                "--! Previous: {hash}\n--! Hash: {second_hash}\n--! Message: add email\n\n{second_body}\n"
            ),
        )
        .unwrap();

        let status = run(&config).unwrap();

        assert!(status.pending_committed);
        assert_eq!(status.exit_code(), 1);
    }
}
