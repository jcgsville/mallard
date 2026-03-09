use std::fs;

use anyhow::{Context, Result, bail};
use duckdb::Connection;

use crate::{
    config::Config, current_migration, migration_files::load_committed_migrations,
    migration_state::load_applied_migrations_if_present,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UncommitResult {
    pub removed_committed_path: std::path::PathBuf,
    pub restored_current_path: std::path::PathBuf,
}

pub fn run(config: &Config) -> Result<UncommitResult> {
    let committed_dir = config.migrations_dir.join("committed");
    let committed = load_committed_migrations(&committed_dir)?;
    let latest = committed
        .last()
        .ok_or_else(|| anyhow::anyhow!("no committed migrations to uncommit"))?;

    let applied = if config.database_path.exists() {
        let connection = Connection::open(&config.database_path)
            .with_context(|| format!("failed to open {}", config.database_path.display()))?;
        load_applied_migrations_if_present(&connection, &config.internal_schema)?
    } else {
        Vec::new()
    };

    if applied.len() == committed.len() {
        bail!(
            "cannot uncommit {} because it has already been applied to {}",
            latest.filename,
            config.database_path.display()
        );
    }

    let restored_current_path =
        current_migration::overwrite_empty_with_body(config, &latest.filename, &latest.body)?;
    fs::remove_file(&latest.path)
        .with_context(|| format!("failed to remove {}", latest.path.display()))?;

    Ok(UncommitResult {
        removed_committed_path: latest.path.clone(),
        restored_current_path,
    })
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::run;
    use crate::{config::Config, migrate, migration_hash};

    #[test]
    fn uncommits_latest_unapplied_migration_into_current_sql() {
        let temp_dir = tempdir().unwrap();
        let config_path = temp_dir.path().join("mallard.toml");
        fs::write(&config_path, "version = 1").unwrap();
        let committed_dir = temp_dir.path().join("migrations/committed");
        fs::create_dir_all(&committed_dir).unwrap();
        fs::write(temp_dir.path().join("migrations/current.sql"), "").unwrap();

        let first_body = "create table users (id integer primary key);";
        let first_hash = migration_hash::calculate(None, first_body);
        fs::write(
            committed_dir.join("000001-init.sql"),
            format!("--! Previous: \n--! Hash: {first_hash}\n--! Message: init\n\n{first_body}\n"),
        )
        .unwrap();

        let config = Config::load(&config_path).unwrap();
        migrate::run(&config).unwrap();

        let second_body = "alter table users add column email text;";
        let second_hash = migration_hash::calculate(Some(&first_hash), second_body);
        fs::write(
            committed_dir.join("000002-add-email.sql"),
            format!("--! Previous: {first_hash}\n--! Hash: {second_hash}\n--! Message: add email\n\n{second_body}\n"),
        )
        .unwrap();

        let result = run(&config).unwrap();

        assert_eq!(
            result
                .removed_committed_path
                .file_name()
                .unwrap()
                .to_string_lossy(),
            "000002-add-email.sql"
        );
        assert_eq!(
            fs::read_to_string(temp_dir.path().join("migrations/current.sql")).unwrap(),
            "alter table users add column email text;\n"
        );
        assert!(!committed_dir.join("000002-add-email.sql").exists());
    }

    #[test]
    fn refuses_to_uncommit_applied_latest_migration() {
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

        let error = run(&config).unwrap_err();

        assert!(error.to_string().contains("already been applied"));
    }
}
