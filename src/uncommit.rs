use std::fs;

use anyhow::{bail, Context, Result};
use duckdb::Connection;

use crate::{
    config::Config,
    current_migration,
    migration_files::load_committed_migrations,
    migration_state::{load_applied_migrations_if_present, verify_applied_history},
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

    verify_applied_history(&committed, &applied)?;

    if applied.len() == committed.len() {
        bail!(
            "cannot uncommit {} because it has already been applied to {}",
            latest.filename,
            config.database_path.display()
        );
    }

    let restored_current_path = current_migration::overwrite_empty_with_body(config, &latest.body)?;
    if let Err(error) = fs::remove_file(&latest.path) {
        fs::write(&restored_current_path, "").with_context(|| {
            format!(
                "failed to roll back {} after failing to remove {}",
                restored_current_path.display(),
                latest.path.display()
            )
        })?;
        return Err(error).with_context(|| format!("failed to remove {}", latest.path.display()));
    }

    Ok(UncommitResult {
        removed_committed_path: latest.path.clone(),
        restored_current_path,
    })
}

#[cfg(test)]
mod tests {
    use std::fs;

    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    use duckdb::Connection;
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
            committed_dir.join("000001.sql"),
            format!("--! Previous: \n--! Hash: {first_hash}\n\n{first_body}\n"),
        )
        .unwrap();

        let config = Config::load(&config_path).unwrap();
        migrate::run(&config).unwrap();

        let second_body = "alter table users add column email text;";
        let second_hash = migration_hash::calculate(Some(&first_hash), second_body);
        fs::write(
            committed_dir.join("000002.sql"),
            format!("--! Previous: {first_hash}\n--! Hash: {second_hash}\n\n{second_body}\n"),
        )
        .unwrap();

        let result = run(&config).unwrap();

        assert_eq!(
            result
                .removed_committed_path
                .file_name()
                .unwrap()
                .to_string_lossy(),
            "000002.sql"
        );
        assert_eq!(
            fs::read_to_string(temp_dir.path().join("migrations/current.sql")).unwrap(),
            "alter table users add column email text;\n"
        );
        assert!(!committed_dir.join("000002.sql").exists());
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
            committed_dir.join("000001.sql"),
            format!("--! Previous: \n--! Hash: {hash}\n\n{body}\n"),
        )
        .unwrap();

        let config = Config::load(&config_path).unwrap();
        migrate::run(&config).unwrap();

        let error = run(&config).unwrap_err();

        assert!(error.to_string().contains("already been applied"));
    }

    #[test]
    fn rejects_uncommit_when_applied_history_diverges() {
        let temp_dir = tempdir().unwrap();
        let config_path = temp_dir.path().join("mallard.toml");
        fs::write(&config_path, "version = 1").unwrap();
        let committed_dir = temp_dir.path().join("migrations/committed");
        fs::create_dir_all(&committed_dir).unwrap();
        fs::write(temp_dir.path().join("migrations/current.sql"), "").unwrap();

        let first_body = "create table users (id integer primary key);";
        let first_hash = migration_hash::calculate(None, first_body);
        fs::write(
            committed_dir.join("000001.sql"),
            format!("--! Previous: \n--! Hash: {first_hash}\n\n{first_body}\n"),
        )
        .unwrap();

        let second_body = "alter table users add column email text;";
        let second_hash = migration_hash::calculate(Some(&first_hash), second_body);
        fs::write(
            committed_dir.join("000002.sql"),
            format!("--! Previous: {first_hash}\n--! Hash: {second_hash}\n\n{second_body}\n"),
        )
        .unwrap();

        let config = Config::load(&config_path).unwrap();
        migrate::run(&config).unwrap();

        let connection = Connection::open(&config.database_path).unwrap();
        let divergent_hash = "b".repeat(64);
        connection
            .execute(
                &format!(
                    "update {}.migrations set hash = ? where filename = ?",
                    config.internal_schema.quoted()
                ),
                [&divergent_hash, &"000001.sql".to_string()],
            )
            .unwrap();

        let error = run(&config).unwrap_err();

        assert!(error
            .to_string()
            .contains("applied migration history diverges at 000001.sql"));
    }

    #[cfg(unix)]
    #[test]
    fn clears_current_sql_if_committed_file_removal_fails() {
        let temp_dir = tempdir().unwrap();
        let config_path = temp_dir.path().join("mallard.toml");
        fs::write(&config_path, "version = 1").unwrap();
        let committed_dir = temp_dir.path().join("migrations/committed");
        fs::create_dir_all(&committed_dir).unwrap();
        let current_sql = temp_dir.path().join("migrations/current.sql");
        fs::write(&current_sql, "").unwrap();

        let first_body = "select 1;";
        let previous_hash = migration_hash::calculate(None, first_body);
        fs::write(
            committed_dir.join("000001.sql"),
            format!("--! Previous: \n--! Hash: {previous_hash}\n\n{first_body}\n"),
        )
        .unwrap();

        let body = "alter table users add column email text;";
        let hash = migration_hash::calculate(Some(&previous_hash), body);
        let committed_path = committed_dir.join("000002.sql");
        fs::write(
            &committed_path,
            format!("--! Previous: {previous_hash}\n--! Hash: {hash}\n\n{body}\n"),
        )
        .unwrap();

        let original_permissions = fs::metadata(&committed_dir).unwrap().permissions();
        let mut read_only_permissions = original_permissions.clone();
        read_only_permissions.set_mode(0o555);
        fs::set_permissions(&committed_dir, read_only_permissions).unwrap();

        let config = Config::load(&config_path).unwrap();
        let error = run(&config).unwrap_err();

        fs::set_permissions(&committed_dir, original_permissions).unwrap();

        assert!(error.to_string().contains("failed to remove"));
        assert_eq!(fs::read_to_string(&current_sql).unwrap(), "");
        assert!(committed_path.exists());
    }
}
