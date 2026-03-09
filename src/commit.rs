use std::{fs, path::PathBuf};

use anyhow::{Context, Result, bail};
use duckdb::Connection;

use crate::{
    compiler, config::Config, current_migration, db_files, migrate,
    migration_files::load_committed_migrations, migration_hash,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitResult {
    pub committed_path: PathBuf,
    pub shadow_database_path: PathBuf,
    pub reset_target_path: PathBuf,
}

pub fn run(config: &Config) -> Result<CommitResult> {
    let current = current_migration::load(config)?;
    let expanded_current = compiler::expand_includes(config, &current.path, &current.contents)?;
    validate_current_migration(&expanded_current)?;

    validate_against_shadow(config, &expanded_current)?;

    let committed_dir = config.migrations_dir.join("committed");
    let committed = load_committed_migrations(&committed_dir)?;
    let next_version = next_migration_version(committed.len())?;
    let previous_hash = committed.last().map(|migration| migration.hash.clone());
    let hash = migration_hash::calculate(previous_hash.as_deref(), &expanded_current);
    let filename = format!("{:06}.sql", next_version);
    let committed_path = committed_dir.join(filename);

    fs::write(
        &committed_path,
        render_committed_migration(previous_hash.as_deref(), &hash, &expanded_current),
    )
    .with_context(|| format!("failed to write {}", committed_path.display()))?;
    if let Err(clear_error) = clear_current_migration(&current) {
        let _ = fs::remove_file(&committed_path);
        return Err(clear_error).context("failed to reset current migration after commit");
    }

    Ok(CommitResult {
        committed_path,
        shadow_database_path: config.shadow_path.clone(),
        reset_target_path: current_source_path(&current),
    })
}

fn validate_current_migration(contents: &str) -> Result<()> {
    let normalized = migration_hash::normalize_body(contents);
    if normalized.is_empty() {
        bail!("current migration is empty");
    }

    for line in normalized.lines() {
        if line.starts_with("--! ") {
            bail!("current migration must not contain committed migration headers");
        }
    }

    Ok(())
}

fn clear_current_migration(current: &current_migration::CurrentMigration) -> Result<()> {
    fs::write(&current.path, "")
        .with_context(|| format!("failed to reset {}", current.path.display()))?;
    Ok(())
}

fn current_source_path(current: &current_migration::CurrentMigration) -> PathBuf {
    current.path.clone()
}

fn validate_against_shadow(config: &Config, current_contents: &str) -> Result<()> {
    db_files::remove_if_exists(&config.shadow_path)?;
    db_files::remove_if_exists(&db_files::wal_path(&config.shadow_path))?;
    if let Some(parent) = config.shadow_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    let mut shadow_config = config.clone();
    shadow_config.database_path = config.shadow_path.clone();
    shadow_config.manage_metadata = true;
    migrate::run(&shadow_config)
        .context("failed to replay committed migrations into shadow database")?;

    let compiled_current = compiler::resolve_placeholders(config, current_contents)?;

    let mut connection = Connection::open(&config.shadow_path)
        .with_context(|| format!("failed to open {}", config.shadow_path.display()))?;
    let transaction = connection.transaction()?;
    transaction
        .execute_batch(&migration_hash::normalize_body(&compiled_current))
        .context("current migration failed shadow validation")?;
    transaction.commit()?;

    Ok(())
}

fn render_committed_migration(previous_hash: Option<&str>, hash: &str, body: &str) -> String {
    format!(
        "--! Previous: {}\n--! Hash: {}\n\n{}",
        previous_hash.unwrap_or(""),
        hash,
        migration_hash::normalize_body(body)
    )
}

fn next_migration_version(committed_len: usize) -> Result<u32> {
    let next_version = committed_len as u64 + 1;
    if next_version > 999_999 {
        bail!("committed migration sequence has reached the maximum of 999,999");
    }

    Ok(next_version as u32)
}

#[cfg(test)]
mod tests {
    use std::fs;

    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    use tempfile::tempdir;

    use super::{next_migration_version, run};
    use crate::{config::Config, migrate, migration_hash};

    #[test]
    fn commits_current_migration_and_resets_current_sql() {
        let temp_dir = tempdir().unwrap();
        let config_path = temp_dir.path().join("mallard.toml");
        fs::write(&config_path, "version = 1").unwrap();
        fs::create_dir_all(temp_dir.path().join("migrations/committed")).unwrap();
        fs::write(
            temp_dir.path().join("migrations/current.sql"),
            "create table users (id integer primary key);\n\n",
        )
        .unwrap();

        let config = Config::load(&config_path).unwrap();
        let result = run(&config).unwrap();

        assert_eq!(
            result.committed_path.file_name().unwrap().to_string_lossy(),
            "000001.sql"
        );
        assert_eq!(
            fs::read_to_string(&result.committed_path).unwrap(),
            format!(
                "--! Previous: \n--! Hash: {}\n\ncreate table users (id integer primary key);\n",
                migration_hash::calculate(None, "create table users (id integer primary key);")
            )
        );
        assert_eq!(
            fs::read_to_string(temp_dir.path().join("migrations/current.sql")).unwrap(),
            ""
        );
        assert!(config.shadow_path.exists());
    }

    #[test]
    fn validates_current_migration_against_committed_shadow_replay() {
        let temp_dir = tempdir().unwrap();
        let config_path = temp_dir.path().join("mallard.toml");
        fs::write(&config_path, "version = 1").unwrap();
        let committed_dir = temp_dir.path().join("migrations/committed");
        fs::create_dir_all(&committed_dir).unwrap();
        let first_body = "create table users (id integer primary key);";
        let first_hash = migration_hash::calculate(None, first_body);
        fs::write(
            committed_dir.join("000001.sql"),
            format!("--! Previous: \n--! Hash: {first_hash}\n\n{first_body}\n"),
        )
        .unwrap();
        fs::write(
            temp_dir.path().join("migrations/current.sql"),
            "insert into users (id) values (1);\n",
        )
        .unwrap();

        let config = Config::load(&config_path).unwrap();
        let result = run(&config).unwrap();

        assert_eq!(
            result.committed_path.file_name().unwrap().to_string_lossy(),
            "000002.sql"
        );

        let migrate_result = migrate::run(&config).unwrap();
        assert_eq!(migrate_result.applied_count, 2);
    }

    #[test]
    fn rejects_invalid_current_migration_before_writing_file() {
        let temp_dir = tempdir().unwrap();
        let config_path = temp_dir.path().join("mallard.toml");
        fs::write(&config_path, "version = 1").unwrap();
        fs::create_dir_all(temp_dir.path().join("migrations/committed")).unwrap();
        fs::write(
            temp_dir.path().join("migrations/current.sql"),
            "--! Hash: abc\nselect 1;\n",
        )
        .unwrap();

        let config = Config::load(&config_path).unwrap();
        let error = run(&config).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("current migration must not contain committed migration headers")
        );
        assert!(
            fs::read_dir(temp_dir.path().join("migrations/committed"))
                .unwrap()
                .next()
                .is_none()
        );
    }

    #[test]
    fn rejects_shadow_validation_failures() {
        let temp_dir = tempdir().unwrap();
        let config_path = temp_dir.path().join("mallard.toml");
        fs::write(&config_path, "version = 1").unwrap();
        fs::create_dir_all(temp_dir.path().join("migrations/committed")).unwrap();
        fs::write(
            temp_dir.path().join("migrations/current.sql"),
            "insert into missing_table values (1);\n",
        )
        .unwrap();

        let config = Config::load(&config_path).unwrap();
        let error = run(&config).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("current migration failed shadow validation")
        );
    }

    #[test]
    fn commits_successfully_when_manage_metadata_is_disabled_for_main_database() {
        let temp_dir = tempdir().unwrap();
        let config_path = temp_dir.path().join("mallard.toml");
        fs::write(&config_path, "version = 1\nmanage_metadata = false\n").unwrap();
        fs::create_dir_all(temp_dir.path().join("migrations/committed")).unwrap();
        fs::write(
            temp_dir.path().join("migrations/current.sql"),
            "select 1;\n",
        )
        .unwrap();

        let config = Config::load(&config_path).unwrap();
        let result = run(&config).unwrap();

        assert_eq!(
            result.committed_path.file_name().unwrap().to_string_lossy(),
            "000001.sql"
        );
        assert_eq!(
            fs::read_to_string(temp_dir.path().join("migrations/current.sql")).unwrap(),
            ""
        );
    }

    #[test]
    fn rejects_committed_versions_beyond_six_digits() {
        let error = next_migration_version(999_999).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("committed migration sequence has reached the maximum")
        );
    }

    #[cfg(unix)]
    #[test]
    fn removes_partial_committed_file_if_current_reset_fails() {
        let temp_dir = tempdir().unwrap();
        let config_path = temp_dir.path().join("mallard.toml");
        fs::write(&config_path, "version = 1").unwrap();
        let committed_dir = temp_dir.path().join("migrations/committed");
        fs::create_dir_all(&committed_dir).unwrap();
        let current_path = temp_dir.path().join("migrations/current.sql");
        fs::write(
            &current_path,
            "create table users (id integer primary key);\n",
        )
        .unwrap();

        let original_permissions = fs::metadata(&current_path).unwrap().permissions();
        let mut read_only_permissions = original_permissions.clone();
        read_only_permissions.set_mode(0o444);
        fs::set_permissions(&current_path, read_only_permissions).unwrap();

        let config = Config::load(&config_path).unwrap();
        let error = run(&config).unwrap_err();

        fs::set_permissions(&current_path, original_permissions).unwrap();

        assert!(
            error
                .to_string()
                .contains("failed to reset current migration after commit")
        );
        assert!(committed_dir.read_dir().unwrap().next().is_none());
        assert_eq!(
            fs::read_to_string(&current_path).unwrap(),
            "create table users (id integer primary key);\n"
        );
    }
}
