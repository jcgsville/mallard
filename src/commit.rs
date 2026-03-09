use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use duckdb::Connection;

use crate::{
    compiler, config::Config, current_migration, migrate,
    migration_files::load_committed_migrations, migration_hash,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitResult {
    pub committed_path: PathBuf,
    pub message: String,
    pub shadow_database_path: PathBuf,
    pub reset_target_path: PathBuf,
}

pub fn run(config: &Config, message: &str) -> Result<CommitResult> {
    let normalized_message = normalize_message(message)?;
    let current = current_migration::load(config)?;
    let expanded_current = compiler::expand_current(config)?;
    validate_current_migration(&expanded_current)?;

    validate_against_shadow(config, &expanded_current)?;

    let committed_dir = config.migrations_dir.join("committed");
    let committed = load_committed_migrations(&committed_dir)?;
    let next_version = committed.len() as u32 + 1;
    let previous_hash = committed.last().map(|migration| migration.hash.clone());
    let hash = migration_hash::calculate(previous_hash.as_deref(), &expanded_current);
    let filename = format!(
        "{:06}-{}.sql",
        next_version,
        slugify_message(&normalized_message)
    );
    let committed_path = committed_dir.join(filename);

    fs::write(
        &committed_path,
        render_committed_migration(
            &normalized_message,
            previous_hash.as_deref(),
            &hash,
            &expanded_current,
        ),
    )
    .with_context(|| format!("failed to write {}", committed_path.display()))?;
    clear_current_migration(&current).context("failed to reset current migration after commit")?;

    Ok(CommitResult {
        committed_path,
        message: normalized_message,
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
    match &current.mode {
        current_migration::CurrentMode::File { path } => {
            fs::write(path, "").with_context(|| format!("failed to reset {}", path.display()))?;
        }
        current_migration::CurrentMode::Directory { .. } => {
            for part in &current.parts {
                fs::remove_file(&part.path)
                    .with_context(|| format!("failed to remove {}", part.path.display()))?;
            }
        }
    }
    Ok(())
}

fn current_source_path(current: &current_migration::CurrentMigration) -> PathBuf {
    match &current.mode {
        current_migration::CurrentMode::File { path } => path.clone(),
        current_migration::CurrentMode::Directory { path } => path.clone(),
    }
}

fn validate_against_shadow(config: &Config, current_contents: &str) -> Result<()> {
    remove_if_exists(&config.shadow_path)?;
    remove_if_exists(&config.shadow_path.with_extension("duckdb.wal"))?;
    if let Some(parent) = config.shadow_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    let mut shadow_config = config.clone();
    shadow_config.database_path = config.shadow_path.clone();
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

fn render_committed_migration(
    message: &str,
    previous_hash: Option<&str>,
    hash: &str,
    body: &str,
) -> String {
    format!(
        "--! Previous: {}\n--! Hash: {}\n--! Message: {}\n\n{}",
        previous_hash.unwrap_or(""),
        hash,
        message,
        migration_hash::normalize_body(body)
    )
}

fn normalize_message(message: &str) -> Result<String> {
    let message = message.trim();
    if message.is_empty() {
        bail!("commit message cannot be empty");
    }
    Ok(message.to_string())
}

fn slugify_message(message: &str) -> String {
    let mut slug = String::new();
    let mut previous_was_dash = false;

    for ch in message.chars() {
        if ch.is_ascii_alphanumeric() {
            slug.push(ch.to_ascii_lowercase());
            previous_was_dash = false;
        } else if !previous_was_dash {
            slug.push('-');
            previous_was_dash = true;
        }
    }

    let slug = slug.trim_matches('-');
    if slug.is_empty() {
        "migration".to_string()
    } else {
        slug.to_string()
    }
}

fn remove_if_exists(path: &Path) -> Result<()> {
    if path.exists() {
        fs::remove_file(path).with_context(|| format!("failed to remove {}", path.display()))?;
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
        let result = run(&config, "Add users table").unwrap();

        assert_eq!(
            result.committed_path.file_name().unwrap().to_string_lossy(),
            "000001-add-users-table.sql"
        );
        assert_eq!(
            fs::read_to_string(&result.committed_path).unwrap(),
            format!(
                "--! Previous: \n--! Hash: {}\n--! Message: Add users table\n\ncreate table users (id integer primary key);\n",
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
            committed_dir.join("000001-init.sql"),
            format!("--! Previous: \n--! Hash: {first_hash}\n--! Message: init\n\n{first_body}\n"),
        )
        .unwrap();
        fs::write(
            temp_dir.path().join("migrations/current.sql"),
            "insert into users (id) values (1);\n",
        )
        .unwrap();

        let config = Config::load(&config_path).unwrap();
        let result = run(&config, "Seed users").unwrap();

        assert_eq!(
            result.committed_path.file_name().unwrap().to_string_lossy(),
            "000002-seed-users.sql"
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
        let error = run(&config, "Bad current").unwrap_err();

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
        let error = run(&config, "Broken migration").unwrap_err();

        assert!(
            error
                .to_string()
                .contains("current migration failed shadow validation")
        );
    }
}
