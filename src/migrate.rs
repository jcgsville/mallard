use std::fs;

use anyhow::{Context, Result, bail};
use duckdb::Connection;

use crate::{
    compiler,
    config::Config,
    migration_files::{CommittedMigration, load_committed_migrations},
    migration_state::{
        AppliedMigration, ensure_metadata_storage, load_applied_migrations, metadata_table_exists,
        record_applied_migration, verify_applied_history,
    },
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrateResult {
    pub database_path: std::path::PathBuf,
    pub applied_count: usize,
    pub total_committed: usize,
}

pub fn run(config: &Config) -> Result<MigrateResult> {
    let committed_dir = config.migrations_dir.join("committed");
    let committed = load_committed_migrations(&committed_dir)?;
    ensure_database_parent_dir(&config.database_path)?;

    let mut connection = Connection::open(&config.database_path)
        .with_context(|| format!("failed to open {}", config.database_path.display()))?;

    ensure_metadata_for_migrate(&connection, config)?;
    let applied = load_applied_migrations(&connection, &config.internal_schema)?;
    verify_applied_history(&committed, &applied)?;

    let mut applied_count = 0;
    for migration in committed.iter().skip(applied.len()) {
        apply_migration(&mut connection, config, migration)?;
        applied_count += 1;
    }

    Ok(MigrateResult {
        database_path: config.database_path.clone(),
        applied_count,
        total_committed: committed.len(),
    })
}

fn ensure_metadata_for_migrate(connection: &Connection, config: &Config) -> Result<()> {
    if config.manage_metadata {
        ensure_metadata_storage(connection, &config.internal_schema)
    } else if metadata_table_exists(connection, &config.internal_schema)? {
        Ok(())
    } else {
        bail!(
            "metadata table {}.migrations does not exist and `manage_metadata` is false",
            config.internal_schema
        );
    }
}

fn ensure_database_parent_dir(path: &std::path::Path) -> Result<()> {
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    Ok(())
}

fn apply_migration(
    connection: &mut Connection,
    config: &Config,
    migration: &CommittedMigration,
) -> Result<()> {
    let compiled_body = compiler::resolve_placeholders(config, &migration.body)?;
    let transaction = connection.transaction()?;
    transaction.execute_batch(&compiled_body)?;
    record_applied_migration(
        &transaction,
        &config.internal_schema,
        &AppliedMigration {
            filename: migration.filename.clone(),
            hash: migration.hash.clone(),
            previous_hash: migration.previous_hash.clone(),
        },
    )?;
    transaction.commit()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use duckdb::Connection;
    use tempfile::tempdir;

    use super::run;
    use crate::{
        config::Config,
        migration_files::CommittedMigration,
        migration_hash,
        migration_state::{AppliedMigration, ensure_metadata_storage, verify_applied_history},
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

        assert!(
            error
                .to_string()
                .contains("database has 2 applied migrations but only 1 exist on disk")
        );
    }

    #[test]
    fn verify_applied_history_rejects_divergent_metadata() {
        let committed = vec![committed_migration(1, "000001.sql", None, &"a".repeat(64))];
        let applied = vec![applied_migration("000001.sql", None, &"b".repeat(64))];

        let error = verify_applied_history(&committed, &applied).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("applied migration history diverges at 000001.sql")
        );
    }

    #[test]
    fn applies_pending_committed_migrations() {
        let temp_dir = tempdir().unwrap();
        let config_path = temp_dir.path().join("mallard.toml");
        fs::write(
            &config_path,
            r#"version = 1

database_path = "dev.duckdb"
"#,
        )
        .unwrap();

        let committed_dir = temp_dir.path().join("migrations/committed");
        fs::create_dir_all(&committed_dir).unwrap();

        let first_body = "create table users (id integer primary key, email text not null);";
        let first_hash = migration_hash::calculate(None, first_body);
        fs::write(
            committed_dir.join("000001.sql"),
            format!("--! Previous: \n--! Hash: {first_hash}\n\n{first_body}\n"),
        )
        .unwrap();

        let second_body = "insert into users (id, email) values (1, 'mallard@example.com');";
        let second_hash = migration_hash::calculate(Some(&first_hash), second_body);
        fs::write(
            committed_dir.join("000002.sql"),
            format!("--! Previous: {first_hash}\n--! Hash: {second_hash}\n\n{second_body}\n"),
        )
        .unwrap();

        let config = Config::load(&config_path).unwrap();
        let result = run(&config).unwrap();

        assert_eq!(result.applied_count, 2);

        let connection = Connection::open(&config.database_path).unwrap();
        let migration_count: i64 = connection
            .query_row("select count(*) from mallard.migrations", [], |row| {
                row.get(0)
            })
            .unwrap();
        let user_count: i64 = connection
            .query_row("select count(*) from users", [], |row| row.get(0))
            .unwrap();

        assert_eq!(migration_count, 2);
        assert_eq!(user_count, 1);
    }

    #[test]
    fn is_idempotent_when_no_migrations_are_pending() {
        let temp_dir = tempdir().unwrap();
        let config_path = temp_dir.path().join("mallard.toml");
        fs::write(&config_path, "version = 1").unwrap();

        let committed_dir = temp_dir.path().join("migrations/committed");
        fs::create_dir_all(&committed_dir).unwrap();
        let body = "create table users (id integer primary key);";
        let hash = migration_hash::calculate(None, body);
        fs::write(
            committed_dir.join("000001.sql"),
            format!("--! Previous: \n--! Hash: {hash}\n\n{body}\n"),
        )
        .unwrap();

        let config = Config::load(&config_path).unwrap();
        let first = run(&config).unwrap();
        let second = run(&config).unwrap();

        assert_eq!(first.applied_count, 1);
        assert_eq!(second.applied_count, 0);
    }

    #[test]
    fn rejects_divergent_applied_history() {
        let temp_dir = tempdir().unwrap();
        let config_path = temp_dir.path().join("mallard.toml");
        fs::write(&config_path, "version = 1").unwrap();

        let committed_dir = temp_dir.path().join("migrations/committed");
        fs::create_dir_all(&committed_dir).unwrap();
        let body = "create table users (id integer primary key);";
        let hash = migration_hash::calculate(None, body);
        fs::write(
            committed_dir.join("000001.sql"),
            format!("--! Previous: \n--! Hash: {hash}\n\n{body}\n"),
        )
        .unwrap();

        let config = Config::load(&config_path).unwrap();
        run(&config).unwrap();

        let connection = Connection::open(&config.database_path).unwrap();
        connection
            .execute(
                "update mallard.migrations set hash = ? where filename = ?",
                ["b".repeat(64), "000001.sql".to_string()],
            )
            .unwrap();

        let error = run(&config).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("applied migration history diverges")
        );
    }

    #[test]
    fn requires_existing_metadata_table_when_management_is_disabled() {
        let temp_dir = tempdir().unwrap();
        let config_path = temp_dir.path().join("mallard.toml");
        fs::write(
            &config_path,
            r#"version = 1

manage_metadata = false
"#,
        )
        .unwrap();
        fs::create_dir_all(temp_dir.path().join("migrations/committed")).unwrap();

        let config = Config::load(&config_path).unwrap();
        let error = run(&config).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("metadata table mallard.migrations does not exist")
        );
    }

    #[test]
    fn uses_existing_metadata_table_when_management_is_disabled() {
        let temp_dir = tempdir().unwrap();
        let config_path = temp_dir.path().join("mallard.toml");
        fs::write(
            &config_path,
            r#"version = 1

manage_metadata = false
"#,
        )
        .unwrap();

        let committed_dir = temp_dir.path().join("migrations/committed");
        fs::create_dir_all(&committed_dir).unwrap();

        let body = "create table users (id integer primary key);";
        let hash = migration_hash::calculate(None, body);
        fs::write(
            committed_dir.join("000001.sql"),
            format!("--! Previous: \n--! Hash: {hash}\n\n{body}\n"),
        )
        .unwrap();

        let config = Config::load(&config_path).unwrap();
        let connection = Connection::open(&config.database_path).unwrap();
        ensure_metadata_storage(&connection, &config.internal_schema).unwrap();

        let result = run(&config).unwrap();

        assert_eq!(result.applied_count, 1);
    }
}
