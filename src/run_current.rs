use anyhow::{Context, Result};
use duckdb::Connection;

use crate::{compiler, config::Config, migrate};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunResult {
    pub database_path: std::path::PathBuf,
    pub applied_current: bool,
}

pub fn run(config: &Config) -> Result<RunResult> {
    // NOTE: committed migrations are applied first and remain applied even if
    // the current migration later fails to compile or execute.
    migrate::run(config)?;
    let compiled_current = compiler::compile_current(config)?;
    let mut connection = Connection::open(&config.database_path)
        .with_context(|| format!("failed to open {}", config.database_path.display()))?;
    apply_current_sql(&mut connection, &compiled_current)?;

    Ok(RunResult {
        database_path: config.database_path.clone(),
        applied_current: !compiled_current.trim().is_empty(),
    })
}

fn apply_current_sql(connection: &mut Connection, compiled_current: &str) -> Result<()> {
    let transaction = connection.transaction()?;
    if !compiled_current.trim().is_empty() {
        transaction.execute_batch(compiled_current)?;
    }
    transaction.commit()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use duckdb::Connection;
    use tempfile::tempdir;

    use super::run;
    use crate::{config::Config, migration_hash};

    #[test]
    fn runs_current_migration_on_main_after_committed_replay() {
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
        let _result = run(&config).unwrap();

        let connection = Connection::open(&config.database_path).unwrap();
        let user_count: i64 = connection
            .query_row("select count(*) from users", [], |row| row.get(0))
            .unwrap();
        assert_eq!(user_count, 1);
    }

    #[test]
    fn rolls_back_current_migration_if_main_run_fails() {
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

        assert!(error.to_string().contains("missing_table"));

        let connection = Connection::open(&config.database_path).unwrap();
        let users_table_exists: i64 = connection
            .query_row(
                "SELECT count(*) FROM information_schema.tables WHERE table_name = 'users'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(users_table_exists, 0);
    }

    #[test]
    fn applies_pending_committed_migrations_before_current_compile_errors() {
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
            "insert into :MISSING_SCHEMA.users values (1);\n",
        )
        .unwrap();

        let config = Config::load(&config_path).unwrap();
        let error = run(&config).unwrap_err();

        assert!(error.to_string().contains("unknown placeholder"));

        let connection = Connection::open(&config.database_path).unwrap();
        let users_table_exists: i64 = connection
            .query_row(
                "SELECT count(*) FROM information_schema.tables WHERE table_name = 'users'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(users_table_exists, 1);
    }
}
