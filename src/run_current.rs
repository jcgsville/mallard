use std::fs;

use anyhow::{Context, Result};
use clap::ValueEnum;
use duckdb::Connection;

use crate::{
    compiler,
    config::Config,
    hooks::{self, HookPhase, HookTarget},
    migrate,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum RunTarget {
    Main,
    Shadow,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunResult {
    pub database_path: std::path::PathBuf,
    pub applied_current: bool,
    pub target: RunTarget,
}

pub fn run(config: &Config, target: RunTarget) -> Result<RunResult> {
    let compiled_current = compiler::compile_current(config)?;

    match target {
        RunTarget::Main => run_on_main(config, &compiled_current),
        RunTarget::Shadow => run_on_shadow(config, &compiled_current),
    }
}

fn run_on_main(config: &Config, compiled_current: &str) -> Result<RunResult> {
    migrate::run(config)?;
    let mut connection = Connection::open(&config.database_path)
        .with_context(|| format!("failed to open {}", config.database_path.display()))?;
    apply_current_sql(&mut connection, config, compiled_current, HookTarget::Main)?;

    Ok(RunResult {
        database_path: config.database_path.clone(),
        applied_current: !compiled_current.trim().is_empty(),
        target: RunTarget::Main,
    })
}

fn run_on_shadow(config: &Config, compiled_current: &str) -> Result<RunResult> {
    remove_if_exists(&config.shadow_path)?;
    remove_if_exists(&config.shadow_path.with_extension("duckdb.wal"))?;

    let mut shadow_config = config.clone();
    shadow_config.database_path = config.shadow_path.clone();
    migrate::run_with_target(&shadow_config)?;

    let mut connection = Connection::open(&config.shadow_path)
        .with_context(|| format!("failed to open {}", config.shadow_path.display()))?;
    apply_current_sql(
        &mut connection,
        config,
        compiled_current,
        HookTarget::Shadow,
    )?;

    Ok(RunResult {
        database_path: config.shadow_path.clone(),
        applied_current: !compiled_current.trim().is_empty(),
        target: RunTarget::Shadow,
    })
}

fn apply_current_sql(
    connection: &mut Connection,
    config: &Config,
    compiled_current: &str,
    target: HookTarget,
) -> Result<()> {
    let transaction = connection.transaction()?;
    hooks::run_hooks(&transaction, config, HookPhase::Before, target)?;
    if !compiled_current.trim().is_empty() {
        transaction.execute_batch(compiled_current)?;
    }
    hooks::run_hooks(&transaction, config, HookPhase::After, target)?;
    transaction.commit()?;
    Ok(())
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

    use super::{RunTarget, run};
    use crate::{config::Config, migration_hash};

    #[test]
    fn runs_current_migration_on_shadow_with_hooks_and_placeholders() {
        let temp_dir = tempdir().unwrap();
        let config_path = temp_dir.path().join("mallard.toml");
        fs::write(
            &config_path,
            r#"version = 1

[placeholders]
APP_SCHEMA = "main"
"#,
        )
        .unwrap();
        let fixtures_dir = temp_dir.path().join("migrations/fixtures");
        let hooks_dir = temp_dir.path().join("migrations/hooks");
        fs::create_dir_all(&fixtures_dir).unwrap();
        fs::create_dir_all(&hooks_dir).unwrap();
        fs::write(
            fixtures_dir.join("users.sql"),
            "create table :APP_SCHEMA.users (id integer primary key);\n",
        )
        .unwrap();
        fs::write(
            hooks_dir.join("before-shadow.sql"),
            "create table if not exists hook_log (stage text);\ninsert into hook_log (stage) values ('before-shadow');\n",
        )
        .unwrap();
        fs::write(
            hooks_dir.join("after-shadow.sql"),
            "insert into hook_log (stage) values ('after-shadow');\n",
        )
        .unwrap();
        fs::write(
            temp_dir.path().join("migrations/current.sql"),
            "--! include fixtures/users.sql\ninsert into :APP_SCHEMA.users (id) values (1);\n",
        )
        .unwrap();
        fs::create_dir_all(temp_dir.path().join("migrations/committed")).unwrap();

        let config = Config::load(&config_path).unwrap();
        let result = run(&config, RunTarget::Shadow).unwrap();

        assert_eq!(result.target, RunTarget::Shadow);
        let connection = Connection::open(&config.shadow_path).unwrap();
        let user_count: i64 = connection
            .query_row("select count(*) from users", [], |row| row.get(0))
            .unwrap();
        let hook_count: i64 = connection
            .query_row("select count(*) from hook_log", [], |row| row.get(0))
            .unwrap();
        assert_eq!(user_count, 1);
        assert_eq!(hook_count, 2);
    }

    #[test]
    fn runs_current_migration_on_main_after_committed_replay() {
        let temp_dir = tempdir().unwrap();
        let config_path = temp_dir.path().join("mallard.toml");
        fs::write(&config_path, "version = 1").unwrap();
        let committed_dir = temp_dir.path().join("migrations/committed");
        fs::create_dir_all(&committed_dir).unwrap();
        fs::create_dir_all(temp_dir.path().join("migrations/hooks")).unwrap();
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
        let result = run(&config, RunTarget::Main).unwrap();

        assert_eq!(result.target, RunTarget::Main);
        let connection = Connection::open(&config.database_path).unwrap();
        let user_count: i64 = connection
            .query_row("select count(*) from users", [], |row| row.get(0))
            .unwrap();
        assert_eq!(user_count, 1);
    }

    #[test]
    fn rolls_back_before_hook_if_main_run_fails() {
        let temp_dir = tempdir().unwrap();
        let config_path = temp_dir.path().join("mallard.toml");
        fs::write(&config_path, "version = 1").unwrap();
        let hooks_dir = temp_dir.path().join("migrations/hooks");
        fs::create_dir_all(temp_dir.path().join("migrations/committed")).unwrap();
        fs::create_dir_all(&hooks_dir).unwrap();
        fs::write(
            hooks_dir.join("before-main.sql"),
            "create table if not exists hook_log (stage text);\ninsert into hook_log (stage) values ('before-main');\n",
        )
        .unwrap();
        fs::write(
            temp_dir.path().join("migrations/current.sql"),
            "insert into missing_table values (1);\n",
        )
        .unwrap();

        let config = Config::load(&config_path).unwrap();
        let error = run(&config, RunTarget::Main).unwrap_err();

        assert!(error.to_string().contains("missing_table"));

        let connection = Connection::open(&config.database_path).unwrap();
        let hook_table_exists: i64 = connection
            .query_row(
                "SELECT count(*) FROM information_schema.tables WHERE table_name = 'hook_log'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(hook_table_exists, 0);
    }
}
