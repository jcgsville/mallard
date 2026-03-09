use std::{
    env,
    fs::{self, OpenOptions},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};

use crate::config::Config;

pub const DEFAULT_CONFIG: &str = r#"version = 1

database_path = "${MALLARD_DB_PATH:-dev.duckdb}"
shadow_path = "${MALLARD_SHADOW_PATH:-.mallard/shadow.duckdb}"

[placeholders]
APP_SCHEMA = "main"
"#;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitResult {
    pub project_root: PathBuf,
    pub config_path: PathBuf,
    pub config_created: bool,
    pub migrations_dir: PathBuf,
    pub committed_dir: PathBuf,
    pub current_migration: PathBuf,
    pub fixtures_dir: PathBuf,
}

pub fn init(explicit_config_path: Option<&Path>) -> Result<InitResult> {
    let working_dir = env::current_dir().context("failed to determine current directory")?;
    let (config_path, config_created) =
        resolve_init_config_path(&working_dir, explicit_config_path)?;

    let config = Config::load(&config_path)?;
    let committed_dir = config.migrations_dir.join("committed");
    let current_migration = config.migrations_dir.join("current.sql");
    let fixtures_dir = config.migrations_dir.join("fixtures");

    fs::create_dir_all(&committed_dir)
        .with_context(|| format!("failed to create {}", committed_dir.display()))?;
    fs::create_dir_all(&fixtures_dir)
        .with_context(|| format!("failed to create {}", fixtures_dir.display()))?;
    ensure_file(&current_migration)?;

    Ok(InitResult {
        project_root: config
            .config_path
            .parent()
            .unwrap_or(&working_dir)
            .to_path_buf(),
        config_path: config.config_path,
        config_created,
        migrations_dir: config.migrations_dir,
        committed_dir,
        current_migration,
        fixtures_dir,
    })
}

fn resolve_init_config_path(
    working_dir: &Path,
    explicit_config_path: Option<&Path>,
) -> Result<(PathBuf, bool)> {
    if let Some(path) = explicit_config_path {
        let config_path = if path.is_absolute() {
            path.to_path_buf()
        } else {
            working_dir.join(path)
        };

        let config_created = if config_path.exists() {
            false
        } else {
            write_default_config(&config_path)?;
            true
        };

        return Ok((config_path, config_created));
    }

    match Config::discover_path(working_dir, None) {
        Ok(config_path) => Ok((config_path, false)),
        Err(_) => {
            let config_path = working_dir.join("mallard.toml");
            write_default_config(&config_path)?;
            Ok((config_path, true))
        }
    }
}

fn write_default_config(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    fs::write(path, DEFAULT_CONFIG)
        .with_context(|| format!("failed to write {}", path.display()))?;

    Ok(())
}

fn ensure_file(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }

    OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(false)
        .open(path)
        .with_context(|| format!("failed to create {}", path.display()))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{DEFAULT_CONFIG, init};
    use std::{env, fs};
    use tempfile::tempdir;

    struct CurrentDirGuard {
        original_dir: std::path::PathBuf,
    }

    impl CurrentDirGuard {
        fn set_to(path: &std::path::Path) -> Self {
            let original_dir = env::current_dir().unwrap();
            env::set_current_dir(path).unwrap();
            Self { original_dir }
        }
    }

    impl Drop for CurrentDirGuard {
        fn drop(&mut self) {
            env::set_current_dir(&self.original_dir).unwrap();
        }
    }

    #[test]
    fn init_bootstraps_project_files() {
        let temp_dir = tempdir().unwrap();
        let _guard = CurrentDirGuard::set_to(temp_dir.path());

        let result = init(None).unwrap();

        assert!(result.config_created);
        assert_eq!(
            fs::read_to_string(temp_dir.path().join("mallard.toml")).unwrap(),
            DEFAULT_CONFIG
        );
        assert!(temp_dir.path().join("migrations/committed").is_dir());
        assert!(temp_dir.path().join("migrations/fixtures").is_dir());
        assert!(temp_dir.path().join("migrations/current.sql").is_file());
    }

    #[test]
    fn init_uses_existing_config_paths() {
        let temp_dir = tempdir().unwrap();
        let config_path = temp_dir.path().join("mallard.toml");

        fs::write(
            &config_path,
            r#"version = 1

migrations_dir = "db/migrations"
"#,
        )
        .unwrap();

        let result = init(Some(&config_path)).unwrap();

        assert!(!result.config_created);
        assert!(temp_dir.path().join("db/migrations/committed").is_dir());
        assert!(temp_dir.path().join("db/migrations/fixtures").is_dir());
        assert!(temp_dir.path().join("db/migrations/current.sql").is_file());
    }

    #[test]
    fn init_walks_upwards_to_existing_config() {
        let temp_dir = tempdir().unwrap();
        let nested_dir = temp_dir.path().join("apps/api");
        fs::create_dir_all(&nested_dir).unwrap();
        fs::write(temp_dir.path().join("mallard.toml"), DEFAULT_CONFIG).unwrap();
        let _guard = CurrentDirGuard::set_to(&nested_dir);

        let result = init(None).unwrap();

        assert!(!result.config_created);
        assert_eq!(result.project_root, temp_dir.path());
        assert_eq!(result.config_path, temp_dir.path().join("mallard.toml"));
        assert!(temp_dir.path().join("migrations/committed").is_dir());
        assert!(temp_dir.path().join("migrations/fixtures").is_dir());
        assert!(temp_dir.path().join("migrations/current.sql").is_file());
        assert!(!nested_dir.join("mallard.toml").exists());
        assert!(!nested_dir.join("migrations").exists());
    }
}
