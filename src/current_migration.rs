use std::{fs, path::PathBuf};

use anyhow::{Context, Result, bail};

use crate::{config::Config, migration_hash};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CurrentMigration {
    pub path: PathBuf,
    pub contents: String,
}

impl CurrentMigration {
    pub fn raw_sql(&self) -> String {
        migration_hash::normalize_body(&self.contents)
    }

    pub fn is_empty(&self) -> bool {
        self.raw_sql().is_empty()
    }
}

pub fn load(config: &Config) -> Result<CurrentMigration> {
    let file_path = config.migrations_dir.join("current.sql");
    let dir_path = config.migrations_dir.join("current");

    if dir_path.exists() {
        bail!(
            "directory mode is no longer supported; move SQL into {} and remove {}",
            file_path.display(),
            dir_path.display()
        );
    }

    let contents = fs::read_to_string(&file_path)
        .with_context(|| format!("failed to read {}", file_path.display()))?;
    Ok(CurrentMigration {
        path: file_path,
        contents,
    })
}

pub fn overwrite_empty_with_body(config: &Config, body: &str) -> Result<PathBuf> {
    let current = load(config)?;
    if !current.is_empty() {
        bail!("current migration is not empty; cannot uncommit into a non-empty current migration");
    }

    fs::write(&current.path, migration_hash::normalize_body(body))
        .with_context(|| format!("failed to write {}", current.path.display()))?;
    Ok(current.path)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::{load, overwrite_empty_with_body};
    use crate::config::Config;

    #[test]
    fn loads_current_sql() {
        let temp_dir = tempdir().unwrap();
        let config_path = temp_dir.path().join("mallard.toml");
        fs::write(&config_path, "version = 1").unwrap();
        let current_sql = temp_dir.path().join("migrations/current.sql");
        fs::create_dir_all(current_sql.parent().unwrap()).unwrap();
        fs::write(&current_sql, "select 1;\n").unwrap();

        let config = Config::load(&config_path).unwrap();
        let current = load(&config).unwrap();

        assert_eq!(current.path, current_sql);
        assert_eq!(current.raw_sql(), "select 1;\n");
    }

    #[test]
    fn rejects_legacy_current_directory_mode() {
        let temp_dir = tempdir().unwrap();
        let config_path = temp_dir.path().join("mallard.toml");
        fs::write(&config_path, "version = 1").unwrap();
        fs::create_dir_all(temp_dir.path().join("migrations/current")).unwrap();

        let config = Config::load(&config_path).unwrap();
        let error = load(&config).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("directory mode is no longer supported")
        );
    }

    #[test]
    fn writes_uncommitted_body_into_current_sql() {
        let temp_dir = tempdir().unwrap();
        let config_path = temp_dir.path().join("mallard.toml");
        fs::write(&config_path, "version = 1").unwrap();
        let current_sql = temp_dir.path().join("migrations/current.sql");
        fs::create_dir_all(current_sql.parent().unwrap()).unwrap();
        fs::write(&current_sql, "").unwrap();

        let config = Config::load(&config_path).unwrap();
        let path = overwrite_empty_with_body(&config, "select 1;").unwrap();

        assert_eq!(path, current_sql);
        assert_eq!(fs::read_to_string(&path).unwrap(), "select 1;\n");
    }
}
