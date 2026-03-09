use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};

use crate::{config::Config, migration_hash};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CurrentMode {
    File { path: PathBuf },
    Directory { path: PathBuf },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CurrentPart {
    pub path: PathBuf,
    pub contents: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CurrentMigration {
    pub mode: CurrentMode,
    pub parts: Vec<CurrentPart>,
}

impl CurrentMigration {
    pub fn raw_sql(&self) -> String {
        let mut combined = String::new();
        for part in &self.parts {
            combined.push_str(&migration_hash::normalize_body(&part.contents));
        }
        combined
    }

    pub fn is_empty(&self) -> bool {
        self.raw_sql().trim().is_empty()
    }
}

pub fn load(config: &Config) -> Result<CurrentMigration> {
    let file_path = config.migrations_dir.join("current.sql");
    let dir_path = config.migrations_dir.join("current");

    if file_path.exists() && dir_path.exists() {
        bail!(
            "found both {} and {}; choose only one current migration source",
            file_path.display(),
            dir_path.display()
        );
    }

    if dir_path.exists() {
        return Ok(CurrentMigration {
            mode: CurrentMode::Directory {
                path: dir_path.clone(),
            },
            parts: load_directory_parts(&dir_path)?,
        });
    }

    let contents = fs::read_to_string(&file_path)
        .with_context(|| format!("failed to read {}", file_path.display()))?;
    Ok(CurrentMigration {
        mode: CurrentMode::File {
            path: file_path.clone(),
        },
        parts: vec![CurrentPart {
            path: file_path,
            contents,
        }],
    })
}

pub fn overwrite_empty_with_body(
    config: &Config,
    suggested_name: &str,
    body: &str,
) -> Result<PathBuf> {
    let current = load(config)?;
    if !current.is_empty() {
        bail!("current migration is not empty; cannot uncommit into a non-empty current migration");
    }

    match current.mode {
        CurrentMode::File { path } => {
            fs::write(&path, migration_hash::normalize_body(body))
                .with_context(|| format!("failed to write {}", path.display()))?;
            Ok(path)
        }
        CurrentMode::Directory { path } => {
            fs::create_dir_all(&path)
                .with_context(|| format!("failed to create {}", path.display()))?;
            let target = path.join(suggested_name);
            fs::write(&target, migration_hash::normalize_body(body))
                .with_context(|| format!("failed to write {}", target.display()))?;
            Ok(target)
        }
    }
}

fn load_directory_parts(dir_path: &Path) -> Result<Vec<CurrentPart>> {
    let mut files = Vec::new();
    collect_sql_files(dir_path, dir_path, &mut files)?;
    files.sort_by(|left, right| left.0.cmp(&right.0));

    let mut parts = Vec::with_capacity(files.len());
    for (_, path) in files {
        let contents = fs::read_to_string(&path)
            .with_context(|| format!("failed to read {}", path.display()))?;
        parts.push(CurrentPart { path, contents });
    }
    Ok(parts)
}

fn collect_sql_files(
    base_dir: &Path,
    dir: &Path,
    files: &mut Vec<(PathBuf, PathBuf)>,
) -> Result<()> {
    for entry in fs::read_dir(dir).with_context(|| format!("failed to read {}", dir.display()))? {
        let entry = entry.with_context(|| format!("failed to read {}", dir.display()))?;
        let path = entry.path();
        if path.is_dir() {
            collect_sql_files(base_dir, &path, files)?;
        } else if path.extension().and_then(|ext| ext.to_str()) == Some("sql") {
            let relative = path.strip_prefix(base_dir).unwrap_or(&path).to_path_buf();
            files.push((relative, path));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::{CurrentMode, load, overwrite_empty_with_body};
    use crate::config::Config;

    #[test]
    fn loads_multifile_current_migration_in_sorted_order() {
        let temp_dir = tempdir().unwrap();
        let config_path = temp_dir.path().join("mallard.toml");
        fs::write(&config_path, "version = 1").unwrap();
        let current_dir = temp_dir.path().join("migrations/current");
        fs::create_dir_all(current_dir.join("nested")).unwrap();
        fs::write(current_dir.join("b.sql"), "select 2;\n").unwrap();
        fs::write(current_dir.join("nested/a.sql"), "select 3;\n").unwrap();
        fs::write(current_dir.join("a.sql"), "select 1;\n").unwrap();

        let config = Config::load(&config_path).unwrap();
        let current = load(&config).unwrap();

        assert!(matches!(current.mode, CurrentMode::Directory { .. }));
        assert_eq!(current.parts.len(), 3);
        assert_eq!(current.raw_sql(), "select 1;\nselect 2;\nselect 3;\n");
    }

    #[test]
    fn writes_uncommitted_body_into_current_directory() {
        let temp_dir = tempdir().unwrap();
        let config_path = temp_dir.path().join("mallard.toml");
        fs::write(&config_path, "version = 1").unwrap();
        fs::create_dir_all(temp_dir.path().join("migrations/current")).unwrap();

        let config = Config::load(&config_path).unwrap();
        let path = overwrite_empty_with_body(&config, "000002-add-users.sql", "select 1;").unwrap();

        assert_eq!(fs::read_to_string(&path).unwrap(), "select 1;\n");
    }
}
