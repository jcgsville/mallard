use std::{
    collections::HashSet,
    fs,
    path::{Path, PathBuf},
};

use anyhow::{bail, Context, Result};

use crate::config::Config;

pub fn compile_current(config: &Config) -> Result<String> {
    let expanded = expand_current(config)?;
    resolve_placeholders(config, &expanded)
}

pub fn expand_current(config: &Config) -> Result<String> {
    let current_path = config.migrations_dir.join("current.sql");
    let raw = fs::read_to_string(&current_path)
        .with_context(|| format!("failed to read {}", current_path.display()))?;
    expand_includes(config, &current_path, &raw)
}

pub fn compile_file(config: &Config, path: &Path) -> Result<String> {
    let raw =
        fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))?;
    let expanded = expand_includes(config, path, &raw)?;
    resolve_placeholders(config, &expanded)
}

pub fn expand_includes(config: &Config, path: &Path, sql: &str) -> Result<String> {
    let mut visited = HashSet::new();
    expand_includes_inner(config, path, sql, &mut visited)
}

pub fn resolve_placeholders(config: &Config, sql: &str) -> Result<String> {
    let chars: Vec<char> = sql.chars().collect();
    let mut result = String::with_capacity(sql.len());
    let mut index = 0;

    while index < chars.len() {
        let current = chars[index];
        let previous = index.checked_sub(1).and_then(|idx| chars.get(idx));
        let next = chars.get(index + 1).copied();

        if current == ':' && previous != Some(&':') && matches!(next, Some('_') | Some('A'..='Z')) {
            let mut end = index + 1;
            while matches!(
                chars.get(end),
                Some('_') | Some('0'..='9') | Some('A'..='Z')
            ) {
                end += 1;
            }

            let name: String = chars[index + 1..end].iter().collect();
            let value = config
                .placeholders
                .get(&name)
                .ok_or_else(|| anyhow::anyhow!("unknown placeholder `:{name}`"))?;
            result.push_str(value);
            index = end;
            continue;
        }

        result.push(current);
        index += 1;
    }

    Ok(result)
}

fn expand_includes_inner(
    config: &Config,
    path: &Path,
    sql: &str,
    visited: &mut HashSet<PathBuf>,
) -> Result<String> {
    let canonical_path = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    if !visited.insert(canonical_path.clone()) {
        bail!("include cycle detected at {}", path.display());
    }

    let mut compiled = String::new();
    for segment in sql.split_inclusive('\n') {
        let trimmed = segment.trim();
        if let Some(include_path) = trimmed.strip_prefix("--! include ") {
            let include_path = resolve_include_path(config, path, include_path.trim())?;
            let included = fs::read_to_string(&include_path)
                .with_context(|| format!("failed to read include {}", include_path.display()))?;
            let expanded = expand_includes_inner(config, &include_path, &included, visited)?;
            compiled.push_str(&expanded);
            if !expanded.is_empty() && !expanded.ends_with('\n') {
                compiled.push('\n');
            }
        } else {
            compiled.push_str(segment);
        }
    }

    visited.remove(&canonical_path);
    Ok(compiled)
}

fn resolve_include_path(config: &Config, source_path: &Path, include: &str) -> Result<PathBuf> {
    let base_dir = source_path.parent().unwrap_or(&config.migrations_dir);
    let candidate = base_dir.join(include);
    let candidate = candidate
        .canonicalize()
        .with_context(|| format!("failed to resolve include {}", candidate.display()))?;
    let fixtures_dir = config.migrations_dir.join("fixtures");
    let fixtures_dir = fixtures_dir.canonicalize().with_context(|| {
        format!(
            "failed to resolve fixtures directory {}",
            fixtures_dir.display()
        )
    })?;

    if !candidate.starts_with(&fixtures_dir) {
        bail!(
            "include {} must stay within {}",
            candidate.display(),
            fixtures_dir.display()
        );
    }

    Ok(candidate)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::{compile_current, expand_current, resolve_placeholders};
    use crate::config::Config;

    #[test]
    fn resolves_placeholders_in_compiled_sql() {
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

        let config = Config::load(&config_path).unwrap();
        let compiled =
            resolve_placeholders(&config, "create schema if not exists :APP_SCHEMA;").unwrap();

        assert_eq!(compiled, "create schema if not exists main;");
    }

    #[test]
    fn expands_fixture_includes_for_current_sql() {
        let temp_dir = tempdir().unwrap();
        let config_path = temp_dir.path().join("mallard.toml");
        fs::write(&config_path, "version = 1").unwrap();
        let fixtures_dir = temp_dir.path().join("migrations/fixtures");
        fs::create_dir_all(&fixtures_dir).unwrap();
        fs::write(
            fixtures_dir.join("users.sql"),
            "create table users (id integer primary key);\n",
        )
        .unwrap();
        fs::write(
            temp_dir.path().join("migrations/current.sql"),
            "--! include fixtures/users.sql\ninsert into users (id) values (1);\n",
        )
        .unwrap();

        let config = Config::load(&config_path).unwrap();
        let expanded = expand_current(&config).unwrap();
        let compiled = compile_current(&config).unwrap();

        assert!(expanded.contains("create table users"));
        assert!(compiled.contains("insert into users"));
    }

    #[test]
    fn fails_for_unknown_placeholders() {
        let temp_dir = tempdir().unwrap();
        let config_path = temp_dir.path().join("mallard.toml");
        fs::write(&config_path, "version = 1").unwrap();

        let config = Config::load(&config_path).unwrap();
        let error =
            resolve_placeholders(&config, "select * from :MISSING_SCHEMA.users;").unwrap_err();

        assert!(error.to_string().contains("unknown placeholder"));
    }
}
