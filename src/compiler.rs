use std::{
    collections::HashSet,
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};

use crate::{config::Config, current_migration};

pub fn compile_current(config: &Config) -> Result<String> {
    let current = current_migration::load(config)?;
    compile_source(config, &current.path, &current.contents)
}

#[cfg(test)]
pub fn expand_current(config: &Config) -> Result<String> {
    let current = current_migration::load(config)?;
    expand_includes(config, &current.path, &current.contents)
}

fn compile_source(config: &Config, path: &Path, raw: &str) -> Result<String> {
    let expanded = expand_includes(config, path, raw)?;
    resolve_placeholders(config, &expanded)
}

pub fn expand_includes(config: &Config, path: &Path, sql: &str) -> Result<String> {
    let mut visiting = HashSet::new();
    let mut seen = HashSet::new();
    expand_includes_inner(config, path, sql, &mut visiting, &mut seen)
}

pub fn resolve_placeholders(config: &Config, sql: &str) -> Result<String> {
    let chars: Vec<char> = sql.chars().collect();
    let mut result = String::with_capacity(sql.len());
    let mut index = 0;
    let mut in_single_quoted_string = false;
    let mut in_line_comment = false;
    let mut in_block_comment = false;

    while index < chars.len() {
        let current = chars[index];
        let next = chars.get(index + 1).copied();

        if in_single_quoted_string {
            result.push(current);
            index += 1;

            if current == '\'' {
                if next == Some('\'') {
                    result.push('\'');
                    index += 1;
                } else {
                    in_single_quoted_string = false;
                }
            }

            continue;
        }

        if in_line_comment {
            result.push(current);
            index += 1;
            if current == '\n' {
                in_line_comment = false;
            }
            continue;
        }

        if in_block_comment {
            result.push(current);
            index += 1;
            if current == '*' && next == Some('/') {
                result.push('/');
                index += 1;
                in_block_comment = false;
            }
            continue;
        }

        if current == '\'' {
            in_single_quoted_string = true;
            result.push(current);
            index += 1;
            continue;
        }

        if current == '-' && next == Some('-') {
            in_line_comment = true;
            result.push(current);
            result.push('-');
            index += 2;
            continue;
        }

        if current == '/' && next == Some('*') {
            in_block_comment = true;
            result.push(current);
            result.push('*');
            index += 2;
            continue;
        }

        let previous = index.checked_sub(1).and_then(|idx| chars.get(idx));

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
    visiting: &mut HashSet<PathBuf>,
    seen: &mut HashSet<PathBuf>,
) -> Result<String> {
    let canonical_path = path
        .canonicalize()
        .with_context(|| format!("failed to resolve {}", path.display()))?;
    if !visiting.insert(canonical_path.clone()) {
        bail!("include cycle detected at {}", path.display());
    }

    let mut compiled = String::new();
    for segment in sql.split_inclusive('\n') {
        let trimmed = segment.trim();
        if let Some(include_path) = trimmed.strip_prefix("--! include ") {
            let include_path = resolve_include_path(config, path, include_path.trim())?;
            if !seen.insert(include_path.clone()) {
                bail!("duplicate include detected: {}", include_path.display());
            }
            let included = fs::read_to_string(&include_path)
                .with_context(|| format!("failed to read include {}", include_path.display()))?;
            let expanded = expand_includes_inner(config, &include_path, &included, visiting, seen)?;
            compiled.push_str(&expanded);
            if !expanded.is_empty() && !expanded.ends_with('\n') {
                compiled.push('\n');
            }
        } else {
            compiled.push_str(segment);
        }
    }

    visiting.remove(&canonical_path);
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
    fn expands_nested_fixture_includes_for_current_sql() {
        let temp_dir = tempdir().unwrap();
        let config_path = temp_dir.path().join("mallard.toml");
        fs::write(&config_path, "version = 1").unwrap();
        let fixtures_dir = temp_dir.path().join("migrations/fixtures");
        fs::create_dir_all(fixtures_dir.join("shared")).unwrap();
        fs::write(
            fixtures_dir.join("shared/base_tables.sql"),
            "create table users (id integer primary key);\n",
        )
        .unwrap();
        fs::write(
            fixtures_dir.join("users.sql"),
            "--! include shared/base_tables.sql\ninsert into users (id) values (1);\n",
        )
        .unwrap();
        fs::write(
            temp_dir.path().join("migrations/current.sql"),
            "--! include fixtures/users.sql\nselect * from users;\n",
        )
        .unwrap();

        let config = Config::load(&config_path).unwrap();
        let expanded = expand_current(&config).unwrap();
        let compiled = compile_current(&config).unwrap();

        assert!(expanded.contains("create table users"));
        assert!(expanded.contains("insert into users"));
        assert!(compiled.contains("select * from users"));
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

    #[test]
    fn rejects_duplicate_includes_with_clear_error() {
        let temp_dir = tempdir().unwrap();
        let config_path = temp_dir.path().join("mallard.toml");
        fs::write(&config_path, "version = 1").unwrap();
        let fixtures_dir = temp_dir.path().join("migrations/fixtures");
        fs::create_dir_all(&fixtures_dir).unwrap();
        fs::write(fixtures_dir.join("users.sql"), "select 1;\n").unwrap();
        fs::write(
            temp_dir.path().join("migrations/current.sql"),
            "--! include fixtures/users.sql\n--! include fixtures/users.sql\n",
        )
        .unwrap();

        let config = Config::load(&config_path).unwrap();
        let error = compile_current(&config).unwrap_err();

        assert!(error.to_string().contains("duplicate include detected"));
    }

    #[test]
    fn ignores_placeholders_inside_strings_and_comments() {
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
        let compiled = resolve_placeholders(
            &config,
            "-- comment mentions :APP_SCHEMA\ninsert into audit values (':RETRY', 'it''s :APP_SCHEMA');\nselect * from :APP_SCHEMA.users;\n",
        )
        .unwrap();

        assert!(compiled.contains("-- comment mentions :APP_SCHEMA"));
        assert!(compiled.contains("':RETRY'"));
        assert!(compiled.contains("'it''s :APP_SCHEMA'"));
        assert!(compiled.contains("select * from main.users;"));
    }

    #[test]
    fn ignores_placeholders_inside_block_comments() {
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
        let compiled = resolve_placeholders(
            &config,
            "/* keep :APP_SCHEMA literal here */\nselect * from :APP_SCHEMA.users;\n",
        )
        .unwrap();

        assert!(compiled.contains("/* keep :APP_SCHEMA literal here */"));
        assert!(compiled.contains("select * from main.users;"));
    }
}
