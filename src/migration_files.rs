use std::{
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};

use crate::migration_hash;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommittedMigration {
    pub version: u32,
    pub filename: String,
    pub path: PathBuf,
    pub previous_hash: Option<String>,
    pub hash: String,
    pub body: String,
}

pub fn load_committed_migrations(committed_dir: &Path) -> Result<Vec<CommittedMigration>> {
    if !committed_dir.exists() {
        bail!(
            "committed migrations directory does not exist: {}",
            committed_dir.display()
        );
    }

    let mut entries = Vec::new();
    for entry in fs::read_dir(committed_dir)
        .with_context(|| format!("failed to read {}", committed_dir.display()))?
    {
        let entry = entry.with_context(|| format!("failed to read {}", committed_dir.display()))?;
        let path = entry.path();
        if path.is_file() && path.extension().and_then(|ext| ext.to_str()) == Some("sql") {
            entries.push(path);
        }
    }

    entries.sort();

    let mut migrations = Vec::with_capacity(entries.len());
    let mut previous_hash: Option<String> = None;

    for (index, path) in entries.into_iter().enumerate() {
        let migration = parse_committed_migration(&path)?;
        let expected_version = (index + 1) as u32;
        if migration.version != expected_version {
            bail!(
                "expected committed migration {:06}, found {}",
                expected_version,
                migration.filename
            );
        }

        if index == 0 && migration.previous_hash.is_some() {
            bail!(
                "first committed migration {} must have an empty previous hash",
                migration.filename
            );
        }

        if let Some(expected_previous) = previous_hash.as_deref() {
            if migration.previous_hash.as_deref() != Some(expected_previous) {
                bail!(
                    "migration {} expected previous hash {}, found {}",
                    migration.filename,
                    expected_previous,
                    migration.previous_hash.as_deref().unwrap_or("<none>")
                );
            }
        }

        previous_hash = Some(migration.hash.clone());
        migrations.push(migration);
    }

    Ok(migrations)
}

pub fn parse_committed_migration(path: &Path) -> Result<CommittedMigration> {
    let filename = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| anyhow::anyhow!("invalid migration filename: {}", path.display()))?
        .to_string();
    let version = parse_filename(&filename)?;
    let contents = fs::read_to_string(path)
        .with_context(|| format!("failed to read migration {}", path.display()))?;
    let parsed = parse_contents(&filename, &contents)?;

    let expected_hash = migration_hash::calculate(parsed.previous_hash.as_deref(), &parsed.body);
    if parsed.hash != expected_hash {
        bail!(
            "migration {} hash mismatch: expected {}, found {}",
            filename,
            expected_hash,
            parsed.hash
        );
    }

    Ok(CommittedMigration {
        version,
        filename,
        path: path.to_path_buf(),
        previous_hash: parsed.previous_hash,
        hash: parsed.hash,
        body: parsed.body,
    })
}

fn parse_filename(filename: &str) -> Result<u32> {
    let Some(version) = filename.strip_suffix(".sql") else {
        bail!("invalid committed migration filename: {filename}");
    };

    if version.len() != 6 || !version.bytes().all(|byte| byte.is_ascii_digit()) {
        bail!("invalid committed migration filename: {filename}");
    }

    version
        .parse()
        .with_context(|| format!("invalid migration version in {filename}"))
}

fn parse_contents(filename: &str, contents: &str) -> Result<ParsedMigrationContents> {
    let normalized = contents.replace("\r\n", "\n");
    let mut previous_hash = None;
    let mut hash = None;
    let mut body_start = 0usize;
    let mut header_lines = 0usize;
    let mut saw_blank_after_headers = false;
    let mut offset = 0usize;

    for line in normalized.split_inclusive('\n') {
        let content = line.strip_suffix('\n').unwrap_or(line);

        if let Some(header) = content.strip_prefix("--! ") {
            if saw_blank_after_headers {
                bail!(
                    "migration headers in {filename} must be contiguous and followed by a blank line"
                );
            }

            let Some((key, value)) = header.split_once(':') else {
                bail!("invalid migration header in {filename}: {content}");
            };
            let value = value.trim();
            match key.trim() {
                "Previous" => {
                    if previous_hash.is_some() {
                        bail!("duplicate Previous header in {filename}");
                    }
                    if !value.is_empty() && !migration_hash::is_valid_hash(value) {
                        bail!("invalid previous hash in {filename}: {value}");
                    }
                    previous_hash = if value.is_empty() {
                        None
                    } else {
                        Some(value.to_ascii_lowercase())
                    };
                }
                "Hash" => {
                    if hash.is_some() {
                        bail!("duplicate Hash header in {filename}");
                    }
                    if !migration_hash::is_valid_hash(value) {
                        bail!("invalid hash in {filename}: {value}");
                    }
                    hash = Some(value.to_ascii_lowercase());
                }
                _ => bail!("unknown migration header in {filename}: {content}"),
            }
            header_lines += 1;
            offset += line.len();
            continue;
        }

        if header_lines == 0 {
            break;
        }

        if content.is_empty() {
            saw_blank_after_headers = true;
            offset += line.len();
            continue;
        }

        body_start = offset;
        break;
    }

    if header_lines > 0 && body_start == 0 {
        body_start = offset;
    }

    let body = migration_hash::normalize_body(&normalized[body_start.min(normalized.len())..]);
    if header_lines == 0 {
        bail!("missing migration headers in {filename}");
    }
    if body.is_empty() {
        bail!("migration {filename} has an empty body");
    }

    Ok(ParsedMigrationContents {
        previous_hash,
        hash: hash.ok_or_else(|| anyhow::anyhow!("missing hash header in {filename}"))?,
        body,
    })
}

struct ParsedMigrationContents {
    previous_hash: Option<String>,
    hash: String,
    body: String,
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::load_committed_migrations;
    use crate::migration_hash;

    #[test]
    fn loads_and_validates_committed_migrations() {
        let temp_dir = tempdir().unwrap();
        let committed_dir = temp_dir.path().join("committed");
        fs::create_dir_all(&committed_dir).unwrap();

        let first_body = "create table users (id integer primary key);";
        let first_hash = migration_hash::calculate(None, first_body);
        fs::write(
            committed_dir.join("000001.sql"),
            format!("--! Previous: \n--! Hash: {first_hash}\n\n{first_body}\n"),
        )
        .unwrap();

        let second_body = "alter table users add column email text;";
        let second_hash = migration_hash::calculate(Some(&first_hash), second_body);
        fs::write(
            committed_dir.join("000002.sql"),
            format!("--! Previous: {first_hash}\n--! Hash: {second_hash}\n\n{second_body}\n"),
        )
        .unwrap();

        let migrations = load_committed_migrations(&committed_dir).unwrap();

        assert_eq!(migrations.len(), 2);
        assert_eq!(migrations[0].hash, first_hash);
        assert_eq!(
            migrations[1].previous_hash.as_deref(),
            Some(first_hash.as_str())
        );
    }

    #[test]
    fn rejects_non_contiguous_versions() {
        let temp_dir = tempdir().unwrap();
        let committed_dir = temp_dir.path().join("committed");
        fs::create_dir_all(&committed_dir).unwrap();
        let body = "select 1;";
        let hash = migration_hash::calculate(None, body);
        fs::write(
            committed_dir.join("000002.sql"),
            format!("--! Previous: \n--! Hash: {hash}\n\n{body}\n"),
        )
        .unwrap();

        let error = load_committed_migrations(&committed_dir).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("expected committed migration 000001")
        );
    }

    #[test]
    fn rejects_hash_mismatches() {
        let temp_dir = tempdir().unwrap();
        let committed_dir = temp_dir.path().join("committed");
        fs::create_dir_all(&committed_dir).unwrap();
        fs::write(
            committed_dir.join("000001.sql"),
            "--! Previous: \n--! Hash: deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef\n\nselect 1;\n",
        )
        .unwrap();

        let error = load_committed_migrations(&committed_dir).unwrap_err();

        assert!(error.to_string().contains("hash mismatch"));
    }

    #[test]
    fn rejects_non_empty_previous_hash_for_first_migration() {
        let temp_dir = tempdir().unwrap();
        let committed_dir = temp_dir.path().join("committed");
        fs::create_dir_all(&committed_dir).unwrap();
        let body = "select 1;";
        let previous_hash = "a".repeat(64);
        let hash = migration_hash::calculate(Some(&previous_hash), body);
        fs::write(
            committed_dir.join("000001.sql"),
            format!("--! Previous: {previous_hash}\n--! Hash: {hash}\n\n{body}\n"),
        )
        .unwrap();

        let error = load_committed_migrations(&committed_dir).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("first committed migration 000001.sql")
        );
    }

    #[test]
    fn rejects_blank_lines_between_headers() {
        let temp_dir = tempdir().unwrap();
        let committed_dir = temp_dir.path().join("committed");
        fs::create_dir_all(&committed_dir).unwrap();
        let body = "select 1;";
        let previous_hash = "a".repeat(64);
        let hash = migration_hash::calculate(Some(&previous_hash), body);
        fs::write(
            committed_dir.join("000001.sql"),
            format!("--! Previous: {previous_hash}\n\n--! Hash: {hash}\n\n{body}\n"),
        )
        .unwrap();

        let error = load_committed_migrations(&committed_dir).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("must be contiguous and followed by a blank line")
        );
    }

    #[test]
    fn rejects_duplicate_hash_headers() {
        let temp_dir = tempdir().unwrap();
        let committed_dir = temp_dir.path().join("committed");
        fs::create_dir_all(&committed_dir).unwrap();
        let body = "select 1;";
        let first_hash = "a".repeat(64);
        let second_hash = migration_hash::calculate(None, body);
        fs::write(
            committed_dir.join("000001.sql"),
            format!("--! Previous: \n--! Hash: {first_hash}\n--! Hash: {second_hash}\n\n{body}\n"),
        )
        .unwrap();

        let error = load_committed_migrations(&committed_dir).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("duplicate Hash header in 000001.sql")
        );
    }

    #[test]
    fn rejects_duplicate_previous_headers() {
        let temp_dir = tempdir().unwrap();
        let committed_dir = temp_dir.path().join("committed");
        fs::create_dir_all(&committed_dir).unwrap();
        let body = "select 1;";
        let previous_hash = "a".repeat(64);
        let hash = migration_hash::calculate(Some(&previous_hash), body);
        fs::write(
            committed_dir.join("000001.sql"),
            format!(
                "--! Previous: {previous_hash}\n--! Previous: {previous_hash}\n--! Hash: {hash}\n\n{body}\n"
            ),
        )
        .unwrap();

        let error = load_committed_migrations(&committed_dir).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("duplicate Previous header in 000001.sql")
        );
    }

    #[test]
    fn ignores_non_sql_files_in_committed_directory() {
        let temp_dir = tempdir().unwrap();
        let committed_dir = temp_dir.path().join("committed");
        fs::create_dir_all(&committed_dir).unwrap();
        fs::write(committed_dir.join(".gitkeep"), "").unwrap();

        let migrations = load_committed_migrations(&committed_dir).unwrap();

        assert!(migrations.is_empty());
    }
}
