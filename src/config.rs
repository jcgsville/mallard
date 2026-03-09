use std::{
    collections::BTreeMap,
    env, fmt, fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, anyhow, bail};
use serde::Deserialize;

const CONFIG_FILE_NAME: &str = "mallard.toml";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub version: u32,
    pub config_path: PathBuf,
    pub project_root: PathBuf,
    pub database_path: PathBuf,
    pub shadow_path: PathBuf,
    pub migrations_dir: PathBuf,
    pub internal_schema: SqlIdentifier,
    pub manage_metadata: bool,
    pub placeholders: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlIdentifier(String);

impl SqlIdentifier {
    pub fn parse(value: &str, label: &str) -> Result<Self> {
        validate_identifier(value, label)?;
        Ok(Self(value.to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn quoted(&self) -> String {
        format!("\"{}\"", self.0.replace('"', "\"\""))
    }
}

impl fmt::Display for SqlIdentifier {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl Config {
    pub fn discover(start_dir: &Path, explicit_path: Option<&Path>) -> Result<Self> {
        let path = Self::discover_path(start_dir, explicit_path)?;
        Self::load(&path)
    }

    pub fn discover_path(start_dir: &Path, explicit_path: Option<&Path>) -> Result<PathBuf> {
        if let Some(explicit_path) = explicit_path {
            let resolved = if explicit_path.is_absolute() {
                explicit_path.to_path_buf()
            } else {
                start_dir.join(explicit_path)
            };

            return Ok(resolved);
        }

        let mut current = start_dir;

        loop {
            let candidate = current.join(CONFIG_FILE_NAME);
            if candidate.exists() {
                return Ok(candidate);
            }

            match current.parent() {
                Some(parent) => current = parent,
                None => {
                    bail!(
                        "could not find `{}` starting from {}",
                        CONFIG_FILE_NAME,
                        start_dir.display()
                    )
                }
            }
        }
    }

    pub fn load(path: &Path) -> Result<Self> {
        let config_path = if path.is_absolute() {
            path.to_path_buf()
        } else {
            env::current_dir()?.join(path)
        };

        let project_root = config_path
            .parent()
            .ok_or_else(|| anyhow!("config path has no parent: {}", config_path.display()))?
            .to_path_buf();

        let contents = fs::read_to_string(&config_path)
            .with_context(|| format!("failed to read {}", config_path.display()))?;
        let raw: RawConfig = toml::from_str(&contents)
            .with_context(|| format!("failed to parse {}", config_path.display()))?;

        if raw.version != 1 {
            bail!(
                "invalid config {}: unsupported config version {}",
                config_path.display(),
                raw.version
            );
        }

        let (database_path, shadow_path, migrations_dir, internal_schema, placeholders) = (|| {
            let database_path = resolve_path(&project_root, &interpolate_env(&raw.database_path)?);
            let shadow_path = resolve_path(&project_root, &interpolate_env(&raw.shadow_path)?);
            let migrations_dir =
                resolve_path(&project_root, &interpolate_env(&raw.migrations_dir)?);
            let internal_schema = SqlIdentifier::parse(&raw.internal_schema, "internal schema")?;

            let mut placeholders = BTreeMap::new();
            for (key, value) in &raw.placeholders {
                placeholders.insert(key.clone(), interpolate_env(value)?);
            }

            Ok::<_, anyhow::Error>((
                database_path,
                shadow_path,
                migrations_dir,
                internal_schema,
                placeholders,
            ))
        })(
        )
        .map_err(|error| anyhow!("invalid config {}: {error}", config_path.display()))?;

        Ok(Self {
            version: raw.version,
            config_path,
            project_root,
            database_path,
            shadow_path,
            migrations_dir,
            internal_schema,
            manage_metadata: raw.manage_metadata,
            placeholders,
        })
    }
}

fn validate_identifier(value: &str, label: &str) -> Result<()> {
    let mut chars = value.chars();
    let Some(first) = chars.next() else {
        bail!("{label} cannot be empty")
    };

    if !(first == '_' || first.is_ascii_alphabetic()) {
        bail!("{label} must start with an ASCII letter or underscore: `{value}`");
    }

    if !chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric()) {
        bail!("{label} must contain only ASCII letters, digits, or underscores: `{value}`");
    }

    Ok(())
}

fn resolve_path(base_dir: &Path, value: &str) -> PathBuf {
    let path = PathBuf::from(value);
    if path.is_absolute() {
        path
    } else {
        base_dir.join(path)
    }
}

fn interpolate_env(input: &str) -> Result<String> {
    let mut result = String::with_capacity(input.len());
    let chars: Vec<char> = input.chars().collect();
    let mut index = 0;

    while index < chars.len() {
        if chars[index] == '$' && chars.get(index + 1) == Some(&'{') {
            let mut end = index + 2;
            while end < chars.len() && chars[end] != '}' {
                end += 1;
            }

            if end == chars.len() {
                bail!("unterminated env interpolation in `{input}`");
            }

            let expression: String = chars[index + 2..end].iter().collect();
            let (name, default) = match expression.split_once(":-") {
                Some((name, default)) => (name, Some(default)),
                None => (expression.as_str(), None),
            };

            if name.is_empty() {
                bail!("empty env var name in `{input}`");
            }

            let value = match env::var(name) {
                Ok(value) => value,
                Err(_) => match default {
                    Some(default) => interpolate_env(default)?,
                    None => bail!("missing environment variable `{name}`"),
                },
            };

            result.push_str(&value);
            index = end + 1;
        } else {
            result.push(chars[index]);
            index += 1;
        }
    }

    Ok(result)
}

#[derive(Debug, Deserialize)]
struct RawConfig {
    version: u32,
    #[serde(default = "default_database_path")]
    database_path: String,
    #[serde(default = "default_shadow_path")]
    shadow_path: String,
    #[serde(default = "default_migrations_dir")]
    migrations_dir: String,
    #[serde(default = "default_internal_schema")]
    internal_schema: String,
    #[serde(default = "default_manage_metadata")]
    manage_metadata: bool,
    #[serde(default)]
    placeholders: BTreeMap<String, String>,
}

fn default_database_path() -> String {
    "${MALLARD_DB_PATH:-dev.duckdb}".to_string()
}

fn default_shadow_path() -> String {
    "${MALLARD_SHADOW_PATH:-.mallard/shadow.duckdb}".to_string()
}

fn default_migrations_dir() -> String {
    "migrations".to_string()
}

fn default_internal_schema() -> String {
    "mallard".to_string()
}

fn default_manage_metadata() -> bool {
    true
}

#[cfg(test)]
mod tests {
    use super::{Config, SqlIdentifier};
    use std::{
        env, fs,
        sync::{LazyLock, Mutex},
    };
    use tempfile::tempdir;

    // NOTE: This only serializes env mutation within this module's tests.
    // Tests elsewhere should avoid relying on ambient MALLARD_* vars or use a
    // compatible shared lock before mutating process environment.
    static ENV_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));

    #[test]
    fn resolves_relative_paths_from_config_directory() {
        let temp_dir = tempdir().unwrap();
        let config_dir = temp_dir.path().join("nested");
        fs::create_dir_all(&config_dir).unwrap();
        let config_path = config_dir.join("mallard.toml");

        fs::write(
            &config_path,
            r#"version = 1

database_path = "db/dev.duckdb"
shadow_path = ".mallard/shadow.duckdb"
migrations_dir = "sql"
"#,
        )
        .unwrap();

        let config = Config::load(&config_path).unwrap();

        assert_eq!(config.project_root, config_dir);
        assert_eq!(config.database_path, config_dir.join("db/dev.duckdb"));
        assert_eq!(
            config.shadow_path,
            config_dir.join(".mallard/shadow.duckdb")
        );
        assert_eq!(config.migrations_dir, config_dir.join("sql"));
        assert_eq!(config.internal_schema.as_str(), "mallard");
        assert!(config.manage_metadata);
    }

    #[test]
    fn allows_disabling_metadata_management() {
        let temp_dir = tempdir().unwrap();
        let config_path = temp_dir.path().join("mallard.toml");

        fs::write(
            &config_path,
            r#"version = 1

manage_metadata = false
"#,
        )
        .unwrap();

        let config = Config::load(&config_path).unwrap();

        assert!(!config.manage_metadata);
    }

    #[test]
    fn interpolates_env_values_and_defaults() {
        let _guard = ENV_LOCK.lock().unwrap();
        let temp_dir = tempdir().unwrap();
        let config_path = temp_dir.path().join("mallard.toml");

        unsafe {
            env::set_var("MALLARD_DB_PATH", "custom.duckdb");
        }

        fs::write(
            &config_path,
            r#"version = 1

database_path = "${MALLARD_DB_PATH}"
shadow_path = "${MALLARD_SHADOW_PATH:-shadow/default.duckdb}"
"#,
        )
        .unwrap();

        let config = Config::load(&config_path).unwrap();

        assert_eq!(config.database_path, temp_dir.path().join("custom.duckdb"));
        assert_eq!(
            config.shadow_path,
            temp_dir.path().join("shadow/default.duckdb")
        );

        unsafe {
            env::remove_var("MALLARD_DB_PATH");
        }
    }

    #[test]
    fn discovers_config_by_walking_upwards() {
        let temp_dir = tempdir().unwrap();
        let root = temp_dir.path();
        let nested = root.join("a/b/c");
        fs::create_dir_all(&nested).unwrap();
        let config_path = root.join("mallard.toml");
        fs::write(&config_path, "version = 1").unwrap();

        let discovered = Config::discover_path(&nested, None).unwrap();

        assert_eq!(discovered, config_path);
    }

    #[test]
    fn loads_discovered_config() {
        let temp_dir = tempdir().unwrap();
        let nested = temp_dir.path().join("a/b/c");
        fs::create_dir_all(&nested).unwrap();
        let config_path = temp_dir.path().join("mallard.toml");
        fs::write(&config_path, "version = 1").unwrap();

        let config = Config::discover(&nested, None).unwrap();

        assert_eq!(config.config_path, config_path);
        assert_eq!(config.project_root, temp_dir.path());
    }

    #[test]
    fn rejects_invalid_internal_schema_names() {
        let temp_dir = tempdir().unwrap();
        let config_path = temp_dir.path().join("mallard.toml");
        fs::write(
            &config_path,
            r#"version = 1

internal_schema = "bad-schema"
"#,
        )
        .unwrap();

        let error = Config::load(&config_path).unwrap_err();

        assert!(
            error
                .to_string()
                .contains(&config_path.display().to_string())
        );
        assert!(
            error.to_string().contains(
                "internal schema must contain only ASCII letters, digits, or underscores"
            )
        );
    }

    #[test]
    fn includes_config_path_for_invalid_env_interpolation() {
        let temp_dir = tempdir().unwrap();
        let config_path = temp_dir.path().join("mallard.toml");
        fs::write(
            &config_path,
            r#"version = 1

database_path = "${MISSING_ENV}"
"#,
        )
        .unwrap();

        let error = Config::load(&config_path).unwrap_err();

        assert_eq!(
            error.to_string(),
            format!(
                "invalid config {}: missing environment variable `MISSING_ENV`",
                config_path.display()
            )
        );
    }

    #[test]
    fn parses_valid_sql_identifiers() {
        let identifier = SqlIdentifier::parse("mallard_123", "schema").unwrap();

        assert_eq!(identifier.as_str(), "mallard_123");
        assert_eq!(identifier.quoted(), "\"mallard_123\"");
    }

    #[test]
    fn rejects_empty_sql_identifiers() {
        let error = SqlIdentifier::parse("", "schema").unwrap_err();

        assert_eq!(error.to_string(), "schema cannot be empty");
    }

    #[test]
    fn rejects_sql_identifiers_with_invalid_start() {
        let error = SqlIdentifier::parse("1mallard", "schema").unwrap_err();

        assert_eq!(
            error.to_string(),
            "schema must start with an ASCII letter or underscore: `1mallard`"
        );
    }
}
