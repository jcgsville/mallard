use std::{
    collections::BTreeMap,
    env, fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, anyhow, bail};
use serde::Deserialize;

const CONFIG_FILE_NAME: &str = "mallard.toml";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub version: u32,
    pub config_path: PathBuf,
    pub database_path: PathBuf,
    pub shadow_path: PathBuf,
    pub migrations_dir: PathBuf,
    pub internal_schema: String,
    pub placeholders: BTreeMap<String, String>,
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

        let config_dir = config_path
            .parent()
            .ok_or_else(|| anyhow!("config path has no parent: {}", config_path.display()))?;

        let contents = fs::read_to_string(&config_path)
            .with_context(|| format!("failed to read {}", config_path.display()))?;
        let raw: RawConfig = toml::from_str(&contents)
            .with_context(|| format!("failed to parse {}", config_path.display()))?;

        if raw.version != 1 {
            bail!(
                "unsupported config version {} in {}",
                raw.version,
                config_path.display()
            );
        }

        let database_path = resolve_path(config_dir, &interpolate_env(&raw.database.path)?);
        let shadow_path = resolve_path(config_dir, &interpolate_env(&raw.shadow.path)?);
        let migrations_dir = resolve_path(config_dir, &interpolate_env(&raw.migrations.dir)?);
        validate_identifier(&raw.migrations.internal_schema, "internal schema")?;

        let mut placeholders = BTreeMap::new();
        for (key, value) in raw.placeholders {
            placeholders.insert(key, interpolate_env(&value)?);
        }

        Ok(Self {
            version: raw.version,
            config_path,
            database_path,
            shadow_path,
            migrations_dir,
            internal_schema: raw.migrations.internal_schema,
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
    #[serde(default)]
    database: RawDatabase,
    #[serde(default)]
    shadow: RawShadow,
    #[serde(default)]
    migrations: RawMigrations,
    #[serde(default)]
    placeholders: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
struct RawDatabase {
    #[serde(default = "default_database_path")]
    path: String,
}

impl Default for RawDatabase {
    fn default() -> Self {
        Self {
            path: default_database_path(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct RawShadow {
    #[serde(default = "default_shadow_path")]
    path: String,
}

impl Default for RawShadow {
    fn default() -> Self {
        Self {
            path: default_shadow_path(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct RawMigrations {
    #[serde(default = "default_migrations_dir")]
    dir: String,
    #[serde(default = "default_internal_schema")]
    internal_schema: String,
}

impl Default for RawMigrations {
    fn default() -> Self {
        Self {
            dir: default_migrations_dir(),
            internal_schema: default_internal_schema(),
        }
    }
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

#[cfg(test)]
mod tests {
    use super::Config;
    use std::{
        env, fs,
        sync::{LazyLock, Mutex},
    };
    use tempfile::tempdir;

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

[database]
path = "db/dev.duckdb"

[shadow]
path = ".mallard/shadow.duckdb"

[migrations]
dir = "sql"
"#,
        )
        .unwrap();

        let config = Config::load(&config_path).unwrap();

        assert_eq!(config.database_path, config_dir.join("db/dev.duckdb"));
        assert_eq!(
            config.shadow_path,
            config_dir.join(".mallard/shadow.duckdb")
        );
        assert_eq!(config.migrations_dir, config_dir.join("sql"));
        assert_eq!(config.internal_schema, "mallard");
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

[database]
path = "${MALLARD_DB_PATH}"

[shadow]
path = "${MALLARD_SHADOW_PATH:-shadow/default.duckdb}"
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
    }

    #[test]
    fn rejects_invalid_internal_schema_names() {
        let temp_dir = tempdir().unwrap();
        let config_path = temp_dir.path().join("mallard.toml");
        fs::write(
            &config_path,
            r#"version = 1

[migrations]
internal_schema = "bad-schema"
"#,
        )
        .unwrap();

        let error = Config::load(&config_path).unwrap_err();

        assert!(
            error.to_string().contains(
                "internal schema must contain only ASCII letters, digits, or underscores"
            )
        );
    }
}
