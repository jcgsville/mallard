use std::{
    fs,
    path::PathBuf,
    thread,
    time::{Duration, UNIX_EPOCH},
};

use anyhow::Result;

use crate::{config::Config, run_current};

pub fn run(config: &Config, interval: Duration) -> Result<()> {
    let mut previous_state: Option<Vec<FileStamp>> = None;

    loop {
        match collect_watch_state(config) {
            Ok(state) => maybe_run_for_state(config, &mut previous_state, state),
            Err(error) => eprintln!("error: {error:#}"),
        }

        thread::sleep(interval);
    }
}

fn maybe_run_for_state(
    config: &Config,
    previous_state: &mut Option<Vec<FileStamp>>,
    state: Vec<FileStamp>,
) {
    if previous_state.as_ref() == Some(&state) {
        return;
    }

    match run_current::run(config) {
        Ok(_) => *previous_state = Some(state),
        Err(error) => {
            eprintln!("error: {error:#}");
            *previous_state = Some(state);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct FileStamp {
    path: PathBuf,
    modified_millis: u128,
    size: u64,
}

fn collect_watch_state(config: &Config) -> Result<Vec<FileStamp>> {
    let mut paths = vec![
        config.migrations_dir.join("current.sql"),
        config.migrations_dir.join("committed"),
        config.migrations_dir.join("fixtures"),
    ];
    let mut stamps = Vec::new();

    while let Some(path) = paths.pop() {
        if !path.exists() {
            continue;
        }

        let metadata = fs::metadata(&path)?;
        if metadata.is_dir() {
            for entry in fs::read_dir(&path)? {
                paths.push(entry?.path());
            }
            continue;
        }

        let modified_millis = metadata
            .modified()?
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        stamps.push(FileStamp {
            path,
            modified_millis,
            size: metadata.len(),
        });
    }

    stamps.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(stamps)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;

    use tempfile::tempdir;

    use super::{maybe_run_for_state, FileStamp};
    use crate::config::Config;

    fn sample_state(path: &str) -> Vec<FileStamp> {
        vec![FileStamp {
            path: PathBuf::from(path),
            modified_millis: 1,
            size: 1,
        }]
    }

    #[test]
    fn records_state_after_successful_run() {
        let temp_dir = tempdir().unwrap();
        let config_path = temp_dir.path().join("mallard.toml");
        fs::write(&config_path, "version = 1").unwrap();
        fs::create_dir_all(temp_dir.path().join("migrations/committed")).unwrap();
        fs::write(
            temp_dir.path().join("migrations/current.sql"),
            "select 1;\n",
        )
        .unwrap();

        let config = Config::load(&config_path).unwrap();
        let state = sample_state("current.sql");
        let mut previous_state = None;

        maybe_run_for_state(&config, &mut previous_state, state.clone());

        assert_eq!(previous_state, Some(state));
    }

    #[test]
    fn records_state_after_failed_run() {
        let temp_dir = tempdir().unwrap();
        let config_path = temp_dir.path().join("mallard.toml");
        fs::write(&config_path, "version = 1").unwrap();
        fs::create_dir_all(temp_dir.path().join("migrations/committed")).unwrap();
        fs::write(
            temp_dir.path().join("migrations/current.sql"),
            "select * from :MISSING_SCHEMA.users;\n",
        )
        .unwrap();

        let config = Config::load(&config_path).unwrap();
        let state = sample_state("current.sql");
        let mut previous_state = None;

        maybe_run_for_state(&config, &mut previous_state, state.clone());

        assert_eq!(previous_state, Some(state));
    }
}
