use std::{
    fs,
    path::PathBuf,
    thread,
    time::{Duration, UNIX_EPOCH},
};

use anyhow::Result;

use crate::{
    config::Config,
    run_current::{self, RunResult, RunTarget},
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WatchResult {
    pub last_run: RunResult,
    pub cycles: usize,
}

pub fn run(
    config: &Config,
    target: RunTarget,
    once: bool,
    interval: Duration,
) -> Result<WatchResult> {
    let mut cycles = 0;
    let mut previous_state: Option<Vec<FileStamp>> = None;

    loop {
        let state = collect_watch_state(config)?;
        if previous_state.as_ref() != Some(&state) {
            let result = run_current::run(config, target)?;
            cycles += 1;
            previous_state = Some(state);
            if once {
                return Ok(WatchResult {
                    last_run: result,
                    cycles,
                });
            }
        }

        thread::sleep(interval);
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
        config.migrations_dir.join("hooks"),
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
    use std::{fs, time::Duration};

    use tempfile::tempdir;

    use super::run;
    use crate::{config::Config, run_current::RunTarget};

    #[test]
    fn watch_once_runs_a_single_cycle() {
        let temp_dir = tempdir().unwrap();
        let config_path = temp_dir.path().join("mallard.toml");
        fs::write(&config_path, "version = 1").unwrap();
        fs::create_dir_all(temp_dir.path().join("migrations/committed")).unwrap();
        fs::create_dir_all(temp_dir.path().join("migrations/fixtures")).unwrap();
        fs::create_dir_all(temp_dir.path().join("migrations/hooks")).unwrap();
        fs::write(
            temp_dir.path().join("migrations/current.sql"),
            "select 1;\n",
        )
        .unwrap();

        let config = Config::load(&config_path).unwrap();
        let result = run(&config, RunTarget::Shadow, true, Duration::from_millis(0)).unwrap();

        assert_eq!(result.cycles, 1);
        assert!(config.shadow_path.exists());
    }
}
