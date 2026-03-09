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
        let state = collect_watch_state(config)?;
        if previous_state.as_ref() != Some(&state) {
            run_current::run(config)?;
            previous_state = Some(state);
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
