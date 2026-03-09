use std::{
    ffi::OsString,
    fs,
    io::ErrorKind,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};

pub fn remove_if_exists(path: &Path) -> Result<()> {
    if let Err(error) = fs::remove_file(path) {
        if error.kind() != ErrorKind::NotFound {
            return Err(error).with_context(|| format!("failed to remove {}", path.display()));
        }
    }
    Ok(())
}

pub fn wal_path(path: &Path) -> PathBuf {
    let mut wal_path = OsString::from(path.as_os_str());
    wal_path.push(".wal");
    PathBuf::from(wal_path)
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::{remove_if_exists, wal_path};

    #[test]
    fn wal_path_appends_suffix_without_replacing_existing_extension() {
        assert_eq!(
            wal_path(std::path::Path::new("db")),
            std::path::PathBuf::from("db.wal")
        );
        assert_eq!(
            wal_path(std::path::Path::new("db.db")),
            std::path::PathBuf::from("db.db.wal")
        );
        assert_eq!(
            wal_path(std::path::Path::new("db.duckdb")),
            std::path::PathBuf::from("db.duckdb.wal")
        );
    }

    #[test]
    fn remove_if_exists_ignores_missing_files() {
        let temp_dir = tempdir().unwrap();

        remove_if_exists(&temp_dir.path().join("missing.duckdb")).unwrap();
    }
}
