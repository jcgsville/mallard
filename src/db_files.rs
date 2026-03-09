use std::{
    ffi::OsString,
    fs,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};

pub fn remove_if_exists(path: &Path) -> Result<()> {
    if path.exists() {
        fs::remove_file(path).with_context(|| format!("failed to remove {}", path.display()))?;
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
    use super::wal_path;

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
}
