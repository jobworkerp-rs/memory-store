//! The SQLite files a local migration may touch.

use super::files::with_suffix;
use super::output::{ErrorCode, Resolution, fail};
use anyhow::Result;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqliteTarget {
    database: PathBuf,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TargetFiles {
    /// No database yet; the migration creates it.
    Missing,
    Present,
}

impl SqliteTarget {
    pub fn at(database: PathBuf) -> Self {
        Self { database }
    }

    /// Resolve the absolute SQLx `sqlite:///` URL that desktop callers pass.
    pub fn from_url(url: &str) -> Result<Self> {
        let database = url::Url::parse(url)
            .ok()
            .filter(|parsed| parsed.scheme() == "sqlite" && parsed.path().starts_with('/'))
            .and_then(|parsed| parsed.to_file_path().ok());
        match database {
            Some(database) => Ok(Self::at(database)),
            None => fail(
                ErrorCode::InvalidTarget,
                Resolution::ToolUpdateRequired,
                "local migration requires an absolute sqlite:/// database URL",
            ),
        }
    }

    pub fn database(&self) -> &Path {
        &self.database
    }

    pub fn wal(&self) -> PathBuf {
        self.side_file("-wal")
    }

    pub fn shm(&self) -> PathBuf {
        self.side_file("-shm")
    }

    fn side_file(&self, suffix: &str) -> PathBuf {
        with_suffix(&self.database, suffix)
    }

    /// Reject file layouts that SQLite would silently reinterpret: a link may
    /// point outside the backed-up location, and side files without their
    /// database mean the database was moved or deleted.
    pub fn inspect_files(&self) -> Result<TargetFiles> {
        match std::fs::symlink_metadata(&self.database) {
            Ok(metadata) if metadata.file_type().is_file() => Ok(TargetFiles::Present),
            Ok(_) => fail(
                ErrorCode::InvalidTarget,
                Resolution::ToolUpdateRequired,
                format!(
                    "{} is not a regular file (symlink or directory)",
                    self.database.display()
                ),
            ),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                if self.wal().exists() || self.shm().exists() {
                    return fail(
                        ErrorCode::InvalidTarget,
                        Resolution::ToolUpdateRequired,
                        format!(
                            "{} is missing while its WAL or SHM file exists",
                            self.database.display()
                        ),
                    );
                }
                Ok(TargetFiles::Missing)
            }
            Err(error) => Err(anyhow::Error::new(error)
                .context(format!("inspecting {}", self.database.display()))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db_migrate::local::output::{ErrorCode, LocalFailure};

    fn error_code(error: &anyhow::Error) -> Option<ErrorCode> {
        error.downcast_ref::<LocalFailure>().map(|f| f.error_code)
    }

    #[test]
    fn percent_encoded_absolute_url_resolves_to_the_database_file() {
        let target =
            SqliteTarget::from_url("sqlite:///tmp/Application%20Support/%E3%83%A1%25.db?mode=rwc")
                .unwrap();
        assert_eq!(
            target.database(),
            std::path::Path::new("/tmp/Application Support/メ%.db")
        );
        assert_eq!(
            target.wal(),
            std::path::Path::new("/tmp/Application Support/メ%.db-wal")
        );
        assert_eq!(
            target.shm(),
            std::path::Path::new("/tmp/Application Support/メ%.db-shm")
        );
    }

    #[test]
    fn non_file_urls_are_rejected_as_invalid_targets() {
        for url in ["sqlite::memory:", "sqlite://relative.db", "postgres://h/db"] {
            let error = SqliteTarget::from_url(url).unwrap_err();
            assert_eq!(error_code(&error), Some(ErrorCode::InvalidTarget), "{url}");
        }
    }

    #[test]
    fn missing_database_without_side_files_is_a_new_database() {
        let directory = tempfile::tempdir().unwrap();
        let target = SqliteTarget::at(directory.path().join("new.db"));
        assert_eq!(target.inspect_files().unwrap(), TargetFiles::Missing);
    }

    #[test]
    fn regular_database_file_is_present() {
        let directory = tempfile::tempdir().unwrap();
        let target = SqliteTarget::at(directory.path().join("db.sqlite3"));
        std::fs::write(target.database(), b"").unwrap();
        std::fs::write(target.wal(), b"").unwrap();
        assert_eq!(target.inspect_files().unwrap(), TargetFiles::Present);
    }

    #[test]
    fn symlinked_database_is_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let real = directory.path().join("real.db");
        std::fs::write(&real, b"").unwrap();
        let target = SqliteTarget::at(directory.path().join("link.db"));
        std::os::unix::fs::symlink(&real, target.database()).unwrap();
        let error = target.inspect_files().unwrap_err();
        assert_eq!(error_code(&error), Some(ErrorCode::InvalidTarget));
    }

    #[test]
    fn directory_in_place_of_the_database_is_rejected() {
        let directory = tempfile::tempdir().unwrap();
        let target = SqliteTarget::at(directory.path().join("db"));
        std::fs::create_dir(target.database()).unwrap();
        let error = target.inspect_files().unwrap_err();
        assert_eq!(error_code(&error), Some(ErrorCode::InvalidTarget));
    }

    #[test]
    fn side_files_without_the_database_are_rejected() {
        for side in ["-wal", "-shm"] {
            let directory = tempfile::tempdir().unwrap();
            let target = SqliteTarget::at(directory.path().join("db"));
            std::fs::write(format!("{}{side}", target.database().display()), b"").unwrap();
            let error = target.inspect_files().unwrap_err();
            assert_eq!(error_code(&error), Some(ErrorCode::InvalidTarget), "{side}");
        }
    }
}
