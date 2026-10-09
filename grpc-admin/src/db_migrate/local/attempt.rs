//! Durable record of the last local migration attempt.
//!
//! It lives next to the database rather than inside it so that a failure that
//! leaves the database unreadable, or a run without a backup, can still be
//! classified on the next start.

use super::files::{with_suffix, write_atomically};
use super::output::{Resolution, Stage};
use super::target::SqliteTarget;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

const RECORD_FORMAT: &str = "memories-local-attempt-v1";
const RECORD_SUFFIX: &str = ".memories-migrate-attempt.json";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttemptStatus {
    Running,
    Succeeded,
    Failed(Resolution),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttemptRecord {
    pub attempt_id: String,
    pub started_at: i64,
    pub bundle_digest: Option<String>,
    /// `None` when the attempt ran without a backup.
    pub backup: Option<PathBuf>,
    /// The last stage the attempt started.
    pub stage: Stage,
    pub status: AttemptStatus,
}

#[derive(Serialize, Deserialize)]
struct StoredRecord {
    format: String,
    attempt_id: String,
    started_at: i64,
    bundle_digest: Option<String>,
    backup: Option<PathBuf>,
    stage: String,
    status: String,
    resolution: Option<String>,
}

impl AttemptRecord {
    pub fn path(target: &SqliteTarget) -> PathBuf {
        with_suffix(target.database(), RECORD_SUFFIX)
    }

    /// A record that cannot be read is ignored: writes are atomic, so it can
    /// only come from outside this tool and carries no trustworthy state.
    pub fn load(target: &SqliteTarget) -> Result<Option<Self>> {
        let path = Self::path(target);
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => {
                return Err(anyhow::Error::new(error)
                    .context(format!("reading attempt record {}", path.display())));
            }
        };
        Ok(serde_json::from_slice::<StoredRecord>(&bytes)
            .ok()
            .and_then(Self::from_stored))
    }

    pub fn store(&self, target: &SqliteTarget) -> Result<()> {
        let (status, resolution) = match self.status {
            AttemptStatus::Running => ("running", None),
            AttemptStatus::Succeeded => ("succeeded", None),
            AttemptStatus::Failed(resolution) => ("failed", Some(resolution.as_str().to_string())),
        };
        let stored = StoredRecord {
            format: RECORD_FORMAT.to_string(),
            attempt_id: self.attempt_id.clone(),
            started_at: self.started_at,
            bundle_digest: self.bundle_digest.clone(),
            backup: self.backup.clone(),
            stage: self.stage.as_str().to_string(),
            status: status.to_string(),
            resolution,
        };
        let bytes = serde_json::to_vec_pretty(&stored).context("serializing attempt record")?;
        write_atomically(&Self::path(target), &bytes)
    }

    fn from_stored(stored: StoredRecord) -> Option<Self> {
        if stored.format != RECORD_FORMAT {
            return None;
        }
        let status = match (stored.status.as_str(), stored.resolution.as_deref()) {
            ("running", None) => AttemptStatus::Running,
            ("succeeded", None) => AttemptStatus::Succeeded,
            ("failed", Some(resolution)) => AttemptStatus::Failed(Resolution::parse(resolution)?),
            _ => return None,
        };
        Some(Self {
            attempt_id: stored.attempt_id,
            started_at: stored.started_at,
            bundle_digest: stored.bundle_digest,
            backup: stored.backup,
            stage: Stage::parse(&stored.stage)?,
            status,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db_migrate::local::output::{Resolution, Stage};
    use crate::db_migrate::local::target::SqliteTarget;

    fn sample(status: AttemptStatus) -> AttemptRecord {
        AttemptRecord {
            attempt_id: "attempt-1".to_string(),
            started_at: 10,
            bundle_digest: Some("digest".to_string()),
            backup: Some("/backups/memories-backup-1".into()),
            stage: Stage::PostMigrate,
            status,
        }
    }

    #[test]
    fn record_round_trips_next_to_the_database() {
        let directory = tempfile::tempdir().unwrap();
        let target = SqliteTarget::at(directory.path().join("default.sqlite3"));
        assert_eq!(AttemptRecord::load(&target).unwrap(), None);

        let record = sample(AttemptStatus::Failed(Resolution::RestoreRequired));
        record.store(&target).unwrap();
        assert!(
            directory
                .path()
                .join("default.sqlite3.memories-migrate-attempt.json")
                .is_file()
        );
        assert_eq!(AttemptRecord::load(&target).unwrap(), Some(record));
        // Only the record itself remains; the temporary file was renamed.
        assert_eq!(std::fs::read_dir(directory.path()).unwrap().count(), 1);
    }

    #[test]
    fn storing_again_replaces_the_previous_record() {
        let directory = tempfile::tempdir().unwrap();
        let target = SqliteTarget::at(directory.path().join("db"));
        sample(AttemptStatus::Running).store(&target).unwrap();
        sample(AttemptStatus::Succeeded).store(&target).unwrap();
        assert_eq!(
            AttemptRecord::load(&target).unwrap().unwrap().status,
            AttemptStatus::Succeeded
        );
    }

    #[test]
    fn unreadable_record_is_ignored() {
        let directory = tempfile::tempdir().unwrap();
        let target = SqliteTarget::at(directory.path().join("db"));
        std::fs::write(AttemptRecord::path(&target), b"{not json").unwrap();
        assert_eq!(AttemptRecord::load(&target).unwrap(), None);
    }
}
