//! The embedding migration attempt record, kept in the embedding state
//! directory and written atomically (spec §3.3 "試行記録").

use anyhow::{Context as _, Result};
use infra::infra::embedding_space::storage::write_atomic;
use infra::infra::embedding_space::{MarkerState, MigrationMarker, SpaceComponents};
use serde::{Deserialize, Serialize};
use std::path::Path;

const ATTEMPT_FILE: &str = "attempt.json";
pub const FORMAT_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Method {
    Backup,
    NoBackup,
}

impl Method {
    /// `backup_mode=` value.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Backup => "backup",
            Self::NoBackup => "none",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Stage {
    Backup,
    Replace,
    RebuildPending,
    Commit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Running,
    Completed,
    /// Ended before any data changed, or discarded (no-backup attempt).
    Abandoned,
    Restored,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CancelOperation {
    Restore,
    Discard,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AttemptRecord {
    pub format_version: u32,
    pub attempt_id: String,
    pub method: Method,
    /// `None` when the source space was unknown.
    pub source_space_id: Option<String>,
    pub target_space_id: String,
    pub target_space: SpaceComponents,
    pub backup_path: Option<String>,
    pub backup_complete: bool,
    pub stage: Stage,
    pub status: Status,
    pub cancel_operation: Option<CancelOperation>,
    /// The vector tables were discarded (no-backup cancellation).
    pub discarded: bool,
    /// Failures accepted by `finalize --accept-failed`.
    pub accepted_failed: Option<u64>,
    /// Embedding backups kept by the retention of `switch` / `finalize`.
    #[serde(default)]
    pub backup_keep: Option<usize>,
    pub started_at: i64,
}

impl AttemptRecord {
    /// This attempt's marker in `state`.
    pub fn marker(&self, state: MarkerState) -> MigrationMarker {
        MigrationMarker {
            state,
            attempt_id: self.attempt_id.clone(),
        }
    }

    /// The `attempt` / `attempt_stage` / `backup_mode` output fields.
    pub fn info(&self, stage: &str) -> super::output::AttemptInfo {
        super::output::AttemptInfo {
            attempt: self.attempt_id.clone(),
            stage: stage.to_string(),
            backup_mode: self.method.as_str().to_string(),
        }
    }

    /// The backup to restore from: complete and verifying (spec §3.10:
    /// an unfinished backup is never offered).
    pub fn usable_backup(&self) -> Option<String> {
        let path = self.backup_path.as_deref()?;
        (self.method == Method::Backup
            && self.backup_complete
            && super::backup::verify(Path::new(path), &self.attempt_id).is_ok())
        .then(|| path.to_string())
    }
}

/// Why the record could not be read.
#[derive(Debug)]
pub enum LoadError {
    /// Written by a newer tool; nothing may be changed.
    UnknownFormat(u32),
    Other(anyhow::Error),
}

impl std::fmt::Display for LoadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnknownFormat(v) => write!(f, "attempt record format {v} is not supported"),
            Self::Other(e) => write!(f, "{e:#}"),
        }
    }
}

impl std::error::Error for LoadError {}

pub fn load(state_dir: &Path) -> std::result::Result<Option<AttemptRecord>, LoadError> {
    let path = state_dir.join(ATTEMPT_FILE);
    let bytes = match std::fs::read(&path) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(LoadError::Other(e.into())),
    };
    let value: serde_json::Value =
        serde_json::from_slice(&bytes).map_err(|e| LoadError::Other(e.into()))?;
    let version = value
        .get("format_version")
        .and_then(|v| v.as_u64())
        .unwrap_or(0) as u32;
    if version != FORMAT_VERSION {
        return Err(LoadError::UnknownFormat(version));
    }
    serde_json::from_value(value)
        .map(Some)
        .map_err(|e| LoadError::Other(e.into()))
}

pub fn save(state_dir: &Path, record: &AttemptRecord) -> Result<()> {
    write_atomic(
        &state_dir.join(ATTEMPT_FILE),
        &serde_json::to_vec_pretty(record).context("serializing the attempt record")?,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(crate) fn record() -> AttemptRecord {
        AttemptRecord {
            format_version: FORMAT_VERSION,
            attempt_id: "att".into(),
            method: Method::Backup,
            source_space_id: Some("s".into()),
            target_space_id: "t".into(),
            target_space: SpaceComponents {
                model_id: "m".into(),
                tokenizer_model_id: String::new(),
                revision: "r".into(),
                dimension: 4,
                distance: "cosine".into(),
            },
            backup_path: Some("/b".into()),
            backup_complete: false,
            stage: Stage::Backup,
            status: Status::Running,
            cancel_operation: None,
            discarded: false,
            accepted_failed: None,
            backup_keep: None,
            started_at: 1,
        }
    }

    #[test]
    fn roundtrip_and_absent() {
        let dir = tempfile::tempdir().unwrap();
        assert!(load(dir.path()).unwrap().is_none());
        save(dir.path(), &record()).unwrap();
        assert_eq!(load(dir.path()).unwrap(), Some(record()));
    }

    #[test]
    fn unknown_format_is_reported_without_parsing_further() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join(ATTEMPT_FILE),
            r#"{"format_version": 9, "anything": true}"#,
        )
        .unwrap();
        assert!(matches!(load(dir.path()), Err(LoadError::UnknownFormat(9))));
    }
}
