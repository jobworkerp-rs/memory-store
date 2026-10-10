//! Guard of `local apply`, `local restore`, and `release apply` against an
//! unfinished embedding migration (spec §3.8, §3.13): before doing any
//! work they check the storage identifiers, take the embedding operation
//! lock (and on PostgreSQL the writer key), and refuse work the migration
//! cannot tolerate at its stage. They read the attempt record and the
//! table markers but never change them.

use super::attempt::{self, AttemptRecord};
use super::lock::{self, Exclusion, LockError};
use super::observe::{StorageCheck, markers_of, open_all, storage_mismatch, stores_from_env};
use super::output::{AttemptInfo, ErrorCode, Resolution};
use super::stage::{EffectiveStage, Operation, Response, effective_stage, stage_response};
use infra::infra::embedding_space::MigrationMarker;
use infra::infra::embedding_space::record::read_table_record;
use infra_utils::infra::rdb::RdbPool;
use std::collections::BTreeSet;
use std::path::Path;

/// What the guarded command is about to do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Work {
    /// Schema migrations and tasks; `neutral` when every one of them is
    /// declared embedding neutral.
    Apply { neutral: bool },
    /// Restore of a `local apply` backup; `vector_tables` when the backup
    /// contains vector stores.
    Restore { vector_tables: bool },
}

/// The refusal while a migration is unfinished
/// (`embedding_migration_in_progress`), with the fields of spec §3.10.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InProgress {
    pub resolution: Resolution,
    pub attempt: Option<AttemptInfo>,
    pub backup: Option<String>,
    pub cancel_operation: Option<&'static str>,
}

#[derive(Debug)]
pub enum GuardError {
    StorageMismatch,
    /// `operation_in_progress` or `writer_active`.
    Locked(ErrorCode),
    InProgress(InProgress),
    /// The attempt record was written by a newer tool.
    ToolUpdateRequired,
    /// The backup holds vector tables of another embedding space.
    BackupSpaceMismatch,
    Unavailable(anyhow::Error),
}

impl std::fmt::Display for GuardError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StorageMismatch => f.write_str(
                "the database, vector stores, and embedding state directory do not belong together",
            ),
            Self::Locked(code) => write!(f, "{code}"),
            Self::InProgress(p) => {
                write!(f, "an embedding migration is unfinished ({})", p.resolution)
            }
            Self::ToolUpdateRequired => {
                f.write_str("the embedding attempt record needs a newer tool")
            }
            Self::BackupSpaceMismatch => {
                f.write_str("the backup holds vector tables of another embedding space")
            }
            Self::Unavailable(e) => write!(f, "{e:#}"),
        }
    }
}

impl std::error::Error for GuardError {}

/// A refusal in output words, shared by the commands that are guarded
/// (`local` lines and the `release` / `schema` / `post-migrate` messages).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refused {
    pub error_code: &'static str,
    pub resolution: &'static str,
    pub attempt: Option<AttemptInfo>,
    /// The embedding backup to restore, when the resolution needs it.
    pub backup: Option<String>,
    pub cancel_operation: Option<&'static str>,
}

impl GuardError {
    pub fn refused(&self) -> Refused {
        let plain = |error_code, resolution| Refused {
            error_code,
            resolution,
            attempt: None,
            backup: None,
            cancel_operation: None,
        };
        match self {
            Self::StorageMismatch => plain("storage_mismatch", "check_environment"),
            Self::Locked(code) => plain(code.as_str(), "retry"),
            Self::InProgress(p) => Refused {
                error_code: "embedding_migration_in_progress",
                resolution: p.resolution.as_str(),
                attempt: p.attempt.clone(),
                backup: p.backup.clone(),
                cancel_operation: p.cancel_operation,
            },
            Self::ToolUpdateRequired => {
                plain("embedding_migration_in_progress", "tool_update_required")
            }
            Self::BackupSpaceMismatch => plain("backup_space_mismatch", "manual_recovery"),
            Self::Unavailable(_) => plain("invalid_target", "check_environment"),
        }
    }
}

/// Held while the guarded command runs.
pub struct Guard {
    _exclusion: Option<Exclusion>,
}

impl Guard {
    /// Nothing to guard (no vector store configured).
    pub fn none() -> Self {
        Self { _exclusion: None }
    }
}

/// Whether `work` may run given the attempt record and the markers of
/// every configured table (spec §3.8 table).
pub fn decide(
    attempt: Option<&AttemptRecord>,
    markers: &[Option<MigrationMarker>],
    work: Work,
) -> Result<(), InProgress> {
    let Some(a) = attempt else {
        return if markers.iter().any(Option::is_some) {
            // Markers no attempt record accounts for.
            Err(InProgress {
                resolution: Resolution::ManualRecovery,
                attempt: None,
                backup: None,
                cancel_operation: None,
            })
        } else {
            Ok(())
        };
    };
    let Some(stage) = effective_stage(a, markers) else {
        return Ok(());
    };
    let before_commit = matches!(
        stage,
        EffectiveStage::Backup | EffectiveStage::Replace | EffectiveStage::RebuildPending
    );
    let tolerated = match work {
        Work::Apply { neutral } => neutral,
        Work::Restore { vector_tables } => !vector_tables,
    };
    if before_commit && tolerated {
        return Ok(());
    }
    match stage_response(a.method, stage, Operation::ApplyOrRestore) {
        Response::Refuse(r) => Err(InProgress {
            resolution: r.resolution,
            attempt: r.with_attempt.then(|| a.info(stage.as_str())),
            backup: if r.with_backup && a.backup_complete {
                a.backup_path.clone()
            } else {
                None
            },
            cancel_operation: r.cancel_operation,
        }),
        Response::Continue | Response::Succeed(_) => Ok(()),
    }
}

/// A backup's vector tables may replace the current ones only within one
/// space; unknown spaces (older backups, unrecorded tables) are not
/// compared.
pub fn backup_space_mismatch(backup: Option<&str>, current: Option<&str>) -> bool {
    matches!((backup, current), (Some(b), Some(c)) if b != c)
}

/// The space recorded on the configured vector tables when they agree.
pub async fn current_space() -> anyhow::Result<Option<String>> {
    let mut spaces = BTreeSet::new();
    for (_, table) in open_all(&stores_from_env()?).await? {
        if let Some(t) = table
            && let Some(space) = read_table_record(&t).await?.space
        {
            spaces.insert(space.space_id.to_string());
        }
    }
    Ok((spaces.len() == 1).then(|| spaces.into_iter().next().expect("one")))
}

/// Check and lock for `work`. `pool` is `None` when the database does not
/// exist yet (SQLite). `backup_space` is the space recorded in the backup
/// to restore (checked against the current tables).
pub async fn acquire(
    pool: Option<&RdbPool>,
    state_dir: &Path,
    work: Work,
    backup_space: Option<&str>,
) -> Result<Guard, GuardError> {
    let stores = stores_from_env().map_err(GuardError::Unavailable)?;
    if stores.is_empty() {
        // No vector store: nothing to protect, and no migration can exist.
        return Ok(Guard::none());
    }
    let tables = open_all(&stores).await.map_err(GuardError::Unavailable)?;
    if let Some(pool) = pool
        && storage_mismatch(pool, state_dir, &stores, StorageCheck::Conflicts)
            .await
            .map_err(GuardError::Unavailable)?
    {
        return Err(GuardError::StorageMismatch);
    }
    // No state directory means no embedding command ever ran, so no
    // migration exists; creating it here would be a side effect of an RDB
    // command. (An embedding command starting meanwhile is refused by the
    // writer check of the stores' owner instead.)
    if !state_dir.exists() {
        return Ok(Guard::none());
    }
    #[cfg(not(feature = "postgres"))]
    let locked = lock::acquire_file(state_dir);
    #[cfg(feature = "postgres")]
    let locked = match pool {
        Some(pool) => lock::acquire(pool, state_dir).await,
        None => Err(LockError::Other(anyhow::anyhow!(
            "the PostgreSQL guard needs a connection"
        ))),
    };
    let exclusion = match locked {
        Ok(e) if e.writer_present() => return Err(GuardError::Locked(ErrorCode::WriterActive)),
        Ok(e) => e,
        Err(LockError::Refused(code)) => return Err(GuardError::Locked(code)),
        Err(LockError::Other(e)) => return Err(GuardError::Unavailable(e)),
    };
    let attempt = match attempt::load(state_dir) {
        Ok(a) => a,
        Err(attempt::LoadError::UnknownFormat(_)) => return Err(GuardError::ToolUpdateRequired),
        Err(attempt::LoadError::Other(e)) => return Err(GuardError::Unavailable(e)),
    };
    let markers = markers_of(&tables).await.map_err(GuardError::Unavailable)?;
    decide(attempt.as_ref(), &markers, work).map_err(GuardError::InProgress)?;
    if let Work::Restore {
        vector_tables: true,
    } = work
    {
        let current = current_space().await.map_err(GuardError::Unavailable)?;
        if backup_space_mismatch(backup_space, current.as_deref()) {
            return Err(GuardError::BackupSpaceMismatch);
        }
    }
    Ok(Guard {
        _exclusion: Some(exclusion),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db_migrate::embedding::attempt::{
        CancelOperation, FORMAT_VERSION, Method, Stage, Status,
    };
    use infra::infra::embedding_space::{MarkerState, SpaceComponents};

    fn record(method: Method, stage: Stage, status: Status) -> AttemptRecord {
        AttemptRecord {
            format_version: FORMAT_VERSION,
            attempt_id: "att".into(),
            method,
            source_space_id: Some("s".into()),
            target_space_id: "t".into(),
            target_space: SpaceComponents {
                model_id: "m".into(),
                tokenizer_model_id: String::new(),
                revision: "r".into(),
                dimension: 4,
                distance: "cosine".into(),
            },
            backup_path: (method == Method::Backup).then(|| "/b".into()),
            backup_complete: method == Method::Backup,
            stage,
            status,
            cancel_operation: None,
            discarded: false,
            accepted_failed: None,
            backup_keep: None,
            started_at: 0,
        }
    }

    fn marker(state: MarkerState) -> Vec<Option<MigrationMarker>> {
        vec![Some(MigrationMarker {
            state,
            attempt_id: "att".into(),
        })]
    }

    const NEUTRAL: Work = Work::Apply { neutral: true };
    const NOT_NEUTRAL: Work = Work::Apply { neutral: false };
    const SQLITE_ONLY: Work = Work::Restore {
        vector_tables: false,
    };
    const WITH_VECTORS: Work = Work::Restore {
        vector_tables: true,
    };

    #[test]
    fn nothing_unfinished_allows_everything() {
        for work in [NEUTRAL, NOT_NEUTRAL, SQLITE_ONLY, WITH_VECTORS] {
            assert_eq!(decide(None, &[None], work), Ok(()));
            let done = record(Method::Backup, Stage::Commit, Status::Completed);
            assert_eq!(decide(Some(&done), &[None], work), Ok(()));
        }
    }

    #[test]
    fn neutral_work_and_sqlite_only_restores_run_before_the_commit() {
        let pending = record(Method::Backup, Stage::RebuildPending, Status::Running);
        let markers = marker(MarkerState::Pending);
        assert_eq!(decide(Some(&pending), &markers, NEUTRAL), Ok(()));
        assert_eq!(decide(Some(&pending), &markers, SQLITE_ONLY), Ok(()));
        let refused = decide(Some(&pending), &markers, NOT_NEUTRAL).unwrap_err();
        assert_eq!(refused.resolution, Resolution::ContinueOrRestore);
        assert_eq!(refused.attempt.unwrap().stage, "rebuild_pending");
        assert_eq!(refused.backup.as_deref(), Some("/b"));
        assert_eq!(
            decide(Some(&pending), &markers, WITH_VECTORS)
                .unwrap_err()
                .resolution,
            Resolution::ContinueOrRestore
        );
        let no_backup = record(Method::NoBackup, Stage::RebuildPending, Status::Running);
        let refused = decide(Some(&no_backup), &markers, NOT_NEUTRAL).unwrap_err();
        assert_eq!(
            (refused.resolution, refused.backup),
            (Resolution::ContinueOrAbandon, None)
        );
    }

    #[test]
    fn from_the_commit_on_every_work_is_refused() {
        let committing = record(Method::Backup, Stage::Commit, Status::Running);
        for work in [NEUTRAL, SQLITE_ONLY] {
            let r = decide(Some(&committing), &marker(MarkerState::Committing), work).unwrap_err();
            assert_eq!(r.resolution, Resolution::FinalizeRequired);
            assert_eq!(r.attempt.unwrap().stage, "commit");
        }
        let mut restoring = record(Method::Backup, Stage::Commit, Status::Running);
        restoring.cancel_operation = Some(CancelOperation::Restore);
        let r = decide(Some(&restoring), &marker(MarkerState::Restoring), NEUTRAL).unwrap_err();
        assert_eq!(r.resolution, Resolution::RestoreRequired);
        let mut discarding = record(Method::NoBackup, Stage::Replace, Status::Running);
        discarding.cancel_operation = Some(CancelOperation::Discard);
        let r = decide(
            Some(&discarding),
            &marker(MarkerState::Discarding),
            SQLITE_ONLY,
        )
        .unwrap_err();
        assert_eq!(r.resolution, Resolution::AbandonRequired);
    }

    #[test]
    fn markers_without_an_attempt_need_manual_recovery() {
        let r = decide(None, &marker(MarkerState::Pending), NEUTRAL).unwrap_err();
        assert_eq!(r.resolution, Resolution::ManualRecovery);
    }

    #[test]
    fn backup_space_is_compared_only_when_both_are_known() {
        assert!(backup_space_mismatch(Some("a"), Some("b")));
        assert!(!backup_space_mismatch(Some("a"), Some("a")));
        assert!(!backup_space_mismatch(None, Some("b")));
        assert!(!backup_space_mismatch(Some("a"), None));
    }
}

#[cfg(test)]
mod refused_tests {
    use super::*;

    #[test]
    fn refusals_carry_the_stage_table_fields() {
        let r = GuardError::InProgress(InProgress {
            resolution: Resolution::ContinueOrRestore,
            attempt: Some(AttemptInfo {
                attempt: "a1".into(),
                stage: "rebuild_pending".into(),
                backup_mode: "backup".into(),
            }),
            backup: Some("/b".into()),
            cancel_operation: None,
        })
        .refused();
        assert_eq!(
            (r.error_code, r.resolution, r.backup.as_deref()),
            (
                "embedding_migration_in_progress",
                "continue_or_restore",
                Some("/b")
            )
        );
        assert_eq!(
            GuardError::Locked(ErrorCode::WriterActive)
                .refused()
                .error_code,
            "writer_active"
        );
        assert_eq!(
            GuardError::Unavailable(anyhow::anyhow!("x"))
                .refused()
                .resolution,
            "check_environment"
        );
    }
}
