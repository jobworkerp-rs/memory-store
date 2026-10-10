//! Machine-readable output contract for desktop callers.
//!
//! Callers decide success, the user-facing message and recovery only from the
//! exit code and the final stdout line built here; every value is drawn from a
//! fixed vocabulary so that applications can localize by `error_code`.

use std::fmt;
use std::path::PathBuf;

pub use crate::db_migrate::vocabulary::encode_value;
use crate::db_migrate::vocabulary::vocabulary;

vocabulary!(Command {
    Apply => "local_apply",
    Restore => "local_restore",
});

vocabulary!(Stage {
    Preflight => "preflight",
    WriterCheck => "writer_check",
    Plan => "plan",
    Backup => "backup",
    Baseline => "baseline",
    SchemaApply => "schema_apply",
    PostMigrate => "post_migrate",
    Verify => "verify",
    Retention => "retention",
    RestoreValidate => "restore_validate",
    RestorePreflight => "restore_preflight",
    RestoreReplace => "restore_replace",
});

vocabulary!(ErrorCode {
    UnsupportedBackend => "unsupported_backend",
    BackupOptionRequired => "backup_option_required",
    InvalidTarget => "invalid_target",
    LegacySchemaUnsupported => "legacy_schema_unsupported",
    DbNewerThanTool => "db_newer_than_tool",
    IntegrityViolation => "integrity_violation",
    WriterActive => "writer_active",
    UnsupportedResource => "unsupported_resource",
    InsufficientSpace => "insufficient_space",
    BaselineSchemaMismatch => "baseline_schema_mismatch",
    RestoreRequired => "restore_required",
    RestoreRequiredNoBackup => "restore_required_no_backup",
    BackupIncomplete => "backup_incomplete",
    BackupNewerThanTool => "backup_newer_than_tool",
    MigrationFailed => "migration_failed",
    VerifyFailed => "verify_failed",
    BackupFailed => "backup_failed",
    RestoreFailed => "restore_failed",
    BundleInvalid => "bundle_invalid",
    // Refusals of the embedding migration guard (embedding space
    // management spec §3.8, §3.13).
    OperationInProgress => "operation_in_progress",
    StorageMismatch => "storage_mismatch",
    EmbeddingMigrationInProgress => "embedding_migration_in_progress",
    BackupSpaceMismatch => "backup_space_mismatch",
});

vocabulary!(
    /// What the caller may do after a failure.
    Resolution {
        Retry => "retry",
        RestoreRequired => "restore_required",
        ToolUpdateRequired => "tool_update_required",
        // The database predates what this release can adopt; an older
        // release must migrate it first.
        LegacyUpgradeRequired => "legacy_upgrade_required",
        CheckEnvironment => "check_environment",
        ManualRecovery => "manual_recovery",
        // The next steps of an unfinished embedding migration, with the
        // meaning of the embedding commands' resolution table.
        AbandonRequired => "abandon_required",
        ContinueOrRestore => "continue_or_restore",
        ContinueOrAbandon => "continue_or_abandon",
        FinalizeRequired => "finalize_required",
    }
);

/// The unfinished embedding migration a refusal refers to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbeddingAttempt {
    pub attempt: String,
    pub attempt_stage: String,
    pub backup_mode: String,
}

vocabulary!(Outcome {
    NoOp => "no_op",
    Migrated => "migrated",
});

/// Non-final progress line; callers may show it but never decide on it.
pub fn progress_line(stage: Stage) -> String {
    format!("local_progress stage={stage}")
}

fn write_backup(formatter: &mut fmt::Formatter<'_>, backup: &Option<PathBuf>) -> fmt::Result {
    match backup {
        Some(path) => write!(
            formatter,
            " backup={}",
            encode_value(&path.to_string_lossy())
        ),
        None => Ok(()),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SuccessLine {
    Apply {
        outcome: Outcome,
        backup: Option<PathBuf>,
    },
    Restore,
}

impl fmt::Display for SuccessLine {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Apply { outcome, backup } => {
                write!(
                    formatter,
                    "{} status=completed outcome={outcome}",
                    Command::Apply
                )?;
                write_backup(formatter, backup)
            }
            Self::Restore => write!(
                formatter,
                "{} status=completed next_action=apply",
                Command::Restore
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailureLine {
    pub command: Command,
    pub stage: Stage,
    pub error_code: ErrorCode,
    pub resolution: Resolution,
    pub backup: Option<PathBuf>,
    pub embedding_attempt: Option<EmbeddingAttempt>,
    pub cancel_operation: Option<&'static str>,
}

impl FailureLine {
    pub fn new(
        command: Command,
        stage: Stage,
        error_code: ErrorCode,
        resolution: Resolution,
        backup: Option<PathBuf>,
    ) -> Self {
        Self {
            command,
            stage,
            error_code,
            resolution,
            backup,
            embedding_attempt: None,
            cancel_operation: None,
        }
    }
}

impl FailureLine {
    /// Add the embedding fields a classified failure carries.
    pub fn with_embedding_fields(mut self, failure: Option<&LocalFailure>) -> Self {
        if let Some(f) = failure {
            self.embedding_attempt = f.embedding_attempt.clone();
            self.cancel_operation = f.cancel_operation;
        }
        self
    }
}

impl fmt::Display for FailureLine {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} status=failed stage={} error_code={} resolution={}",
            self.command, self.stage, self.error_code, self.resolution
        )?;
        if let Some(a) = &self.embedding_attempt {
            write!(
                formatter,
                " attempt={} attempt_stage={} backup_mode={}",
                encode_value(&a.attempt),
                a.attempt_stage,
                a.backup_mode
            )?;
        }
        write_backup(formatter, &self.backup)?;
        if let Some(op) = self.cancel_operation {
            write!(formatter, " cancel_operation={op}")?;
        }
        Ok(())
    }
}

/// A classified failure. Components return it through `anyhow` so the command
/// can recover the classification with `downcast_ref`.
#[derive(Debug)]
pub struct LocalFailure {
    pub error_code: ErrorCode,
    pub resolution: Resolution,
    pub message: String,
    /// Backup to report instead of the attempt's own (an embedding backup
    /// for refusals of the embedding guard).
    pub backup: Option<PathBuf>,
    pub embedding_attempt: Option<EmbeddingAttempt>,
    pub cancel_operation: Option<&'static str>,
}

impl LocalFailure {
    pub fn new(error_code: ErrorCode, resolution: Resolution, message: impl Into<String>) -> Self {
        Self {
            error_code,
            resolution,
            message: message.into(),
            backup: None,
            embedding_attempt: None,
            cancel_operation: None,
        }
    }
}

impl fmt::Display for LocalFailure {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.error_code, self.message)
    }
}

impl std::error::Error for LocalFailure {}

/// Keep a classification made closer to the failure; otherwise classify it.
pub fn classify(
    error: anyhow::Error,
    error_code: ErrorCode,
    resolution: Resolution,
) -> anyhow::Error {
    if error.downcast_ref::<LocalFailure>().is_some() {
        return error;
    }
    LocalFailure::new(error_code, resolution, format!("{error:#}")).into()
}

/// Shorthand for returning a classified failure.
pub fn fail<T>(
    error_code: ErrorCode,
    resolution: Resolution,
    message: impl Into<String>,
) -> anyhow::Result<T> {
    Err(LocalFailure::new(error_code, resolution, message).into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failure_line_uses_fixed_vocabulary() {
        let line = FailureLine::new(
            Command::Apply,
            Stage::Backup,
            ErrorCode::InsufficientSpace,
            Resolution::Retry,
            None,
        )
        .to_string();
        assert_eq!(
            line,
            "local_apply status=failed stage=backup error_code=insufficient_space resolution=retry"
        );
    }

    #[test]
    fn values_with_spaces_percent_equals_and_non_ascii_are_encoded() {
        let line = SuccessLine::Apply {
            outcome: Outcome::Migrated,
            backup: Some("/Users/a b/100%=ok/バックアップ".into()),
        }
        .to_string();
        assert_eq!(
            line,
            "local_apply status=completed outcome=migrated \
             backup=/Users/a%20b/100%25%3Dok/%E3%83%90%E3%83%83%E3%82%AF%E3%82%A2%E3%83%83%E3%83%97"
        );
    }

    #[test]
    fn restore_lines_report_the_next_action_and_restore_stages() {
        assert_eq!(
            SuccessLine::Restore.to_string(),
            "local_restore status=completed next_action=apply"
        );
        let failure = FailureLine::new(
            Command::Restore,
            Stage::RestoreValidate,
            ErrorCode::BackupIncomplete,
            Resolution::Retry,
            Some("/b".into()),
        );
        assert_eq!(
            failure.to_string(),
            "local_restore status=failed stage=restore_validate error_code=backup_incomplete resolution=retry backup=/b"
        );
    }

    #[test]
    fn embedding_refusals_add_the_attempt_fields_in_contract_order() {
        let mut line = FailureLine::new(
            Command::Apply,
            Stage::Preflight,
            ErrorCode::EmbeddingMigrationInProgress,
            Resolution::RestoreRequired,
            Some("/e b".into()),
        );
        line.embedding_attempt = Some(EmbeddingAttempt {
            attempt: "a1".into(),
            attempt_stage: "restoring".into(),
            backup_mode: "backup".into(),
        });
        line.cancel_operation = Some("restore");
        assert_eq!(
            line.to_string(),
            "local_apply status=failed stage=preflight error_code=embedding_migration_in_progress \
             resolution=restore_required attempt=a1 attempt_stage=restoring backup_mode=backup \
             backup=/e%20b cancel_operation=restore"
        );
    }

    #[test]
    fn progress_line_names_the_stage_and_is_distinct_from_the_result_line() {
        assert_eq!(
            progress_line(Stage::PostMigrate),
            "local_progress stage=post_migrate"
        );
    }

    #[test]
    fn version_and_legacy_failures_have_their_own_vocabulary() {
        assert_eq!(ErrorCode::DbNewerThanTool.as_str(), "db_newer_than_tool");
        assert_eq!(
            ErrorCode::LegacySchemaUnsupported.as_str(),
            "legacy_schema_unsupported"
        );
        assert_eq!(
            Resolution::LegacyUpgradeRequired.as_str(),
            "legacy_upgrade_required"
        );
    }

    #[test]
    fn every_error_code_has_a_distinct_snake_case_name() {
        let names = ErrorCode::ALL
            .iter()
            .map(|code| code.as_str())
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(names.len(), ErrorCode::ALL.len());
        assert!(names.iter().all(|name| {
            name.bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte == b'_')
        }));
    }
}
