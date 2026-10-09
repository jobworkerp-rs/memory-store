//! Machine-readable output contract for desktop callers.
//!
//! Callers decide success, the user-facing message and recovery only from the
//! exit code and the final stdout line built here; every value is drawn from a
//! fixed vocabulary so that applications can localize by `error_code`.

use std::fmt;
use std::path::PathBuf;

macro_rules! vocabulary {
    ($(#[$meta:meta])* $name:ident { $first:ident => $first_text:literal $(, $variant:ident => $text:literal)* $(,)? }) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
        pub enum $name {
            #[default]
            $first,
            $($variant),*
        }

        impl $name {
            pub const ALL: &'static [Self] = &[Self::$first $(, Self::$variant)*];

            pub fn as_str(self) -> &'static str {
                match self {
                    Self::$first => $first_text,
                    $(Self::$variant => $text),*
                }
            }

            pub fn parse(text: &str) -> Option<Self> {
                Self::ALL.iter().copied().find(|value| value.as_str() == text)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(self.as_str())
            }
        }
    };
}

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
    }
);

vocabulary!(Outcome {
    NoOp => "no_op",
    Migrated => "migrated",
});

/// Non-final progress line; callers may show it but never decide on it.
pub fn progress_line(stage: Stage) -> String {
    format!("local_progress stage={stage}")
}

/// Percent-encode everything that could break `key=value` parsing or is not
/// printable ASCII; path separators stay readable.
pub fn encode_value(value: &str) -> String {
    value
        .bytes()
        .map(|byte| match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' | b'/' | b':' => {
                char::from(byte).to_string()
            }
            _ => format!("%{byte:02X}"),
        })
        .collect()
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
}

impl fmt::Display for FailureLine {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} status=failed stage={} error_code={} resolution={}",
            self.command, self.stage, self.error_code, self.resolution
        )?;
        write_backup(formatter, &self.backup)
    }
}

/// A classified failure. Components return it through `anyhow` so the command
/// can recover the classification with `downcast_ref`.
#[derive(Debug)]
pub struct LocalFailure {
    pub error_code: ErrorCode,
    pub resolution: Resolution,
    pub message: String,
}

impl LocalFailure {
    pub fn new(error_code: ErrorCode, resolution: Resolution, message: impl Into<String>) -> Self {
        Self {
            error_code,
            resolution,
            message: message.into(),
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
        let line = FailureLine {
            command: Command::Apply,
            stage: Stage::Backup,
            error_code: ErrorCode::InsufficientSpace,
            resolution: Resolution::Retry,
            backup: None,
        }
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
        let failure = FailureLine {
            command: Command::Restore,
            stage: Stage::RestoreValidate,
            error_code: ErrorCode::BackupIncomplete,
            resolution: Resolution::Retry,
            backup: Some("/b".into()),
        };
        assert_eq!(
            failure.to_string(),
            "local_restore status=failed stage=restore_validate error_code=backup_incomplete resolution=retry backup=/b"
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
