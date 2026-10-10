//! Machine-readable output of `memories-db-migrate embedding`. Callers
//! decide only from the exit code and the final stdout line; every value
//! is drawn from a fixed vocabulary (spec §3.9, §3.10).

use crate::db_migrate::vocabulary::{encode_value, vocabulary};
use std::fmt::{self, Write as _};

vocabulary!(Command {
    Inspect => "embedding_inspect",
    Plan => "embedding_plan",
    Switch => "embedding_switch",
    Abandon => "embedding_abandon",
    Finalize => "embedding_finalize",
    Restore => "embedding_restore",
});

vocabulary!(State {
    Consistent => "consistent",
    Incomplete => "incomplete",
    Unverified => "unverified",
    Failed => "failed",
    Unknown => "unknown",
    Migrating => "migrating",
    RebuildInconsistent => "rebuild_inconsistent",
    Unavailable => "unavailable",
});

vocabulary!(UnavailableReason {
    ResourceUnavailable => "resource_unavailable",
    ResourceCorrupt => "resource_corrupt",
});

vocabulary!(NextAction {
    None => "none",
    Wait => "wait",
    Reconcile => "reconcile",
    ResolveFailed => "resolve_failed",
    Switch => "switch",
    CheckEnvironment => "check_environment",
    ManualRecovery => "manual_recovery",
    Restore => "restore",
    Abandon => "abandon",
    ResumeOrAbandon => "resume_or_abandon",
    ResumeOrRestore => "resume_or_restore",
    StartRebuild => "start_rebuild",
    Finalize => "finalize",
    Apply => "apply",
});

vocabulary!(Decision {
    NoChange => "no_change",
    ReconcileRequired => "reconcile_required",
    AdoptOnStart => "adopt_on_start",
    ReembedRequired => "reembed_required",
});

vocabulary!(Stage {
    Open => "open",
    Preflight => "preflight",
    Record => "record",
    Backup => "backup",
    Replace => "replace",
    Cleanup => "cleanup",
    Discard => "discard",
    Verify => "verify",
    Commit => "commit",
    Retention => "retention",
    Validate => "validate",
});

vocabulary!(ErrorCode {
    StorageMismatch => "storage_mismatch",
    OperationInProgress => "operation_in_progress",
    WriterActive => "writer_active",
    EmbeddingSwitchInProgress => "embedding_switch_in_progress",
    FinalizeInProgress => "finalize_in_progress",
    CancelInProgress => "cancel_in_progress",
    ApplyRequired => "apply_required",
    ApplyRestoreRequired => "apply_restore_required",
    SpaceChanged => "space_changed",
    NoReembedNeeded => "no_reembed_needed",
    BackupOptionRequired => "backup_option_required",
    InsufficientSpace => "insufficient_space",
    UnsupportedResource => "unsupported_resource",
    ResourceNotWritable => "resource_not_writable",
    ResourceUnavailable => "resource_unavailable",
    ResourceCorrupt => "resource_corrupt",
    SwitchStateUnverifiable => "switch_state_unverifiable",
    AbandonNotAllowed => "abandon_not_allowed",
    RestoreNotAvailable => "restore_not_available",
    AttemptNotFound => "attempt_not_found",
    AttemptFinished => "attempt_finished",
    RebuildNotPending => "rebuild_not_pending",
    RebuildIncomplete => "rebuild_incomplete",
    RebuildHasFailures => "rebuild_has_failures",
    DbNewerThanTool => "db_newer_than_tool",
    ToolUpdateRequired => "tool_update_required",
});

vocabulary!(Resolution {
    Retry => "retry",
    PlanRequired => "plan_required",
    CheckEnvironment => "check_environment",
    ManualRecovery => "manual_recovery",
    ToolUpdateRequired => "tool_update_required",
    ResumeOrAbandon => "resume_or_abandon",
    AbandonRequired => "abandon_required",
    ResumeOrRestore => "resume_or_restore",
    ContinueOrRestore => "continue_or_restore",
    ContinueOrAbandon => "continue_or_abandon",
    ResumeRebuild => "resume_rebuild",
    ResolveFailures => "resolve_failures",
    FinalizeRequired => "finalize_required",
    RestoreRequired => "restore_required",
    ApplyRequired => "apply_required",
    ApplyRestoreRequired => "apply_restore_required",
});

/// A count that may not have been observable.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Count {
    #[default]
    Unknown,
    Known(u64),
}

impl fmt::Display for Count {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unknown => f.write_str("unknown"),
            Self::Known(n) => write!(f, "{n}"),
        }
    }
}

/// `space=` value: an ID, `unknown` (cannot be determined), or `none`
/// (nothing recorded and every table empty).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum SpaceValue {
    #[default]
    Unknown,
    None,
    Id(String),
}

impl fmt::Display for SpaceValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unknown => f.write_str("unknown"),
            Self::None => f.write_str("none"),
            Self::Id(id) => f.write_str(&encode_value(id)),
        }
    }
}

/// Unfinished attempt details printed by `inspect` and failure lines.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttemptInfo {
    pub attempt: String,
    pub stage: String,
    /// `backup` or `none`.
    pub backup_mode: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct InspectLine {
    pub state: State,
    pub space: SpaceValue,
    pub required: Count,
    pub complete: Count,
    pub missing: Count,
    pub stale: Count,
    pub unverified: Count,
    pub failed_transient: Count,
    pub failed_permanent: Count,
    pub orphan: Count,
    pub backup_supported: Option<bool>,
    pub attempt: Option<AttemptInfo>,
    pub backup: Option<String>,
    pub unavailable_reason: Option<UnavailableReason>,
    pub next_action: NextAction,
}

impl fmt::Display for InspectLine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} status=completed state={} space={} required={} complete={} missing={} stale={} \
             unverified={} failed_transient={} failed_permanent={} orphan={} backup_supported={}",
            Command::Inspect,
            self.state,
            self.space,
            self.required,
            self.complete,
            self.missing,
            self.stale,
            self.unverified,
            self.failed_transient,
            self.failed_permanent,
            self.orphan,
            match self.backup_supported {
                Some(true) => "true",
                Some(false) => "false",
                None => "unknown",
            }
        )?;
        if let Some(a) = &self.attempt {
            write!(
                f,
                " attempt={} attempt_stage={} backup_mode={}",
                encode_value(&a.attempt),
                a.stage,
                a.backup_mode
            )?;
        }
        if let Some(b) = &self.backup {
            write!(f, " backup={}", encode_value(b))?;
        }
        if let Some(r) = self.unavailable_reason {
            write!(f, " unavailable_reason={r}")?;
        }
        write!(f, " next_action={}", self.next_action)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanLine {
    pub decision: Decision,
    pub executable: bool,
    pub reason: Option<ErrorCode>,
    pub current_space: SpaceValue,
    pub target_space: String,
    pub memory_text: u64,
    pub memory_media: u64,
    pub thread: u64,
    pub reflection_intent: u64,
    pub failed_transient: Count,
    pub failed_permanent: Count,
}

impl fmt::Display for PlanLine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} status=completed decision={} executable={}",
            Command::Plan,
            self.decision,
            self.executable
        )?;
        if let Some(r) = self.reason {
            write!(f, " reason={r}")?;
        }
        write!(
            f,
            " current_space={} target_space={} memory_text={} memory_media={} thread={} \
             reflection_intent={} failed_transient={} failed_permanent={}",
            self.current_space,
            encode_value(&self.target_space),
            self.memory_text,
            self.memory_media,
            self.thread,
            self.reflection_intent,
            self.failed_transient,
            self.failed_permanent
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FailureLine {
    pub command: Command,
    pub stage: Stage,
    pub error_code: ErrorCode,
    pub resolution: Resolution,
    pub attempt: Option<AttemptInfo>,
    pub backup: Option<String>,
    pub cancel_operation: Option<&'static str>,
    /// Extra `key=value` pairs (e.g. per-kind counts of `finalize`).
    pub extra: Vec<(String, String)>,
}

impl FailureLine {
    pub fn new(
        command: Command,
        stage: Stage,
        error_code: ErrorCode,
        resolution: Resolution,
    ) -> Self {
        Self {
            command,
            stage,
            error_code,
            resolution,
            attempt: None,
            backup: None,
            cancel_operation: None,
            extra: Vec::new(),
        }
    }
}

impl fmt::Display for FailureLine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut line = format!(
            "{} status=failed stage={} error_code={} resolution={}",
            self.command, self.stage, self.error_code, self.resolution
        );
        if let Some(a) = &self.attempt {
            let _ = write!(
                line,
                " attempt={} attempt_stage={} backup_mode={}",
                encode_value(&a.attempt),
                a.stage,
                a.backup_mode
            );
        }
        if let Some(b) = &self.backup {
            let _ = write!(line, " backup={}", encode_value(b));
        }
        if let Some(op) = self.cancel_operation {
            let _ = write!(line, " cancel_operation={op}");
        }
        for (k, v) in &self.extra {
            let _ = write!(line, " {k}={}", encode_value(v));
        }
        f.write_str(&line)
    }
}

/// Non-final progress line.
pub fn progress_line(stage: Stage) -> String {
    format!("embedding_progress stage={stage}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inspect_line_has_every_required_field_in_order() {
        let line = InspectLine {
            state: State::Incomplete,
            space: SpaceValue::Id("ab12".into()),
            required: Count::Known(10),
            complete: Count::Known(7),
            missing: Count::Known(1),
            stale: Count::Known(1),
            unverified: Count::Known(0),
            failed_transient: Count::Known(1),
            failed_permanent: Count::Known(0),
            orphan: Count::Known(2),
            backup_supported: Some(true),
            next_action: NextAction::Reconcile,
            ..Default::default()
        };
        assert_eq!(
            line.to_string(),
            "embedding_inspect status=completed state=incomplete space=ab12 required=10 \
             complete=7 missing=1 stale=1 unverified=0 failed_transient=1 failed_permanent=0 \
             orphan=2 backup_supported=true next_action=reconcile"
        );
    }

    #[test]
    fn unobservable_values_are_unknown_not_zero() {
        let line = InspectLine {
            state: State::Unavailable,
            unavailable_reason: Some(UnavailableReason::ResourceUnavailable),
            next_action: NextAction::CheckEnvironment,
            ..Default::default()
        }
        .to_string();
        assert!(line.contains("space=unknown required=unknown complete=unknown"));
        assert!(line.contains("backup_supported=unknown"));
        assert!(
            line.ends_with("unavailable_reason=resource_unavailable next_action=check_environment")
        );
    }

    #[test]
    fn plan_line_shape() {
        let line = PlanLine {
            decision: Decision::ReembedRequired,
            executable: true,
            reason: None,
            current_space: SpaceValue::None,
            target_space: "ff".into(),
            memory_text: 3,
            memory_media: 1,
            thread: 2,
            reflection_intent: 0,
            failed_transient: Count::Known(0),
            failed_permanent: Count::Unknown,
        };
        assert_eq!(
            line.to_string(),
            "embedding_plan status=completed decision=reembed_required executable=true \
             current_space=none target_space=ff memory_text=3 memory_media=1 thread=2 \
             reflection_intent=0 failed_transient=0 failed_permanent=unknown"
        );
    }

    #[test]
    fn failure_line_with_attempt_and_backup() {
        let mut line = FailureLine::new(
            Command::Finalize,
            Stage::Verify,
            ErrorCode::RebuildIncomplete,
            Resolution::ResumeRebuild,
        );
        line.attempt = Some(AttemptInfo {
            attempt: "a1".into(),
            stage: "rebuild_pending".into(),
            backup_mode: "backup".into(),
        });
        line.backup = Some("/b k".into());
        line.extra.push(("missing_text".into(), "3".into()));
        assert_eq!(
            line.to_string(),
            "embedding_finalize status=failed stage=verify error_code=rebuild_incomplete \
             resolution=resume_rebuild attempt=a1 attempt_stage=rebuild_pending backup_mode=backup \
             backup=/b%20k missing_text=3"
        );
    }

    #[test]
    fn vocabularies_are_distinct_snake_case() {
        fn check(names: Vec<&'static str>) {
            let set: std::collections::BTreeSet<_> = names.iter().collect();
            assert_eq!(set.len(), names.len());
            assert!(
                names
                    .iter()
                    .all(|n| n.bytes().all(|b| b.is_ascii_lowercase() || b == b'_'))
            );
        }
        check(ErrorCode::ALL.iter().map(|v| v.as_str()).collect());
        check(Resolution::ALL.iter().map(|v| v.as_str()).collect());
        check(NextAction::ALL.iter().map(|v| v.as_str()).collect());
        check(State::ALL.iter().map(|v| v.as_str()).collect());
    }
}
