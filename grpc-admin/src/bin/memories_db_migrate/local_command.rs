//! `local apply` / `local restore`: the single entry point for desktop
//! applications that own a local SQLite database.
//!
//! The caller only stops every writer, runs one command, and reads the exit
//! code and the final stdout line; this module decides the steps, the backup,
//! and whether a failure may be retried.

use super::{
    PostMigrateCommand, PostMigrateRunArgs, SchemaState, atlas_artifact_root,
    bundle_command::running_bundle_digest, current_schema_contract_version,
    latest_migration_version, migration_database_url, pending_required_tasks,
    pending_work_is_embedding_neutral, required_tasks_left_to_run, run_apply, run_baseline,
    run_post_migrate, run_verify, schema_state, selected_tasks_for_schema_version,
    verify_atlas_sum,
};
use anyhow::{Context, Result};
use clap::{Args, Subcommand};
use grpc_admin::db_migrate::{
    catalog,
    embedding::guard::{self as embedding_guard, GuardError, Work},
    local::{
        attempt::{AttemptRecord, AttemptStatus},
        backup::{BackupInfo, BackupManifest, apply_retention, create_backup},
        output::{
            Command, ErrorCode, FailureLine, LocalFailure, Outcome, Resolution, Stage, SuccessLine,
            classify, fail, progress_line,
        },
        preflight::{check_memory_kind_era, check_thread_group_references},
        resources::backup_resources,
        restore::{interrupted_restore, restore_backup},
        target::{SqliteTarget, TargetFiles},
        writer::ensure_no_other_connection,
    },
    task_resources,
};
use infra_utils::infra::rdb::RdbPool;
use std::path::PathBuf;

const DEFAULT_BACKUP_KEEP: usize = 1;

#[derive(Debug, Subcommand)]
pub(super) enum LocalCommand {
    Apply(LocalApplyArgs),
    Restore(LocalRestoreArgs),
}

#[derive(Debug, Args)]
pub(super) struct LocalApplyArgs {
    #[arg(long, required = true)]
    maintenance_window_ack: bool,
    /// Parent directory for the pre-migration backup.
    #[arg(long)]
    backup_dir: Option<PathBuf>,
    /// Complete backups to keep after a successful migration.
    #[arg(long)]
    backup_keep: Option<usize>,
    /// Migrate without any backup; a failure may then be unrecoverable.
    #[arg(long)]
    no_backup_unsafe: bool,
}

#[derive(Debug, Args)]
pub(super) struct LocalRestoreArgs {
    #[arg(long, required = true)]
    maintenance_window_ack: bool,
    #[arg(long)]
    backup: PathBuf,
}

/// Run a local command, print its structured result, and return the exit code.
pub(super) async fn run_local(command: LocalCommand) -> i32 {
    let (line, error) = match command {
        LocalCommand::Apply(args) => {
            let mut progress = Progress::default();
            match apply(&args, &mut progress).await {
                Ok(success) => (success.to_string(), None),
                Err(error) => (progress.failure_line(&error).await.to_string(), Some(error)),
            }
        }
        LocalCommand::Restore(args) => match restore(&args).await {
            Ok(()) => (SuccessLine::Restore.to_string(), None),
            Err(error) => (restore_failure_line(&args, &error).to_string(), Some(error)),
        },
    };
    if let Some(error) = &error {
        eprintln!("{error:#}");
    }
    println!("{line}");
    i32::from(error.is_some())
}

/// The selected backup behaviour; exactly one must be requested explicitly.
#[derive(Debug, Clone, PartialEq, Eq)]
enum BackupPolicy {
    Keep { parent: PathBuf, keep: usize },
    Disabled,
}

fn backup_policy(args: &LocalApplyArgs) -> Result<BackupPolicy> {
    match (&args.backup_dir, args.no_backup_unsafe, args.backup_keep) {
        (Some(parent), false, keep) => Ok(BackupPolicy::Keep {
            parent: parent.clone(),
            keep: keep.unwrap_or(DEFAULT_BACKUP_KEEP),
        }),
        (None, true, None) => Ok(BackupPolicy::Disabled),
        _ => fail(
            ErrorCode::BackupOptionRequired,
            Resolution::ToolUpdateRequired,
            "specify exactly one of --backup-dir [--backup-keep N] or --no-backup-unsafe",
        ),
    }
}

fn sqlite_target() -> Result<SqliteTarget> {
    let url = migration_database_url()?;
    if !url.starts_with("sqlite:") {
        return fail(
            ErrorCode::UnsupportedBackend,
            Resolution::ToolUpdateRequired,
            "local commands support only SQLite databases",
        );
    }
    SqliteTarget::from_url(&url)
}

fn bundle_invalid(error: anyhow::Error) -> anyhow::Error {
    classify(
        error,
        ErrorCode::BundleInvalid,
        Resolution::ToolUpdateRequired,
    )
}

/// What the database needs, determined before anything changes.
struct Plan {
    state: SchemaState,
    contract_version: Option<String>,
    schema_up_to_date: bool,
    /// Required tasks that are not completed with the current definition.
    pending_tasks: Vec<catalog::TaskCatalogEntry>,
}

impl Plan {
    fn has_work(&self) -> bool {
        !self.schema_up_to_date || !self.pending_tasks.is_empty()
    }
}

/// Where an apply stands; the attempt record is the durable copy of it.
#[derive(Default)]
struct Progress {
    stage: Stage,
    target: Option<SqliteTarget>,
    attempt: Option<AttemptRecord>,
    /// Backup to report when this run refuses because of an earlier one.
    report_backup: Option<PathBuf>,
}

impl Progress {
    fn enter(&mut self, stage: Stage) -> Result<()> {
        self.stage = stage;
        println!("{}", progress_line(stage));
        if let (Some(target), Some(attempt)) = (&self.target, &mut self.attempt) {
            attempt.stage = stage;
            attempt.store(target)?;
        }
        Ok(())
    }

    /// Classify a failure and persist the classification for the next run.
    async fn failure_line(&mut self, error: &anyhow::Error) -> FailureLine {
        let failure = error.downcast_ref::<LocalFailure>();
        let (error_code, mut resolution) = match failure {
            Some(failure) => (failure.error_code, failure.resolution),
            None => default_classification(self.stage),
        };
        let changed_data = matches!(
            self.stage,
            Stage::Baseline | Stage::SchemaApply | Stage::PostMigrate | Stage::Verify
        );
        if changed_data
            && let Some(target) = &self.target
            && schema_is_corrupt(target).await
        {
            resolution = Resolution::RestoreRequired;
        }
        if let (Some(target), Some(attempt)) = (&self.target, &mut self.attempt) {
            attempt.status = AttemptStatus::Failed(resolution);
            if let Err(store_error) = attempt.store(target) {
                eprintln!("warning: could not record the failed attempt: {store_error:#}");
            }
        }
        let line = FailureLine::new(
            Command::Apply,
            self.stage,
            error_code,
            resolution,
            failure
                .and_then(|f| f.backup.clone())
                .or_else(|| {
                    self.attempt
                        .as_ref()
                        .and_then(|attempt| attempt.backup.clone())
                })
                .or_else(|| self.report_backup.clone()),
        );
        line.with_embedding_fields(failure)
    }
}

/// A refusal of the embedding guard as a classified local failure.
fn guard_failure(error: GuardError) -> anyhow::Error {
    use grpc_admin::db_migrate::local::output::EmbeddingAttempt;
    let refused = error.refused();
    // Every word of `refused` is in the local vocabulary.
    let mut failure = LocalFailure::new(
        ErrorCode::parse(refused.error_code).unwrap_or(ErrorCode::InvalidTarget),
        Resolution::parse(refused.resolution).unwrap_or(Resolution::ManualRecovery),
        error.to_string(),
    );
    failure.backup = refused.backup.map(PathBuf::from);
    failure.embedding_attempt = refused.attempt.map(|a| EmbeddingAttempt {
        attempt: a.attempt,
        attempt_stage: a.stage,
        backup_mode: a.backup_mode,
    });
    failure.cancel_operation = refused.cancel_operation;
    failure.into()
}

/// Take the embedding guard for `work`; the database is opened only to
/// read its storage identifier and closed again, since the writer checks
/// must not see this process's connection.
async fn guard_embedding(
    target: &SqliteTarget,
    work: Work,
    backup_space: Option<&str>,
) -> Result<embedding_guard::Guard> {
    // Without a vector store there is nothing to guard; the database is
    // then left alone (a damaged one must stay restorable).
    if grpc_admin::db_migrate::embedding::observe::stores_from_env()
        .map_err(|e| guard_failure(GuardError::Unavailable(e)))?
        .is_empty()
    {
        return Ok(embedding_guard::Guard::none());
    }
    // A database that cannot be opened only skips the identifier check.
    let pool = if target.database().exists() {
        open_existing(target).await.ok()
    } else {
        None
    };
    let state_dir = infra::infra::embedding_space::storage::state_dir_from_env();
    let guarded = embedding_guard::acquire(pool.as_ref(), &state_dir, work, backup_space).await;
    if let Some(pool) = pool {
        pool.close().await;
    }
    guarded.map_err(guard_failure)
}

async fn apply(args: &LocalApplyArgs, progress: &mut Progress) -> Result<SuccessLine> {
    progress.enter(Stage::Preflight)?;
    let policy = backup_policy(args)?;
    let target = sqlite_target()?;
    progress.target = Some(target.clone());
    let files = target.inspect_files()?;
    refuse_pending_restore(&target, files, progress)?;
    let artifact_root = atlas_artifact_root().map_err(bundle_invalid)?;
    catalog::load_catalog().map_err(bundle_invalid)?;
    verify_atlas_sum(&artifact_root, "sqlite").map_err(bundle_invalid)?;
    let latest_version =
        latest_migration_version(&artifact_root, "sqlite").map_err(bundle_invalid)?;
    if files == TargetFiles::Present {
        // Closed again before the writer check, which must not see this
        // process's own connection.
        let pool = open_existing(&target).await?;
        let checked = check_memory_kind_era(&pool).await;
        pool.close().await;
        checked?;
    }

    progress.enter(Stage::WriterCheck)?;
    ensure_no_other_connection(&target)?;

    progress.enter(Stage::Plan)?;
    let plan = match files {
        TargetFiles::Missing => Plan {
            state: SchemaState::Uninitialized,
            contract_version: None,
            schema_up_to_date: false,
            pending_tasks: selected_required_tasks(&latest_version)?,
        },
        TargetFiles::Present => {
            let pool = open_existing(&target).await?;
            let planned = plan_present(&pool, &latest_version, progress).await;
            pool.close().await;
            planned?
        }
    };
    if !plan.has_work() {
        mark_recovered(&target)?;
        return Ok(SuccessLine::Apply {
            outcome: Outcome::NoOp,
            backup: None,
        });
    }

    if progress.stage != Stage::Preflight {
        progress.enter(Stage::Preflight)?;
    }
    let neutral = match files {
        TargetFiles::Present => {
            let pool = open_existing(&target).await?;
            let to_run =
                required_tasks_left_to_run(&pool, plan.state, &latest_version, "sqlite").await;
            pool.close().await;
            pending_work_is_embedding_neutral(plan.state, &to_run?)
        }
        TargetFiles::Missing => false,
    };
    let _embedding_guard = guard_embedding(&target, Work::Apply { neutral }, None).await?;

    let started_at = command_utils::util::datetime::now_millis();
    let bundle_digest = running_bundle_digest();
    progress.attempt = Some(AttemptRecord {
        attempt_id: format!("{started_at}-{}", std::process::id()),
        started_at,
        bundle_digest: bundle_digest.clone(),
        backup: None,
        stage: Stage::Backup,
        status: AttemptStatus::Running,
    });
    progress.enter(Stage::Backup)?;
    if let BackupPolicy::Keep { parent, .. } = &policy
        && plan.state != SchemaState::Uninitialized
    {
        ensure_no_other_connection(&target)?;
        let resources = plan
            .pending_tasks
            .iter()
            .flat_map(|task| task_resources(&task.implementation).iter().copied())
            .collect::<Vec<_>>();
        let resources = backup_resources(&resources, |key| std::env::var(key).ok())?;
        let embedding_space_id = if resources.is_empty() {
            None
        } else {
            embedding_guard::current_space().await.ok().flatten()
        };
        let info = BackupInfo {
            schema_status: plan.state.as_str().to_string(),
            schema_version: plan.contract_version.clone(),
            bundle_digest,
            embedding_space_id,
        };
        let backup = create_backup(parent, &target, &resources, &info).await?;
        if let Some(attempt) = &mut progress.attempt {
            attempt.backup = Some(backup);
        }
    }

    progress.enter(Stage::Baseline)?;
    if plan.state == SchemaState::BaselineRequired {
        run_baseline().await?;
    }
    progress.enter(Stage::SchemaApply)?;
    if matches!(
        plan.state,
        SchemaState::Uninitialized | SchemaState::Pending { .. }
    ) {
        run_apply(false).await?;
    }
    progress.enter(Stage::PostMigrate)?;
    run_post_migrate(PostMigrateCommand::Run(PostMigrateRunArgs {
        id: None,
        generation: None,
        all_required: true,
        dry_run: false,
        maintenance_window_ack: true,
    }))
    .await?;
    progress.enter(Stage::Verify)?;
    run_verify(None).await?;
    run_post_migrate(PostMigrateCommand::Verify).await?;

    let mut backup = None;
    if let Some(attempt) = &mut progress.attempt {
        attempt.status = AttemptStatus::Succeeded;
        attempt.store(&target)?;
        backup = attempt.backup.clone();
    }
    if let BackupPolicy::Keep { parent, keep } = &policy {
        // A failed cleanup must not turn a completed migration into a failure.
        match apply_retention(parent, *keep) {
            Ok(removed) => backup = backup.filter(|path| !removed.contains(path)),
            Err(error) => eprintln!("warning: backup retention failed: {error:#}"),
        }
    }
    Ok(SuccessLine::Apply {
        outcome: Outcome::Migrated,
        backup,
    })
}

/// Plan an existing database. The reference checks are preflight checks
/// deferred until the plan shows a change: a run that changes nothing must
/// stay cheap.
async fn plan_present(
    pool: &RdbPool,
    latest_version: &str,
    progress: &mut Progress,
) -> Result<Plan> {
    let plan = plan_existing(pool, latest_version).await?;
    if plan.has_work() {
        progress.enter(Stage::Preflight)?;
        check_thread_group_references(pool).await?;
    }
    Ok(plan)
}

async fn plan_existing(pool: &RdbPool, latest_version: &str) -> Result<Plan> {
    let state = schema_state(pool).await?;
    match state {
        SchemaState::SchemaCorrupt => {
            return fail(
                ErrorCode::RestoreRequired,
                Resolution::RestoreRequired,
                "schema history, contract and task state are inconsistent",
            );
        }
        // Not damage: a newer release owns this database, so no record may
        // demand a restore that would discard its newer data.
        SchemaState::NewerThanTool => {
            return fail(
                ErrorCode::DbNewerThanTool,
                Resolution::ToolUpdateRequired,
                "a newer release has migrated this database",
            );
        }
        _ => {}
    }
    let contract_version = match state {
        SchemaState::Managed | SchemaState::Pending { .. } => {
            Some(current_schema_contract_version(pool).await?)
        }
        _ => None,
    };
    let pending_tasks = pending_required_tasks(pool, state, latest_version, "sqlite").await?;
    Ok(Plan {
        schema_up_to_date: state == SchemaState::Managed
            && contract_version.as_deref() == Some(latest_version),
        state,
        contract_version,
        pending_tasks,
    })
}

/// Classification of failures that components did not classify themselves.
fn default_classification(stage: Stage) -> (ErrorCode, Resolution) {
    match stage {
        Stage::Backup => (ErrorCode::BackupFailed, Resolution::Retry),
        // Data that passed every task but fails verification needs a fix in
        // memories; repeating the same release cannot succeed.
        Stage::Verify => (ErrorCode::VerifyFailed, Resolution::ToolUpdateRequired),
        _ => (ErrorCode::MigrationFailed, Resolution::Retry),
    }
}

async fn schema_is_corrupt(target: &SqliteTarget) -> bool {
    let Ok(pool) = open_existing(target).await else {
        return false;
    };
    let corrupt = matches!(schema_state(&pool).await, Ok(SchemaState::SchemaCorrupt));
    pool.close().await;
    corrupt
}

fn selected_required_tasks(latest_version: &str) -> Result<Vec<catalog::TaskCatalogEntry>> {
    Ok(selected_tasks_for_schema_version(latest_version, "sqlite")?
        .into_iter()
        .filter(|task| task.completion_required_by_schema_version.is_some())
        .collect())
}

/// Refuse to touch a database whose last attempt requires a restore, or whose
/// restore was interrupted, until `local restore` completes. The backup to
/// restore is reported without rewriting the record that requires it.
///
/// A record about a database that no longer exists belongs to a discarded
/// database and is ignored; an interrupted restore is not, because the
/// original database may then sit in the restore's stash.
fn refuse_pending_restore(
    target: &SqliteTarget,
    files: TargetFiles,
    progress: &mut Progress,
) -> Result<()> {
    if let Some(interrupted) = interrupted_restore(target) {
        progress.report_backup = interrupted.backup;
        return fail(
            ErrorCode::RestoreRequired,
            Resolution::RestoreRequired,
            "a previous local restore was interrupted; run local restore again",
        );
    }
    if files == TargetFiles::Missing {
        return Ok(());
    }
    if let Some(record) = AttemptRecord::load(target)?
        && record.status == AttemptStatus::Failed(Resolution::RestoreRequired)
    {
        let code = if record.backup.is_some() {
            ErrorCode::RestoreRequired
        } else {
            ErrorCode::RestoreRequiredNoBackup
        };
        progress.report_backup = record.backup;
        return fail(
            code,
            Resolution::RestoreRequired,
            "the previous migration attempt left the database in a state that requires a restore",
        );
    }
    Ok(())
}

/// A database that needs no work clears a stale failure record.
fn mark_recovered(target: &SqliteTarget) -> Result<()> {
    if let Some(mut record) = AttemptRecord::load(target)?
        && record.status != AttemptStatus::Succeeded
    {
        record.status = AttemptStatus::Succeeded;
        record.store(target)?;
    }
    Ok(())
}

async fn open_existing(target: &SqliteTarget) -> Result<RdbPool> {
    use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

    let options = SqliteConnectOptions::new()
        .filename(target.database())
        .create_if_missing(false);
    SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .context("opening the local database")
}

async fn restore(args: &LocalRestoreArgs) -> Result<()> {
    let target = sqlite_target()?;
    let artifact_root = atlas_artifact_root().map_err(bundle_invalid)?;
    let latest_version =
        latest_migration_version(&artifact_root, "sqlite").map_err(bundle_invalid)?;
    let manifest = BackupManifest::load(&args.backup)?;
    let _embedding_guard = guard_embedding(
        &target,
        Work::Restore {
            // An absent resource is restored as absent, which also removes
            // the current directory.
            vector_tables: !manifest.resources.is_empty(),
        },
        manifest.embedding_space_id.as_deref(),
    )
    .await?;
    restore_backup(&args.backup, &target, &latest_version).await?;
    let record = AttemptRecord::path(&target);
    if record.exists() {
        std::fs::remove_file(&record)
            .with_context(|| format!("clearing the attempt record {}", record.display()))?;
    }
    Ok(())
}

fn restore_failure_line(args: &LocalRestoreArgs, error: &anyhow::Error) -> FailureLine {
    let (error_code, resolution) = error
        .downcast_ref::<LocalFailure>()
        .map(|failure| (failure.error_code, failure.resolution))
        .unwrap_or((ErrorCode::RestoreFailed, Resolution::Retry));
    let stage = match error_code {
        ErrorCode::BackupIncomplete | ErrorCode::BackupNewerThanTool => Stage::RestoreValidate,
        ErrorCode::RestoreFailed => Stage::RestoreReplace,
        _ => Stage::RestorePreflight,
    };
    let failure = error.downcast_ref::<LocalFailure>();
    let line = FailureLine::new(
        Command::Restore,
        stage,
        error_code,
        resolution,
        // An embedding refusal names only the embedding backup (if any).
        match failure {
            Some(f) if f.embedding_attempt.is_some() => f.backup.clone(),
            _ => Some(args.backup.clone()),
        },
    );
    line.with_embedding_fields(failure)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(backup_dir: Option<&str>, keep: Option<usize>, no_backup: bool) -> LocalApplyArgs {
        LocalApplyArgs {
            maintenance_window_ack: true,
            backup_dir: backup_dir.map(PathBuf::from),
            backup_keep: keep,
            no_backup_unsafe: no_backup,
        }
    }

    fn code(result: Result<BackupPolicy>) -> Option<ErrorCode> {
        result
            .err()
            .and_then(|error| error.downcast_ref::<LocalFailure>().map(|f| f.error_code))
    }

    #[test]
    fn backup_policy_requires_exactly_one_explicit_choice() {
        assert_eq!(
            backup_policy(&args(Some("/b"), None, false)).unwrap(),
            BackupPolicy::Keep {
                parent: "/b".into(),
                keep: 1
            }
        );
        assert_eq!(
            backup_policy(&args(Some("/b"), Some(0), false)).unwrap(),
            BackupPolicy::Keep {
                parent: "/b".into(),
                keep: 0
            }
        );
        assert_eq!(
            backup_policy(&args(None, None, true)).unwrap(),
            BackupPolicy::Disabled
        );
        for invalid in [
            args(None, None, false),
            args(Some("/b"), None, true),
            args(None, Some(2), true),
            args(None, Some(2), false),
        ] {
            assert_eq!(
                code(backup_policy(&invalid)),
                Some(ErrorCode::BackupOptionRequired)
            );
        }
    }

    fn record(resolution: Option<Resolution>, backup: Option<&str>) -> AttemptRecord {
        AttemptRecord {
            attempt_id: "a".to_string(),
            started_at: 1,
            bundle_digest: None,
            backup: backup.map(PathBuf::from),
            stage: Stage::PostMigrate,
            status: resolution.map_or(AttemptStatus::Running, AttemptStatus::Failed),
        }
    }

    #[test]
    fn only_a_failure_that_requires_a_restore_blocks_the_next_run() {
        let directory = tempfile::tempdir().unwrap();
        let target = SqliteTarget::at(directory.path().join("db"));
        refuse_pending_restore(&target, TargetFiles::Present, &mut Progress::default()).unwrap();

        for retryable in [
            record(None, None),
            record(Some(Resolution::Retry), Some("/b")),
            record(Some(Resolution::ToolUpdateRequired), None),
        ] {
            retryable.store(&target).unwrap();
            refuse_pending_restore(&target, TargetFiles::Present, &mut Progress::default())
                .unwrap();
        }

        let cases = [
            (Some("/b"), ErrorCode::RestoreRequired),
            (None, ErrorCode::RestoreRequiredNoBackup),
        ];
        for (backup, expected) in cases {
            record(Some(Resolution::RestoreRequired), backup)
                .store(&target)
                .unwrap();
            let mut progress = Progress::default();
            let error =
                refuse_pending_restore(&target, TargetFiles::Present, &mut progress).unwrap_err();
            let failure = error.downcast_ref::<LocalFailure>().unwrap();
            assert_eq!(failure.error_code, expected);
            assert_eq!(failure.resolution, Resolution::RestoreRequired);
            // The refusal reports the backup of the attempt that needs it,
            // without rewriting that attempt's record.
            assert_eq!(progress.report_backup, backup.map(PathBuf::from));
            assert!(progress.attempt.is_none());
        }
    }

    #[test]
    fn interrupted_restore_blocks_migration_even_without_a_database() {
        let directory = tempfile::tempdir().unwrap();
        let target = SqliteTarget::at(directory.path().join("db"));
        std::fs::write(
            directory.path().join("db.memories-restore.json"),
            serde_json::json!({
                "format": "memories-local-restore-v1",
                "backup": "/b/memories-backup-1",
                "items": [],
            })
            .to_string(),
        )
        .unwrap();
        // The original database may sit in the restore's stash, so a missing
        // database must not be taken for a new one.
        for files in [TargetFiles::Present, TargetFiles::Missing] {
            let mut progress = Progress::default();
            let error = refuse_pending_restore(&target, files, &mut progress).unwrap_err();
            assert_eq!(
                error.downcast_ref::<LocalFailure>().map(|f| f.error_code),
                Some(ErrorCode::RestoreRequired)
            );
            assert_eq!(
                progress.report_backup,
                Some(PathBuf::from("/b/memories-backup-1"))
            );
            assert!(progress.attempt.is_none());
        }
    }

    #[test]
    fn attempt_record_of_a_removed_database_is_ignored() {
        let directory = tempfile::tempdir().unwrap();
        let target = SqliteTarget::at(directory.path().join("db"));
        record(Some(Resolution::RestoreRequired), None)
            .store(&target)
            .unwrap();
        refuse_pending_restore(&target, TargetFiles::Missing, &mut Progress::default()).unwrap();
    }

    #[test]
    fn unclassified_failures_map_to_the_documented_resolution() {
        for (stage, expected) in [
            (
                Stage::SchemaApply,
                (ErrorCode::MigrationFailed, Resolution::Retry),
            ),
            (
                Stage::Verify,
                (ErrorCode::VerifyFailed, Resolution::ToolUpdateRequired),
            ),
            (Stage::Backup, (ErrorCode::BackupFailed, Resolution::Retry)),
        ] {
            assert_eq!(default_classification(stage), expected, "{stage}");
        }
    }

    #[test]
    fn restore_failures_are_reported_at_the_stage_they_belong_to() {
        let args = LocalRestoreArgs {
            maintenance_window_ack: true,
            backup: "/b/memories-backup-1".into(),
        };
        let cases = [
            (ErrorCode::BackupIncomplete, Stage::RestoreValidate),
            (ErrorCode::BackupNewerThanTool, Stage::RestoreValidate),
            (ErrorCode::WriterActive, Stage::RestorePreflight),
            (ErrorCode::InsufficientSpace, Stage::RestorePreflight),
            (ErrorCode::RestoreFailed, Stage::RestoreReplace),
        ];
        for (error_code, stage) in cases {
            let error = LocalFailure::new(error_code, Resolution::Retry, "x").into();
            let line = restore_failure_line(&args, &error);
            assert_eq!(line.stage, stage, "{error_code}");
            assert_eq!(line.backup.as_deref(), Some(args.backup.as_path()));
        }
    }
}
