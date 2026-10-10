//! Changing embedding commands: `switch` (new and `--resume`) and
//! `abandon` (spec §3.5), and what `finalize` / `restore` share with them. They follow the decision order of §3.3: storage
//! identifiers and the operation lock, the attempt, the stage tables,
//! then the ordinary checks, before changing anything.

use super::{SchemaState, open_target_pool, schema_state};
use clap::Args;
use grpc_admin::db_migrate::embedding::{
    attempt::{self, AttemptRecord, CancelOperation, Method, Stage as AttemptStage, Status},
    backup, inspect,
    lock::{self, Exclusion, LockError},
    observe::{EmbeddingEnv, StorageCheck, markers_of, observe, open_all, storage_mismatch},
    output::{Command, ErrorCode, FailureLine, Resolution, SpaceValue, Stage},
    plan,
    stage::{
        EffectiveStage, Operation, Refusal, Response, effective_stage, finished_response,
        stage_response,
    },
};
use grpc_admin::db_migrate::vocabulary::encode_value;
use infra::infra::embedding_space::record::{
    MarkerState, MigrationMarker, SpaceRecord, read_table_record, write_marker,
};
use infra::infra::embedding_space::replace::{
    StoreSpec, drop_table, open_or_create, replace_with_empty, reset_index,
};
use infra::infra::embedding_space::{SpaceComponents, storage};
use infra_utils::infra::rdb::RdbPool;
use std::collections::BTreeSet;
use std::path::PathBuf;

#[derive(Debug, Args)]
pub struct BackupArgs {
    /// Parent directory of the backup (backup attempt).
    #[arg(long)]
    backup_dir: Option<PathBuf>,
    /// Number of embedding backups to keep.
    #[arg(long)]
    backup_keep: Option<usize>,
    /// Take no backup; cancelling then discards the vector tables.
    #[arg(long)]
    no_backup_unsafe: bool,
}

#[derive(Debug, Args)]
pub struct SwitchArgs {
    /// Resume an unfinished attempt instead of starting one.
    #[arg(long)]
    resume: Option<String>,
    #[arg(long)]
    model_id: Option<String>,
    #[arg(long, default_value = "")]
    tokenizer_model_id: String,
    #[arg(long, default_value = "unversioned")]
    revision: String,
    #[arg(long)]
    dimension: Option<u32>,
    /// The current space ID reported by inspect / plan (`unknown` or
    /// `none` accepted).
    #[arg(long)]
    expected_space: Option<String>,
    #[arg(long)]
    maintenance_window_ack: bool,
    #[command(flatten)]
    backup: BackupArgs,
}

#[derive(Debug, Args)]
pub struct AttemptArgs {
    #[arg(long)]
    pub attempt: String,
}

/// A failure: the final line and exit code 1.
pub struct Failed(pub Box<FailureLine>);

impl From<FailureLine> for Failed {
    fn from(line: FailureLine) -> Self {
        Self(Box::new(line))
    }
}

pub type Outcome = std::result::Result<String, Failed>;

pub fn fail(command: Command, stage: Stage, code: ErrorCode, resolution: Resolution) -> Failed {
    FailureLine::new(command, stage, code, resolution).into()
}

/// Opened state of a changing command, holding the exclusion.
pub struct Ctx {
    pub command: Command,
    pub env: EmbeddingEnv,
    pub pool: &'static RdbPool,
    #[cfg_attr(feature = "postgres", allow(dead_code))]
    pub database_url: String,
    pub specs: Vec<StoreSpec>,
    pub attempt: Option<AttemptRecord>,
    /// Held until the command ends; read only for PostgreSQL writers.
    #[cfg_attr(not(feature = "postgres"), allow(dead_code))]
    pub exclusion: Exclusion,
}

impl Ctx {
    pub async fn markers(&self) -> anyhow::Result<Vec<Option<MigrationMarker>>> {
        markers_of(&open_all(&self.specs).await?).await
    }

    /// Fail with a refusal from the stage tables.
    pub fn refused(&self, a: &AttemptRecord, stage: EffectiveStage, r: Refusal) -> Failed {
        let mut line = FailureLine::new(
            self.command,
            Stage::Preflight,
            r.error_code.unwrap_or(ErrorCode::EmbeddingSwitchInProgress),
            r.resolution,
        );
        if r.with_attempt {
            line.attempt = Some(a.info(stage.as_str()));
        }
        if r.with_backup && a.backup_complete {
            line.backup = a.backup_path.clone();
        }
        line.cancel_operation = r.cancel_operation;
        line.into()
    }

    /// Writers must be absent (SQLite: found through the database file's
    /// locks, so this process closes its own connections first).
    pub async fn ensure_no_writer(&mut self, resolution: Resolution) -> Result<(), Failed> {
        #[cfg(not(feature = "postgres"))]
        {
            self.pool.close().await;
            let present = lock::sqlite_writer_present(&self.database_url).unwrap_or(true);
            let reopened = open_target_pool()
                .await
                .map_err(|_| unavailable(self.command))?;
            self.pool = Box::leak(Box::new(reopened));
            if present {
                return Err(fail(
                    self.command,
                    Stage::Preflight,
                    ErrorCode::WriterActive,
                    resolution,
                ));
            }
        }
        #[cfg(feature = "postgres")]
        if self.exclusion.writer_present() {
            return Err(fail(
                self.command,
                Stage::Preflight,
                ErrorCode::WriterActive,
                resolution,
            ));
        }
        Ok(())
    }

    pub fn save(&self, record: &AttemptRecord) -> Result<(), Failed> {
        attempt::save(&self.env.state_dir, record).map_err(|_| {
            fail(
                self.command,
                Stage::Record,
                ErrorCode::ResourceNotWritable,
                Resolution::CheckEnvironment,
            )
        })
    }
}

/// Stage 1 and 2 of the decision order.
pub async fn prepare(command: Command) -> Result<Ctx, Failed> {
    let unavailable = || unavailable(command);
    let env = EmbeddingEnv::from_env().map_err(|_| unavailable())?;
    let database_url = super::migration_database_url().map_err(|_| unavailable())?;
    let pool: &'static RdbPool = Box::leak(Box::new(
        open_target_pool().await.map_err(|_| unavailable())?,
    ));
    let specs = env.stores.clone();
    if storage_mismatch(pool, &env.state_dir, &specs, StorageCheck::ForChange)
        .await
        .unwrap_or(true)
    {
        return Err(fail(
            command,
            Stage::Preflight,
            ErrorCode::StorageMismatch,
            Resolution::CheckEnvironment,
        ));
    }
    let exclusion = match lock::acquire(pool, &env.state_dir).await {
        Ok(e) => e,
        Err(LockError::Refused(code)) => {
            return Err(fail(command, Stage::Preflight, code, Resolution::Retry));
        }
        Err(LockError::Other(_)) => return Err(unavailable()),
    };
    let attempt = match attempt::load(&env.state_dir) {
        Ok(a) => a,
        Err(attempt::LoadError::UnknownFormat(_)) => {
            return Err(fail(
                command,
                Stage::Preflight,
                ErrorCode::ToolUpdateRequired,
                Resolution::ToolUpdateRequired,
            ));
        }
        Err(attempt::LoadError::Other(_)) => return Err(unavailable()),
    };
    Ok(Ctx {
        command,
        env,
        pool,
        database_url,
        specs,
        attempt,
        exclusion,
    })
}

/// Embedding backups kept when `--backup-keep` is not given (same as
/// `local apply`).
const DEFAULT_BACKUP_KEEP: usize = 1;

/// Retention of embedding backups after `switch` / `finalize` (spec §3.8):
/// only embedding backups in the attempt's backup parent are counted, and
/// the backup of an unfinished attempt is never removed. A failure is
/// reported but does not fail the command, whose work is already done.
pub fn retain_backups(record: &AttemptRecord, finished: bool) {
    let (Some(path), Some(keep)) = (record.backup_path.as_deref(), record.backup_keep) else {
        return;
    };
    let path = std::path::Path::new(path);
    let Some(parent) = path.parent() else {
        return;
    };
    let protect: Vec<&std::path::Path> = if finished { vec![] } else { vec![path] };
    if let Err(e) = backup::apply_retention(parent, keep, &protect) {
        eprintln!("warning: embedding backup retention failed: {e:#}");
    }
}

/// The SQLite `local apply` attempt must not be waiting for its restore
/// (spec §3.8); switch, finalize, and restore all refuse then.
pub fn apply_restore_settled(ctx: &Ctx) -> Result<(), Failed> {
    #[cfg(not(feature = "postgres"))]
    if grpc_admin::db_migrate::local::restore::restore_pending(&ctx.database_url) {
        return Err(fail(
            ctx.command,
            Stage::Preflight,
            ErrorCode::ApplyRestoreRequired,
            Resolution::ApplyRestoreRequired,
        ));
    }
    #[cfg(feature = "postgres")]
    let _ = ctx;
    Ok(())
}

/// Ordinary checks shared by switch and finalize (stage 4).
pub async fn schema_ready(ctx: &Ctx) -> Result<(), Failed> {
    let state = schema_state(ctx.pool)
        .await
        .map_err(|_| unavailable(ctx.command))?;
    if state != SchemaState::Managed {
        return Err(fail(
            ctx.command,
            Stage::Preflight,
            ErrorCode::ApplyRequired,
            Resolution::ApplyRequired,
        ));
    }
    apply_restore_settled(ctx)
}

pub fn now_ms() -> i64 {
    command_utils::util::datetime::now_millis()
}

fn success_switch(record: &AttemptRecord, outcome: &str) -> String {
    format!(
        "embedding_switch status=completed outcome={outcome} attempt={} source_space={} target_space={} backup={} next_action=start_rebuild",
        encode_value(&record.attempt_id),
        encode_value(record.source_space_id.as_deref().unwrap_or("unknown")),
        encode_value(&record.target_space_id),
        record
            .backup_path
            .as_deref()
            .map(encode_value)
            .unwrap_or_else(|| "none".into()),
    )
}

pub async fn run_switch(args: SwitchArgs) -> Outcome {
    let command = Command::Switch;
    if let Some(id) = args.resume.clone() {
        // Method, spaces, and backup come from the attempt record.
        let mut ctx = prepare(command).await?;
        return resume(&mut ctx, &id).await;
    }
    let method = match (&args.backup.backup_dir, args.backup.no_backup_unsafe) {
        (Some(_), false) => Method::Backup,
        (None, true) => Method::NoBackup,
        _ => {
            return Err(fail(
                command,
                Stage::Preflight,
                ErrorCode::BackupOptionRequired,
                Resolution::ToolUpdateRequired,
            ));
        }
    };
    let mut ctx = prepare(command).await?;
    switch_new(&mut ctx, &args, method).await
}

async fn switch_new(ctx: &mut Ctx, args: &SwitchArgs, method: Method) -> Outcome {
    let command = ctx.command;
    let markers = ctx.markers().await.map_err(|_| unavailable(command))?;
    if let Some(a) = ctx.attempt.clone()
        && let Some(stage) = effective_stage(&a, &markers)
        && let Response::Refuse(r) = stage_response(a.method, stage, Operation::SwitchNew)
    {
        return Err(ctx.refused(&a, stage, r));
    }
    if markers.iter().any(Option::is_some) {
        // Markers no attempt accounts for.
        return Err(fail(
            command,
            Stage::Preflight,
            ErrorCode::EmbeddingSwitchInProgress,
            Resolution::ManualRecovery,
        ));
    }
    let (Some(model_id), Some(dimension), Some(expected)) = (
        args.model_id.clone(),
        args.dimension,
        args.expected_space.clone(),
    ) else {
        return Err(fail(
            command,
            Stage::Preflight,
            ErrorCode::ToolUpdateRequired,
            Resolution::ToolUpdateRequired,
        ));
    };
    if !args.maintenance_window_ack {
        return Err(fail(
            command,
            Stage::Preflight,
            ErrorCode::ToolUpdateRequired,
            Resolution::ToolUpdateRequired,
        ));
    }
    ctx.ensure_no_writer(Resolution::Retry).await?;
    schema_ready(ctx).await?;
    let Some(current) = ctx.env.current.clone() else {
        return Err(unavailable(command));
    };
    let target = SpaceComponents {
        model_id,
        tokenizer_model_id: args.tokenizer_model_id.clone(),
        revision: args.revision.clone(),
        dimension,
        distance: current.distance.clone(),
    };
    let obs = observe(ctx.pool, &ctx.env).await;
    if obs.unavailable.is_some() {
        return Err(unavailable(command));
    }
    if obs.space.to_string() != expected {
        return Err(fail(
            command,
            Stage::Preflight,
            ErrorCode::SpaceChanged,
            Resolution::PlanRequired,
        ));
    }
    let planned = plan::derive(
        &obs,
        inspect::derive(&obs).state,
        target.space_id().as_str(),
    );
    if planned.decision != grpc_admin::db_migrate::embedding::output::Decision::ReembedRequired {
        return Err(fail(
            command,
            Stage::Preflight,
            ErrorCode::NoReembedNeeded,
            Resolution::PlanRequired,
        ));
    }
    if ctx
        .specs
        .iter()
        .any(|s| storage::local_path(&s.uri).is_none())
    {
        return Err(fail(
            command,
            Stage::Preflight,
            ErrorCode::UnsupportedResource,
            Resolution::ToolUpdateRequired,
        ));
    }
    let attempt_id = storage::new_identifier();
    let record = AttemptRecord {
        format_version: attempt::FORMAT_VERSION,
        attempt_id: attempt_id.clone(),
        method,
        source_space_id: match &obs.space {
            SpaceValue::Id(id) => Some(id.clone()),
            _ => None,
        },
        target_space_id: target.space_id().to_string(),
        target_space: target,
        backup_path: args.backup.backup_dir.as_ref().map(|p| {
            backup::backup_path(p, &attempt_id)
                .to_string_lossy()
                .into_owned()
        }),
        backup_complete: false,
        stage: AttemptStage::Backup,
        status: Status::Running,
        cancel_operation: None,
        discarded: false,
        accepted_failed: None,
        backup_keep: (method == Method::Backup)
            .then(|| args.backup.backup_keep.unwrap_or(DEFAULT_BACKUP_KEEP)),
        started_at: now_ms(),
    };
    ctx.save(&record)?;
    ctx.attempt = Some(record.clone());
    run_from_marking(ctx, record).await
}

pub fn unavailable(command: Command) -> Failed {
    fail(
        command,
        Stage::Preflight,
        ErrorCode::ResourceUnavailable,
        Resolution::CheckEnvironment,
    )
}

/// Steps 2–7 of switch, also the restart point of `--resume` at stage
/// `backup`.
async fn run_from_marking(ctx: &mut Ctx, mut record: AttemptRecord) -> Outcome {
    let command = ctx.command;
    let switching = record.marker(MarkerState::Switching);
    let dim = record.target_space.dimension as usize;
    for spec in &ctx.specs {
        let table = open_or_create(spec, dim)
            .await
            .map_err(|_| unavailable(command))?;
        write_marker(&table, Some(&switching))
            .await
            .map_err(|_| unavailable(command))?;
    }
    if let Err(Failed(mut line)) = ctx.ensure_no_writer(Resolution::ResumeOrAbandon).await {
        line.attempt = Some(record.info(EffectiveStage::Backup.as_str()));
        return Err(Failed(line));
    }
    if record.method == Method::Backup {
        let path = PathBuf::from(
            record
                .backup_path
                .as_deref()
                .expect("backup attempts have a path"),
        );
        let parent = path.parent().unwrap_or(std::path::Path::new(""));
        let sources = distinct_dirs(&ctx.specs);
        let rdb_id = storage::read_rdb_id(ctx.pool).await.ok();
        if let Err(e) = backup::create(
            parent,
            &record.attempt_id,
            record.source_space_id.clone(),
            rdb_id,
            &sources,
        ) {
            let (code, resolution) = match e {
                backup::BackupError::InsufficientSpace => {
                    (ErrorCode::InsufficientSpace, Resolution::ResumeOrAbandon)
                }
                backup::BackupError::NotWritable(_) | backup::BackupError::Other(_) => {
                    (ErrorCode::ResourceNotWritable, Resolution::CheckEnvironment)
                }
            };
            let mut line = FailureLine::new(command, Stage::Backup, code, resolution);
            line.attempt = Some(record.info(EffectiveStage::Backup.as_str()));
            return Err(line.into());
        }
        record.backup_complete = true;
    }
    record.stage = AttemptStage::Replace;
    ctx.save(&record)?;
    replace_tables(ctx, &record).await?;
    record.stage = AttemptStage::RebuildPending;
    ctx.save(&record)?;
    retain_backups(&record, false);
    Ok(success_switch(&record, "switched"))
}

pub fn distinct_dirs(specs: &[StoreSpec]) -> Vec<PathBuf> {
    let mut seen = BTreeSet::new();
    specs
        .iter()
        .filter_map(|s| storage::local_path(&s.uri))
        .filter(|p| seen.insert(p.clone()))
        .collect()
}

/// Step 6: replace every table still marked `switching` with an empty
/// table of the target space marked `pending`, and empty the indexes.
async fn replace_tables(ctx: &Ctx, record: &AttemptRecord) -> Result<(), Failed> {
    let command = ctx.command;
    let pending = record.marker(MarkerState::Pending);
    let space = SpaceRecord::new(&record.target_space, false);
    let dim = record.target_space.dimension as usize;
    let markers = ctx.markers().await.map_err(|_| unavailable(command))?;
    let mut reset = BTreeSet::new();
    for (spec, marker) in ctx.specs.iter().zip(markers) {
        if marker.as_ref() == Some(&pending) {
            continue;
        }
        if reset.insert(spec.uri.clone()) {
            reset_index(&spec.uri)
                .await
                .map_err(|_| replace_failed(command, record))?;
        }
        replace_with_empty(spec, dim, Some(&space), Some(&pending))
            .await
            .map_err(|_| replace_failed(command, record))?;
    }
    Ok(())
}

fn replace_failed(command: Command, record: &AttemptRecord) -> Failed {
    let mut line = FailureLine::new(
        command,
        Stage::Replace,
        ErrorCode::ResourceNotWritable,
        Resolution::CheckEnvironment,
    );
    line.attempt = Some(record.info(EffectiveStage::Replace.as_str()));
    line.into()
}

/// The attempt named by `--resume` / `--attempt`, or `attempt_not_found`.
pub fn named_attempt(ctx: &Ctx, id: &str) -> Result<AttemptRecord, Failed> {
    match &ctx.attempt {
        Some(a) if a.attempt_id == id => Ok(a.clone()),
        _ => Err(fail(
            ctx.command,
            Stage::Preflight,
            ErrorCode::AttemptNotFound,
            Resolution::PlanRequired,
        )),
    }
}

/// What a command naming an attempt does after stages 2a and 3.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decided {
    /// A resend of work that already succeeded (each operation has one
    /// such answer, e.g. `already_restored` for `restore`).
    AlreadyDone,
    /// Go on with the operation at this effective stage.
    Run(EffectiveStage),
}

/// Stages 2a and 3 for a named attempt.
pub async fn decide(ctx: &Ctx, a: &AttemptRecord, op: Operation) -> Result<Decided, Failed> {
    let markers = ctx.markers().await.map_err(|_| unavailable(ctx.command))?;
    let finished = |code: Option<ErrorCode>, resolution| {
        Failed::from(FailureLine::new(
            ctx.command,
            Stage::Preflight,
            code.unwrap_or(ErrorCode::AttemptFinished),
            resolution,
        ))
    };
    let Some(stage) = effective_stage(a, &markers) else {
        return match finished_response(a.status, op) {
            Response::Succeed(_) => Ok(Decided::AlreadyDone),
            Response::Refuse(r) => Err(finished(r.error_code, r.resolution)),
            // Only commands without an attempt continue past a finished
            // one; a named attempt that is finished has nothing to run.
            Response::Continue => Err(finished(None, Resolution::PlanRequired)),
        };
    };
    match stage_response(a.method, stage, op) {
        Response::Continue => Ok(Decided::Run(stage)),
        Response::Succeed(_) => Ok(Decided::AlreadyDone),
        Response::Refuse(r) => Err(ctx.refused(a, stage, r)),
    }
}

async fn resume(ctx: &mut Ctx, id: &str) -> Outcome {
    let command = ctx.command;
    let a = named_attempt(ctx, id)?;
    let Decided::Run(stage) = decide(ctx, &a, Operation::SwitchResume).await? else {
        return Ok(success_switch(&a, "already_switched"));
    };
    ctx.ensure_no_writer(Resolution::Retry).await?;
    schema_ready(ctx).await?;
    let markers = ctx.markers().await.map_err(|_| unavailable(command))?;
    let mine = |state: MarkerState| a.marker(state);
    let usable_backup = a.usable_backup();
    let unverifiable = || {
        let resolution = match a.method {
            Method::NoBackup => Resolution::AbandonRequired,
            Method::Backup if usable_backup.is_some() => Resolution::RestoreRequired,
            Method::Backup => Resolution::ManualRecovery,
        };
        let mut line = FailureLine::new(
            command,
            Stage::Preflight,
            ErrorCode::SwitchStateUnverifiable,
            resolution,
        );
        line.attempt = Some(a.info(stage.as_str()));
        if resolution == Resolution::RestoreRequired {
            line.backup = usable_backup.clone();
        }
        line.into()
    };
    match stage {
        EffectiveStage::Backup => {
            // Nothing replaced yet: every table is unmarked or this
            // attempt's `switching`, and not in the target space.
            let mut ok = markers
                .iter()
                .all(|m| m.is_none() || m.as_ref() == Some(&mine(MarkerState::Switching)));
            if a.source_space_id.as_deref() != Some(a.target_space_id.as_str()) {
                for spec in &ctx.specs {
                    if let Ok(Some(t)) =
                        infra::infra::vector_table::open_existing(&spec.uri, &spec.table_name).await
                        && let Ok(r) = read_table_record(&t).await
                        && r.space.as_ref().map(|s| s.space_id.as_str())
                            == Some(a.target_space_id.as_str())
                    {
                        ok = false;
                    }
                }
            }
            if !ok {
                return Err(unverifiable());
            }
            run_from_marking(ctx, a).await
        }
        EffectiveStage::Replace => {
            let ok = markers.iter().all(|m| {
                m.as_ref() == Some(&mine(MarkerState::Switching))
                    || m.as_ref() == Some(&mine(MarkerState::Pending))
            });
            let backup_ok = a.method == Method::NoBackup || usable_backup.is_some();
            if !ok || !backup_ok {
                return Err(unverifiable());
            }
            replace_tables(ctx, &a).await?;
            let mut record = a;
            record.stage = AttemptStage::RebuildPending;
            ctx.save(&record)?;
            retain_backups(&record, false);
            Ok(success_switch(&record, "switched"))
        }
        _ => unreachable!("other stages are answered by the stage tables"),
    }
}

pub async fn run_abandon(args: AttemptArgs) -> Outcome {
    let mut ctx = prepare(Command::Abandon).await?;
    let command = ctx.command;
    let a = named_attempt(&ctx, &args.attempt)?;
    let Decided::Run(stage) = decide(&ctx, &a, Operation::Abandon).await? else {
        let next = if a.discarded { "reconcile" } else { "none" };
        return Ok(format!(
            "embedding_abandon status=completed outcome=already_abandoned attempt={} next_action={next}",
            encode_value(&a.attempt_id)
        ));
    };
    ctx.ensure_no_writer(Resolution::Retry).await?;
    let mut record = a;
    match stage {
        EffectiveStage::Backup => {
            let switching = record.marker(MarkerState::Switching);
            for spec in &ctx.specs {
                if let Ok(Some(t)) =
                    infra::infra::vector_table::open_existing(&spec.uri, &spec.table_name).await
                    && read_table_record(&t).await.ok().and_then(|r| r.marker)
                        == Some(switching.clone())
                {
                    write_marker(&t, None)
                        .await
                        .map_err(|_| unavailable(command))?;
                }
            }
            if let Some(path) = &record.backup_path {
                backup::remove(std::path::Path::new(path)).map_err(|_| unavailable(command))?;
            }
            record.status = Status::Abandoned;
            ctx.save(&record)?;
            Ok(format!(
                "embedding_abandon status=completed outcome=abandoned attempt={} next_action=none",
                encode_value(&record.attempt_id)
            ))
        }
        _ => {
            // Discard (no-backup attempt): empty every table, without a
            // space record, then end the attempt.
            let already_ended = record.status == Status::Abandoned;
            let discarding = record.marker(MarkerState::Discarding);
            if !already_ended {
                record.cancel_operation = Some(CancelOperation::Discard);
                record.discarded = true;
                ctx.save(&record)?;
                let mut reset = BTreeSet::new();
                for spec in &ctx.specs {
                    if let Ok(t) =
                        open_or_create(spec, record.target_space.dimension as usize).await
                    {
                        write_marker(&t, Some(&discarding))
                            .await
                            .map_err(|_| unavailable(command))?;
                    }
                }
                for spec in &ctx.specs {
                    if reset.insert(spec.uri.clone()) {
                        reset_index(&spec.uri)
                            .await
                            .map_err(|_| unavailable(command))?;
                    }
                    replace_with_empty(
                        spec,
                        record.target_space.dimension as usize,
                        None,
                        Some(&discarding),
                    )
                    .await
                    .map_err(|_| unavailable(command))?;
                }
                record.status = Status::Abandoned;
                ctx.save(&record)?;
            }
            // Dropping the discarded tables (instead of only clearing the
            // marker) lets the next start create them with whatever
            // dimension it is configured for, e.g. the previous model.
            for spec in &ctx.specs {
                if let Ok(Some(t)) =
                    infra::infra::vector_table::open_existing(&spec.uri, &spec.table_name).await
                    && read_table_record(&t).await.ok().and_then(|r| r.marker)
                        == Some(discarding.clone())
                {
                    drop_table(spec).await.map_err(|_| unavailable(command))?;
                }
            }
            let outcome = if already_ended {
                "already_abandoned"
            } else {
                "discarded"
            };
            Ok(format!(
                "embedding_abandon status=completed outcome={outcome} attempt={} next_action=reconcile",
                encode_value(&record.attempt_id)
            ))
        }
    }
}
