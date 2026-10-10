//! `embedding finalize` and `embedding restore` (spec §3.7): end an
//! attempt by committing the rebuilt tables, or by putting the backup
//! back. Both follow the decision order of §3.3 through the helpers of
//! `embedding_migrate`, and every step is idempotent so an interrupted run
//! is completed by running the same command again.

use super::embedding_migrate::{
    AttemptArgs, Ctx, Decided, Failed, Outcome, decide, named_attempt, prepare, retain_backups,
    schema_ready,
};
use clap::Args;
use grpc_admin::db_migrate::embedding::{
    attempt::{AttemptRecord, CancelOperation, Stage as AttemptStage, Status},
    backup, finalize,
    observe::{observe, open_all},
    output::{
        Command, ErrorCode, FailureLine, Resolution, SpaceValue, Stage, UnavailableReason,
        progress_line,
    },
    stage::{EffectiveStage, Operation},
};
use grpc_admin::db_migrate::vocabulary::encode_value;
use infra::infra::embedding_index::EmbeddingIndex;
use infra::infra::embedding_index::scan::{ScanConfig, ScanTable, delete_orphans};
use infra::infra::embedding_space::record::{
    MarkerState, MigrationMarker, write_marker, write_rebuild_chunking,
};
use infra::infra::embedding_space::replace::{StoreSpec, schema_matches};
use infra::infra::embedding_space::{SpaceId, storage};
use infra::infra::vector_table::{Table, open_existing};
use std::path::{Path, PathBuf};

/// Page size of the orphan scan after a restore.
const CLEANUP_PAGE_SIZE: i64 = 500;

#[derive(Debug, Args)]
pub struct FinalizeArgs {
    #[arg(long)]
    pub attempt: String,
    /// Number of failed targets the user accepted; must equal the current
    /// number of failures.
    #[arg(long)]
    pub accept_failed: Option<u64>,
}

fn success_finalize(record: &AttemptRecord, outcome: &str) -> String {
    format!(
        "embedding_finalize status=completed outcome={outcome} attempt={} space={} accepted_failed={} next_action=apply",
        encode_value(&record.attempt_id),
        encode_value(&record.target_space_id),
        record.accepted_failed.unwrap_or(0),
    )
}

fn success_restore(record: &AttemptRecord, space: &SpaceValue, outcome: &str) -> String {
    format!(
        "embedding_restore status=completed outcome={outcome} attempt={} space={space} next_action=reconcile",
        encode_value(&record.attempt_id),
    )
}

/// The space the restored tables are recorded in: `none` when none is
/// recorded (startup then records the configured one), `unknown` when
/// they disagree or cannot be read.
async fn restored_space(ctx: &Ctx) -> SpaceValue {
    let Ok(tables) = existing_tables(ctx).await else {
        return SpaceValue::Unknown;
    };
    let mut spaces = std::collections::BTreeSet::new();
    for (_, table) in tables {
        let Some(t) = table else { continue };
        match infra::infra::embedding_space::record::read_table_record(&t).await {
            Ok(r) => {
                spaces.insert(r.space.map(|s| s.space_id.to_string()));
            }
            Err(_) => return SpaceValue::Unknown,
        }
    }
    match spaces.len() {
        0 => SpaceValue::None,
        1 => match spaces.into_iter().next().flatten() {
            Some(id) => SpaceValue::Id(id),
            None => SpaceValue::None,
        },
        _ => SpaceValue::Unknown,
    }
}

/// A failure of a step after the preflight, naming the attempt (and its
/// backup, when one can be restored).
fn step_failed(
    ctx: &Ctx,
    record: &AttemptRecord,
    stage: Stage,
    effective: EffectiveStage,
    code: ErrorCode,
    resolution: Resolution,
) -> Failed {
    let mut line = FailureLine::new(ctx.command, stage, code, resolution);
    line.attempt = Some(record.info(effective.as_str()));
    if matches!(
        resolution,
        Resolution::RestoreRequired | Resolution::ResumeOrRestore | Resolution::ContinueOrRestore
    ) {
        line.backup = record.usable_backup();
    }
    line.into()
}

/// `resource_corrupt` with `restore_required` when a usable backup exists,
/// otherwise `manual_recovery`.
fn corrupt(ctx: &Ctx, record: &AttemptRecord, stage: Stage, effective: EffectiveStage) -> Failed {
    let resolution = if record.usable_backup().is_some() {
        Resolution::RestoreRequired
    } else {
        Resolution::ManualRecovery
    };
    step_failed(
        ctx,
        record,
        stage,
        effective,
        ErrorCode::ResourceCorrupt,
        resolution,
    )
}

async fn existing_tables(ctx: &Ctx) -> anyhow::Result<Vec<(StoreSpec, Option<Table>)>> {
    open_all(&ctx.specs).await
}

/// Set `marker` on every existing table (`None` also drops the chunking
/// pin of the rebuild, which only matters while it is pending).
async fn mark_all(ctx: &Ctx, marker: Option<&MigrationMarker>) -> anyhow::Result<()> {
    for (_, table) in existing_tables(ctx).await? {
        if let Some(t) = table {
            if marker.is_none() {
                write_rebuild_chunking(&t, None).await?;
            }
            write_marker(&t, marker).await?;
        }
    }
    Ok(())
}

pub async fn run_finalize(args: FinalizeArgs) -> Outcome {
    let mut ctx = prepare(Command::Finalize).await?;
    let a = named_attempt(&ctx, &args.attempt)?;
    let Decided::Run(stage) = decide(&ctx, &a, Operation::Finalize).await? else {
        return Ok(success_finalize(&a, "already_completed"));
    };
    ctx.ensure_no_writer(Resolution::Retry).await?;
    let mut record = a;
    let was_completed = record.status == Status::Completed;
    if stage == EffectiveStage::RebuildPending {
        schema_ready(&ctx).await?;
        println!("{}", progress_line(Stage::Verify));
        let failed = verify(&ctx, &record, args.accept_failed).await?;
        // Kept before any marker changes, so a commit resumed without
        // verification still reports what was accepted.
        record.accepted_failed = (failed > 0).then_some(failed);
        ctx.save(&record)?;
    }
    // A `committing` marker means verification passed under this same
    // exclusion (spec §3.7), so a resumed commit does not verify again.
    println!("{}", progress_line(Stage::Commit));
    let commit_failed = |ctx: &Ctx, r: &AttemptRecord, stage: Stage| {
        step_failed(
            ctx,
            r,
            stage,
            EffectiveStage::Commit,
            ErrorCode::ResourceNotWritable,
            Resolution::CheckEnvironment,
        )
    };
    if record.status != Status::Completed {
        let committing = record.marker(MarkerState::Committing);
        mark_all(&ctx, Some(&committing))
            .await
            .map_err(|_| commit_failed(&ctx, &record, Stage::Commit))?;
        record.stage = AttemptStage::Commit;
        ctx.save(&record)?;
        record.status = Status::Completed;
        ctx.save(&record)?;
    }
    println!("{}", progress_line(Stage::Cleanup));
    mark_all(&ctx, None)
        .await
        .map_err(|_| commit_failed(&ctx, &record, Stage::Cleanup))?;
    println!("{}", progress_line(Stage::Retention));
    retain_backups(&record, true);
    Ok(success_finalize(
        &record,
        if was_completed {
            "already_completed"
        } else {
            "finalized"
        },
    ))
}

/// The checks of spec §3.7 "検証". Returns the number of failed targets
/// (accepted when non-zero).
async fn verify(ctx: &Ctx, record: &AttemptRecord, accept: Option<u64>) -> Result<u64, Failed> {
    let pending = EffectiveStage::RebuildPending;
    let obs = observe(ctx.pool, &ctx.env).await;
    match obs.unavailable {
        Some(UnavailableReason::ResourceUnavailable) => {
            return Err(step_failed(
                ctx,
                record,
                Stage::Verify,
                pending,
                ErrorCode::ResourceUnavailable,
                Resolution::CheckEnvironment,
            ));
        }
        Some(UnavailableReason::ResourceCorrupt) => {
            return Err(corrupt(ctx, record, Stage::Verify, pending));
        }
        None => {}
    }
    if obs.space != SpaceValue::Id(record.target_space_id.clone()) {
        return Err(corrupt(ctx, record, Stage::Verify, pending));
    }
    let dimension = record.target_space.dimension as usize;
    let tables = existing_tables(ctx).await.map_err(|_| {
        step_failed(
            ctx,
            record,
            Stage::Verify,
            pending,
            ErrorCode::ResourceUnavailable,
            Resolution::CheckEnvironment,
        )
    })?;
    for (spec, table) in &tables {
        let matches = match table {
            Some(t) => schema_matches(spec, t, dimension).await.unwrap_or(false),
            None => false,
        };
        if !matches {
            return Err(corrupt(ctx, record, Stage::Verify, pending));
        }
    }
    let counts = obs.counts.unwrap_or_default();
    if let Some((code, resolution)) = finalize::refusal(&counts, accept) {
        let Failed(mut line) = step_failed(ctx, record, Stage::Verify, pending, code, resolution);
        line.extra = finalize::count_fields(&counts);
        return Err(Failed(line));
    }
    Ok(counts.total().failed())
}

pub async fn run_restore(args: AttemptArgs) -> Outcome {
    let mut ctx = prepare(Command::Restore).await?;
    let a = named_attempt(&ctx, &args.attempt)?;
    let Decided::Run(stage) = decide(&ctx, &a, Operation::Restore).await? else {
        return Ok(success_restore(
            &a,
            &restored_space(&ctx).await,
            "already_restored",
        ));
    };
    ctx.ensure_no_writer(Resolution::Retry).await?;
    let mut record = a;
    if record.cancel_operation.is_none() {
        // A restore already under way must be completable whatever else
        // is pending, since `local apply` and `local restore` are refused
        // while the tables are being restored. A new one needs the current
        // schema for its orphan scan.
        schema_ready(&ctx).await?;
    }
    let restoring = EffectiveStage::Restoring;
    println!("{}", progress_line(Stage::Validate));
    let path = PathBuf::from(record.backup_path.clone().unwrap_or_default());
    let Ok(manifest) = backup::verify(&path, &record.attempt_id) else {
        return Err(step_failed(
            &ctx,
            &record,
            Stage::Validate,
            stage,
            ErrorCode::ResourceCorrupt,
            Resolution::ManualRecovery,
        ));
    };
    let marker = record.marker(MarkerState::Restoring);
    let failed_at = |ctx: &Ctx, r: &AttemptRecord, s: Stage| {
        step_failed(
            ctx,
            r,
            s,
            restoring,
            ErrorCode::ResourceNotWritable,
            Resolution::CheckEnvironment,
        )
    };
    let already_restored = record.status == Status::Restored;
    if !already_restored && !backup::restore_fits(&path, &manifest).unwrap_or(false) {
        // Checked before any change: the staged copy needs the space of
        // the backup next to each live store.
        return Err(step_failed(
            &ctx,
            &record,
            Stage::Validate,
            stage,
            ErrorCode::InsufficientSpace,
            Resolution::Retry,
        ));
    }
    if !already_restored {
        record.cancel_operation = Some(CancelOperation::Restore);
        ctx.save(&record)?;
        mark_all(&ctx, Some(&marker))
            .await
            .map_err(|_| failed_at(&ctx, &record, Stage::Replace))?;
        println!("{}", progress_line(Stage::Replace));
        put_back(&ctx, &path, &manifest, &marker)
            .await
            .map_err(|_| failed_at(&ctx, &record, Stage::Replace))?;
        println!("{}", progress_line(Stage::Cleanup));
        remove_orphans(&ctx, &record)
            .await
            .map_err(|_| failed_at(&ctx, &record, Stage::Cleanup))?;
        record.status = Status::Restored;
        ctx.save(&record)?;
    }
    mark_all(&ctx, None)
        .await
        .map_err(|_| failed_at(&ctx, &record, Stage::Cleanup))?;
    Ok(success_restore(
        &record,
        &restored_space(&ctx).await,
        if already_restored {
            "already_restored"
        } else {
            "restored"
        },
    ))
}

/// Swap every backed-up store directory in, with the `restoring` marker
/// already on its tables so the restored state never appears unmarked.
async fn put_back(
    ctx: &Ctx,
    path: &Path,
    manifest: &backup::BackupManifest,
    marker: &MigrationMarker,
) -> anyhow::Result<()> {
    for (live, staged) in backup::stage_restore(path, manifest)? {
        let staged_uri = staged.to_string_lossy().into_owned();
        for spec in ctx
            .specs
            .iter()
            .filter(|s| storage::local_path(&s.uri).as_deref() == Some(live.as_path()))
        {
            if let Some(t) = open_existing(&staged_uri, &spec.table_name).await? {
                write_marker(&t, Some(marker)).await?;
            }
        }
        backup::swap_in(&live, &staged)?;
    }
    Ok(())
}

/// Rows and entries of entities deleted from the RDB after the backup.
async fn remove_orphans(ctx: &Ctx, record: &AttemptRecord) -> anyhow::Result<u64> {
    let mut tables = Vec::with_capacity(ctx.specs.len());
    for spec in &ctx.specs {
        tables.push(ScanTable {
            label: spec.label,
            table: open_existing(&spec.uri, &spec.table_name).await?,
            index: EmbeddingIndex::open_existing(&spec.uri).await?,
        });
    }
    let media = infra::infra::media_object::rdb::MediaObjectRepositoryImpl::new(
        infra::infra::IdGeneratorWrapper::new(),
        ctx.pool,
    );
    // Orphans do not depend on the space; it only labels target states.
    let space = SpaceId(
        record
            .source_space_id
            .clone()
            .unwrap_or_else(|| record.target_space_id.clone()),
    );
    // Only entities gone from the RDB: the configuration still names the
    // target space here, so its target rules must not decide which of the
    // restored rows are orphans.
    delete_orphans(
        ctx.pool,
        true,
        &media,
        ScanConfig {
            space,
            image_search_mode: ctx.env.image_search_mode,
            max_content_len: ctx.env.max_content_len,
            page_size: CLEANUP_PAGE_SIZE,
        },
        tables,
    )
    .await
}
