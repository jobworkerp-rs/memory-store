//! Deferred media GC sweep. The immediate ref_count path (memory tx
//! bumps `media_object.ref_count` in the same tx) is primary; a server
//! restart / crash / storage-delete failure still leaves orphans. This
//! batch reclaims them.
//!
//! Each leak class has a fixed recovery (the order below is chosen for
//! idempotence — a confirmed row is never routed through the
//! sha256→final recompute path reserved for unconfirmed reservations):
//!
//! | class               | detect                                    | recover                                  |
//! |---------------------|-------------------------------------------|------------------------------------------|
//! | confirmed orphan    | gc=1 ∧ uri NOT NULL ∧ created<G           | claim(1→5)→finish_delete (backend split) |
//! | reservation residue | gc=1 ∧ uri NULL ∧ ref=0 ∧ created<G        | sha256→final delete (if any) + row delete|
//! | promoting residue   | gc=4 ∧ uri NULL ∧ updated<G                | sha256→final delete + revert 4→3 (keep)  |
//! | deleting residue    | gc=5 ∧ uri NOT NULL ∧ updated<G            | revert 5→1 (next sweep deletes it)       |
//! | deleted-failed      | gc=2 ∧ uri NOT NULL                        | reclaim 2→5→finish_delete (storage retry)|
//! | ref_count desync    | confirmed ∧ ref_count ≠ #memory refs       | CAS reconcile; if 0 → delete flow        |
//! | temp residue        | `{prefix}_tmp/*` object age > G            | storage delete (no DB row)               |
//!
//! `unresolvable` (gc=3 ∧ uri NULL) is never scanned: it has no leaked
//! final to reclaim and must not be sha256→final deleted.
//!
//! `--dry-run` tallies without writing; without it the deletes are
//! real. `--grace-sec` overrides `MEDIA_GC_GRACE_SEC` (default 3600).

use anyhow::{Context, Result};
use app::app::media::MediaAppImpl;
use clap::Parser;
use infra::infra::media_object::rdb::{
    GC_ACTIVE, GC_ORPHAN, MediaObjectRepository, MediaObjectRepositoryImpl, MediaObjectRow,
    RefCountDesync,
};
use infra::infra::media_storage::{MediaConfig, StorageBackend, final_key};
use infra::infra::module::RepositoryModule;
use infra_utils::infra::rdb::UseRdbPool;
use std::sync::Arc;

const DEFAULT_GRACE_SEC: u64 = 3600;

#[derive(Parser, Debug)]
#[command(
    name = "cleanup-orphan-media",
    about = "Deferred media GC: reclaim orphaned media_object rows / leaked storage objects"
)]
struct Cli {
    /// Report the would-be reclamation breakdown without deleting.
    #[arg(long)]
    dry_run: bool,
    /// Grace period (sec) before a stale row/object is reclaimed.
    /// Overrides MEDIA_GC_GRACE_SEC (default 3600).
    #[arg(long)]
    grace_sec: Option<u64>,
}

/// Recovery for a stale reservation (`storage_uri IS NULL`). The leaked
/// final (if any) is at the sha256-derived key; a copy-after-crash may
/// have written it, so we delete that key (idempotent) then drop the
/// row. A row with no usable sha256 cannot have a derivable final, so
/// only the DB row is dropped.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ReservationPlan {
    DeleteFinalThenRow { final_key: String },
    DeleteRowOnlyNoSha,
}

/// Recovery for a stale promoting row (`gc_state=4`, `storage_uri
/// NULL`). The promotion copy may have leaked a final; delete it then
/// revert 4→3 (the row is kept — a memory may still reference it via
/// ref_count, and a later same-sha256 Upload re-claims it). No sha256 →
/// no derivable final, so only the revert runs.
#[derive(Debug, Clone, PartialEq, Eq)]
enum PromotingPlan {
    DeleteFinalThenRevert { final_key: String },
    RevertOnlyNoSha,
}

/// What to do with a ref_count desync row. Correction is a CAS keyed on
/// the read-time value; only `{0,1}` gc_state is corrected (the
/// dedicated GC states {2,3,4,5} are off-limits — their own sweep owns
/// them). `actual==0` means the media is unreferenced after correction,
/// so it joins the delete flow.
#[derive(Debug, Clone, PartialEq, Eq)]
enum DesyncAction {
    Reconcile { to: i64 },
    ReconcileThenDelete,
    LogOnly { error: bool, reason: String },
}

/// `--grace-sec` > `MEDIA_GC_GRACE_SEC` > default. A non-numeric env
/// value falls back to the default rather than aborting the sweep.
fn resolve_grace_sec(cli: Option<u64>, env: Option<String>) -> u64 {
    if let Some(v) = cli {
        return v;
    }
    match env {
        Some(s) => s.trim().parse::<u64>().unwrap_or(DEFAULT_GRACE_SEC),
        None => DEFAULT_GRACE_SEC,
    }
}

/// Rows older than `now - grace` are eligible. `grace=0` makes the
/// cutoff `now` (everything in the past). An absurd grace clamps the
/// cutoff to `i64::MIN` (the whole past) instead of wrapping through the
/// u64→i64 cast.
fn compute_before(now_millis: i64, grace_sec: u64) -> i64 {
    let grace_ms = grace_sec.saturating_mul(1000).min(i64::MAX as u64) as i64;
    now_millis.saturating_sub(grace_ms)
}

fn usable_sha(row: &MediaObjectRow) -> Option<&str> {
    match row.sha256.as_deref() {
        Some(s) if !s.is_empty() => Some(s),
        _ => None,
    }
}

fn reservation_recovery_plan(row: &MediaObjectRow, prefix: &str) -> ReservationPlan {
    match usable_sha(row) {
        Some(sha) => ReservationPlan::DeleteFinalThenRow {
            final_key: final_key(prefix, sha),
        },
        None => ReservationPlan::DeleteRowOnlyNoSha,
    }
}

fn promoting_recovery_plan(row: &MediaObjectRow, prefix: &str) -> PromotingPlan {
    match usable_sha(row) {
        Some(sha) => PromotingPlan::DeleteFinalThenRevert {
            final_key: final_key(prefix, sha),
        },
        None => PromotingPlan::RevertOnlyNoSha,
    }
}

fn desync_action(d: &RefCountDesync) -> DesyncAction {
    // Only active/orphan are reconcilable here; the other gc_states are
    // owned by their dedicated sweeps and touching ref_count would
    // double-handle the same row.
    if !matches!(d.gc_state, GC_ACTIVE | GC_ORPHAN) {
        return DesyncAction::LogOnly {
            error: false,
            reason: format!("gc_state={} not reconcilable", d.gc_state),
        };
    }
    if d.actual_ref_count < 0 {
        // COUNT(*) cannot be negative; observing this means a broken
        // invariant upstream — surface it, do not silently "fix".
        return DesyncAction::LogOnly {
            error: true,
            reason: format!("negative actual_ref_count={}", d.actual_ref_count),
        };
    }
    if d.actual_ref_count == 0 {
        DesyncAction::ReconcileThenDelete
    } else {
        DesyncAction::Reconcile {
            to: d.actual_ref_count,
        }
    }
}

#[derive(Debug, Default)]
struct Stats {
    confirmed_orphan_deleted: u64,
    confirmed_orphan_skipped: u64,
    reservation_reclaimed: u64,
    promoting_reverted: u64,
    deleting_reverted: u64,
    deleted_failed_retried: u64,
    deleted_failed_skipped: u64,
    desync_reconciled: u64,
    desync_deleted: u64,
    desync_logged: u64,
    temp_objects_deleted: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    // Fall back to the default logging config when no LOG_* env is set
    // (k8s ConfigMaps carry none — only the local .env does). Mirrors
    // the `front` server's init so the batch boots in-cluster instead of
    // erroring out before the first log line.
    use command_utils::util::tracing::{load_tracing_config_from_env, tracing_init};
    tracing_init(load_tracing_config_from_env().unwrap_or_default()).await?;

    let cli = Cli::parse();
    let grace_sec = resolve_grace_sec(cli.grace_sec, std::env::var("MEDIA_GC_GRACE_SEC").ok());

    let repositories = RepositoryModule::new_by_env().await;
    let media_repo = repositories.create_media_object_repository();
    let mcfg = MediaConfig::from_env();
    let storage = Arc::new(
        StorageBackend::from_env(&mcfg).context("MEDIA_STORAGE_BACKEND configuration error")?,
    );
    // Reuse the production delete discipline (claim→storage→conditional
    // DB delete, backend split, 5→2 retry) instead of re-implementing it.
    let media_app = MediaAppImpl::new(
        repositories.create_media_object_repository(),
        storage.clone(),
        repositories.id_generator(),
        mcfg.s3_prefix.clone(),
        mcfg.presign_ttl_sec,
        mcfg.upload_max_bytes,
    );

    let now = command_utils::util::datetime::now_millis();
    let before = compute_before(now, grace_sec);

    let mut stats = Stats::default();
    sweep_confirmed_orphans(&cli, &media_repo, &media_app, before, &mut stats).await?;
    sweep_reservations(
        &cli,
        &media_repo,
        storage.as_ref(),
        &mcfg,
        before,
        &mut stats,
    )
    .await?;
    sweep_promoting(
        &cli,
        &media_repo,
        storage.as_ref(),
        &mcfg,
        before,
        &mut stats,
    )
    .await?;
    sweep_deleting(&cli, &media_repo, before, &mut stats).await?;
    sweep_deleted_failed(&cli, &media_repo, &media_app, &mut stats).await?;
    sweep_ref_count_desync(&cli, &media_repo, &media_app, &mut stats).await?;
    sweep_temp_residue(&cli, storage.as_ref(), &mcfg, grace_sec, &mut stats).await?;

    print_summary(&cli, &stats);
    Ok(())
}

/// Confirmed orphan: claim(1→5) then the backend-split finish_delete
/// (s3/file = storage delete + DB row; url/inline = DB row only). A lost
/// claim (a concurrent incr re-referenced the row) is a no-op skip.
async fn sweep_confirmed_orphans(
    cli: &Cli,
    repo: &MediaObjectRepositoryImpl,
    media_app: &MediaAppImpl,
    before: i64,
    stats: &mut Stats,
) -> Result<()> {
    for row in repo.list_confirmed_orphans(before).await? {
        if cli.dry_run {
            stats.confirmed_orphan_deleted += 1;
            continue;
        }
        match claim_then_finish(repo, media_app, &row).await {
            Ok(true) => stats.confirmed_orphan_deleted += 1,
            Ok(false) => stats.confirmed_orphan_skipped += 1,
            Err(e) => {
                tracing::warn!(
                    "confirmed-orphan recover failed (id={}): {e}; will retry next sweep",
                    row.id
                );
                stats.confirmed_orphan_skipped += 1;
            }
        }
    }
    Ok(())
}

/// Reservation residue: the leaked final (if the sha256 derives one)
/// is deleted (idempotent), then the unconfirmed row is dropped via the
/// conditional delete (`ref_count=0 ∧ gc_state IN (0,1,5)`; gc_state=1
/// here). Never routed through finish_delete — that is for confirmed
/// rows whose storage_uri is the source of truth.
async fn sweep_reservations(
    cli: &Cli,
    repo: &MediaObjectRepositoryImpl,
    storage: &StorageBackend,
    mcfg: &MediaConfig,
    before: i64,
    stats: &mut Stats,
) -> Result<()> {
    for row in repo.list_orphan_reservations(before).await? {
        if cli.dry_run {
            stats.reservation_reclaimed += 1;
            continue;
        }
        let plan = reservation_recovery_plan(&row, &mcfg.s3_prefix);
        if let ReservationPlan::DeleteFinalThenRow { final_key } = &plan
            && let Err(e) = storage.as_dyn().delete(final_key).await
        {
            tracing::warn!(
                "reservation leaked-final delete failed (id={}, key={final_key}): {e}",
                row.id
            );
        }
        let mut tx = repo.db_pool().begin().await?;
        let deleted = repo.delete_if_unreferenced_tx(&mut *tx, row.id).await?;
        tx.commit().await?;
        if deleted {
            stats.reservation_reclaimed += 1;
        } else {
            tracing::warn!(
                "reservation row {} not deleted (re-referenced?); skipping",
                row.id
            );
        }
    }
    Ok(())
}

/// Promoting residue: delete the leaked final then revert 4→3 (the
/// row is kept — ref_count may be >0 and a later same-sha256 Upload
/// re-claims the placeholder).
async fn sweep_promoting(
    cli: &Cli,
    repo: &MediaObjectRepositoryImpl,
    storage: &StorageBackend,
    mcfg: &MediaConfig,
    before: i64,
    stats: &mut Stats,
) -> Result<()> {
    for row in repo.list_promoting_stale(before).await? {
        if cli.dry_run {
            stats.promoting_reverted += 1;
            continue;
        }
        let plan = promoting_recovery_plan(&row, &mcfg.s3_prefix);
        if let PromotingPlan::DeleteFinalThenRevert { final_key } = &plan
            && let Err(e) = storage.as_dyn().delete(final_key).await
        {
            tracing::warn!(
                "promoting leaked-final delete failed (id={}, key={final_key}): {e}",
                row.id
            );
        }
        let mut tx = repo.db_pool().begin().await?;
        let reverted = repo.revert_promoting_tx(&mut *tx, row.id).await?;
        tx.commit().await?;
        if reverted {
            stats.promoting_reverted += 1;
        }
    }
    Ok(())
}

/// Deleting residue: just revert 5→1. The next sweep picks it up via
/// the confirmed-orphan path (the normal delete-authority route for a
/// confirmed row) — idempotent, no storage touch here.
async fn sweep_deleting(
    cli: &Cli,
    repo: &MediaObjectRepositoryImpl,
    before: i64,
    stats: &mut Stats,
) -> Result<()> {
    for row in repo.list_deleting_stale(before).await? {
        if cli.dry_run {
            stats.deleting_reverted += 1;
            continue;
        }
        let mut tx = repo.db_pool().begin().await?;
        let reverted = repo.revert_deleting_tx(&mut *tx, row.id).await?;
        tx.commit().await?;
        if reverted {
            stats.deleting_reverted += 1;
        }
    }
    Ok(())
}

/// Storage-delete-failed: re-claim 2→5 (ref_count=0 guarded so a
/// recovery-Upload re-referenced row is left alone) then finish_delete
/// retries the storage delete; a re-failure flips 5→2 again for the
/// next sweep.
async fn sweep_deleted_failed(
    cli: &Cli,
    repo: &MediaObjectRepositoryImpl,
    media_app: &MediaAppImpl,
    stats: &mut Stats,
) -> Result<()> {
    for row in repo.list_deleted_failed_confirmed().await? {
        if cli.dry_run {
            stats.deleted_failed_retried += 1;
            continue;
        }
        let mut tx = repo.db_pool().begin().await?;
        let reclaimed = repo.reclaim_deleted_failed_tx(&mut *tx, row.id).await?;
        tx.commit().await?;
        if !reclaimed {
            stats.deleted_failed_skipped += 1;
            continue;
        }
        match media_app.finish_delete(row.id, &row).await {
            Ok(()) => stats.deleted_failed_retried += 1,
            Err(e) => {
                tracing::warn!(
                    "deleted-failed retry errored (id={}): {e}; will retry next sweep",
                    row.id
                );
                stats.deleted_failed_skipped += 1;
            }
        }
    }
    Ok(())
}

/// Ref_count desync: CAS-reconcile to the real count; if it lands on
/// 0, route into the delete flow (same claim→finish_delete as #1). A
/// lost CAS (primary path moved ref_count) is skipped — re-evaluated
/// next sweep, the primary path always wins.
async fn sweep_ref_count_desync(
    cli: &Cli,
    repo: &MediaObjectRepositoryImpl,
    media_app: &MediaAppImpl,
    stats: &mut Stats,
) -> Result<()> {
    for d in repo.list_ref_count_desync().await? {
        match desync_action(&d) {
            DesyncAction::LogOnly { error, reason } => {
                if error {
                    tracing::error!("ref_count desync invariant (id={}): {reason}", d.id);
                } else {
                    tracing::warn!("ref_count desync not reconciled (id={}): {reason}", d.id);
                }
                stats.desync_logged += 1;
            }
            DesyncAction::Reconcile { to } => {
                if cli.dry_run {
                    stats.desync_reconciled += 1;
                    continue;
                }
                let mut tx = repo.db_pool().begin().await?;
                let ok = repo
                    .reconcile_ref_count_tx(&mut *tx, d.id, d.db_ref_count, to)
                    .await?;
                tx.commit().await?;
                if ok {
                    stats.desync_reconciled += 1;
                } else {
                    tracing::warn!(
                        "ref_count reconcile lost CAS (id={}); re-evaluated next sweep",
                        d.id
                    );
                }
            }
            DesyncAction::ReconcileThenDelete => {
                if cli.dry_run {
                    stats.desync_deleted += 1;
                    continue;
                }
                let mut tx = repo.db_pool().begin().await?;
                let ok = repo
                    .reconcile_ref_count_tx(&mut *tx, d.id, d.db_ref_count, 0)
                    .await?;
                tx.commit().await?;
                if !ok {
                    tracing::warn!(
                        "ref_count reconcile-to-zero lost CAS (id={}); re-evaluated next sweep",
                        d.id
                    );
                    continue;
                }
                let Some(row) = repo.find_by_id(d.id).await? else {
                    continue;
                };
                match claim_then_finish(repo, media_app, &row).await {
                    Ok(true) => stats.desync_deleted += 1,
                    Ok(false) | Err(_) => {
                        tracing::warn!(
                            "post-reconcile delete skipped (id={}); retry next sweep",
                            d.id
                        );
                    }
                }
            }
        }
    }
    Ok(())
}

/// Temp residue: prefix age scan over `{prefix}_tmp/`. DB-independent
/// (reservation/promoting GC deliberately do not delete temp objects;
/// this single scan owns them). inline backend returns an empty list.
async fn sweep_temp_residue(
    cli: &Cli,
    storage: &StorageBackend,
    mcfg: &MediaConfig,
    grace_sec: u64,
    stats: &mut Stats,
) -> Result<()> {
    let temp_prefix = format!("{}_tmp/", mcfg.s3_prefix);
    let stale = storage
        .as_dyn()
        .list_temp_older_than(&temp_prefix, grace_sec)
        .await
        .context("scanning temp prefix")?;
    for obj in stale {
        if cli.dry_run {
            stats.temp_objects_deleted += 1;
            continue;
        }
        match storage.as_dyn().delete(&obj.key).await {
            Ok(()) => stats.temp_objects_deleted += 1,
            Err(e) => tracing::warn!("temp object delete failed (key={}): {e}", obj.key),
        }
    }
    Ok(())
}

/// Shared confirmed-row delete: claim {0,1}→5 then the backend-split
/// finish_delete. Returns `false` when the claim is lost (a concurrent
/// incr re-referenced the row) — that is a safe no-op, not an error.
async fn claim_then_finish(
    repo: &MediaObjectRepositoryImpl,
    media_app: &MediaAppImpl,
    row: &MediaObjectRow,
) -> Result<bool> {
    let mut tx = repo.db_pool().begin().await?;
    let claimed = repo.claim_deleting_tx(&mut *tx, row.id).await?;
    tx.commit().await?;
    if !claimed {
        return Ok(false);
    }
    media_app.finish_delete(row.id, row).await?;
    Ok(true)
}

fn print_summary(cli: &Cli, s: &Stats) {
    let mode = if cli.dry_run { "[DRY-RUN] " } else { "" };
    tracing::info!(
        "{mode}cleanup-orphan-media done: confirmed_orphan(deleted={}, skipped={}) \
         reservation_reclaimed={} promoting_reverted={} deleting_reverted={} \
         deleted_failed(retried={}, skipped={}) \
         desync(reconciled={}, deleted={}, logged={}) temp_objects_deleted={}",
        s.confirmed_orphan_deleted,
        s.confirmed_orphan_skipped,
        s.reservation_reclaimed,
        s.promoting_reverted,
        s.deleting_reverted,
        s.deleted_failed_retried,
        s.deleted_failed_skipped,
        s.desync_reconciled,
        s.desync_deleted,
        s.desync_logged,
        s.temp_objects_deleted,
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row_with_sha(sha: Option<&str>) -> MediaObjectRow {
        MediaObjectRow {
            id: 1,
            kind: 2,
            media_type: "image/png".into(),
            byte_size: Some(1),
            sha256: sha.map(|s| s.to_string()),
            width: None,
            height: None,
            duration_ms: None,
            storage_backend: "s3".into(),
            storage_uri: None,
            alt: None,
            ref_count: 0,
            gc_state: 1,
            metadata: None,
            created_at: 0,
            updated_at: 0,
        }
    }

    fn desync(gc_state: i32, db: i64, actual: i64) -> RefCountDesync {
        RefCountDesync {
            id: 7,
            db_ref_count: db,
            actual_ref_count: actual,
            gc_state,
            storage_backend: "s3".into(),
        }
    }

    #[test]
    fn resolve_grace_cli_wins() {
        assert_eq!(resolve_grace_sec(Some(42), Some("999".into())), 42);
    }

    #[test]
    fn resolve_grace_env_used_when_no_cli() {
        assert_eq!(resolve_grace_sec(None, Some("777".into())), 777);
    }

    #[test]
    fn resolve_grace_bad_env_falls_back_to_default() {
        assert_eq!(
            resolve_grace_sec(None, Some("not-a-number".into())),
            DEFAULT_GRACE_SEC
        );
    }

    #[test]
    fn resolve_grace_both_none_is_default() {
        assert_eq!(resolve_grace_sec(None, None), DEFAULT_GRACE_SEC);
    }

    #[test]
    fn resolve_grace_env_trimmed() {
        assert_eq!(resolve_grace_sec(None, Some("  60 ".into())), 60);
    }

    #[test]
    fn compute_before_subtracts_grace() {
        assert_eq!(compute_before(10_000, 3), 10_000 - 3_000);
    }

    #[test]
    fn compute_before_zero_grace_is_now() {
        assert_eq!(compute_before(10_000, 0), 10_000);
    }

    #[test]
    fn compute_before_huge_grace_saturates() {
        // An absurd grace must clamp to the far past, not wrap positive.
        assert_eq!(compute_before(0, u64::MAX), 0i64.saturating_sub(i64::MAX));
    }

    #[test]
    fn reservation_plan_with_sha_derives_final_key() {
        let row = row_with_sha(Some("abcd1234"));
        assert_eq!(
            reservation_recovery_plan(&row, "memories/"),
            ReservationPlan::DeleteFinalThenRow {
                final_key: "memories/ab/cd/abcd1234".into()
            }
        );
    }

    #[test]
    fn reservation_plan_without_sha_is_row_only() {
        assert_eq!(
            reservation_recovery_plan(&row_with_sha(None), "memories/"),
            ReservationPlan::DeleteRowOnlyNoSha
        );
    }

    #[test]
    fn reservation_plan_empty_sha_is_row_only() {
        assert_eq!(
            reservation_recovery_plan(&row_with_sha(Some("")), "memories/"),
            ReservationPlan::DeleteRowOnlyNoSha
        );
    }

    #[test]
    fn promoting_plan_with_sha_derives_final_key() {
        let row = row_with_sha(Some("abcd1234"));
        assert_eq!(
            promoting_recovery_plan(&row, "memories/"),
            PromotingPlan::DeleteFinalThenRevert {
                final_key: "memories/ab/cd/abcd1234".into()
            }
        );
    }

    #[test]
    fn promoting_plan_without_sha_is_revert_only() {
        assert_eq!(
            promoting_recovery_plan(&row_with_sha(None), "memories/"),
            PromotingPlan::RevertOnlyNoSha
        );
    }

    #[test]
    fn desync_active_nonzero_reconciles_to_actual() {
        assert_eq!(
            desync_action(&desync(0, 5, 2)),
            DesyncAction::Reconcile { to: 2 }
        );
    }

    #[test]
    fn desync_orphan_nonzero_reconciles_to_actual() {
        assert_eq!(
            desync_action(&desync(1, 3, 1)),
            DesyncAction::Reconcile { to: 1 }
        );
    }

    #[test]
    fn desync_zero_actual_reconciles_then_deletes() {
        assert_eq!(
            desync_action(&desync(0, 2, 0)),
            DesyncAction::ReconcileThenDelete
        );
    }

    #[test]
    fn desync_negative_actual_is_error_log() {
        match desync_action(&desync(0, 1, -1)) {
            DesyncAction::LogOnly { error, .. } => assert!(error),
            other => panic!("expected error LogOnly, got {other:?}"),
        }
    }

    #[test]
    fn desync_dedicated_gc_states_are_log_only() {
        for gc in [2, 3, 4, 5] {
            match desync_action(&desync(gc, 9, 0)) {
                DesyncAction::LogOnly { error, .. } => assert!(!error),
                other => panic!("expected warn LogOnly for gc={gc}, got {other:?}"),
            }
        }
    }

    // --- sweep_* end-to-end over an in-process inline backend + a real
    // sqlite test pool. The schema lives in the infra crate, so the
    // migrator dir is crate-relative (`../infra/sql/sqlite`); the test DB
    // file is grpc-admin-local so it never races the infra test pool.
    mod e2e {
        use super::super::*;
        use infra::infra::UseIdGenerator;
        use infra::infra::media_object::rdb::{MediaObjectRepository, MediaObjectReservation};
        use infra::infra::media_storage::StorageBackend;
        use infra::infra::media_storage::inline::InlineMediaStorage;
        use infra::test_helper::shared_id_generator;
        use infra_utils::infra::rdb::RdbPool;
        use infra_utils::infra::test::{TEST_RUNTIME, setup_test_rdb_from};
        use std::sync::Arc;

        #[cfg(feature = "postgres")]
        const PARAM1: &str = "$1";
        #[cfg(not(feature = "postgres"))]
        const PARAM1: &str = "?";

        async fn pool() -> &'static RdbPool {
            #[cfg(feature = "postgres")]
            {
                setup_test_rdb_from("../infra/sql/postgres").await
            }
            #[cfg(not(feature = "postgres"))]
            {
                setup_test_rdb_from("../infra/sql/sqlite").await
            }
        }

        fn repo(p: &'static RdbPool) -> MediaObjectRepositoryImpl {
            MediaObjectRepositoryImpl::new(shared_id_generator(), p)
        }

        fn media_app(p: &'static RdbPool) -> MediaAppImpl {
            let storage = Arc::new(StorageBackend::Inline(InlineMediaStorage::new()));
            MediaAppImpl::new(
                MediaObjectRepositoryImpl::new(shared_id_generator(), p),
                storage,
                shared_id_generator(),
                "memories/".to_string(),
                900,
                20_971_520,
            )
        }

        fn inline_backend() -> StorageBackend {
            StorageBackend::Inline(InlineMediaStorage::new())
        }

        fn cli(dry_run: bool) -> Cli {
            Cli {
                dry_run,
                grace_sec: None,
            }
        }

        fn reservation(id: i64, sha: &str) -> MediaObjectReservation {
            let now = command_utils::util::datetime::now_millis();
            MediaObjectReservation {
                id,
                kind: 2,
                media_type: "image/png".into(),
                byte_size: Some(1),
                sha256: sha.into(),
                width: None,
                height: None,
                duration_ms: None,
                alt: None,
                created_at: now,
                updated_at: now,
            }
        }

        async fn age_out(p: &RdbPool, id: i64) {
            sqlx::query::<infra_utils::infra::rdb::Rdb>(sqlx::AssertSqlSafe(format!(
                "UPDATE media_object SET created_at = 1, updated_at = 1 WHERE id = {PARAM1}"
            )))
            .bind(id)
            .execute(p)
            .await
            .unwrap();
        }

        /// A confirmed orphan past grace is claimed and the (inline) DB
        /// row is deleted; the immediate ref_count path never ran.
        #[test]
        fn confirmed_orphan_is_reclaimed() {
            TEST_RUNTIME.block_on(async {
                let p = pool().await;
                let r = repo(p);
                let app = media_app(p);
                let id = r.id_generator().generate_id().unwrap();
                let sha = format!("sha_e2e_co_{id}");

                let mut tx = p.begin().await.unwrap();
                r.insert_reservation_tx(&mut *tx, &reservation(id, &sha))
                    .await
                    .unwrap();
                r.confirm_reservation_tx(&mut *tx, id, "inline://k", "inline", Some(1), None)
                    .await
                    .unwrap();
                tx.commit().await.unwrap();
                age_out(p, id).await;

                let before = compute_before(command_utils::util::datetime::now_millis(), 3600);
                let mut stats = Stats::default();
                sweep_confirmed_orphans(&cli(false), &r, &app, before, &mut stats)
                    .await
                    .unwrap();
                // Assert this row's outcome, not a global tally: the
                // postgres test pool is a shared singleton (not
                // truncated between tests) so other tests' rows can be
                // swept in the same call.
                assert!(stats.confirmed_orphan_deleted >= 1);
                assert!(r.find_by_id(id).await.unwrap().is_none());
            });
        }

        /// dry-run tallies the confirmed orphan but leaves the row.
        #[test]
        fn dry_run_does_not_delete() {
            TEST_RUNTIME.block_on(async {
                let p = pool().await;
                let r = repo(p);
                let app = media_app(p);
                let id = r.id_generator().generate_id().unwrap();
                let sha = format!("sha_e2e_dry_{id}");

                let mut tx = p.begin().await.unwrap();
                r.insert_reservation_tx(&mut *tx, &reservation(id, &sha))
                    .await
                    .unwrap();
                r.confirm_reservation_tx(&mut *tx, id, "inline://k", "inline", Some(1), None)
                    .await
                    .unwrap();
                tx.commit().await.unwrap();
                age_out(p, id).await;

                let before = compute_before(command_utils::util::datetime::now_millis(), 3600);
                let mut stats = Stats::default();
                sweep_confirmed_orphans(&cli(true), &r, &app, before, &mut stats)
                    .await
                    .unwrap();
                assert!(stats.confirmed_orphan_deleted >= 1);
                assert!(
                    r.find_by_id(id).await.unwrap().is_some(),
                    "dry-run must not delete the row"
                );
            });
        }

        /// An aged-out unconfirmed reservation (storage_uri NULL) is
        /// dropped (DB row), independent of any leaked final.
        #[test]
        fn stale_reservation_row_is_dropped() {
            TEST_RUNTIME.block_on(async {
                let p = pool().await;
                let r = repo(p);
                let backend = inline_backend();
                let mcfg = MediaConfig {
                    backend: "inline".into(),
                    s3_prefix: "memories/".into(),
                    presign_ttl_sec: 900,
                    upload_max_bytes: 20_971_520,
                };
                let id = r.id_generator().generate_id().unwrap();
                let sha = format!("sha_e2e_resv_{id}");

                let mut tx = p.begin().await.unwrap();
                r.insert_reservation_tx(&mut *tx, &reservation(id, &sha))
                    .await
                    .unwrap();
                tx.commit().await.unwrap();
                age_out(p, id).await;

                let before = compute_before(command_utils::util::datetime::now_millis(), 3600);
                let mut stats = Stats::default();
                sweep_reservations(&cli(false), &r, &backend, &mcfg, before, &mut stats)
                    .await
                    .unwrap();
                assert!(stats.reservation_reclaimed >= 1);
                assert!(r.find_by_id(id).await.unwrap().is_none());
            });
        }

        /// A stale deleting row (5) is reverted to orphan (1) — the next
        /// sweep's confirmed-orphan path then reclaims it.
        #[test]
        fn stale_deleting_is_reverted_to_orphan() {
            TEST_RUNTIME.block_on(async {
                let p = pool().await;
                let r = repo(p);
                let id = r.id_generator().generate_id().unwrap();
                let sha = format!("sha_e2e_del_{id}");

                let mut tx = p.begin().await.unwrap();
                r.insert_reservation_tx(&mut *tx, &reservation(id, &sha))
                    .await
                    .unwrap();
                r.confirm_reservation_tx(&mut *tx, id, "inline://k", "inline", Some(1), None)
                    .await
                    .unwrap();
                r.incr_ref_tx(&mut *tx, id).await.unwrap();
                r.decr_ref_tx(&mut *tx, id).await.unwrap();
                r.claim_deleting_tx(&mut *tx, id).await.unwrap();
                tx.commit().await.unwrap();
                age_out(p, id).await;

                let before = compute_before(command_utils::util::datetime::now_millis(), 3600);
                let mut stats = Stats::default();
                sweep_deleting(&cli(false), &r, before, &mut stats)
                    .await
                    .unwrap();
                assert!(stats.deleting_reverted >= 1);
                assert_eq!(r.find_by_id(id).await.unwrap().unwrap().gc_state, 1);
            });
        }

        /// ref_count desync with zero real references: reconcile to 0
        /// then the row joins the delete flow and is removed.
        #[test]
        fn desync_zero_refs_reconciles_and_deletes() {
            TEST_RUNTIME.block_on(async {
                let p = pool().await;
                let r = repo(p);
                let app = media_app(p);
                let id = r.id_generator().generate_id().unwrap();
                let sha = format!("sha_e2e_ds_{id}");

                let mut tx = p.begin().await.unwrap();
                r.insert_reservation_tx(&mut *tx, &reservation(id, &sha))
                    .await
                    .unwrap();
                r.confirm_reservation_tx(&mut *tx, id, "inline://k", "inline", Some(1), None)
                    .await
                    .unwrap();
                r.incr_ref_tx(&mut *tx, id).await.unwrap(); // ref=1, 0 memory
                tx.commit().await.unwrap();

                let mut stats = Stats::default();
                sweep_ref_count_desync(&cli(false), &r, &app, &mut stats)
                    .await
                    .unwrap();
                assert!(stats.desync_deleted >= 1);
                assert!(r.find_by_id(id).await.unwrap().is_none());
            });
        }

        /// inline backend has no temp residue (in-process buffers vanish
        /// on restart): the scan is a no-op, never errors.
        #[test]
        fn temp_residue_inline_is_noop() {
            TEST_RUNTIME.block_on(async {
                let backend = inline_backend();
                let mcfg = MediaConfig {
                    backend: "inline".into(),
                    s3_prefix: "memories/".into(),
                    presign_ttl_sec: 900,
                    upload_max_bytes: 20_971_520,
                };
                let mut stats = Stats::default();
                sweep_temp_residue(&cli(false), &backend, &mcfg, 3600, &mut stats)
                    .await
                    .unwrap();
                assert_eq!(stats.temp_objects_deleted, 0);
            });
        }
    }
}
