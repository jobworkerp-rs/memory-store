//! Backfill thread message extrema during a controlled maintenance window.
//!
//! The command deliberately keeps existing thread audit timestamps unless an
//! extrema value changes. This is the only safe behaviour for historical rows:
//! their original record creation time cannot be reconstructed from imports.

use anyhow::{Context, Result, bail};
use app::app::thread_vector::ThreadVectorAppImpl;
use clap::{Parser, ValueEnum};
use infra::infra::module::RepositoryModule;
use infra::infra::thread::rdb::ThreadRepository;
use infra::infra::thread_vector::config::ThreadVectorDBConfig;
use infra::infra::thread_vector::repository::ThreadVectorRepositoryImpl;
use infra_utils::infra::rdb::UseRdbPool;
use protobuf::llm_memory::data::ThreadId;

const MIGRATION_KEY: &str = "thread-time-fields-v1";

#[derive(Debug, Clone)]
struct MigrationState {
    rdb_completed_at: Option<i64>,
    vector_status: String,
    staging_table_name: Option<String>,
    vector_completed_at: Option<i64>,
}

type MigrationStateRow = (Option<i64>, String, Option<String>, Option<i64>);

#[derive(Debug, Parser)]
#[command(name = "migrate-thread-time-fields")]
struct Cli {
    /// Persist the calculated extrema. Without this flag the command is dry-run.
    #[arg(long, conflicts_with = "dry_run")]
    apply: bool,
    /// Explicitly select dry-run mode. This is the default when neither
    /// mode flag is supplied.
    #[arg(long, conflicts_with = "apply")]
    dry_run: bool,
    /// Acknowledge that normal writers are stopped for this maintenance window.
    #[arg(long)]
    maintenance_window_ack: bool,
    /// Threads processed per transaction.
    #[arg(long, default_value_t = 500)]
    batch_size: i32,
    /// Migration phase. `all` backfills RDB extrema, then synchronizes
    /// existing ThreadVector rows and verifies the RDB result.
    #[arg(long, value_enum, default_value_t = Phase::All)]
    phase: Phase,
    /// Require a ThreadVector table even when no explicit vector connection
    /// configuration is present.
    #[arg(long)]
    thread_vector_required: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum Phase {
    All,
    Rdb,
    Vector,
    Verify,
}

#[derive(Default)]
struct Stats {
    scanned: u64,
    empty: u64,
    changes: u64,
    invalid: u64,
}

#[cfg(feature = "postgres")]
const BOUNDS_SQL: &str = "SELECT MIN(memory.created_at), MAX(memory.created_at), COUNT(*) \
    FROM thread_memory JOIN memory ON memory.id = thread_memory.memory_id WHERE thread_memory.thread_id = $1";
#[cfg(not(feature = "postgres"))]
const BOUNDS_SQL: &str = "SELECT MIN(memory.created_at), MAX(memory.created_at), COUNT(*) \
    FROM thread_memory JOIN memory ON memory.id = thread_memory.memory_id WHERE thread_memory.thread_id = ?";

#[cfg(feature = "postgres")]
const INVALID_MEMORY_SQL: &str = "SELECT thread_memory.thread_id FROM thread_memory \
    LEFT JOIN memory ON memory.id = thread_memory.memory_id \
    WHERE memory.id IS NULL OR memory.created_at <= 0 LIMIT 10";

#[cfg(feature = "postgres")]
const INSERT_STATE_SQL: &str = "INSERT INTO thread_time_migration_state \
    (migration_key, rdb_completed_at, vector_status, staging_table_name, vector_completed_at) \
    VALUES ($1, NULL, 'PENDING', NULL, NULL) ON CONFLICT (migration_key) DO NOTHING";
#[cfg(not(feature = "postgres"))]
const INSERT_STATE_SQL: &str = "INSERT OR IGNORE INTO thread_time_migration_state \
    (migration_key, rdb_completed_at, vector_status, staging_table_name, vector_completed_at) \
    VALUES (?, NULL, 'PENDING', NULL, NULL)";
#[cfg(feature = "postgres")]
const LOAD_STATE_SQL: &str = "SELECT rdb_completed_at, vector_status, staging_table_name, vector_completed_at \
    FROM thread_time_migration_state WHERE migration_key = $1";
#[cfg(not(feature = "postgres"))]
const LOAD_STATE_SQL: &str = "SELECT rdb_completed_at, vector_status, staging_table_name, vector_completed_at \
    FROM thread_time_migration_state WHERE migration_key = ?";
#[cfg(feature = "postgres")]
const MARK_RDB_COMPLETED_SQL: &str = "UPDATE thread_time_migration_state SET rdb_completed_at = $1 \
    WHERE migration_key = $2";
#[cfg(not(feature = "postgres"))]
const MARK_RDB_COMPLETED_SQL: &str = "UPDATE thread_time_migration_state SET rdb_completed_at = ? \
    WHERE migration_key = ?";
#[cfg(feature = "postgres")]
const MARK_VECTOR_SQL: &str = "UPDATE thread_time_migration_state \
    SET vector_status = $1, staging_table_name = NULL, vector_completed_at = $2 WHERE migration_key = $3";
#[cfg(not(feature = "postgres"))]
const MARK_VECTOR_SQL: &str = "UPDATE thread_time_migration_state \
    SET vector_status = ?, staging_table_name = NULL, vector_completed_at = ? WHERE migration_key = ?";
#[cfg(feature = "postgres")]
const MARK_VECTOR_STAGE_SQL: &str = "UPDATE thread_time_migration_state \
    SET vector_status = $1, staging_table_name = $2, vector_completed_at = NULL WHERE migration_key = $3";
#[cfg(not(feature = "postgres"))]
const MARK_VECTOR_STAGE_SQL: &str = "UPDATE thread_time_migration_state \
    SET vector_status = ?, staging_table_name = ?, vector_completed_at = NULL WHERE migration_key = ?";
#[cfg(not(feature = "postgres"))]
const INVALID_MEMORY_SQL: &str = "SELECT thread_memory.thread_id FROM thread_memory \
    LEFT JOIN memory ON memory.id = thread_memory.memory_id \
    WHERE memory.id IS NULL OR memory.created_at <= 0 LIMIT 10";

fn explicit_thread_vector_config(required: bool) -> Result<Option<ThreadVectorDBConfig>> {
    let keys = [
        "THREAD_LANCEDB_URI",
        "THREAD_LANCEDB_TABLE",
        "THREAD_VECTOR_SIZE",
    ];
    let present = keys.map(|key| std::env::var_os(key).is_some());
    if present.iter().any(|value| *value) && !present.iter().all(|value| *value) {
        bail!(
            "THREAD_LANCEDB_URI, THREAD_LANCEDB_TABLE, and THREAD_VECTOR_SIZE must be set together for vector migration"
        );
    }
    if !present.iter().all(|value| *value) {
        if required {
            bail!(
                "--thread-vector-required requires THREAD_LANCEDB_URI, THREAD_LANCEDB_TABLE, and THREAD_VECTOR_SIZE"
            );
        }
        return Ok(None);
    }
    ThreadVectorDBConfig::from_env().map(Some)
}

fn runs_rdb(phase: Phase) -> bool {
    matches!(phase, Phase::All | Phase::Rdb)
}

fn runs_vector(phase: Phase) -> bool {
    matches!(phase, Phase::All | Phase::Vector)
}

fn runs_verify(phase: Phase) -> bool {
    matches!(phase, Phase::All | Phase::Verify)
}

async fn load_migration_state(
    pool: &infra_utils::infra::rdb::RdbPool,
) -> Result<Option<MigrationState>> {
    let row: Option<MigrationStateRow> = sqlx::query_as(LOAD_STATE_SQL)
        .bind(MIGRATION_KEY)
        .fetch_optional(pool)
        .await
        .context("loading thread time migration state")?;
    Ok(row.map(
        |(rdb_completed_at, vector_status, staging_table_name, vector_completed_at)| {
            MigrationState {
                rdb_completed_at,
                vector_status,
                staging_table_name,
                vector_completed_at,
            }
        },
    ))
}

async fn ensure_migration_state(pool: &infra_utils::infra::rdb::RdbPool) -> Result<()> {
    sqlx::query(INSERT_STATE_SQL)
        .bind(MIGRATION_KEY)
        .execute(pool)
        .await
        .context("creating thread time migration state")?;
    Ok(())
}

async fn mark_rdb_completed(pool: &infra_utils::infra::rdb::RdbPool) -> Result<()> {
    sqlx::query(MARK_RDB_COMPLETED_SQL)
        .bind(command_utils::util::datetime::now_millis())
        .bind(MIGRATION_KEY)
        .execute(pool)
        .await
        .context("recording successful RDB migration")?;
    Ok(())
}

async fn mark_vector_status(pool: &infra_utils::infra::rdb::RdbPool, status: &str) -> Result<()> {
    sqlx::query(MARK_VECTOR_SQL)
        .bind(status)
        .bind(command_utils::util::datetime::now_millis())
        .bind(MIGRATION_KEY)
        .execute(pool)
        .await
        .context("recording ThreadVector migration status")?;
    Ok(())
}

async fn mark_vector_stage(
    pool: &infra_utils::infra::rdb::RdbPool,
    status: &str,
    staging_table_name: &str,
) -> Result<()> {
    sqlx::query(MARK_VECTOR_STAGE_SQL)
        .bind(status)
        .bind(staging_table_name)
        .bind(MIGRATION_KEY)
        .execute(pool)
        .await
        .context("recording ThreadVector migration stage")?;
    Ok(())
}

fn require_rdb_completed_for_vector(state: Option<&MigrationState>) -> Result<()> {
    if state.is_none_or(|state| state.rdb_completed_at.is_none()) {
        bail!("vector phase requires a successfully completed RDB phase");
    }
    Ok(())
}

fn require_completed_state_for_verify(
    state: Option<&MigrationState>,
    vector_required: bool,
) -> Result<()> {
    let state = state.ok_or_else(|| anyhow::anyhow!("verify phase requires migration state"))?;
    if state.rdb_completed_at.is_none() {
        bail!("verify phase requires a successfully completed RDB phase");
    }
    let expected_vector_status = if vector_required {
        "COMPLETED"
    } else {
        "NOT_REQUIRED"
    };
    if state.vector_status != expected_vector_status {
        bail!(
            "verify phase requires vector status {expected_vector_status}, found {}",
            state.vector_status
        );
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let cli = Cli::parse();
    if !cli.maintenance_window_ack {
        bail!("--maintenance-window-ack is required");
    }
    if cli.batch_size <= 0 {
        bail!("--batch-size must be positive");
    }
    let mode = match (cli.apply, cli.dry_run) {
        (true, false) => "apply",
        (false, true) | (false, false) => "dry-run",
        (true, true) => unreachable!("clap rejects --apply with --dry-run"),
    };
    println!(
        "migrate-thread-time-fields: mode={mode} phase={:?}",
        cli.phase
    );

    let repositories = RepositoryModule::new_by_env().await;
    let thread_repo = repositories.create_thread_repository();
    let pool = thread_repo.db_pool();
    if cli.apply {
        ensure_migration_state(pool).await?;
    }
    let mut migration_state = load_migration_state(pool).await?;
    let invalid_ids: Vec<i64> = sqlx::query_scalar(INVALID_MEMORY_SQL)
        .fetch_all(pool)
        .await
        .context("checking dangling thread_memory rows and invalid memory timestamps")?;
    if !invalid_ids.is_empty() {
        bail!("invalid thread_memory or memory.created_at <= 0; sample thread_ids={invalid_ids:?}");
    }
    let vector_config = explicit_thread_vector_config(cli.thread_vector_required)?;
    let vector_is_configured = vector_config.is_some();
    let mut stats = Stats::default();
    if runs_rdb(cli.phase) {
        // Snapshot IDs before changing `last_message_at`: pagination by the
        // mutable list order would otherwise skip or duplicate rows.
        let rdb_ids = thread_repo.find_all_thread_ids().await?;
        for ids in rdb_ids.chunks(cli.batch_size as usize) {
            let mut tx = pool.begin().await.context("begin migration batch")?;
            for thread_id in ids {
                let id = ThreadId { value: *thread_id };
                let thread = thread_repo.find(&id).await?.ok_or_else(|| {
                    anyhow::anyhow!("thread disappeared during migration: {}", id.value)
                })?;
                let data = thread.data.context("thread row without data")?;
                let (first, last, count): (Option<i64>, Option<i64>, i64) =
                    sqlx::query_as(BOUNDS_SQL)
                        .bind(id.value)
                        .fetch_one(&mut *tx)
                        .await
                        .context("calculating message bounds")?;
                stats.scanned += 1;
                if count == 0 {
                    stats.empty += 1;
                }
                if first.is_some_and(|value| value <= 0) || last.is_some_and(|value| value <= 0) {
                    bail!(
                        "invalid memory.created_at for thread_id={} (invalid_count={})",
                        id.value,
                        stats.invalid + 1
                    );
                }
                if data.first_message_at != first || data.last_message_at != last {
                    stats.changes += 1;
                    if cli.apply {
                        thread_repo
                            .backfill_message_bounds_preserving_audit_tx(&mut tx, &id)
                            .await?;
                    }
                }
            }
            if cli.apply {
                tx.commit().await.context("commit migration batch")?;
            } else {
                tx.rollback().await.context("rollback dry-run batch")?;
            }
            println!(
                "migrate-thread-time-fields: mode={mode} scanned={} empty={} extrema_changed={} invalid={}",
                stats.scanned, stats.empty, stats.changes, stats.invalid
            );
        }
    }
    if runs_rdb(cli.phase) {
        println!(
            "migrate-thread-time-fields: rdb_complete scanned={} empty={} extrema_changed={} invalid={}",
            stats.scanned, stats.empty, stats.changes, stats.invalid
        );
        if cli.apply {
            mark_rdb_completed(pool).await?;
            migration_state = load_migration_state(pool).await?;
        }
    }

    if runs_vector(cli.phase) {
        if cli.apply || cli.phase == Phase::Vector {
            require_rdb_completed_for_vector(migration_state.as_ref())?;
        }
        match vector_config {
            None => {
                println!(
                    "migrate-thread-time-fields: vector_status=not_required explicit vector configuration is absent"
                );
                if cli.apply {
                    mark_vector_status(pool, "NOT_REQUIRED").await?;
                    migration_state = load_migration_state(pool).await?;
                }
            }
            Some(config) if !cli.apply => {
                let table_exists = ThreadVectorRepositoryImpl::table_exists(&config)
                    .await
                    .context("checking ThreadVector table existence")?;
                if table_exists || cli.thread_vector_required {
                    println!(
                        "migrate-thread-time-fields: vector_status=planned table={}",
                        config.table_name
                    );
                } else {
                    println!(
                        "migrate-thread-time-fields: vector_status=not_required table={} does not exist",
                        config.table_name
                    );
                }
            }
            Some(config) => {
                let table_name = config.table_name.clone();
                let table_exists = ThreadVectorRepositoryImpl::table_exists(&config)
                    .await
                    .context("checking ThreadVector table existence")?;
                let staging_name = format!("{table_name}__thread_time_fields_v1_staging");
                let resumable_stage = migration_state.as_ref().is_some_and(|state| {
                    matches!(state.vector_status.as_str(), "STAGED" | "SWITCHING")
                        && state.staging_table_name.as_deref() == Some(staging_name.as_str())
                });
                if !table_exists && !cli.thread_vector_required && !resumable_stage {
                    println!(
                        "migrate-thread-time-fields: vector_status=not_required table={table_name} does not exist"
                    );
                    mark_vector_status(pool, "NOT_REQUIRED").await?;
                    migration_state = load_migration_state(pool).await?;
                } else {
                    let ids = thread_repo.find_all_thread_ids().await?;
                    let rdb_ids: std::collections::HashSet<i64> = ids.iter().copied().collect();
                    if !resumable_stage {
                        ThreadVectorRepositoryImpl::drop_table_if_exists(&config, &staging_name)
                            .await?;
                        let source = ThreadVectorRepositoryImpl::new(config.clone())
                            .await
                            .context("opening canonical ThreadVector table")?;
                        let mut staging_config = config.clone();
                        staging_config.table_name = staging_name.clone();
                        let staging = ThreadVectorRepositoryImpl::new(staging_config)
                            .await
                            .context("creating ThreadVector staging table")?;
                        let mut copied_rows = 0usize;
                        for source_id in source.get_all_thread_ids().await? {
                            if !rdb_ids.contains(&source_id) {
                                continue;
                            }
                            copied_rows += staging
                                .batch_upsert(source.find_records_by_thread_id(source_id).await?)
                                .await?;
                        }
                        mark_vector_stage(pool, "STAGED", &staging_name).await?;
                        println!(
                            "migrate-thread-time-fields: vector_status=staged table={table_name} staging_table={staging_name} copied_rows={copied_rows}"
                        );
                    }

                    mark_vector_stage(pool, "SWITCHING", &staging_name).await?;
                    ThreadVectorRepositoryImpl::replace_table_from_staging(&config, &staging_name)
                        .await?;
                    let vector_repo = ThreadVectorRepositoryImpl::new(config.clone())
                        .await
                        .context("opening rebuilt canonical ThreadVector table")?;
                    let vector_app = ThreadVectorAppImpl::new(
                        repositories.create_thread_repository(),
                        repositories.create_thread_label_repository(),
                        vector_repo,
                        None,
                    );
                    for id in &ids {
                        vector_app.sync_thread_scalars(*id).await.with_context(|| {
                            format!("syncing ThreadVector scalar for thread_id={id}")
                        })?;
                    }
                    vector_app
                        .rebuild_search_indexes()
                        .await
                        .context("rebuilding ThreadVector FTS/ANN indexes after cutover")?;
                    ThreadVectorRepositoryImpl::drop_table_if_exists(&config, &staging_name)
                        .await?;
                    println!(
                        "migrate-thread-time-fields: vector_status=completed table={table_name} scalar_sync_threads={} search_indexes=rebuilt",
                        ids.len()
                    );
                    mark_vector_status(pool, "COMPLETED").await?;
                    migration_state = load_migration_state(pool).await?;
                }
            }
        }
    }

    if runs_verify(cli.phase) && !cli.apply && cli.phase != Phase::Verify {
        println!("migrate-thread-time-fields: verify_skipped=dry_run_requires_apply");
    } else if runs_verify(cli.phase) {
        if cli.apply || cli.phase == Phase::Verify {
            require_completed_state_for_verify(
                migration_state.as_ref(),
                vector_is_configured || cli.thread_vector_required,
            )?;
            let state = migration_state.as_ref().expect("verified state exists");
            println!(
                "migrate-thread-time-fields: state rdb_completed_at={:?} vector_status={} staging_table_name={:?} vector_completed_at={:?}",
                state.rdb_completed_at,
                state.vector_status,
                state.staging_table_name,
                state.vector_completed_at
            );
        }
        let mut verify_offset = 0i64;
        let mut verified = 0u64;
        loop {
            let page = thread_repo
                .find_list(Some(&cli.batch_size), Some(&verify_offset))
                .await
                .context("listing threads for verification")?;
            if page.is_empty() {
                break;
            }
            verify_offset += page.len() as i64;
            for thread in page {
                let id = thread.id.context("thread row without id")?;
                let data = thread.data.context("thread row without data")?;
                let (first, last, _): (Option<i64>, Option<i64>, i64) = sqlx::query_as(BOUNDS_SQL)
                    .bind(id.value)
                    .fetch_one(pool)
                    .await?;
                if data.first_message_at != first || data.last_message_at != last {
                    bail!("verification failed for thread_id={}", id.value);
                }
                verified += 1;
            }
        }
        println!("migrate-thread-time-fields: verify_complete threads={verified}");
    }
    if cli.apply {
        println!(
            "migrate-thread-time-fields: committed_threads={}",
            stats.scanned
        );
    } else {
        println!("migrate-thread-time-fields: no database changes were made");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        Cli, MigrationState, Phase, explicit_thread_vector_config,
        require_completed_state_for_verify, require_rdb_completed_for_vector, runs_rdb,
        runs_vector, runs_verify,
    };
    use clap::Parser;

    #[test]
    fn phase_selection_is_disjoint_except_all() {
        assert!(runs_rdb(Phase::All));
        assert!(runs_vector(Phase::All));
        assert!(runs_verify(Phase::All));
        assert!(runs_rdb(Phase::Rdb));
        assert!(!runs_vector(Phase::Rdb));
        assert!(!runs_verify(Phase::Rdb));
    }

    #[test]
    fn command_defaults_to_dry_run_all_phase() {
        let cli = Cli::try_parse_from(["migrate-thread-time-fields", "--maintenance-window-ack"])
            .expect("arguments must parse");
        assert!(!cli.apply);
        assert_eq!(cli.phase, Phase::All);
    }

    #[test]
    fn apply_and_dry_run_are_mutually_exclusive() {
        assert!(
            Cli::try_parse_from([
                "migrate-thread-time-fields",
                "--maintenance-window-ack",
                "--apply",
                "--dry-run",
            ])
            .is_err()
        );
    }

    #[test]
    fn vector_requirement_needs_explicit_connection_settings() {
        let saved: Vec<(&str, Option<std::ffi::OsString>)> = [
            "THREAD_LANCEDB_URI",
            "THREAD_LANCEDB_TABLE",
            "THREAD_VECTOR_SIZE",
        ]
        .into_iter()
        .map(|key| (key, std::env::var_os(key)))
        .collect();
        unsafe {
            for (key, _) in &saved {
                std::env::remove_var(key);
            }
        }
        assert!(explicit_thread_vector_config(true).is_err());
        unsafe {
            for (key, value) in saved {
                if let Some(value) = value {
                    std::env::set_var(key, value);
                }
            }
        }
    }

    fn state(rdb_completed_at: Option<i64>, vector_status: &str) -> MigrationState {
        MigrationState {
            rdb_completed_at,
            vector_status: vector_status.to_string(),
            staging_table_name: None,
            vector_completed_at: None,
        }
    }

    #[test]
    fn vector_phase_requires_completed_rdb_state() {
        assert!(require_rdb_completed_for_vector(None).is_err());
        assert!(require_rdb_completed_for_vector(Some(&state(None, "PENDING"))).is_err());
        assert!(require_rdb_completed_for_vector(Some(&state(Some(1), "PENDING"))).is_ok());
    }

    #[test]
    fn verify_requires_matching_vector_completion_state() {
        assert!(require_completed_state_for_verify(None, false).is_err());
        assert!(
            require_completed_state_for_verify(Some(&state(None, "NOT_REQUIRED")), false).is_err()
        );
        assert!(
            require_completed_state_for_verify(Some(&state(Some(1), "PENDING")), false).is_err()
        );
        assert!(
            require_completed_state_for_verify(Some(&state(Some(1), "NOT_REQUIRED")), false)
                .is_ok()
        );
        assert!(
            require_completed_state_for_verify(Some(&state(Some(1), "COMPLETED")), true).is_ok()
        );
        assert!(
            require_completed_state_for_verify(Some(&state(Some(1), "NOT_REQUIRED")), true)
                .is_err()
        );
    }
}
