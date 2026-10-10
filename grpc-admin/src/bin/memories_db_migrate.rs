//! Release schema-migration adapter and post-schema task coordinator.

use anyhow::{Context, Result, bail};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use clap::{Args, Parser, Subcommand};
use grpc_admin::db_migrate::{
    catalog::{self},
    local::output::{ErrorCode, LocalFailure, Resolution},
    state::{self, TaskStateKind},
    task_from_catalog,
};
use infra_utils::infra::rdb::{Rdb, RdbPool};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::fs;
use std::process::Stdio;
use url::Url;

// Crate-root modules resolve next to the root file, where Cargo would treat
// them as separate binaries; the path keeps them under this binary's name.
#[path = "memories_db_migrate/bundle_command.rs"]
mod bundle_command;
#[cfg(not(feature = "postgres"))]
#[path = "memories_db_migrate/local_command.rs"]
mod local_command;

#[path = "memories_db_migrate/embedding_command.rs"]
mod embedding_command;

#[path = "memories_db_migrate/embedding_migrate.rs"]
mod embedding_migrate;

#[path = "memories_db_migrate/embedding_finish.rs"]
mod embedding_finish;

const ATLAS_TOOL_LOCK_FILE: &str = "atlas-tool.lock.json";
#[cfg(feature = "postgres")]
const ATLAS_SUM: &str = include_str!("../../../infra/atlas/postgres/migrations/atlas.sum");
#[cfg(not(feature = "postgres"))]
const ATLAS_SUM: &str = include_str!("../../../infra/atlas/sqlite/migrations/atlas.sum");

#[derive(Debug, Deserialize)]
struct AtlasToolLock {
    version: String,
    platforms: std::collections::BTreeMap<String, AtlasToolPlatform>,
}

#[derive(Debug, Deserialize)]
struct AtlasToolPlatform {
    url: String,
    sha256: String,
}

#[derive(Debug, Deserialize)]
struct SeedExpectations {
    version: String,
    tables: Vec<SeedExpectation>,
}

#[derive(Debug, Deserialize)]
struct SeedExpectation {
    table: String,
    key_column: String,
    keys: Vec<String>,
}

impl AtlasToolLock {
    fn platform(&self, name: &str) -> Result<&AtlasToolPlatform> {
        self.platforms
            .get(name)
            .with_context(|| format!("Atlas tool lock has no {name} platform"))
    }
}

#[derive(Debug, Parser)]
#[command(name = "memories-db-migrate")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Schema {
        #[command(subcommand)]
        command: SchemaCommand,
    },
    #[command(name = "post-migrate")]
    PostMigrate {
        #[command(subcommand)]
        command: PostMigrateCommand,
    },
    Release {
        #[command(subcommand)]
        command: ReleaseCommand,
    },
    /// Self-contained migration of a local SQLite database for desktop apps.
    #[cfg(not(feature = "postgres"))]
    Local {
        #[command(subcommand)]
        command: local_command::LocalCommand,
    },
    /// Status, planning, and migration of the embedding space (both RDB
    /// backends).
    Embedding {
        #[command(subcommand)]
        command: embedding_command::EmbeddingCommand,
    },
    /// Identity and integrity of the release bundle this binary belongs to.
    Bundle {
        #[command(subcommand)]
        command: bundle_command::BundleCommand,
    },
}

#[derive(Debug, Subcommand)]
enum SchemaCommand {
    Validate,
    Status,
    Apply(ApplyArgs),
    Verify(VerifyArgs),
    Baseline,
}

#[derive(Debug, Args)]
struct ApplyArgs {
    #[arg(long)]
    dry_run: bool,
}

#[derive(Debug, Args)]
struct VerifyArgs {
    #[arg(long)]
    to_version: Option<String>,
}

#[derive(Debug, Subcommand)]
enum PostMigrateCommand {
    Status,
    Verify,
    Run(PostMigrateRunArgs),
}

#[derive(Debug, Subcommand)]
enum ReleaseCommand {
    Plan,
    Baseline,
    Apply(ReleaseApplyArgs),
}

#[derive(Debug, Args)]
struct ReleaseApplyArgs {
    #[arg(long)]
    maintenance_window_ack: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SchemaState {
    Uninitialized,
    BaselineRequired,
    Pending {
        applied_count: usize,
    },
    Managed,
    /// A newer release migrated the database past this tool's catalog.
    NewerThanTool,
    SchemaCorrupt,
}

const SCHEMA_CORRUPT_MESSAGE: &str =
    "schema_corrupt: Atlas history, schema contract, and task state must be introduced together";
const NEWER_THAN_TOOL_MESSAGE: &str = "newer_than_tool: the database was migrated by a newer release; use that release or a newer one";

impl SchemaState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Uninitialized => "uninitialized",
            Self::BaselineRequired => "baseline_required",
            Self::Pending { .. } => "pending",
            Self::Managed => "managed",
            Self::NewerThanTool => "newer_than_tool",
            Self::SchemaCorrupt => "schema_corrupt",
        }
    }
}

#[derive(Debug, Args)]
struct PostMigrateRunArgs {
    #[arg(long)]
    id: Option<String>,
    #[arg(long)]
    generation: Option<u32>,
    #[arg(long)]
    all_required: bool,
    #[arg(long)]
    dry_run: bool,
    #[arg(long)]
    maintenance_window_ack: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let cli = Cli::parse();
    match cli.command {
        Command::Schema { command } => match command {
            SchemaCommand::Validate => run_schema_validate().await,
            SchemaCommand::Status => run_status().await,
            SchemaCommand::Apply(args) => {
                let _embedding_guard = if args.dry_run {
                    None
                } else {
                    embedding_guard_for_rdb_work(RdbWork::Schema).await?
                };
                run_apply(args.dry_run).await
            }
            SchemaCommand::Verify(verify_args) => run_verify(verify_args.to_version).await,
            SchemaCommand::Baseline => {
                let _embedding_guard = embedding_guard_for_rdb_work(RdbWork::Schema).await?;
                run_baseline().await
            }
        },
        Command::PostMigrate { command } => {
            let _embedding_guard = match &command {
                PostMigrateCommand::Run(args) if !args.dry_run => {
                    embedding_guard_for_rdb_work(RdbWork::Tasks).await?
                }
                _ => None,
            };
            run_post_migrate(command).await
        }
        Command::Release { command } => run_release(command).await,
        #[cfg(not(feature = "postgres"))]
        Command::Local { command } => std::process::exit(local_command::run_local(command).await),
        Command::Embedding { command } => {
            std::process::exit(embedding_command::run_embedding(command).await)
        }
        Command::Bundle { command } => {
            std::process::exit(bundle_command::run_bundle(command).await)
        }
    }
}

async fn run_schema_validate() -> Result<()> {
    run_atlas(&["migrate", "validate"]).await?;
    println!("schema_validate status=valid");
    Ok(())
}

async fn run_release(command: ReleaseCommand) -> Result<()> {
    match command {
        ReleaseCommand::Plan => {
            run_schema_validate().await?;
            run_status().await?;
            run_apply(true).await?;
            run_post_migrate(PostMigrateCommand::Status).await?;
            println!("release_plan status=completed");
        }
        ReleaseCommand::Baseline => {
            run_schema_validate().await?;
            run_status().await?;
            let _embedding_guard = embedding_guard_for_rdb_work(RdbWork::Both).await?;
            run_baseline().await?;
            run_status().await?;
            run_verify(None).await?;
            println!("release_baseline status=completed");
        }
        ReleaseCommand::Apply(args) => {
            if !args.maintenance_window_ack {
                bail!("--maintenance-window-ack is required for release apply");
            }
            run_schema_validate().await?;
            run_status().await?;
            let _embedding_guard = embedding_guard_for_rdb_work(RdbWork::Both).await?;
            run_apply(true).await?;
            run_apply(false).await?;
            run_post_migrate(PostMigrateCommand::Status).await?;
            run_post_migrate(PostMigrateCommand::Run(PostMigrateRunArgs {
                id: None,
                generation: None,
                all_required: true,
                dry_run: true,
                maintenance_window_ack: false,
            }))
            .await?;
            run_post_migrate(PostMigrateCommand::Run(PostMigrateRunArgs {
                id: None,
                generation: None,
                all_required: true,
                dry_run: false,
                maintenance_window_ack: true,
            }))
            .await?;
            run_verify(None).await?;
            run_post_migrate(PostMigrateCommand::Verify).await?;
            run_status().await?;
            println!("release_apply status=completed");
        }
    }
    Ok(())
}

async fn run_status() -> Result<()> {
    let pool = open_target_pool().await?;
    let state = schema_state(&pool).await?;
    if state == SchemaState::SchemaCorrupt {
        bail!(SCHEMA_CORRUPT_MESSAGE);
    }
    let pending_count = pending_count_for_schema_state(state, atlas_migration_versions().len())
        .map_or_else(|| "unknown".to_string(), |count| count.to_string());
    println!(
        "schema_status status={} pending_count={pending_count}",
        state.as_str()
    );
    if matches!(state, SchemaState::Pending { .. } | SchemaState::Managed) {
        run_atlas(&["migrate", "status"]).await?;
    }
    Ok(())
}

/// The fixed migration catalog is the source of an automatable pending count.
/// A baseline candidate has no trustworthy revision history, so its count must
/// remain unknown until the explicit baseline procedure completes.
fn pending_count_for_schema_state(state: SchemaState, migration_count: usize) -> Option<usize> {
    match state {
        SchemaState::Uninitialized => Some(migration_count),
        SchemaState::Pending { applied_count } => migration_count.checked_sub(applied_count),
        SchemaState::Managed => Some(0),
        SchemaState::BaselineRequired | SchemaState::NewerThanTool | SchemaState::SchemaCorrupt => {
            None
        }
    }
}

async fn run_apply(dry_run: bool) -> Result<()> {
    let pool = open_target_pool().await?;
    let state = schema_state(&pool).await?;
    match (state, dry_run) {
        (SchemaState::SchemaCorrupt, _) => bail!(SCHEMA_CORRUPT_MESSAGE),
        (SchemaState::NewerThanTool, _) => bail!(NEWER_THAN_TOOL_MESSAGE),
        (SchemaState::BaselineRequired, true) => {
            println!(
                "apply_dry_run status=baseline_required required_action=baseline adoption_baseline_versions={}",
                adoption_baseline_versions().join(",")
            );
            Ok(())
        }
        (SchemaState::BaselineRequired, false) => {
            bail!("baseline_required: run memories-db-migrate baseline before apply")
        }
        (SchemaState::Uninitialized, true) => {
            run_uninitialized_dry_run().await?;
            print_schema_dry_run_tasks().await
        }
        (SchemaState::Pending { .. } | SchemaState::Managed, true) => {
            run_atlas(&["migrate", "apply", "--dry-run"]).await?;
            print_schema_dry_run_tasks().await
        }
        (
            SchemaState::Uninitialized | SchemaState::Pending { .. } | SchemaState::Managed,
            false,
        ) => {
            run_atlas(&["migrate", "apply"]).await?;
            let artifact_root = atlas_artifact_root()?;
            let backend = target_backend()?;
            ensure_schema_prerequisites(&pool, &latest_migration_version(&artifact_root, backend)?)
                .await?;
            println!("apply status=completed");
            Ok(())
        }
    }
}

async fn print_schema_dry_run_tasks() -> Result<()> {
    let artifact_root = atlas_artifact_root()?;
    let backend = target_backend()?;
    let target_version = latest_migration_version(&artifact_root, backend)?;
    for task in selected_tasks_for_schema_version(&target_version, backend)? {
        println!(
            "apply_dry_run_selected_task task_identity={} canonical_definition_digest={} maintenance_window_required={}",
            task.identity(),
            task.canonical_definition_digest,
            task.maintenance_window_required,
        );
    }
    Ok(())
}

fn selected_tasks_for_schema_version(
    schema_version: &str,
    backend: &str,
) -> Result<Vec<catalog::TaskCatalogEntry>> {
    catalog::select_tasks_for_schema_version(&catalog::load_catalog()?, schema_version, backend)
}

/// Required tasks of `latest_version` that are not completed with their
/// current definition. Task state is trusted only once the schema is fully
/// managed; before that every required task is pending.
async fn pending_required_tasks(
    pool: &RdbPool,
    state: SchemaState,
    latest_version: &str,
    backend: &str,
) -> Result<Vec<catalog::TaskCatalogEntry>> {
    required_tasks_not_completed(pool, state == SchemaState::Managed, latest_version, backend).await
}

/// Required tasks that will actually run: a task already completed with
/// its current definition is skipped by `post-migrate run` even while
/// schema migrations are pending, so the embedding neutrality of the work
/// is judged without it.
async fn required_tasks_left_to_run(
    pool: &RdbPool,
    state: SchemaState,
    latest_version: &str,
    backend: &str,
) -> Result<Vec<catalog::TaskCatalogEntry>> {
    let has_task_state = matches!(state, SchemaState::Managed | SchemaState::Pending { .. })
        && table_exists(pool, "memories_data_migration_task_state").await?;
    required_tasks_not_completed(pool, has_task_state, latest_version, backend).await
}

async fn required_tasks_not_completed(
    pool: &RdbPool,
    trust_task_state: bool,
    latest_version: &str,
    backend: &str,
) -> Result<Vec<catalog::TaskCatalogEntry>> {
    let mut pending = Vec::new();
    for task in selected_tasks_for_schema_version(latest_version, backend)?
        .into_iter()
        .filter(|task| task.completion_required_by_schema_version.is_some())
    {
        let completed = trust_task_state
            && state::load(pool, &task.identity())
                .await?
                .map(|row| -> Result<bool> {
                    Ok(row.kind()? == TaskStateKind::Completed
                        && row.canonical_definition_digest == task.canonical_definition_digest)
                })
                .transpose()?
                .unwrap_or(false);
        if !completed {
            pending.push(task);
        }
    }
    Ok(pending)
}

/// Schema migrations a database in `state` has yet to apply (all of them
/// when the state does not tell).
fn pending_schema_versions(state: SchemaState) -> Vec<String> {
    let versions = atlas_migration_versions();
    match state {
        SchemaState::Managed => Vec::new(),
        SchemaState::Pending { applied_count } => {
            versions.into_iter().skip(applied_count).collect()
        }
        _ => versions,
    }
}

/// Whether pending schema migrations and tasks are all embedding neutral.
fn pending_work_is_embedding_neutral(
    state: SchemaState,
    tasks: &[catalog::TaskCatalogEntry],
) -> bool {
    let versions = pending_schema_versions(state);
    grpc_admin::db_migrate::work_is_embedding_neutral(
        versions.iter().map(String::as_str),
        tasks.iter().map(|t| t.implementation.as_str()),
    )
}

/// Commands that change the RDB outside `local` (`release`, `schema`,
/// `post-migrate`) take the embedding exclusion and are refused by an
/// unfinished embedding migration like `local apply` (embedding space
/// management spec §3.8). Taken once per command, at its entry. `None`
/// when there is no work.
/// Which pending work a guarded command performs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RdbWork {
    /// `schema apply` / `schema baseline`.
    Schema,
    /// `post-migrate run`.
    Tasks,
    /// `release apply` / `release baseline`.
    Both,
}

async fn embedding_guard_for_rdb_work(
    kind: RdbWork,
) -> Result<Option<grpc_admin::db_migrate::embedding::guard::Guard>> {
    use grpc_admin::db_migrate::embedding::guard::{self, Work};
    let pool = open_target_pool().await?;
    let state = schema_state(&pool).await?;
    let latest = atlas_migration_versions()
        .last()
        .cloned()
        .context("the migration catalog is empty")?;
    let schema_work = kind != RdbWork::Tasks && !pending_schema_versions(state).is_empty();
    let task_work = kind != RdbWork::Schema
        && !pending_required_tasks(&pool, state, &latest, target_backend()?)
            .await?
            .is_empty();
    if !schema_work && !task_work {
        return Ok(None);
    }
    let to_run = if kind == RdbWork::Schema {
        Vec::new()
    } else {
        required_tasks_left_to_run(&pool, state, &latest, target_backend()?).await?
    };
    let neutral = if kind == RdbWork::Tasks {
        grpc_admin::db_migrate::work_is_embedding_neutral(
            [],
            to_run.iter().map(|t| t.implementation.as_str()),
        )
    } else {
        pending_work_is_embedding_neutral(state, &to_run)
    };
    let state_dir = infra::infra::embedding_space::storage::state_dir_from_env();
    match guard::acquire(Some(&pool), &state_dir, Work::Apply { neutral }, None).await {
        Ok(g) => Ok(Some(g)),
        Err(e) => {
            let encode = grpc_admin::db_migrate::vocabulary::encode_value;
            let refused = e.refused();
            let mut extra = String::new();
            if let Some(a) = &refused.attempt {
                extra.push_str(&format!(
                    " attempt={} attempt_stage={} backup_mode={}",
                    encode(&a.attempt),
                    a.stage,
                    a.backup_mode
                ));
            }
            if let Some(b) = &refused.backup {
                extra.push_str(&format!(" backup={}", encode(b)));
            }
            if let Some(op) = refused.cancel_operation {
                extra.push_str(&format!(" cancel_operation={op}"));
            }
            let (code, resolution) = (refused.error_code, refused.resolution);
            bail!("refused error_code={code} resolution={resolution}{extra}: {e}")
        }
    }
}

async fn run_uninitialized_dry_run() -> Result<()> {
    let pool = open_target_pool().await?;
    let had_history_schema = atlas_history_schema_exists(&pool).await?;
    drop(pool);
    let atlas_result = run_atlas(&["migrate", "apply", "--dry-run"]).await;
    let cleanup_result = cleanup_uninitialized_dry_run_history(had_history_schema).await;
    match (atlas_result, cleanup_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Ok(()), Err(error)) => Err(error),
        (Err(error), Ok(())) => Err(error),
        (Err(error), Err(cleanup_error)) => Err(error.context(format!(
            "Atlas dry-run also left an unsafe migration-control state: {cleanup_error}"
        ))),
    }
}

async fn cleanup_uninitialized_dry_run_history(had_history_schema: bool) -> Result<()> {
    let pool = open_target_pool().await?;
    let has_application_table = table_exists(&pool, "thread").await?;
    let has_history = atlas_history_exists(&pool).await?;
    let has_contract = table_exists(&pool, "memories_schema_contract").await?;
    let has_task_state = table_exists(&pool, "memories_data_migration_task_state").await?;
    if !has_history {
        if has_application_table || has_contract || has_task_state {
            bail!(
                "Atlas dry-run changed an uninitialized target beyond its empty revision history"
            );
        }
        cleanup_new_atlas_history_schema(&pool, had_history_schema).await?;
        return Ok(());
    }
    if has_application_table || has_contract || has_task_state {
        bail!("Atlas dry-run changed an uninitialized target beyond its empty revision history");
    }

    let (count_sql, _, drop_sql) = atlas_history_sql()?;
    let revision_count: i64 = sqlx::query_scalar(count_sql)
        .fetch_one(&pool)
        .await
        .context("checking Atlas dry-run revision history")?;
    if revision_count != 0 {
        bail!("Atlas dry-run left a non-empty revision history on an uninitialized target");
    }
    // Atlas creates its revision table before rendering a dry-run plan. An
    // empty table is not migration state, so remove it to preserve dry-run.
    sqlx::query(drop_sql)
        .execute(&pool)
        .await
        .context("restoring uninitialized target after Atlas dry-run")?;
    cleanup_new_atlas_history_schema(&pool, had_history_schema).await?;
    if schema_state(&pool).await? != SchemaState::Uninitialized {
        bail!("Atlas dry-run cleanup did not restore the uninitialized target state");
    }
    Ok(())
}

async fn atlas_history_schema_exists(pool: &RdbPool) -> Result<bool> {
    #[cfg(feature = "postgres")]
    if !postgres_history_is_scoped()? {
        return sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'atlas_schema_revisions')",
        )
        .fetch_one(pool)
        .await
        .context("checking dedicated Atlas revision schema");
    }
    let _ = pool;
    Ok(false)
}

async fn cleanup_new_atlas_history_schema(pool: &RdbPool, had_history_schema: bool) -> Result<()> {
    #[cfg(feature = "postgres")]
    if !had_history_schema
        && !postgres_history_is_scoped()?
        && atlas_history_schema_exists(pool).await?
    {
        sqlx::query("DROP SCHEMA atlas_schema_revisions")
            .execute(pool)
            .await
            .context("restoring dedicated Atlas revision schema after dry-run")?;
    }
    let _ = (pool, had_history_schema);
    Ok(())
}

async fn run_baseline() -> Result<()> {
    let pool = open_target_pool().await?;
    if schema_state(&pool).await? != SchemaState::BaselineRequired {
        bail!("baseline is allowed only when status is baseline_required");
    }
    let baseline_version = verify_adoption_baseline_candidate().await?;
    let migration_count = remaining_migration_count_after_baseline(baseline_version)?.to_string();
    run_atlas(&[
        "migrate",
        "apply",
        &migration_count,
        "--baseline",
        baseline_version,
    ])
    .await?;
    let artifact_root = atlas_artifact_root()?;
    let backend = target_backend()?;
    ensure_schema_prerequisites(&pool, &latest_migration_version(&artifact_root, backend)?).await?;
    if schema_state(&pool).await? != SchemaState::Managed {
        bail!("baseline did not introduce the schema contract and common task state");
    }
    println!("baseline status=completed baseline_version={baseline_version}");
    Ok(())
}

async fn run_post_migrate(command: PostMigrateCommand) -> Result<()> {
    let pool = open_target_pool().await?;
    if matches!(&command, PostMigrateCommand::Status)
        && post_migration_state_unavailable(&pool).await?
    {
        println!("post_migrate_status status=task_state_unavailable");
        return Ok(());
    }
    let backend = target_backend()?;
    let schema_version = current_schema_contract_version(&pool).await?;
    let selected_tasks = selected_tasks_for_schema_version(&schema_version, backend)?;
    if selected_tasks.is_empty() {
        println!("post_migrate_status status=no_selected_tasks");
        return Ok(());
    }
    for entry in &selected_tasks {
        ensure_schema_prerequisites(&pool, &entry.introduced_by_schema_version).await?;
    }
    match command {
        PostMigrateCommand::Status => {
            for entry in selected_tasks {
                let identity = entry.identity();
                let inspection = task_from_catalog(pool.clone(), entry)?.inspect().await?;
                println!("post_migrate_status task_identity={identity} inspection={inspection}");
            }
            Ok(())
        }
        PostMigrateCommand::Verify => {
            for entry in selected_tasks {
                let identity = entry.identity();
                let task_state = state::load(&pool, &identity)
                    .await?
                    .context("required post-migration task has not been started")?;
                if task_state.kind()? != TaskStateKind::Completed
                    || task_state.canonical_definition_digest != entry.canonical_definition_digest
                {
                    bail!(
                        "required post-migration task is not completed with the current definition"
                    );
                }
                task_from_catalog(pool.clone(), entry)?.verify().await?;
                println!("post_migrate_verify task_identity={identity} status=verified");
            }
            Ok(())
        }
        PostMigrateCommand::Run(args) => {
            if args.all_required && (args.id.is_some() || args.generation.is_some()) {
                bail!("--all-required cannot be combined with --id or --generation");
            }
            let tasks = if args.all_required {
                selected_tasks
                    .into_iter()
                    .filter(|entry| entry.completion_required_by_schema_version.is_some())
                    .collect::<Vec<_>>()
            } else {
                let id = args
                    .id
                    .as_deref()
                    .context("--id is required unless --all-required is specified")?;
                let generation = args
                    .generation
                    .context("--generation is required unless --all-required is specified")?;
                selected_tasks
                    .into_iter()
                    .filter(|entry| entry.id == id && entry.generation == generation)
                    .collect::<Vec<_>>()
            };
            if tasks.len() != 1 && !args.all_required {
                bail!(
                    "the requested task is not selected for the current schema version and backend"
                );
            }
            if tasks.is_empty() {
                bail!(
                    "no required post-migration tasks are selected for the current schema version and backend"
                );
            }
            if args.dry_run {
                for entry in tasks {
                    let identity = entry.identity();
                    let maintenance_window_required = entry.maintenance_window_required;
                    let inspection = task_from_catalog(pool.clone(), entry)?.dry_run().await?;
                    println!(
                        "post_migrate_dry_run task_identity={identity} maintenance_window_required={maintenance_window_required} inspection={inspection}"
                    );
                }
                return Ok(());
            }
            if tasks.iter().any(|entry| entry.maintenance_window_required)
                && !args.maintenance_window_ack
            {
                bail!("--maintenance-window-ack is required for the selected post-migration task");
            }
            for entry in tasks {
                let identity = entry.identity();
                let now = command_utils::util::datetime::now_millis();
                let execution_id = format!("{}-{identity}-{now}", std::process::id());
                let result = task_from_catalog(pool.clone(), entry)?
                    .apply(&execution_id, "memories-db-migrate")
                    .await?;
                println!(
                    "post_migrate_run task_identity={identity} status=completed result={result}"
                );
            }
            Ok(())
        }
    }
}

async fn current_schema_contract_version(pool: &RdbPool) -> Result<String> {
    let version: Option<String> = sqlx::query_scalar(
        "SELECT version FROM memories_schema_contract WHERE contract_key = 'rdb_schema'",
    )
    .fetch_optional(pool)
    .await
    .context("reading schema contract for post-migration task selection")?;
    let version =
        version.context("schema contract is unavailable; apply the schema migration first")?;
    validate_schema_version(&version)?;
    Ok(version)
}

async fn open_target_pool() -> Result<RdbPool> {
    let url = migration_database_url()?;
    target_backend_for_url(&url)?;
    #[cfg(feature = "postgres")]
    let url = postgres_sqlx_database_url(&url)?;
    sqlx::Pool::<Rdb>::connect(&url)
        .await
        .context("connecting to migration target database")
}

#[cfg(feature = "postgres")]
fn postgres_sqlx_database_url(target_url: &str) -> Result<String> {
    let url = Url::parse(target_url).context("parsing PostgreSQL target URL")?;
    if url
        .query_pairs()
        .any(|(key, _)| key == "search_path" || key == "options[search_path]")
    {
        postgres_schema_url(target_url, &postgres_target_schema(target_url)?)
    } else {
        Ok(target_url.to_string())
    }
}

fn migration_database_url() -> Result<String> {
    std::env::var("MEMORIES_ATLAS_DATABASE_URL")
        .or_else(|_| std::env::var("POSTGRES_URL"))
        .context("MEMORIES_ATLAS_DATABASE_URL or POSTGRES_URL is required")
}

fn atlas_database_url(target_url: &str) -> Result<String> {
    if target_url.starts_with("sqlite:") {
        let url = Url::parse(target_url).context("parsing SQLite target URL for Atlas")?;
        // SQLx must receive a normal absolute SQLite URL. Atlas instead
        // requires SQLite's URI-filename form for percent-encoded paths.
        if url.host_str().is_none() && url.path().starts_with('/') {
            let query = url
                .query()
                .map(|query| format!("?{query}"))
                .unwrap_or_default();
            return Ok(format!("sqlite://file:{}{query}", url.path()));
        }
        return Ok(target_url.to_string());
    }
    if !target_url.starts_with("postgres:") && !target_url.starts_with("postgresql:") {
        return Ok(target_url.to_string());
    }
    let mut url = Url::parse(target_url).context("parsing PostgreSQL target URL for Atlas")?;
    let pairs = url
        .query_pairs()
        .filter(|(key, _)| !(key.starts_with("options[") && key.ends_with(']')))
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect::<Vec<_>>();
    let options_only_search_path = url
        .query_pairs()
        .find_map(|(key, value)| (key == "options[search_path]").then(|| value.into_owned()));
    {
        let mut query = url.query_pairs_mut();
        query.clear();
        query.extend_pairs(
            pairs
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str())),
        );
        if !pairs.iter().any(|(key, _)| key == "search_path")
            && let Some(schema) = options_only_search_path
        {
            query.append_pair("search_path", &schema);
        }
    }
    Ok(url.into())
}

async fn ensure_schema_prerequisites(pool: &RdbPool, minimum_version: &str) -> Result<()> {
    validate_schema_version(minimum_version)?;
    match schema_state(pool).await? {
        SchemaState::Managed => {}
        SchemaState::NewerThanTool => bail!(NEWER_THAN_TOOL_MESSAGE),
        SchemaState::SchemaCorrupt => bail!(SCHEMA_CORRUPT_MESSAGE),
        SchemaState::Uninitialized
        | SchemaState::BaselineRequired
        | SchemaState::Pending { .. } => {
            bail!("schema contract is unavailable; apply the schema migration first")
        }
    }
    let contract: Option<String> = sqlx::query_scalar(
        "SELECT version FROM memories_schema_contract WHERE contract_key = 'rdb_schema'",
    )
    .fetch_optional(pool)
    .await
    .context("reading schema contract for post-migration task")?;
    let contract =
        contract.context("schema contract is unavailable; apply the schema migration first")?;
    validate_schema_version(&contract)?;
    if contract.as_str() < minimum_version {
        bail!("schema contract is older than this post-migration task");
    }
    Ok(())
}

async fn post_migration_state_unavailable(pool: &RdbPool) -> Result<bool> {
    match schema_state(pool).await? {
        SchemaState::Uninitialized
        | SchemaState::BaselineRequired
        | SchemaState::Pending { .. } => Ok(true),
        SchemaState::Managed => Ok(false),
        SchemaState::NewerThanTool => bail!(NEWER_THAN_TOOL_MESSAGE),
        SchemaState::SchemaCorrupt => {
            bail!(SCHEMA_CORRUPT_MESSAGE)
        }
    }
}

async fn schema_state(pool: &RdbPool) -> Result<SchemaState> {
    let has_application_table = table_exists(pool, "thread").await?;
    let has_history = atlas_history_exists(pool).await?;
    let has_contract = table_exists(pool, "memories_schema_contract").await?;
    let has_task_state = table_exists(pool, "memories_data_migration_task_state").await?;
    if !has_history {
        return match (has_application_table, has_contract, has_task_state) {
            (false, false, false) => Ok(SchemaState::Uninitialized),
            (true, false, false) => Ok(SchemaState::BaselineRequired),
            _ => Ok(SchemaState::SchemaCorrupt),
        };
    }

    if !has_application_table {
        return Ok(SchemaState::SchemaCorrupt);
    }
    let applied_count = match schema_history_shape(pool).await? {
        HistoryShape::Prefix(applied_count) => applied_count,
        HistoryShape::NewerThanTool { latest } => {
            let newer_contract =
                has_contract && has_task_state && contract_is(pool, &latest).await?;
            return Ok(if newer_contract {
                SchemaState::NewerThanTool
            } else {
                SchemaState::SchemaCorrupt
            });
        }
        HistoryShape::Invalid => return Ok(SchemaState::SchemaCorrupt),
    };
    if !schema_control_tables_match_prefix(pool, applied_count, has_contract, has_task_state)
        .await?
    {
        return Ok(SchemaState::SchemaCorrupt);
    }
    if applied_count == atlas_migration_versions().len() {
        Ok(SchemaState::Managed)
    } else {
        Ok(SchemaState::Pending { applied_count })
    }
}

enum HistoryShape {
    /// A valid prefix of the fixed catalog with this many applied versions.
    Prefix(usize),
    /// The full fixed catalog followed only by later versions this tool does
    /// not know: a newer release migrated the database.
    NewerThanTool {
        latest: String,
    },
    Invalid,
}

/// Atlas history may stop at a fixed-catalog boundary. Fresh databases start
/// at v1 with a normal applied revision; adopted schemas require a baseline.
async fn schema_history_shape(pool: &RdbPool) -> Result<HistoryShape> {
    let (_, select_sql, _) = atlas_history_sql()?;
    let history: Vec<(String, i64)> = sqlx::query_as(select_sql)
        .fetch_all(pool)
        .await
        .context("reading Atlas schema revision history")?;
    let expected = atlas_migration_versions();
    let Some((first_version, first_type)) = history.first() else {
        return Ok(HistoryShape::Invalid);
    };
    let Some(start_index) = expected.iter().position(|version| version == first_version) else {
        return Ok(HistoryShape::Invalid);
    };
    let first_revision_is_valid = if start_index == 0 {
        *first_type == ATLAS_BASELINE_REVISION_TYPE || *first_type == ATLAS_APPLIED_REVISION_TYPE
    } else {
        adoption_baseline_versions().contains(&first_version.as_str())
            && *first_type == ATLAS_BASELINE_REVISION_TYPE
    };
    if !first_revision_is_valid {
        return Ok(HistoryShape::Invalid);
    }
    let known_count = expected.len() - start_index;
    let (known, later) = history.split_at(history.len().min(known_count));
    for (offset, (version, revision_type)) in known.iter().enumerate() {
        if version != &expected[start_index + offset]
            || (offset > 0 && *revision_type != ATLAS_APPLIED_REVISION_TYPE)
        {
            return Ok(HistoryShape::Invalid);
        }
    }
    let Some((latest, _)) = later.last() else {
        return Ok(HistoryShape::Prefix(start_index + history.len()));
    };
    let catalog_latest = expected.last().map(String::as_str).unwrap_or_default();
    let later_is_valid = later.iter().all(|(version, revision_type)| {
        validate_schema_version(version).is_ok()
            && version.as_str() > catalog_latest
            && *revision_type == ATLAS_APPLIED_REVISION_TYPE
    });
    Ok(if later_is_valid {
        HistoryShape::NewerThanTool {
            latest: latest.clone(),
        }
    } else {
        HistoryShape::Invalid
    })
}

/// Whether the contract table holds exactly the `rdb_schema` row for `version`.
async fn contract_is(pool: &RdbPool, version: &str) -> Result<bool> {
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT contract_key, version FROM memories_schema_contract ORDER BY contract_key ASC",
    )
    .fetch_all(pool)
    .await
    .context("reading the schema contract")?;
    Ok(rows == vec![("rdb_schema".to_string(), version.to_string())])
}

#[cfg(feature = "postgres")]
const ATLAS_HISTORY_COUNT_SQL: &str =
    "SELECT COUNT(*) FROM atlas_schema_revisions.atlas_schema_revisions";
#[cfg(feature = "postgres")]
const ATLAS_HISTORY_SELECT_SQL: &str =
    "SELECT version, type FROM atlas_schema_revisions.atlas_schema_revisions ORDER BY version ASC";
#[cfg(feature = "postgres")]
const ATLAS_HISTORY_DROP_SQL: &str = "DROP TABLE atlas_schema_revisions.atlas_schema_revisions";

#[cfg(feature = "postgres")]
const ATLAS_SCOPED_HISTORY_COUNT_SQL: &str = "SELECT COUNT(*) FROM atlas_schema_revisions";
#[cfg(feature = "postgres")]
const ATLAS_SCOPED_HISTORY_SELECT_SQL: &str =
    "SELECT version, type FROM atlas_schema_revisions ORDER BY version ASC";
#[cfg(feature = "postgres")]
const ATLAS_SCOPED_HISTORY_DROP_SQL: &str = "DROP TABLE atlas_schema_revisions";

#[cfg(not(feature = "postgres"))]
const ATLAS_HISTORY_COUNT_SQL: &str = "SELECT COUNT(*) FROM atlas_schema_revisions";
#[cfg(not(feature = "postgres"))]
const ATLAS_HISTORY_SELECT_SQL: &str =
    "SELECT version, type FROM atlas_schema_revisions ORDER BY version ASC";
#[cfg(not(feature = "postgres"))]
const ATLAS_HISTORY_DROP_SQL: &str = "DROP TABLE atlas_schema_revisions";

fn atlas_history_sql() -> Result<(&'static str, &'static str, &'static str)> {
    #[cfg(feature = "postgres")]
    if postgres_history_is_scoped()? {
        return Ok((
            ATLAS_SCOPED_HISTORY_COUNT_SQL,
            ATLAS_SCOPED_HISTORY_SELECT_SQL,
            ATLAS_SCOPED_HISTORY_DROP_SQL,
        ));
    }
    Ok((
        ATLAS_HISTORY_COUNT_SQL,
        ATLAS_HISTORY_SELECT_SQL,
        ATLAS_HISTORY_DROP_SQL,
    ))
}

#[cfg(feature = "postgres")]
fn postgres_history_is_scoped() -> Result<bool> {
    let url = Url::parse(&migration_database_url()?).context("parsing PostgreSQL target URL")?;
    Ok(url
        .query_pairs()
        .any(|(key, _)| key == "search_path" || key == "options[search_path]"))
}

async fn atlas_history_exists(pool: &RdbPool) -> Result<bool> {
    #[cfg(feature = "postgres")]
    {
        if !postgres_history_is_scoped()? {
            return sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'atlas_schema_revisions' AND table_name = 'atlas_schema_revisions')",
            )
            .fetch_one(pool)
            .await
            .context("checking dedicated Atlas revision history");
        }
        table_exists(pool, "atlas_schema_revisions").await
    }
    #[cfg(not(feature = "postgres"))]
    {
        table_exists(pool, "atlas_schema_revisions").await
    }
}

/// The contract and common task state are introduced by the third fixed
/// migration. Before that boundary neither table may exist; from that
/// boundary onward the single contract row must name the applied prefix tip.
async fn schema_control_tables_match_prefix(
    pool: &RdbPool,
    applied_count: usize,
    has_contract: bool,
    has_task_state: bool,
) -> Result<bool> {
    let expected = atlas_migration_versions();
    let controls_are_introduced = applied_count >= schema_contract_migration_index();
    if !controls_are_introduced {
        return Ok(!has_contract && !has_task_state);
    }
    if !has_contract || !has_task_state {
        return Ok(false);
    }
    contract_is(pool, &expected[applied_count - 1]).await
}

fn atlas_migration_versions() -> Vec<String> {
    ATLAS_SUM
        .lines()
        .filter_map(|line| line.split_once('_').map(|(version, _)| version))
        .filter(|version| validate_schema_version(version).is_ok())
        .map(str::to_string)
        .collect()
}

async fn table_exists(pool: &RdbPool, table_name: &str) -> Result<bool> {
    #[cfg(feature = "postgres")]
    let sql = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = $1)";
    #[cfg(not(feature = "postgres"))]
    let sql = "SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?)";
    sqlx::query_scalar(sql)
        .bind(table_name)
        .fetch_one(pool)
        .await
        .context("checking migration control table existence")
}

const ADOPTION_BASELINE_VERSION: &str = "20260803000001";
const ADOPTION_BASELINE_VERSIONS: [&str; 2] = ["20260803000002", ADOPTION_BASELINE_VERSION];

/// Return candidates from the most specific historical schema to the oldest.
/// The verification step is authoritative; ordering only improves diagnostics.
fn adoption_baseline_versions() -> &'static [&'static str] {
    &ADOPTION_BASELINE_VERSIONS
}

fn schema_contract_migration_index() -> usize {
    3
}

fn remaining_migration_count_after_baseline(baseline_version: &str) -> Result<usize> {
    let versions = atlas_migration_versions();
    let baseline_index = versions
        .iter()
        .position(|version| version == baseline_version)
        .context("adoption baseline version is absent from the fixed migration catalog")?;
    let remaining = versions.len().saturating_sub(baseline_index + 1);
    if remaining == 0 {
        bail!("adoption baseline must have a later schema-contract migration");
    }
    Ok(remaining)
}

const ATLAS_BASELINE_REVISION_TYPE: i64 = 1;
const ATLAS_APPLIED_REVISION_TYPE: i64 = 2;

fn load_atlas_tool_lock(artifact_root: &std::path::Path) -> Result<AtlasToolLock> {
    let path = artifact_root.join(ATLAS_TOOL_LOCK_FILE);
    let lock: AtlasToolLock = serde_json::from_slice(
        &fs::read(&path).with_context(|| format!("reading Atlas tool lock {}", path.display()))?,
    )
    .context("parsing Atlas tool lock")?;
    if lock.version.is_empty() {
        bail!("Atlas tool lock version must not be empty");
    }
    for (platform, metadata) in &lock.platforms {
        if !metadata.url.starts_with("https://")
            || metadata.sha256.len() != 64
            || !metadata.sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
        {
            bail!("Atlas tool lock has invalid metadata for {platform}");
        }
    }
    Ok(lock)
}

fn load_seed_expectations(
    artifact_root: &std::path::Path,
    backend: &str,
) -> Result<SeedExpectations> {
    if backend != "sqlite" && backend != "postgres" {
        bail!("seed expectations backend must be sqlite or postgres");
    }
    let path = artifact_root.join(backend).join("seed-expectations.json");
    let expectations: SeedExpectations = serde_json::from_slice(
        &fs::read(&path)
            .with_context(|| format!("reading seed expectations {}", path.display()))?,
    )
    .context("parsing seed expectations")?;
    validate_schema_version(&expectations.version)?;
    if expectations.tables.is_empty() {
        bail!("seed expectations must contain at least one table");
    }
    for expectation in &expectations.tables {
        if !is_safe_sql_identifier(&expectation.table)
            || !is_safe_sql_identifier(&expectation.key_column)
            || expectation.keys.is_empty()
            || expectation.keys.iter().any(|key| key.is_empty())
            || expectation.keys.windows(2).any(|pair| pair[0] >= pair[1])
        {
            bail!("seed expectations contain an invalid table, key column, or key ordering");
        }
    }
    Ok(expectations)
}

fn is_safe_sql_identifier(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte == b'_')
}

async fn verify_seed_expectations(
    pool: &RdbPool,
    artifact_root: &std::path::Path,
    backend: &str,
    target_version: &str,
) -> Result<()> {
    let expectations = load_seed_expectations(artifact_root, backend)?;
    if expectations.version.as_str() > target_version {
        return Ok(());
    }
    for expectation in expectations.tables {
        let sql = format!(
            "SELECT {} FROM {} ORDER BY {}",
            expectation.key_column, expectation.table, expectation.key_column
        );
        let mut actual: Vec<String> = sqlx::query_scalar(sqlx::AssertSqlSafe(sql))
            .fetch_all(pool)
            .await
            .with_context(|| format!("reading seed table {}", expectation.table))?;
        // Database collation is deployment-specific; compare canonical key order.
        actual.sort_unstable();
        if actual != expectation.keys {
            bail!(
                "seed_mismatch: table={} key_column={} expected={:?} actual={:?}",
                expectation.table,
                expectation.key_column,
                expectation.keys,
                actual
            );
        }
    }
    Ok(())
}

fn validate_schema_version(version: &str) -> Result<()> {
    if version.len() != 14 || !version.bytes().all(|byte| byte.is_ascii_digit()) {
        bail!("schema version must be a 14-digit ASCII timestamp");
    }
    Ok(())
}

async fn run_atlas(args: &[&str]) -> Result<()> {
    run_atlas_owned(&args.iter().map(ToString::to_string).collect::<Vec<_>>()).await
}

async fn run_atlas_owned(args: &[String]) -> Result<()> {
    run_atlas_owned_with_config(args, "migrate.hcl").await
}

async fn run_atlas_owned_with_config(args: &[String], config_file: &str) -> Result<()> {
    let output = run_atlas_capture(args, config_file, &[]).await?;
    if !output.is_empty() {
        print!("{output}");
    }
    Ok(())
}

async fn run_atlas_capture(
    args: &[String],
    config_file: &str,
    child_env: &[(String, String)],
) -> Result<String> {
    let artifact_root = atlas_artifact_root()?;
    let atlas = artifact_root.join("bin").join("atlas");
    if !atlas.is_file() {
        bail!("fixed Atlas binary is missing from the release artifact");
    }
    verify_atlas_binary(&atlas, &load_atlas_tool_lock(&artifact_root)?).await?;
    let config = atlas_config_path(config_file)?;
    let backend = target_backend()?;
    verify_atlas_sum(&artifact_root, backend)?;
    let database_url = atlas_database_url(&migration_database_url()?)?;
    let mut command = tokio::process::Command::new(&atlas);
    let output = command
        .current_dir(&artifact_root)
        .arg("--config")
        .arg(config)
        .arg("--env")
        .arg(backend)
        .args(args)
        .env("MEMORIES_ATLAS_DATABASE_URL", database_url)
        .envs(child_env.iter().map(|(name, value)| (name, value)))
        .stdin(Stdio::null())
        .output()
        .await
        .context("starting the fixed Atlas migration engine")?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!(
            "Atlas migration engine failed with status {}: {}",
            output.status,
            redact_database_urls(&stderr)
        );
    }
    String::from_utf8(output.stdout).context("Atlas migration engine returned non-UTF-8 output")
}

async fn verify_atlas_binary(atlas: &std::path::Path, lock: &AtlasToolLock) -> Result<()> {
    let platform = lock.platform(current_atlas_platform_name()?)?;
    let actual = sha256_file(atlas)?;
    if !actual.eq_ignore_ascii_case(&platform.sha256) {
        bail!("fixed Atlas binary SHA-256 does not match atlas-tool.lock.json");
    }
    let output = tokio::process::Command::new(atlas)
        .arg("version")
        .stdin(Stdio::null())
        .output()
        .await
        .context("running fixed Atlas binary version check")?;
    if !output.status.success() {
        bail!(
            "fixed Atlas binary version check failed with status {}",
            output.status
        );
    }
    let version = String::from_utf8_lossy(&output.stdout);
    if !version.contains(&lock.version) {
        bail!("fixed Atlas binary version does not match atlas-tool.lock.json");
    }
    Ok(())
}

fn current_atlas_platform_name() -> Result<&'static str> {
    atlas_platform_name(std::env::consts::OS, std::env::consts::ARCH)
}

fn atlas_platform_name(os: &str, arch: &str) -> Result<&'static str> {
    match (os, arch) {
        ("linux", "x86_64") => Ok("linux-amd64"),
        ("macos", "aarch64") => Ok("darwin-arm64"),
        _ => bail!(
            "this memories-db-migrate build does not support Atlas on {os}/{arch}; supported platforms are linux/x86_64 and macos/aarch64"
        ),
    }
}

fn sha256_file(path: &std::path::Path) -> Result<String> {
    grpc_admin::db_migrate::local::files::hash_file(path)
}

async fn run_verify(requested_version: Option<String>) -> Result<()> {
    let artifact_root = atlas_artifact_root()?;
    let backend = target_backend()?;
    let version = match requested_version {
        Some(version) => {
            validate_schema_version(&version)?;
            version
        }
        None => latest_migration_version(&artifact_root, backend)?,
    };
    let args = schema_diff_args(backend, &version)?;
    let output = match backend {
        "sqlite" => run_atlas_capture(&args, "verify.hcl", &[]).await?,
        "postgres" => run_postgres_verify(&args).await?,
        _ => unreachable!("target_backend only returns supported backends"),
    };
    if !output.trim().is_empty() {
        bail!("drift_detected: {}", redact_database_urls(output.trim()));
    }
    let pool = open_target_pool().await?;
    match schema_state(&pool).await? {
        SchemaState::Managed => ensure_schema_prerequisites(&pool, &version).await?,
        SchemaState::BaselineRequired => {
            bail!("schema contract is unavailable; run baseline before verify")
        }
        SchemaState::NewerThanTool => bail!(NEWER_THAN_TOOL_MESSAGE),
        SchemaState::Uninitialized | SchemaState::Pending { .. } | SchemaState::SchemaCorrupt => {
            bail!("schema contract is unavailable; apply the schema migration first")
        }
    }
    verify_seed_expectations(&pool, &artifact_root, backend, &version).await?;
    println!("verify status=verified version={version}");
    Ok(())
}

/// Verify exactly one known unmanaged schema before Atlas records its baseline.
/// A candidate mismatch is expected and must leave the target untouched.
async fn verify_adoption_baseline_candidate() -> Result<&'static str> {
    let backend = target_backend()?;
    let mut mismatches = Vec::new();
    let mut matches = Vec::new();
    for &version in adoption_baseline_versions() {
        let args = schema_diff_args(backend, version)?;
        let output = match backend {
            "sqlite" => run_atlas_capture(&args, "verify.hcl", &[]).await?,
            "postgres" => run_postgres_verify(&args).await?,
            _ => unreachable!("target_backend only returns supported backends"),
        };
        if output.trim().is_empty() {
            matches.push(version);
            continue;
        }
        mismatches.push(format!(
            "{version}: {}",
            redact_database_urls(output.trim())
        ));
    }
    match matches.as_slice() {
        [version] => {
            let pool = open_target_pool().await?;
            let artifact_root = atlas_artifact_root()?;
            verify_seed_expectations(&pool, &artifact_root, backend, version).await?;
            Ok(version)
        }
        [] => Err(LocalFailure::new(
            ErrorCode::BaselineSchemaMismatch,
            Resolution::ToolUpdateRequired,
            format!(
                "target does not match a supported adoption schema; candidates={}",
                mismatches.join(" | ")
            ),
        )
        .into()),
        _ => bail!(
            "baseline_schema_ambiguous: target matches multiple adoption schemas: {}",
            matches.join(",")
        ),
    }
}

fn latest_migration_version(artifact_root: &std::path::Path, backend: &str) -> Result<String> {
    let directory = artifact_root.join(backend).join("migrations");
    let mut versions = fs::read_dir(&directory)
        .with_context(|| format!("reading Atlas migration directory {}", directory.display()))?
        .filter_map(|entry| entry.ok())
        .filter_map(|entry| entry.file_name().into_string().ok())
        .filter_map(|name| name.split_once('_').map(|(version, _)| version.to_owned()))
        .filter(|version| validate_schema_version(version).is_ok())
        .collect::<Vec<_>>();
    versions.sort();
    versions
        .pop()
        .context("Atlas migration directory has no valid migration version")
}

#[cfg(feature = "postgres")]
async fn run_postgres_verify(args: &[String]) -> Result<String> {
    let pool = open_target_pool().await?;
    let target_url = migration_database_url()?;
    let schema = format!(
        "memories_atlas_verify_{}_{}",
        std::process::id(),
        command_utils::util::datetime::now_millis()
    );
    let environment = postgres_verify_environment(&target_url, &schema)?;
    let create = format!("CREATE SCHEMA {}", quote_postgres_identifier(&schema));
    sqlx::query(sqlx::AssertSqlSafe(create))
        .execute(&pool)
        .await
        .context("creating temporary PostgreSQL schema for Atlas verification")?;
    let result = run_atlas_capture(args, "verify.hcl", &environment).await;
    let drop = format!("DROP SCHEMA {} CASCADE", quote_postgres_identifier(&schema));
    let cleanup = sqlx::query(sqlx::AssertSqlSafe(drop))
        .execute(&pool)
        .await
        .context("removing temporary PostgreSQL schema for Atlas verification");
    match (result, cleanup) {
        (Ok(output), Ok(_)) => Ok(output),
        (Err(error), Ok(_)) => Err(error),
        (Ok(_), Err(error)) => Err(error),
        (Err(run_error), Err(cleanup_error)) => Err(run_error.context(format!(
            "Atlas verification also failed to remove its temporary schema: {cleanup_error:#}"
        ))),
    }
}

#[cfg(feature = "postgres")]
fn postgres_verify_environment(
    target_url: &str,
    dev_schema: &str,
) -> Result<Vec<(String, String)>> {
    let source_schema = postgres_target_schema(target_url)?;
    Ok(vec![
        (
            "MEMORIES_ATLAS_DATABASE_URL".to_string(),
            atlas_database_url(&postgres_schema_url(target_url, &source_schema)?)?,
        ),
        (
            "MEMORIES_ATLAS_INTERNAL_DEV_URL".to_string(),
            atlas_database_url(&postgres_schema_url(target_url, dev_schema)?)?,
        ),
    ])
}

#[cfg(feature = "postgres")]
fn postgres_target_schema(target_url: &str) -> Result<String> {
    let url = Url::parse(target_url).context("parsing PostgreSQL target URL")?;
    if url.scheme() != "postgres" && url.scheme() != "postgresql" {
        bail!("PostgreSQL verification requires a PostgreSQL target URL");
    }
    let pairs = url.query_pairs().collect::<Vec<_>>();
    let search_path = pairs
        .iter()
        .find_map(|(key, value)| (*key == "search_path").then(|| value.to_string()));
    let sqlx_search_path = pairs
        .iter()
        .find_map(|(key, value)| (*key == "options[search_path]").then(|| value.to_string()));
    if let (Some(search_path), Some(sqlx_search_path)) = (&search_path, &sqlx_search_path)
        && search_path != sqlx_search_path
    {
        bail!("conflicting PostgreSQL search_path options are not supported");
    }
    let schema = search_path
        .or(sqlx_search_path)
        .unwrap_or_else(|| "public".to_string());
    // Reuse the URL builder's identifier validation before the schema reaches
    // a process boundary or dynamic SQL.
    postgres_schema_url(target_url, &schema)?;
    Ok(schema)
}

#[cfg(not(feature = "postgres"))]
async fn run_postgres_verify(_args: &[String]) -> Result<String> {
    bail!("this memories-db-migrate build does not include PostgreSQL support")
}

#[cfg(feature = "postgres")]
fn postgres_schema_url(target_url: &str, schema: &str) -> Result<String> {
    if schema.is_empty()
        || !schema
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        bail!("temporary PostgreSQL schema name is invalid");
    }
    let mut url = Url::parse(target_url).context("parsing PostgreSQL target URL")?;
    if url.scheme() != "postgres" && url.scheme() != "postgresql" {
        bail!("temporary PostgreSQL schema requires a PostgreSQL target URL");
    }
    let pairs = url
        .query_pairs()
        .filter(|(key, _)| key != "search_path" && key != "options[search_path]")
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect::<Vec<_>>();
    {
        let mut query = url.query_pairs_mut();
        query.clear();
        query.extend_pairs(
            pairs
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str())),
        );
        query.append_pair("search_path", schema);
        query.append_pair("options[search_path]", schema);
    }
    Ok(url.into())
}

#[cfg(feature = "postgres")]
fn quote_postgres_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

fn redact_database_urls(value: &str) -> String {
    let mut redacted = value.to_owned();
    for scheme in ["sqlite:", "postgres:", "postgresql:"] {
        let mut offset = 0;
        while let Some(found) = redacted[offset..].find(scheme) {
            let start = offset + found;
            let end = redacted[start..]
                .find(char::is_whitespace)
                .map(|index| start + index)
                .unwrap_or(redacted.len());
            redacted.replace_range(start..end, "<database-url>");
            offset = start + "<database-url>".len();
        }
    }
    redacted
}

fn verify_atlas_sum(artifact_root: &std::path::Path, backend: &str) -> Result<()> {
    let migration_dir = artifact_root.join(backend).join("migrations");
    let sum_path = migration_dir.join("atlas.sum");
    let expected = fs::read_to_string(&sum_path)
        .with_context(|| format!("reading Atlas checksum file {}", sum_path.display()))?;
    let actual = atlas_sum_text(&migration_dir)?;
    if expected != actual {
        bail!("Atlas migration directory checksum does not match atlas.sum");
    }
    Ok(())
}

fn atlas_sum_text(migration_dir: &std::path::Path) -> Result<String> {
    let mut entries = fs::read_dir(migration_dir)
        .with_context(|| {
            format!(
                "reading Atlas migration directory {}",
                migration_dir.display()
            )
        })?
        .collect::<std::io::Result<Vec<_>>>()?;
    entries.retain(|entry| entry.file_type().is_ok_and(|kind| kind.is_file()));
    entries.sort_by_key(|entry| entry.file_name());

    let mut cumulative = Sha256::new();
    let mut lines = Vec::new();
    for entry in entries {
        let name = entry.file_name().to_string_lossy().into_owned();
        if !name.ends_with(".sql") {
            continue;
        }
        cumulative.update(name.as_bytes());
        cumulative.update(
            fs::read(entry.path()).with_context(|| {
                format!("reading Atlas migration file {}", entry.path().display())
            })?,
        );
        let digest = STANDARD.encode(cumulative.clone().finalize());
        lines.push((name, digest));
    }
    if lines.is_empty() {
        bail!("Atlas migration directory must contain at least one SQL migration");
    }
    let mut directory = Sha256::new();
    for (name, digest) in &lines {
        directory.update(name.as_bytes());
        directory.update(digest.as_bytes());
    }
    let mut text = format!("h1:{}\n", STANDARD.encode(directory.finalize()));
    for (name, digest) in lines {
        text.push_str(&format!("{name} h1:{digest}\n"));
    }
    Ok(text)
}

fn atlas_config_path(config_file: &str) -> Result<String> {
    let path = atlas_artifact_root()?.join(config_file);
    if !path.is_file() {
        bail!("fixed Atlas configuration is missing");
    }
    Ok(format!("file://{}", path.to_string_lossy()))
}

fn schema_diff_args(backend: &str, version: &str) -> Result<Vec<String>> {
    validate_schema_version(version)?;
    if backend != "sqlite" && backend != "postgres" {
        bail!("schema diff backend must be sqlite or postgres");
    }
    Ok(vec![
        "schema".to_string(),
        "diff".to_string(),
        "--from".to_string(),
        "env://url".to_string(),
        "--to".to_string(),
        format!("file://{backend}/migrations?version={version}"),
        "--exclude".to_string(),
        "atlas_schema_revisions".to_string(),
        "--format".to_string(),
        "{{ sql . \"\" }}".to_string(),
    ])
}

#[cfg(test)]
fn verify_config_uses_internal_dev_url(artifact_root: &std::path::Path) -> Result<bool> {
    let path = artifact_root.join("verify.hcl");
    let text = fs::read_to_string(&path)
        .with_context(|| format!("reading Atlas verification config {}", path.display()))?;
    Ok(
        text.contains("MEMORIES_ATLAS_INTERNAL_DEV_URL")
            && !text.contains("MEMORIES_ATLAS_DEV_URL"),
    )
}

fn atlas_artifact_root() -> Result<std::path::PathBuf> {
    let path = if let Some(path) = std::env::var_os("MEMORIES_ATLAS_DIR") {
        std::path::PathBuf::from(path)
    } else if let Ok(executable) = std::env::current_exe()
        && let Some(parent) = executable.parent()
        && parent.join("atlas").is_dir()
    {
        parent.join("atlas")
    } else {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("infra")
            .join("atlas")
    };
    if !path.is_dir() {
        bail!("fixed Atlas release artifact directory is missing");
    }
    Ok(path)
}

fn target_backend() -> Result<&'static str> {
    let url = migration_database_url()?;
    target_backend_for_url(&url)
}

fn target_backend_for_url(url: &str) -> Result<&'static str> {
    if url.starts_with("sqlite:") {
        #[cfg(feature = "postgres")]
        {
            bail!(
                "this memories-db-migrate build supports PostgreSQL only; use the SQLite release artifact"
            );
        }
        #[cfg(not(feature = "postgres"))]
        {
            Ok("sqlite")
        }
    } else if url.starts_with("postgres:") || url.starts_with("postgresql:") {
        #[cfg(feature = "postgres")]
        {
            Ok("postgres")
        }
        #[cfg(not(feature = "postgres"))]
        {
            bail!(
                "this memories-db-migrate build supports SQLite only; use the PostgreSQL server image"
            )
        }
    } else {
        bail!("MEMORIES_ATLAS_DATABASE_URL must use sqlite:, postgres:, or postgresql:")
    }
}

#[cfg(test)]
mod tests {
    use super::atlas_database_url;
    #[cfg(feature = "postgres")]
    use super::postgres_verify_environment;
    use super::{
        ADOPTION_BASELINE_VERSION, Cli, adoption_baseline_versions, atlas_artifact_root,
        atlas_config_path, atlas_migration_versions, atlas_platform_name, load_atlas_tool_lock,
        load_seed_expectations, pending_count_for_schema_state,
        remaining_migration_count_after_baseline, schema_diff_args,
        selected_tasks_for_schema_version, target_backend_for_url, validate_schema_version,
        verify_atlas_sum, verify_config_uses_internal_dev_url,
    };
    #[cfg(not(feature = "postgres"))]
    use super::{
        ATLAS_APPLIED_REVISION_TYPE, ATLAS_BASELINE_REVISION_TYPE, SchemaState,
        post_migration_state_unavailable, run_baseline, run_verify, schema_state,
        verify_seed_expectations,
    };
    #[cfg(feature = "postgres")]
    use super::{
        ATLAS_HISTORY_COUNT_SQL, ATLAS_HISTORY_DROP_SQL, ATLAS_HISTORY_SELECT_SQL, SchemaState,
        postgres_schema_url, quote_postgres_identifier, schema_state,
    };
    use anyhow::{Context, Result, bail};
    use clap::Parser;
    use infra_utils::infra::rdb::{Rdb, RdbPool};
    use std::process::Stdio;
    use std::time::Duration;

    const E2E_ENVIRONMENT_VARIABLES: [&str; 11] = [
        "MEMORIES_ATLAS_DIR",
        "MEMORIES_ATLAS_DATABASE_URL",
        "THREAD_VECTOR_ENABLED",
        "THREAD_LANCEDB_URI",
        "THREAD_LANCEDB_TABLE",
        "THREAD_VECTOR_SIZE",
        "MEMORY_FTS_TOKENIZER",
        "THREAD_DISTANCE_TYPE",
        "THREAD_VECTOR_INDEX_ENABLED",
        "THREAD_VECTOR_INDEX_MIN_ROWS",
        "THREAD_VECTOR_INDEX_NPROBES",
    ];

    #[test]
    fn pending_work_neutrality_follows_the_declarations() {
        use super::{atlas_migration_versions, catalog, pending_work_is_embedding_neutral};
        let versions = atlas_migration_versions();
        let latest_only = SchemaState::Pending {
            applied_count: versions.len() - 1,
        };
        assert!(pending_work_is_embedding_neutral(latest_only, &[]));
        assert!(pending_work_is_embedding_neutral(SchemaState::Managed, &[]));
        let thread_message_times = catalog::thread_message_times_v1().unwrap();
        assert!(!pending_work_is_embedding_neutral(
            SchemaState::Managed,
            std::slice::from_ref(&thread_message_times)
        ));
        let thread_groups = catalog::thread_groups_user_ids_v4().unwrap();
        assert!(pending_work_is_embedding_neutral(
            latest_only,
            std::slice::from_ref(&thread_groups)
        ));
        // A database that has applied nothing runs every migration,
        // including undeclared ones.
        assert!(!pending_work_is_embedding_neutral(
            SchemaState::Uninitialized,
            &[]
        ));
    }

    #[test]
    fn release_apply_command_accepts_maintenance_acknowledgement() {
        Cli::try_parse_from([
            "memories-db-migrate",
            "release",
            "apply",
            "--maintenance-window-ack",
        ])
        .expect("release apply command must parse");
    }

    const E2E_FIXED_SEARCH_ENVIRONMENT: [(&str, &str); 5] = [
        ("MEMORY_FTS_TOKENIZER", "simple"),
        ("THREAD_DISTANCE_TYPE", "cosine"),
        ("THREAD_VECTOR_INDEX_ENABLED", "false"),
        ("THREAD_VECTOR_INDEX_MIN_ROWS", "1000"),
        ("THREAD_VECTOR_INDEX_NPROBES", "20"),
    ];

    struct ScopedE2eEnvironment {
        previous: Vec<(&'static str, Option<std::ffi::OsString>)>,
    }

    impl ScopedE2eEnvironment {
        fn configure(
            artifact_root: &str,
            database_url: &str,
            vector_uri: &std::path::Path,
        ) -> Self {
            let previous = E2E_ENVIRONMENT_VARIABLES
                .into_iter()
                .map(|name| (name, std::env::var_os(name)))
                .collect();
            // Acceptance tests run with --test-threads=1, so this scoped
            // process environment cannot be observed by another test.
            unsafe {
                std::env::set_var("MEMORIES_ATLAS_DIR", artifact_root);
                std::env::set_var("MEMORIES_ATLAS_DATABASE_URL", database_url);
                std::env::set_var("THREAD_VECTOR_ENABLED", "true");
                std::env::set_var("THREAD_LANCEDB_URI", vector_uri);
                std::env::set_var("THREAD_LANCEDB_TABLE", "threads");
                std::env::set_var("THREAD_VECTOR_SIZE", "4");
                for (name, value) in E2E_FIXED_SEARCH_ENVIRONMENT {
                    std::env::set_var(name, value);
                }
            }
            Self { previous }
        }
    }

    impl Drop for ScopedE2eEnvironment {
        fn drop(&mut self) {
            // Restore the caller's environment even when the E2E assertion
            // fails, keeping subsequent tests independent.
            unsafe {
                for (name, value) in self.previous.drain(..) {
                    match value {
                        Some(value) => std::env::set_var(name, value),
                        None => std::env::remove_var(name),
                    }
                }
            }
        }
    }

    fn fixed_e2e_atlas_artifact_root() -> Result<String> {
        let artifact_root = std::env::var("MEMORIES_DB_MIGRATE_E2E_ATLAS_DIR")
            .context("MEMORIES_DB_MIGRATE_E2E_ATLAS_DIR must be set for the E2E test")?;
        if !std::path::Path::new(&artifact_root)
            .join("bin/atlas")
            .is_file()
        {
            bail!("fixed Atlas release artifact must contain bin/atlas");
        }
        Ok(artifact_root)
    }

    fn e2e_workspace_root() -> Result<std::path::PathBuf> {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .parent()
            .map(std::path::Path::to_path_buf)
            .context("locating the workspace root for the E2E release binary")
    }

    fn resolve_e2e_release_binary_from_workspace_root(
        binary: std::path::PathBuf,
        workspace_root: &std::path::Path,
    ) -> Result<std::path::PathBuf> {
        let binary = if binary.is_absolute() {
            binary
        } else {
            workspace_root.join(binary)
        };
        let binary = binary
            .canonicalize()
            .context("resolving MEMORIES_DB_MIGRATE_E2E_BINARY")?;
        if !binary.is_file() {
            bail!("MEMORIES_DB_MIGRATE_E2E_BINARY is not an executable file");
        }
        Ok(binary)
    }

    fn fixed_e2e_release_binary() -> Result<std::path::PathBuf> {
        let binary = std::env::var("MEMORIES_DB_MIGRATE_E2E_BINARY").context(
            "MEMORIES_DB_MIGRATE_E2E_BINARY must point to the release binary for the E2E test",
        )?;
        resolve_e2e_release_binary_from_workspace_root(
            std::path::PathBuf::from(binary),
            &e2e_workspace_root()?,
        )
    }

    #[cfg(not(feature = "postgres"))]
    fn sqlite_e2e_target_paths(root: &std::path::Path) -> (String, std::path::PathBuf) {
        let directory = root.join("Lookback Test #100% 日本語");
        std::fs::create_dir_all(&directory).unwrap();
        let database = directory.join("memories.sqlite3");
        let url = url::Url::from_file_path(&database).unwrap();
        (
            format!("sqlite://{}?mode=rwc", url.path()),
            directory.join("threads.lancedb"),
        )
    }

    #[test]
    fn e2e_release_binary_resolves_ci_relative_path_before_child_changes_directory() {
        let workspace_root = tempfile::tempdir().unwrap();
        let relative_binary = std::path::PathBuf::from("target/release/memories-db-migrate");
        let binary = workspace_root.path().join(&relative_binary);
        std::fs::create_dir_all(binary.parent().unwrap()).unwrap();
        std::fs::File::create(&binary).unwrap();
        let child_working_directory = tempfile::tempdir().unwrap();

        let resolved = resolve_e2e_release_binary_from_workspace_root(
            relative_binary.clone(),
            workspace_root.path(),
        )
        .unwrap();

        assert!(resolved.is_absolute());
        assert_eq!(resolved, binary.canonicalize().unwrap());
        assert!(
            !child_working_directory
                .path()
                .join(relative_binary)
                .is_file()
        );
    }

    #[test]
    fn e2e_release_binary_rejects_missing_path() {
        let workspace_root = tempfile::tempdir().unwrap();
        let missing_binary = std::path::PathBuf::from("target/release/memories-db-migrate");

        let error =
            resolve_e2e_release_binary_from_workspace_root(missing_binary, workspace_root.path())
                .unwrap_err();

        assert!(
            error
                .chain()
                .any(|cause| cause.to_string().contains("MEMORIES_DB_MIGRATE_E2E_BINARY"))
        );
    }

    struct MigrationE2eCommand<'a> {
        binary: &'a std::path::Path,
        artifact_root: &'a str,
        database_url: &'a str,
        vector_uri: &'a std::path::Path,
    }

    impl MigrationE2eCommand<'_> {
        async fn run(&self, arguments: &[&str]) -> Result<String> {
            let (code, stdout, stderr) = self.run_with_status(arguments).await?;
            if code != Some(0) {
                bail!(
                    "memories-db-migrate {:?} exited with {code:?}; stdout={stdout:?}; stderr={stderr}",
                    arguments,
                );
            }
            Ok(stdout)
        }

        /// Run a command whose failure is part of the expected contract.
        async fn run_with_status(
            &self,
            arguments: &[&str],
        ) -> Result<(Option<i32>, String, String)> {
            let mut process = tokio::process::Command::new(self.binary);
            process
                .current_dir(
                    self.vector_uri
                        .parent()
                        .context("E2E LanceDB fixture must have a parent directory")?,
                )
                .env_clear()
                .args(arguments)
                .env("MEMORIES_ATLAS_DIR", self.artifact_root)
                .env("MEMORIES_ATLAS_DATABASE_URL", self.database_url)
                .env("THREAD_VECTOR_ENABLED", "true")
                .env("THREAD_LANCEDB_URI", self.vector_uri)
                .env("THREAD_LANCEDB_TABLE", "threads")
                .env("THREAD_VECTOR_SIZE", "4")
                .env(
                    "MEMORY_EMBEDDING_STATE_DIR",
                    self.vector_uri
                        .parent()
                        .context("E2E LanceDB fixture must have a parent directory")?
                        .join("embedding-state"),
                )
                .envs(E2E_FIXED_SEARCH_ENVIRONMENT)
                .stdin(Stdio::null())
                .kill_on_drop(true);
            let output = tokio::time::timeout(Duration::from_secs(120), process.output())
                .await
                .context("memories-db-migrate release binary timed out")?
                .context("starting memories-db-migrate release binary")?;
            Ok((
                output.status.code(),
                String::from_utf8_lossy(&output.stdout).into_owned(),
                String::from_utf8_lossy(&output.stderr).into_owned(),
            ))
        }
    }

    async fn insert_thread_message_times_e2e_fixture(pool: &RdbPool) -> Result<()> {
        #[cfg(feature = "postgres")]
        {
            sqlx::query("INSERT INTO thread (id, user_id, created_at, updated_at, memory_kind) VALUES ($1, $2, $3, $4, $5)")
                .bind(1_i64).bind(1_i64).bind(10_i64).bind(20_i64).bind(1_i32)
                .execute(pool).await?;
            sqlx::query("INSERT INTO memory (id, user_id, content, content_type, created_at, updated_at, memory_kind) VALUES ($1, $2, $3, $4, $5, $6, $7)")
                .bind(101_i64).bind(1_i64).bind("fixture").bind(1_i32).bind(100_i64).bind(100_i64).bind(1_i32)
                .execute(pool).await?;
            sqlx::query("INSERT INTO thread_memory (thread_id, memory_id, position, created_at) VALUES ($1, $2, $3, $4)")
                .bind(1_i64).bind(101_i64).bind(0_i32).bind(100_i64)
                .execute(pool).await?;
        }
        #[cfg(not(feature = "postgres"))]
        {
            sqlx::query("INSERT INTO thread (id, user_id, created_at, updated_at, memory_kind) VALUES (?, ?, ?, ?, ?)")
                .bind(1_i64).bind(1_i64).bind(10_i64).bind(20_i64).bind(1_i32)
                .execute(pool).await?;
            sqlx::query("INSERT INTO memory (id, user_id, content, content_type, created_at, updated_at, memory_kind) VALUES (?, ?, ?, ?, ?, ?, ?)")
                .bind(101_i64).bind(1_i64).bind("fixture").bind(1_i32).bind(100_i64).bind(100_i64).bind(1_i32)
                .execute(pool).await?;
            sqlx::query("INSERT INTO thread_memory (thread_id, memory_id, position, created_at) VALUES (?, ?, ?, ?)")
                .bind(1_i64).bind(101_i64).bind(0_i32).bind(100_i64)
                .execute(pool).await?;
        }
        Ok(())
    }

    async fn run_thread_message_times_migration_e2e(
        command: &MigrationE2eCommand<'_>,
        database_url: &str,
        expected_postgres_schema: Option<&str>,
    ) -> Result<()> {
        assert_eq!(
            command.database_url, database_url,
            "fixture pool and Atlas child process must use the same database URL"
        );
        let output = command.run(&["schema", "validate"]).await?;
        assert!(output.contains("schema_validate status=valid"));
        let output = command.run(&["schema", "status"]).await?;
        assert!(output.contains(&format!(
            "schema_status status=uninitialized pending_count={}",
            atlas_migration_versions().len()
        )));
        let output = command.run(&["schema", "apply", "--dry-run"]).await?;
        assert!(
            output.contains("apply_dry_run_selected_task task_identity=thread-message-times-v1@1")
        );
        let output = command.run(&["schema", "apply"]).await?;
        assert!(output.contains("apply status=completed"));

        #[cfg(feature = "postgres")]
        let fixture_url = super::postgres_sqlx_database_url(database_url)?;
        #[cfg(not(feature = "postgres"))]
        let fixture_url = database_url.to_string();
        let pool = sqlx::Pool::<Rdb>::connect(&fixture_url).await?;
        #[cfg(feature = "postgres")]
        {
            let expected_schema = expected_postgres_schema
                .context("PostgreSQL E2E fixture requires an expected temporary schema")?;
            let current_schema: String = sqlx::query_scalar("SELECT current_schema()")
                .fetch_one(&pool)
                .await?;
            assert_eq!(current_schema, expected_schema);
        }
        #[cfg(not(feature = "postgres"))]
        assert!(expected_postgres_schema.is_none());
        assert_eq!(schema_state(&pool).await?, SchemaState::Managed);

        insert_thread_message_times_e2e_fixture(&pool).await?;
        drop(pool);

        let output = command.run(&["schema", "status"]).await?;
        assert!(output.contains("schema_status status=managed pending_count=0"));
        #[cfg(feature = "postgres")]
        {
            let output = command.run(&["schema", "apply"]).await?;
            assert!(output.contains("apply status=completed"));
            let output = command.run(&["schema", "status"]).await?;
            assert!(output.contains("schema_status status=managed pending_count=0"));
        }

        let vector_config = infra::infra::thread_vector::config::ThreadVectorDBConfig::from_env()?;
        let vector = infra::infra::thread_vector::repository::ThreadVectorRepositoryImpl::new(
            vector_config.clone(),
        )
        .await?;
        vector
            .batch_upsert(vec![
                infra::infra::thread_vector::record::ThreadVectorRecord {
                    thread_id: 1,
                    vector_kind: "text".to_string(),
                    chunk_index: 0,
                    begin_position: 0,
                    end_position: 7,
                    user_id: 1,
                    memory_kind: 1,
                    content: "fixture".to_string(),
                    description: Some("fixture".to_string()),
                    labels: vec![],
                    embedding: vec![0.1, 0.2, 0.3, 0.4],
                    embedding_model: Some("test".to_string()),
                    channel: None,
                    created_at: 10,
                    updated_at: 20,
                    first_message_at: None,
                    last_message_at: None,
                    indexed_at: 30,
                },
            ])
            .await?;

        let output = command.run(&["post-migrate", "status"]).await?;
        assert!(output.contains("post_migrate_status task_identity=thread-message-times-v1@1"));
        let output = command
            .run(&[
                "post-migrate",
                "run",
                "--id",
                "thread-message-times-v1",
                "--generation",
                "1",
                "--dry-run",
            ])
            .await?;
        assert!(output.contains("post_migrate_dry_run task_identity=thread-message-times-v1@1"));
        let output = command
            .run(&[
                "post-migrate",
                "run",
                "--id",
                "thread-message-times-v1",
                "--generation",
                "1",
                "--maintenance-window-ack",
            ])
            .await?;
        assert!(
            output.contains(
                "post_migrate_run task_identity=thread-message-times-v1@1 status=completed"
            )
        );
        let output = command
            .run(&[
                "post-migrate",
                "run",
                "--id",
                "thread-groups-canonical-keys-v1",
                "--generation",
                "2",
                "--maintenance-window-ack",
            ])
            .await?;
        assert!(output.contains(
            "post_migrate_run task_identity=thread-groups-canonical-keys-v1@2 status=completed"
        ));
        let output = command
            .run(&[
                "post-migrate",
                "run",
                "--id",
                "thread-groups-user-ids-v1",
                "--generation",
                "4",
                "--maintenance-window-ack",
            ])
            .await?;
        assert!(output.contains(
            "post_migrate_run task_identity=thread-groups-user-ids-v1@4 status=completed"
        ));

        let migrated =
            infra::infra::thread_vector::repository::ThreadVectorRepositoryImpl::new(vector_config)
                .await?;
        let rows = migrated.find_records_by_thread_id(1).await?;
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        assert_eq!(row.first_message_at, Some(100));
        assert_eq!(row.last_message_at, Some(100));
        assert_eq!(row.user_id, 1);
        assert_eq!(row.memory_kind, 1);
        assert_eq!(row.created_at, 10);
        assert_eq!(row.updated_at, 20);
        assert_eq!(row.content, "fixture");
        assert_eq!(row.embedding, vec![0.1, 0.2, 0.3, 0.4]);
        let output = command.run(&["schema", "verify"]).await?;
        assert!(output.contains("verify status=verified version=20261009000001"));
        let output = command.run(&["post-migrate", "verify"]).await?;
        assert!(output.contains(
            "post_migrate_verify task_identity=thread-groups-canonical-keys-v1@2 status=verified"
        ));
        assert!(output.contains(
            "post_migrate_verify task_identity=thread-groups-user-ids-v1@4 status=verified"
        ));
        assert!(output.contains(
            "post_migrate_verify task_identity=thread-message-times-v1@1 status=verified"
        ));
        Ok(())
    }

    async fn seed_adoption_candidate_schema(pool: &RdbPool, version: &str) -> Result<()> {
        #[cfg(feature = "postgres")]
        let baseline = include_str!(
            "../../../infra/atlas/postgres/migrations/20260803000001_adoption_baseline.sql"
        );
        #[cfg(not(feature = "postgres"))]
        let baseline = include_str!(
            "../../../infra/atlas/sqlite/migrations/20260803000001_adoption_baseline.sql"
        );
        sqlx::raw_sql(baseline).execute(pool).await?;

        if version == "20260803000002" {
            #[cfg(feature = "postgres")]
            let message_times = include_str!(
                "../../../infra/atlas/postgres/migrations/20260803000002_thread_message_times_schema.sql"
            );
            #[cfg(not(feature = "postgres"))]
            let message_times = include_str!(
                "../../../infra/atlas/sqlite/migrations/20260803000002_thread_message_times_schema.sql"
            );
            sqlx::raw_sql(message_times).execute(pool).await?;
        }
        Ok(())
    }

    async fn run_adoption_baseline_e2e(
        command: &MigrationE2eCommand<'_>,
        database_url: &str,
        candidate_version: &str,
    ) -> Result<()> {
        let pool = sqlx::Pool::<Rdb>::connect(database_url).await?;
        seed_adoption_candidate_schema(&pool, candidate_version).await?;
        assert_eq!(schema_state(&pool).await?, SchemaState::BaselineRequired);
        drop(pool);

        let output = command.run(&["schema", "status"]).await?;
        assert!(output.contains("schema_status status=baseline_required pending_count=unknown"));
        let output = command.run(&["schema", "apply", "--dry-run"]).await?;
        assert!(output.contains("required_action=baseline"));
        let output = command.run(&["schema", "baseline"]).await?;
        assert!(output.contains(&format!(
            "baseline status=completed baseline_version={candidate_version}"
        )));
        let output = command.run(&["schema", "verify"]).await?;
        assert!(output.contains("verify status=verified version=20261009000001"));

        let pool = sqlx::Pool::<Rdb>::connect(database_url).await?;
        assert_eq!(schema_state(&pool).await?, SchemaState::Managed);
        let contract: String = sqlx::query_scalar(
            "SELECT version FROM memories_schema_contract WHERE contract_key = 'rdb_schema'",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(contract, "20261009000001");
        Ok(())
    }

    #[cfg(not(feature = "postgres"))]
    async fn run_adoption_baseline_in_process_e2e(
        artifact_root: &str,
        database_url: &str,
        vector_uri: &std::path::Path,
        candidate_version: &str,
    ) -> Result<()> {
        let pool = sqlx::Pool::<Rdb>::connect(database_url).await?;
        seed_adoption_candidate_schema(&pool, candidate_version).await?;
        assert_eq!(schema_state(&pool).await?, SchemaState::BaselineRequired);
        drop(pool);

        let _environment = ScopedE2eEnvironment::configure(artifact_root, database_url, vector_uri);
        run_baseline().await?;
        let pool = sqlx::Pool::<Rdb>::connect(database_url).await?;
        assert_eq!(schema_state(&pool).await?, SchemaState::Managed);
        drop(pool);
        run_verify(None).await?;
        Ok(())
    }

    #[test]
    fn adoption_baseline_has_a_valid_version() {
        validate_schema_version(ADOPTION_BASELINE_VERSION).unwrap();
    }

    #[test]
    fn adoption_baseline_candidates_cover_pre_time_and_manual_time_schemas() {
        assert_eq!(
            adoption_baseline_versions(),
            &["20260803000002", "20260803000001"]
        );
    }

    #[test]
    fn baseline_applies_every_migration_after_the_selected_candidate() {
        assert_eq!(
            remaining_migration_count_after_baseline("20260803000001").unwrap(),
            6
        );
        assert_eq!(
            remaining_migration_count_after_baseline("20260803000002").unwrap(),
            5
        );
        assert_eq!(
            remaining_migration_count_after_baseline("20260803000003").unwrap(),
            4
        );
    }

    #[test]
    fn schema_version_rejects_non_canonical_values() {
        assert!(validate_schema_version("20260803").is_err());
        assert!(validate_schema_version("2026080300000x").is_err());
    }

    #[test]
    fn schema_status_pending_count_is_deterministic_for_safe_states() {
        assert_eq!(
            pending_count_for_schema_state(SchemaState::Uninitialized, 4),
            Some(4)
        );
        assert_eq!(
            pending_count_for_schema_state(SchemaState::Managed, 4),
            Some(0)
        );
        assert_eq!(
            pending_count_for_schema_state(SchemaState::Pending { applied_count: 1 }, 4),
            Some(3)
        );
        assert_eq!(
            pending_count_for_schema_state(SchemaState::BaselineRequired, 2),
            None
        );
        assert_eq!(
            pending_count_for_schema_state(SchemaState::SchemaCorrupt, 2),
            None
        );
    }

    #[test]
    fn docker_smoke_migration_uses_release_coordinator() {
        const DOCKERFILE: &str = include_str!("../../../Dockerfile");

        for command in [
            "schema validate",
            "schema status",
            "schema apply --dry-run",
            "release apply --maintenance-window-ack",
        ] {
            assert!(
                DOCKERFILE.contains(&format!("memories-db-migrate {command}")),
                "Docker smoke migration must invoke `{command}` through the public schema CLI"
            );
        }
        for legacy_command in ["validate", "status", "apply --dry-run", "apply", "verify"] {
            assert!(
                !DOCKERFILE.contains(&format!("memories-db-migrate {legacy_command}")),
                "Docker smoke migration must not invoke the removed top-level `{legacy_command}` command"
            );
        }

        let release_apply = DOCKERFILE
            .find("memories-db-migrate release apply --maintenance-window-ack")
            .expect("Docker smoke migration must use the release coordinator");
        let final_status = DOCKERFILE[release_apply..]
            .find("memories-db-migrate schema status")
            .map(|offset| release_apply + offset)
            .expect("Docker smoke migration must check schema status after release apply");
        assert!(
            final_status > release_apply,
            "the managed schema status check must follow release apply"
        );
        assert!(
            DOCKERFILE[final_status..]
                .contains("grep -Fx 'schema_status status=managed pending_count=0'"),
            "Docker smoke migration must reject a final schema status other than managed with no pending migrations"
        );
        assert!(
            DOCKERFILE[release_apply..].contains(
                "schema_status=\"$(./target/release/memories-db-migrate schema status)\""
            ),
            "Docker smoke migration must preserve schema status failures while checking its structured output"
        );
    }

    #[test]
    fn task_selection_uses_schema_version_and_backend() {
        assert!(
            selected_tasks_for_schema_version("20260803000001", "sqlite")
                .unwrap()
                .is_empty()
        );
        let tasks = selected_tasks_for_schema_version("20260803000003", "sqlite").unwrap();
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].identity(), "thread-message-times-v1@1");
        let latest_tasks = selected_tasks_for_schema_version("20260920000001", "sqlite").unwrap();
        assert_eq!(latest_tasks.len(), 1);
        assert_eq!(
            latest_tasks
                .iter()
                .map(|task| task.identity())
                .collect::<Vec<_>>(),
            vec!["thread-message-times-v1@1"]
        );
        let typed_owner_tasks =
            selected_tasks_for_schema_version("20260930000001", "sqlite").unwrap();
        assert_eq!(
            typed_owner_tasks
                .iter()
                .map(|task| task.identity())
                .collect::<Vec<_>>(),
            vec![
                "thread-groups-canonical-keys-v1@2",
                "thread-groups-user-ids-v1@4",
                "thread-message-times-v1@1"
            ]
        );
        assert!(typed_owner_tasks.iter().all(|task| {
            task.identity() != "thread-groups-user-ids-v1@2"
                && task.identity() != "thread-groups-user-ids-v1@3"
        }));
        assert!(
            selected_tasks_for_schema_version("20260803000003", "unsupported")
                .unwrap()
                .is_empty()
        );
    }

    #[cfg(not(feature = "postgres"))]
    #[test]
    fn fixed_registry_constructs_every_schema_selected_task() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            use grpc_admin::db_migrate::{catalog, task_from_catalog};
            use sqlx::sqlite::SqlitePoolOptions;

            let pool = SqlitePoolOptions::new()
                .max_connections(1)
                .connect("sqlite::memory:")
                .await
                .unwrap();
            for entry in selected_tasks_for_schema_version("20260803000003", "sqlite").unwrap() {
                let identity = entry.identity();
                assert_eq!(
                    task_from_catalog(pool.clone(), entry)
                        .unwrap()
                        .task_identity(),
                    identity
                );
            }

            let mut forged = catalog::thread_message_times_v1().unwrap();
            forged.description = "forged description".to_string();
            assert!(task_from_catalog(pool, forged).is_err());
        });
    }

    #[cfg(not(feature = "postgres"))]
    #[test]
    fn sqlite_build_rejects_postgresql_url_before_connecting() {
        assert_eq!(
            target_backend_for_url("sqlite:///tmp/memories.sqlite3").unwrap(),
            "sqlite"
        );
        assert!(target_backend_for_url("postgres://example.invalid/memories").is_err());
    }

    #[test]
    fn atlas_sqlite_url_converts_standard_sqlx_absolute_url_only_for_atlas() {
        let target = "sqlite:///tmp/Lookback%20Test%20%23100%25/%E6%97%A5%E6%9C%AC%E8%AA%9E/default.sqlite3?mode=rwc&cache=shared";

        assert_eq!(
            atlas_database_url(target).unwrap(),
            "sqlite://file:/tmp/Lookback%20Test%20%23100%25/%E6%97%A5%E6%9C%AC%E8%AA%9E/default.sqlite3?mode=rwc&cache=shared"
        );
    }

    #[test]
    fn atlas_sqlite_url_keeps_legacy_atlas_uri_unchanged() {
        let target = "sqlite://file:/tmp/Lookback%20Test/default.sqlite3?mode=rwc";

        assert_eq!(atlas_database_url(target).unwrap(), target);
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn postgres_build_rejects_sqlite_url_before_connecting() {
        assert_eq!(
            target_backend_for_url("postgres://example.invalid/memories").unwrap(),
            "postgres"
        );
        assert!(target_backend_for_url("sqlite:///tmp/memories.sqlite3").is_err());
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn postgres_schema_url_scopes_sqlx_and_atlas_to_the_same_schema() {
        use sqlx::postgres::PgConnectOptions;
        use std::str::FromStr;

        let schema = "memories_atlas_verify_123";
        let database_url = postgres_schema_url(
            "postgres://user:secret@example.invalid/memories?search_path=old_schema&options%5Bsearch_path%5D=old_schema&sslmode=disable",
            schema,
        )
        .unwrap();
        assert!(
            database_url.contains("options%5Bsearch_path%5D=memories_atlas_verify_123"),
            "the SQLx-specific option key must be percent-encoded in the PostgreSQL URL"
        );
        let query = url::Url::parse(&database_url)
            .unwrap()
            .query_pairs()
            .map(|(key, value)| (key.into_owned(), value.into_owned()))
            .collect::<Vec<_>>();

        assert_eq!(
            query
                .iter()
                .filter(|(key, _)| key == "search_path")
                .map(|(key, value)| (key.as_str(), value.as_str()))
                .collect::<Vec<_>>(),
            vec![("search_path", schema)]
        );
        assert_eq!(
            query
                .iter()
                .filter(|(key, _)| key == "options[search_path]")
                .map(|(key, value)| (key.as_str(), value.as_str()))
                .collect::<Vec<_>>(),
            vec![("options[search_path]", schema)]
        );
        assert_eq!(
            PgConnectOptions::from_str(&database_url)
                .unwrap()
                .get_options(),
            Some("-c search_path=memories_atlas_verify_123")
        );
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn atlas_postgres_url_removes_sqlx_only_search_path_option() {
        let url = atlas_database_url(
            "postgres://user:secret@example.invalid/memories?sslmode=disable&search_path=thread_scope&options%5Bsearch_path%5D=thread_scope",
        )
        .unwrap();
        let query = url::Url::parse(&url)
            .unwrap()
            .query_pairs()
            .map(|(key, value)| (key.into_owned(), value.into_owned()))
            .collect::<Vec<_>>();

        assert!(query.contains(&("sslmode".to_string(), "disable".to_string())));
        assert!(query.contains(&("search_path".to_string(), "thread_scope".to_string())));
        assert!(
            !query.iter().any(|(key, _)| key == "options[search_path]"),
            "Atlas/libpq must not receive SQLx-only URL parameters"
        );
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn atlas_postgres_url_scopes_options_only_search_path() {
        let url = atlas_database_url(
            "postgres://user:secret@example.invalid/memories?options%5Bsearch_path%5D=tenant_a",
        )
        .unwrap();
        let query = url::Url::parse(&url).unwrap();
        assert!(
            query
                .query_pairs()
                .any(|(key, value)| key == "search_path" && value == "tenant_a")
        );
        assert!(
            !query
                .query_pairs()
                .any(|(key, _)| key == "options[search_path]")
        );
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn postgres_uses_dedicated_schema_for_unscoped_atlas_revision_history() {
        assert_eq!(
            ATLAS_HISTORY_COUNT_SQL,
            "SELECT COUNT(*) FROM atlas_schema_revisions.atlas_schema_revisions"
        );
        assert_eq!(
            ATLAS_HISTORY_SELECT_SQL,
            "SELECT version, type FROM atlas_schema_revisions.atlas_schema_revisions ORDER BY version ASC"
        );
        assert_eq!(
            ATLAS_HISTORY_DROP_SQL,
            "DROP TABLE atlas_schema_revisions.atlas_schema_revisions"
        );
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn postgres_verify_environment_scopes_source_and_dev_to_schemas() {
        let environment = postgres_verify_environment(
            "postgres://user:secret@example.invalid/memories?sslmode=disable",
            "memories_atlas_verify_123",
        )
        .unwrap();

        assert_eq!(
            environment,
            vec![
                (
                    "MEMORIES_ATLAS_DATABASE_URL".to_string(),
                    "postgres://user:secret@example.invalid/memories?sslmode=disable&search_path=public"
                        .to_string(),
                ),
                (
                    "MEMORIES_ATLAS_INTERNAL_DEV_URL".to_string(),
                    "postgres://user:secret@example.invalid/memories?sslmode=disable&search_path=memories_atlas_verify_123"
                        .to_string(),
                ),
            ]
        );
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn postgres_verify_environment_preserves_the_target_schema() {
        let environment = postgres_verify_environment(
            "postgres://user:secret@example.invalid/memories?sslmode=disable&search_path=tenant_a&options%5Bsearch_path%5D=tenant_a",
            "memories_atlas_verify_123",
        )
        .unwrap();

        assert_eq!(
            environment[0],
            (
                "MEMORIES_ATLAS_DATABASE_URL".to_string(),
                "postgres://user:secret@example.invalid/memories?sslmode=disable&search_path=tenant_a"
                    .to_string(),
            )
        );
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn postgres_verify_environment_uses_sqlx_only_target_schema_option() {
        let environment = postgres_verify_environment(
            "postgres://user:secret@example.invalid/memories?sslmode=disable&options%5Bsearch_path%5D=tenant_a",
            "memories_atlas_verify_123",
        )
        .unwrap();

        assert!(environment[0].1.ends_with("search_path=tenant_a"));
    }

    #[cfg(feature = "postgres")]
    #[test]
    fn postgres_verify_environment_rejects_conflicting_schema_options() {
        let error = postgres_verify_environment(
            "postgres://user:secret@example.invalid/memories?search_path=tenant_a&options%5Bsearch_path%5D=tenant_b",
            "memories_atlas_verify_123",
        )
        .unwrap_err();

        assert!(
            error
                .to_string()
                .contains("conflicting PostgreSQL search_path")
        );
    }

    #[test]
    fn atlas_config_is_resolved_from_the_fixed_artifact_root() {
        let path = atlas_config_path("migrate.hcl").unwrap();
        assert!(path.starts_with("file://"));
        assert!(path.ends_with("infra/atlas/migrate.hcl"));
    }

    #[test]
    fn checked_in_atlas_migration_directories_match_their_integrity_sums() {
        let root = atlas_artifact_root().unwrap();
        verify_atlas_sum(&root, "sqlite").unwrap();
        verify_atlas_sum(&root, "postgres").unwrap();
    }

    #[test]
    fn atlas_platform_name_supports_server_and_desktop_targets() {
        assert_eq!(
            atlas_platform_name("linux", "x86_64").unwrap(),
            "linux-amd64"
        );
        assert_eq!(
            atlas_platform_name("macos", "aarch64").unwrap(),
            "darwin-arm64"
        );
        assert!(atlas_platform_name("windows", "x86_64").is_err());
        assert!(atlas_platform_name("macos", "x86_64").is_err());
    }

    #[test]
    fn checked_in_atlas_tool_lock_has_fixed_server_and_desktop_downloads() {
        let lock = load_atlas_tool_lock(&atlas_artifact_root().unwrap()).unwrap();
        assert!(!lock.version.is_empty());
        for name in ["linux-amd64", "darwin-arm64"] {
            let platform = lock.platform(name).unwrap();
            assert_eq!(
                platform.url,
                format!(
                    "https://release.ariga.io/atlas/atlas-{name}-{}",
                    lock.version
                )
            );
            assert_eq!(platform.sha256.len(), 64);
            assert!(platform.sha256.bytes().all(|byte| byte.is_ascii_hexdigit()));
        }
    }

    #[test]
    fn sqlite_bundle_builder_uses_the_native_platform_fetcher() {
        const BUNDLE_BUILDER: &str =
            include_str!("../../../scripts/build-memories-db-migrate-sqlite.sh");
        const ATLAS_FETCHER: &str = include_str!("../../../scripts/fetch-atlas.sh");

        assert!(BUNDLE_BUILDER.contains("native_atlas_platform"));
        assert!(BUNDLE_BUILDER.contains("fetch-atlas.sh"));
        assert!(ATLAS_FETCHER.contains("darwin-arm64"));
        assert!(ATLAS_FETCHER.contains("linux-amd64"));
    }

    #[test]
    fn checked_in_seed_expectations_are_nonempty_and_backend_equivalent() {
        let root = atlas_artifact_root().unwrap();
        let sqlite = load_seed_expectations(&root, "sqlite").unwrap();
        let postgres = load_seed_expectations(&root, "postgres").unwrap();
        assert_eq!(sqlite.version, "20260803000001");
        assert_eq!(sqlite.version, postgres.version);
        assert!(!sqlite.tables.is_empty());
        assert_eq!(
            sqlite
                .tables
                .iter()
                .map(|table| (&table.table, &table.key_column, &table.keys))
                .collect::<Vec<_>>(),
            postgres
                .tables
                .iter()
                .map(|table| (&table.table, &table.key_column, &table.keys))
                .collect::<Vec<_>>()
        );
    }

    #[cfg(not(feature = "postgres"))]
    #[test]
    fn seed_expectations_detect_missing_static_rows() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            use sqlx::sqlite::SqlitePoolOptions;

            let root = atlas_artifact_root().unwrap();
            let expectations = load_seed_expectations(&root, "sqlite").unwrap();
            let pool = SqlitePoolOptions::new()
                .max_connections(1)
                .connect("sqlite::memory:")
                .await
                .unwrap();
            for expectation in &expectations.tables {
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "CREATE TABLE {} ({} TEXT PRIMARY KEY)",
                    expectation.table, expectation.key_column
                )))
                .execute(&pool)
                .await
                .unwrap();
                for key in &expectation.keys {
                    sqlx::query(sqlx::AssertSqlSafe(format!(
                        "INSERT INTO {} ({}) VALUES (?)",
                        expectation.table, expectation.key_column
                    )))
                    .bind(key)
                    .execute(&pool)
                    .await
                    .unwrap();
                }
            }

            verify_seed_expectations(&pool, &root, "sqlite", &expectations.version)
                .await
                .unwrap();
            sqlx::query("DELETE FROM failure_mode_dictionary WHERE mode = ?")
                .bind("OTHER")
                .execute(&pool)
                .await
                .unwrap();
            assert!(
                verify_seed_expectations(&pool, &root, "sqlite", &expectations.version)
                    .await
                    .is_err()
            );
        });
    }

    #[cfg(not(feature = "postgres"))]
    struct LocalE2e {
        _temporary: tempfile::TempDir,
        artifact_root: String,
        binary: std::path::PathBuf,
        database_url: String,
        vector_uri: std::path::PathBuf,
        backups: std::path::PathBuf,
    }

    #[cfg(not(feature = "postgres"))]
    impl LocalE2e {
        fn new() -> Self {
            let temporary = tempfile::tempdir().unwrap();
            let (database_url, vector_uri) = sqlite_e2e_target_paths(temporary.path());
            let backups = temporary.path().join("backups with space");
            Self {
                artifact_root: fixed_e2e_atlas_artifact_root().unwrap(),
                binary: fixed_e2e_release_binary().unwrap(),
                database_url,
                vector_uri,
                backups,
                _temporary: temporary,
            }
        }

        fn command(&self) -> MigrationE2eCommand<'_> {
            MigrationE2eCommand {
                binary: &self.binary,
                artifact_root: &self.artifact_root,
                database_url: &self.database_url,
                vector_uri: &self.vector_uri,
            }
        }

        fn database(&self) -> std::path::PathBuf {
            url::Url::parse(&self.database_url)
                .unwrap()
                .to_file_path()
                .unwrap()
        }

        /// Exit code and the final stdout line of a `local` command.
        async fn local(&self, arguments: &[&str]) -> (Option<i32>, String) {
            let (code, stdout, _) = self.command().run_with_status(arguments).await.unwrap();
            let line = stdout.lines().last().unwrap_or_default().to_string();
            (code, line)
        }

        async fn apply(&self) -> (Option<i32>, String) {
            let backups = self.backups.to_str().unwrap().to_string();
            self.local(&[
                "local",
                "apply",
                "--maintenance-window-ack",
                "--backup-dir",
                &backups,
            ])
            .await
        }

        fn backup_from(line: &str) -> std::path::PathBuf {
            let encoded = line
                .split(' ')
                .find_map(|field| field.strip_prefix("backup="))
                .unwrap_or_else(|| panic!("no backup in {line}"));
            // Output values use URL-style percent-encoding of the path.
            url::Url::parse(&format!("file://{encoded}"))
                .unwrap()
                .to_file_path()
                .unwrap()
        }

        async fn pool(&self) -> RdbPool {
            sqlx::Pool::<Rdb>::connect(&self.database_url)
                .await
                .unwrap()
        }

        fn target(&self) -> grpc_admin::db_migrate::local::target::SqliteTarget {
            grpc_admin::db_migrate::local::target::SqliteTarget::at(self.database())
        }

        fn attempt_record(&self) -> std::path::PathBuf {
            grpc_admin::db_migrate::local::attempt::AttemptRecord::path(&self.target())
        }

        fn embedding_state_dir(&self) -> std::path::PathBuf {
            self.vector_uri.parent().unwrap().join("embedding-state")
        }

        /// Migrate to the previous release only, with Atlas alone (no
        /// task runs), as a database the previous release left behind.
        fn apply_previous_release(&self) {
            let previous = atlas_migration_versions().len() - 1;
            let applied = std::process::Command::new(
                std::path::Path::new(&self.artifact_root).join("bin/atlas"),
            )
            .current_dir(&self.artifact_root)
            .args(["migrate", "apply", &previous.to_string()])
            .args(["--config", "file://migrate.hcl", "--env", "sqlite"])
            .env(
                "MEMORIES_ATLAS_DATABASE_URL",
                atlas_database_url(&self.database_url).unwrap(),
            )
            .output()
            .unwrap();
            assert!(
                applied.status.success(),
                "{}",
                String::from_utf8_lossy(&applied.stderr)
            );
        }

        async fn embedding_attempt(
            &self,
            marker: Option<infra::infra::embedding_space::MarkerState>,
            stage: grpc_admin::db_migrate::embedding::attempt::Stage,
        ) {
            write_embedding_attempt(&self.vector_uri, &self.embedding_state_dir(), marker, stage)
                .await;
        }

        /// Record a previous attempt that left the database needing a restore.
        fn record_restore_required(&self, backup: Option<std::path::PathBuf>) {
            use grpc_admin::db_migrate::local::{
                attempt::{AttemptRecord, AttemptStatus},
                output::{Resolution, Stage},
            };
            AttemptRecord {
                attempt_id: "failed-attempt".to_string(),
                started_at: 1,
                bundle_digest: None,
                backup,
                stage: Stage::PostMigrate,
                status: AttemptStatus::Failed(Resolution::RestoreRequired),
            }
            .store(&self.target())
            .unwrap();
        }
    }

    /// `local apply` and the release commands during an unfinished
    /// embedding migration: work that is not embedding neutral is refused
    /// with the migration's next step, neutral work is applied before the
    /// commit, and nothing is refused when there is no work.
    #[cfg(not(feature = "postgres"))]
    #[test]
    #[ignore = "requires fixed Atlas artifact and MEMORIES_DB_MIGRATE_E2E_BINARY release binary"]
    fn local_apply_e2e_respects_an_unfinished_embedding_migration() {
        use grpc_admin::db_migrate::embedding::attempt::Stage;
        use infra::infra::embedding_space::MarkerState;
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            let local = LocalE2e::new();
            local.apply_previous_release();
            local
                .embedding_attempt(Some(MarkerState::Pending), Stage::RebuildPending)
                .await;
            // The previous release's tasks never ran; one of them touches
            // thread vectors.
            let (code, line) = local.apply().await;
            assert_eq!(code, Some(1));
            assert_eq!(
                line,
                "local_apply status=failed stage=preflight error_code=embedding_migration_in_progress \
                 resolution=continue_or_restore attempt=a1 attempt_stage=rebuild_pending \
                 backup_mode=backup backup=/embedding-backup"
            );
            let pool = local.pool().await;
            assert!(
                !super::table_exists(&pool, "memories_storage_identity")
                    .await
                    .unwrap(),
                "nothing was applied"
            );
            pool.close().await;
            let (code, _, stderr) = local
                .command()
                .run_with_status(&[
                    "post-migrate",
                    "run",
                    "--all-required",
                    "--maintenance-window-ack",
                ])
                .await
                .unwrap();
            assert_ne!(code, Some(0));
            assert!(
                stderr.contains("error_code=embedding_migration_in_progress resolution=continue_or_restore"),
                "{stderr}"
            );

            local
                .embedding_attempt(Some(MarkerState::Committing), Stage::Commit)
                .await;
            let (code, line) = local.apply().await;
            assert_eq!(code, Some(1));
            assert!(
                line.ends_with(
                    "error_code=embedding_migration_in_progress resolution=finalize_required \
                     attempt=a1 attempt_stage=commit backup_mode=backup"
                ),
                "{line}"
            );

            // Once the migration has ended the work applies; with no work
            // left, nothing is refused even in the middle of a commit.
            local.embedding_attempt(None, Stage::Backup).await;
            let (code, line) = local.apply().await;
            assert_eq!(code, Some(0), "{line}");
            assert!(
                line.starts_with("local_apply status=completed outcome=migrated"),
                "{line}"
            );
            local
                .embedding_attempt(Some(MarkerState::Committing), Stage::Commit)
                .await;
            assert_eq!(
                local.apply().await,
                (
                    Some(0),
                    "local_apply status=completed outcome=no_op".to_string()
                )
            );
        });
    }

    /// `local restore` during an unfinished embedding migration, and of a
    /// backup whose vector tables belong to another embedding space.
    #[cfg(not(feature = "postgres"))]
    #[test]
    #[ignore = "requires fixed Atlas artifact and MEMORIES_DB_MIGRATE_E2E_BINARY release binary"]
    fn local_restore_e2e_respects_the_embedding_space() {
        use grpc_admin::db_migrate::embedding::attempt::Stage;
        use infra::infra::embedding_space::MarkerState;
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            let local = LocalE2e::new();
            local.apply_previous_release();
            // The thread table exists, so the backup holds it.
            local.embedding_attempt(None, Stage::Backup).await;
            let (code, line) = local.apply().await;
            assert_eq!(code, Some(0), "{line}");
            let backup = LocalE2e::backup_from(&line);
            let manifest_path = backup.join("manifest.json");
            let manifest: serde_json::Value =
                serde_json::from_slice(&std::fs::read(&manifest_path).unwrap()).unwrap();
            assert!(
                manifest["resources"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|r| r["present"] == true),
                "the backup holds the thread vectors: {manifest}"
            );
            let backup_arg = backup.to_string_lossy().into_owned();
            let restore = [
                "local",
                "restore",
                "--maintenance-window-ack",
                "--backup",
                backup_arg.as_str(),
            ];

            local
                .embedding_attempt(Some(MarkerState::Pending), Stage::RebuildPending)
                .await;
            let (code, line) = local.local(&restore).await;
            assert_eq!(code, Some(1));
            assert_eq!(
                line,
                "local_restore status=failed stage=restore_preflight \
                 error_code=embedding_migration_in_progress resolution=continue_or_restore \
                 attempt=a1 attempt_stage=rebuild_pending backup_mode=backup backup=/embedding-backup"
            );

            // A vector store recorded as absent is restored by removing
            // the current one, so it counts as vector tables.
            let mut absent = manifest.clone();
            for r in absent["resources"].as_array_mut().unwrap() {
                r["present"] = serde_json::json!(false);
                r["files"] = serde_json::json!([]);
            }
            std::fs::write(&manifest_path, serde_json::to_vec(&absent).unwrap()).unwrap();
            let (code, line) = local.local(&restore).await;
            assert_eq!(code, Some(1));
            assert!(
                line.contains("error_code=embedding_migration_in_progress"),
                "{line}"
            );

            // A backup of the database alone may be restored meanwhile.
            let mut sqlite_only = manifest.clone();
            sqlite_only["resources"] = serde_json::json!([]);
            std::fs::write(&manifest_path, serde_json::to_vec(&sqlite_only).unwrap()).unwrap();
            let (code, line) = local.local(&restore).await;
            assert_eq!(code, Some(0), "{line}");
            assert_eq!(line, "local_restore status=completed next_action=apply");

            // Vector tables of another space are never restored.
            local.embedding_attempt(None, Stage::Backup).await;
            let mut other_space = manifest.clone();
            other_space["embedding_space_id"] = serde_json::json!("other-space");
            std::fs::write(&manifest_path, serde_json::to_vec(&other_space).unwrap()).unwrap();
            let table = infra::infra::vector_table::open_existing(
                &local.vector_uri.to_string_lossy(),
                "threads",
            )
            .await
            .unwrap()
            .unwrap();
            infra::infra::embedding_space::record::write_space_record(
                &table,
                &infra::infra::embedding_space::SpaceRecord {
                    space_id: infra::infra::embedding_space::SpaceId("current-space".into()),
                    components: None,
                    legacy_accept: false,
                },
            )
            .await
            .unwrap();
            let (code, line) = local.local(&restore).await;
            assert_eq!(code, Some(1));
            assert!(
                line.contains(
                    "stage=restore_preflight error_code=backup_space_mismatch resolution=manual_recovery"
                ),
                "{line}"
            );
        });
    }

    #[cfg(not(feature = "postgres"))]
    #[test]
    #[ignore = "requires fixed Atlas artifact and MEMORIES_DB_MIGRATE_E2E_BINARY release binary"]
    fn bundle_e2e_records_its_identity_and_detects_changes() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            let artifact_root = fixed_e2e_atlas_artifact_root().unwrap();
            let binary = fixed_e2e_release_binary().unwrap();
            let bundle = tempfile::tempdir().unwrap();
            let bundled_binary = bundle.path().join("memories-db-migrate");
            std::fs::copy(&binary, &bundled_binary).unwrap();
            let copied = std::process::Command::new("cp")
                .args(["-R", &artifact_root])
                .arg(bundle.path().join("atlas"))
                .status()
                .unwrap();
            assert!(copied.success());
            let run = |arguments: &[&str]| {
                let output = std::process::Command::new(&bundled_binary)
                    .env_clear()
                    .args(arguments)
                    .output()
                    .unwrap();
                (
                    output.status.code(),
                    String::from_utf8_lossy(&output.stdout).trim().to_string(),
                )
            };

            let (code, unidentified) = run(&["bundle", "verify"]);
            assert_eq!(code, Some(1));
            assert_eq!(
                unidentified,
                "bundle_verify status=failed error_code=bundle_invalid"
            );
            let (code, written) = run(&["bundle", "manifest", "--source-revision", "abc"]);
            assert_eq!(code, Some(0), "{written}");
            let digest = written
                .strip_prefix("bundle_manifest status=written digest=")
                .unwrap()
                .to_string();
            assert_eq!(
                run(&["bundle", "verify"]),
                (
                    Some(0),
                    format!("bundle_verify status=verified digest={digest}")
                )
            );

            std::fs::write(bundle.path().join("atlas/licenses/ATLAS_LICENSE"), b"").unwrap();
            assert_eq!(run(&["bundle", "verify"]).0, Some(1));
        });
    }

    #[cfg(not(feature = "postgres"))]
    #[test]
    #[ignore = "requires fixed Atlas artifact and MEMORIES_DB_MIGRATE_E2E_BINARY release binary"]
    fn local_apply_e2e_completes_every_database_state_without_caller_branching() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            // Uninitialized: nothing to protect, so no backup is created.
            let fresh = LocalE2e::new();
            assert_eq!(
                fresh.apply().await,
                (
                    Some(0),
                    "local_apply status=completed outcome=migrated".to_string()
                )
            );
            assert!(!fresh.backups.exists());
            // Latest: verified as complete without changes or a backup.
            assert_eq!(
                fresh.apply().await,
                (
                    Some(0),
                    "local_apply status=completed outcome=no_op".to_string()
                )
            );
            assert!(!fresh.backups.exists());

            // baseline_required: adopted automatically, with a backup first.
            let legacy = LocalE2e::new();
            let pool = legacy.pool().await;
            seed_adoption_candidate_schema(&pool, ADOPTION_BASELINE_VERSION)
                .await
                .unwrap();
            pool.close().await;
            let (code, line) = legacy.apply().await;
            assert_eq!(code, Some(0), "{line}");
            let manifest = grpc_admin::db_migrate::local::backup::BackupManifest::load(
                &LocalE2e::backup_from(&line),
            )
            .unwrap();
            assert_eq!(manifest.schema_status, "baseline_required");
            let pool = legacy.pool().await;
            assert_eq!(schema_state(&pool).await.unwrap(), SchemaState::Managed);
            pool.close().await;

            // Schema applied but required tasks not run: the tasks' LanceDB
            // directory is part of the backup.
            let tasks_pending = LocalE2e::new();
            tasks_pending
                .command()
                .run(&["schema", "apply"])
                .await
                .unwrap();
            let (code, line) = tasks_pending.apply().await;
            assert_eq!(code, Some(0), "{line}");
            let manifest = grpc_admin::db_migrate::local::backup::BackupManifest::load(
                &LocalE2e::backup_from(&line),
            )
            .unwrap();
            assert_eq!(manifest.schema_status, "managed");
            assert_eq!(
                manifest
                    .resources
                    .iter()
                    .map(|resource| resource.name.as_str())
                    .collect::<Vec<_>>(),
                vec!["thread_lancedb"]
            );
            assert_eq!(
                tasks_pending.apply().await.1,
                "local_apply status=completed outcome=no_op"
            );
        });
    }

    /// Runs `embedding` commands of the release binary against a migrated
    /// database (a local SQLite file, or a fresh schema in
    /// `TEST_POSTGRES_URL`) whose thread store is the only vector store.
    /// Everything lives in a temporary fixture.
    struct EmbeddingE2e {
        binary: std::path::PathBuf,
        artifact_root: String,
        /// The URL handed to the binary.
        database_url: String,
        vector_uri: std::path::PathBuf,
        workers_yaml: std::path::PathBuf,
        state_dir: std::path::PathBuf,
        backups: std::path::PathBuf,
        _fixture: tempfile::TempDir,
        /// `(service URL, schema)` dropped with the fixture.
        #[cfg(feature = "postgres")]
        schema: (String, String),
    }

    /// An unfinished embedding migration (white-box): the attempt record
    /// in `state_dir` and the marker of the thread table at `vector_uri`
    /// (`None` ends it).
    async fn write_embedding_attempt(
        vector_uri: &std::path::Path,
        state_dir: &std::path::Path,
        marker: Option<infra::infra::embedding_space::MarkerState>,
        stage: grpc_admin::db_migrate::embedding::attempt::Stage,
    ) {
        use grpc_admin::db_migrate::embedding::attempt::{self, AttemptRecord, Method, Status};
        use infra::infra::embedding_space::{MigrationMarker, SpaceComponents, replace};
        let spec = replace::StoreSpec {
            label: infra::infra::embedding_index::TableLabel::Thread,
            uri: vector_uri.to_string_lossy().into_owned(),
            table_name: "threads".into(),
        };
        let table = replace::open_or_create(&spec, 4).await.unwrap();
        infra::infra::embedding_space::record::write_marker(
            &table,
            marker
                .map(|state| MigrationMarker {
                    state,
                    attempt_id: "a1".into(),
                })
                .as_ref(),
        )
        .await
        .unwrap();
        std::fs::create_dir_all(state_dir).unwrap();
        if marker.is_none() {
            let _ = std::fs::remove_file(state_dir.join("attempt.json"));
            return;
        }
        attempt::save(
            state_dir,
            &AttemptRecord {
                format_version: attempt::FORMAT_VERSION,
                attempt_id: "a1".into(),
                method: Method::Backup,
                source_space_id: None,
                target_space_id: "t".into(),
                target_space: SpaceComponents {
                    model_id: "m".into(),
                    tokenizer_model_id: String::new(),
                    revision: "r".into(),
                    dimension: 4,
                    distance: "cosine".into(),
                },
                backup_path: Some("/embedding-backup".into()),
                backup_complete: true,
                stage,
                status: Status::Running,
                cancel_operation: None,
                discarded: false,
                accepted_failed: None,
                backup_keep: None,
                started_at: 0,
            },
        )
        .unwrap();
    }

    /// Bind placeholder of the compiled backend.
    fn e2e_placeholder(index: usize) -> String {
        if cfg!(feature = "postgres") {
            format!("${index}")
        } else {
            "?".to_string()
        }
    }

    impl EmbeddingE2e {
        #[cfg(not(feature = "postgres"))]
        async fn new() -> Self {
            let local = LocalE2e::new();
            assert_eq!(local.apply().await.0, Some(0));
            let LocalE2e {
                _temporary,
                artifact_root,
                binary,
                database_url,
                vector_uri,
                ..
            } = local;
            Self::with_database(binary, artifact_root, database_url, vector_uri, _temporary)
        }

        #[cfg(feature = "postgres")]
        async fn new() -> Self {
            let e = Self::new_postgres().await;
            let (code, line) = e
                .run(&["release", "apply", "--maintenance-window-ack"])
                .await;
            assert_eq!(code, Some(0), "{line}");
            e
        }

        /// A schema the previous release left behind: Atlas alone up to the
        /// previous version, so the latest migration and the tasks are
        /// still pending.
        #[cfg(feature = "postgres")]
        async fn new_previous_release() -> Self {
            let e = Self::new_postgres().await;
            let previous = atlas_migration_versions().len() - 1;
            let applied = std::process::Command::new(
                std::path::Path::new(&e.artifact_root).join("bin/atlas"),
            )
            .current_dir(&e.artifact_root)
            .args(["migrate", "apply", &previous.to_string()])
            .args(["--config", "file://migrate.hcl", "--env", "postgres"])
            .env(
                "MEMORIES_ATLAS_DATABASE_URL",
                atlas_database_url(&e.database_url).unwrap(),
            )
            .output()
            .unwrap();
            assert!(
                applied.status.success(),
                "{}",
                String::from_utf8_lossy(&applied.stderr)
            );
            e
        }

        #[cfg(feature = "postgres")]
        async fn new_postgres() -> Self {
            use sqlx::postgres::PgPoolOptions;
            let service_url = std::env::var("TEST_POSTGRES_URL")
                .expect("TEST_POSTGRES_URL must be set for the PostgreSQL E2E test");
            let schema = format!(
                "memories_embedding_e2e_{}_{}",
                std::process::id(),
                command_utils::util::datetime::now_millis()
            );
            let admin = PgPoolOptions::new()
                .max_connections(1)
                .connect(&service_url)
                .await
                .unwrap();
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "CREATE SCHEMA {}",
                quote_postgres_identifier(&schema)
            )))
            .execute(&admin)
            .await
            .unwrap();
            admin.close().await;
            let fixture = tempfile::tempdir().unwrap();
            let vector_uri = fixture.path().join("threads.lancedb");
            let mut e = Self::with_database(
                fixed_e2e_release_binary().unwrap(),
                fixed_e2e_atlas_artifact_root().unwrap(),
                postgres_schema_url(&service_url, &schema).unwrap(),
                vector_uri,
                fixture,
            );
            e.schema = (service_url, schema);
            e
        }

        #[cfg(feature = "postgres")]
        async fn embedding_attempt(
            &self,
            marker: Option<infra::infra::embedding_space::MarkerState>,
            stage: grpc_admin::db_migrate::embedding::attempt::Stage,
        ) {
            write_embedding_attempt(&self.vector_uri, &self.state_dir, marker, stage).await;
        }

        fn with_database(
            binary: std::path::PathBuf,
            artifact_root: String,
            database_url: String,
            vector_uri: std::path::PathBuf,
            fixture: tempfile::TempDir,
        ) -> Self {
            let root = vector_uri.parent().unwrap().to_path_buf();
            let workers_yaml = root.join("workers.yaml");
            std::fs::write(
                &workers_yaml,
                "workers:\n  - name: mm\n    runner: MultimodalEmbeddingRunner\n    settings:\n      model_id: source-model\n",
            )
            .unwrap();
            Self {
                binary,
                artifact_root,
                database_url,
                state_dir: root.join("embedding-state"),
                backups: root.join("embedding backups"),
                workers_yaml,
                vector_uri,
                _fixture: fixture,
                #[cfg(feature = "postgres")]
                schema: (String::new(), String::new()),
            }
        }

        /// What a running memories server looks like to the changing
        /// commands, until the returned value is dropped.
        async fn hold_writer(&self) -> Box<dyn std::any::Any + Send> {
            let pool = self.pool().await;
            #[cfg(feature = "postgres")]
            {
                use infra::infra::embedding_space::writer_lock::WRITER_KEY;
                let mut conn = pool.acquire().await.unwrap().detach();
                let held: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock_shared($1)")
                    .bind(WRITER_KEY)
                    .fetch_one(&mut conn)
                    .await
                    .unwrap();
                assert!(held);
                Box::new(conn)
            }
            #[cfg(not(feature = "postgres"))]
            {
                // The fixture database is not in WAL mode, where only a
                // connection inside a transaction is detectable.
                let mut tx = pool.begin().await.unwrap();
                sqlx::query("SELECT COUNT(*) FROM thread")
                    .execute(&mut *tx)
                    .await
                    .unwrap();
                Box::new((tx, pool))
            }
        }

        fn root(&self) -> &std::path::Path {
            self.vector_uri.parent().unwrap()
        }

        async fn pool(&self) -> RdbPool {
            #[cfg(feature = "postgres")]
            let url = super::postgres_sqlx_database_url(&self.database_url).unwrap();
            #[cfg(not(feature = "postgres"))]
            let url = self.database_url.clone();
            sqlx::Pool::<Rdb>::connect(&url).await.unwrap()
        }

        /// The binary in the fixture, with only the given environment (it
        /// loads `.env` from its working directory upwards, so it never
        /// runs where a developer's `.env` with real stores is found).
        fn command(&self) -> tokio::process::Command {
            let mut command = tokio::process::Command::new(&self.binary);
            command
                .current_dir(self.root())
                .env_clear()
                .env("MEMORIES_ATLAS_DIR", &self.artifact_root)
                .env("MEMORIES_ATLAS_DATABASE_URL", &self.database_url)
                .env("MEMORY_WORKERS_YAML", &self.workers_yaml)
                .env("MEMORY_EMBEDDING_STATE_DIR", &self.state_dir)
                .stdin(Stdio::null());
            command
        }

        async fn run(&self, arguments: &[&str]) -> (Option<i32>, String) {
            let (code, stdout, _) = self.run_full(arguments).await;
            (code, stdout.lines().last().unwrap_or_default().to_string())
        }

        /// Exit code, stdout, and stderr.
        async fn run_full(&self, arguments: &[&str]) -> (Option<i32>, String, String) {
            let output = self
                .command()
                .args(arguments)
                .env("THREAD_VECTOR_ENABLED", "true")
                .env("THREAD_LANCEDB_URI", &self.vector_uri)
                .env("THREAD_LANCEDB_TABLE", "threads")
                .env("THREAD_VECTOR_SIZE", "4")
                .envs(E2E_FIXED_SEARCH_ENVIRONMENT)
                .output()
                .await
                .unwrap();
            (
                output.status.code(),
                String::from_utf8_lossy(&output.stdout).to_string(),
                String::from_utf8_lossy(&output.stderr).to_string(),
            )
        }

        fn field<'a>(line: &'a str, key: &str) -> &'a str {
            line.split(' ')
                .find_map(|kv| kv.strip_prefix(&format!("{key}=")))
                .unwrap_or_else(|| panic!("{key} missing in {line}"))
        }

        async fn add_thread(&self, description: &str) -> i64 {
            let pool = self.pool().await;
            let id = rand::random::<u32>() as i64 + 1;
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "INSERT INTO thread (id, user_id, channel, description, created_at, updated_at, memory_kind) VALUES ({}, 1, 'c', {}, 1, 1, 1)",
                e2e_placeholder(1),
                e2e_placeholder(2)
            )))
            .bind(id)
            .bind(description)
            .execute(&pool)
            .await
            .unwrap();
            pool.close().await;
            id
        }

        async fn delete_thread(&self, id: i64) {
            let pool = self.pool().await;
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "DELETE FROM thread WHERE id = {}",
                e2e_placeholder(1)
            )))
            .bind(id)
            .execute(&pool)
            .await
            .unwrap();
            pool.close().await;
        }

        fn uri(&self) -> String {
            self.vector_uri.to_string_lossy().into_owned()
        }

        /// The user data embedding commands must never change.
        async fn user_data(&self) -> Vec<String> {
            let pool = self.pool().await;
            let mut rows: Vec<String> = sqlx::query_as::<_, (i64, String, i64)>(
                "SELECT id, COALESCE(description, ''), updated_at FROM thread ORDER BY id",
            )
            .fetch_all(&pool)
            .await
            .unwrap()
            .into_iter()
            .map(|r| format!("thread {r:?}"))
            .collect();
            rows.extend(
                sqlx::query_as::<_, (i64, String, i64)>(
                    "SELECT id, content, updated_at FROM memory ORDER BY id",
                )
                .fetch_all(&pool)
                .await
                .unwrap()
                .into_iter()
                .map(|r| format!("memory {r:?}")),
            );
            pool.close().await;
            rows
        }

        /// Record a failed generation of a thread in `space`, as the
        /// failure report of an embedding workflow would.
        async fn fail_thread(&self, id: i64, description: &str, space: &str) {
            use infra::infra::embedding_index::source_version::{self, TextSource};
            use infra::infra::embedding_index::{EmbeddingIndex, EntryOutcome, IndexEntry};
            EmbeddingIndex::open(&self.uri())
                .await
                .unwrap()
                .put(&[IndexEntry {
                    table: infra::infra::embedding_index::TableLabel::Thread,
                    entity_id: id,
                    vector_kind: "text".into(),
                    space_id: infra::infra::embedding_space::SpaceId(space.into()),
                    source_version: source_version::text(
                        TextSource::ThreadDescription,
                        description,
                    ),
                    generation_id: "g".into(),
                    outcome: EntryOutcome::Failure {
                        reason: "fetch_failed".into(),
                        class: "permanent".into(),
                    },
                    media_digest: None,
                    recorded_at: 0,
                }])
                .await
                .unwrap();
        }

        /// Record the fixture's configured space on the thread table, as
        /// a server start would; returns its ID.
        async fn record_source_space(&self) -> String {
            use infra::infra::embedding_space::{SpaceComponents, SpaceRecord, replace};
            let spec = replace::StoreSpec {
                label: infra::infra::embedding_index::TableLabel::Thread,
                uri: self.uri(),
                table_name: "threads".into(),
            };
            let table = replace::open_or_create(&spec, 4).await.unwrap();
            let source = SpaceComponents {
                model_id: "source-model".into(),
                tokenizer_model_id: String::new(),
                revision: "unversioned".into(),
                dimension: 4,
                distance: "cosine".into(),
            };
            infra::infra::embedding_space::record::write_space_record(
                &table,
                &SpaceRecord::new(&source, false),
            )
            .await
            .unwrap();
            source.space_id().to_string()
        }

        async fn index_entries(&self) -> usize {
            infra::infra::embedding_index::EmbeddingIndex::open(&self.uri())
                .await
                .unwrap()
                .count()
                .await
                .unwrap()
        }

        async fn table(&self) -> infra::infra::vector_table::Table {
            infra::infra::vector_table::open_existing(&self.uri(), "threads")
                .await
                .unwrap()
                .expect("thread table")
        }

        async fn marker(&self) -> Option<infra::infra::embedding_space::MigrationMarker> {
            infra::infra::embedding_space::record::read_table_record(&self.table().await)
                .await
                .unwrap()
                .marker
        }

        async fn set_marker(
            &self,
            state: infra::infra::embedding_space::MarkerState,
            attempt: &str,
        ) {
            infra::infra::embedding_space::record::write_marker(
                &self.table().await,
                Some(&infra::infra::embedding_space::MigrationMarker {
                    state,
                    attempt_id: attempt.into(),
                }),
            )
            .await
            .unwrap();
        }

        /// `switch` to the 8-dimensional target model, with a backup in
        /// `self.backups` or none.
        async fn switch(&self, expected: &str, backup: bool) -> (Option<i32>, String) {
            let backups = self.backups.to_string_lossy().into_owned();
            let mut args = vec![
                "embedding",
                "switch",
                "--expected-space",
                expected,
                "--maintenance-window-ack",
                "--model-id",
                "target-model",
                "--dimension",
                "8",
            ];
            if backup {
                args.extend(["--backup-dir", backups.as_str()]);
            } else {
                args.push("--no-backup-unsafe");
            }
            self.run(&args).await
        }
    }

    #[cfg(feature = "postgres")]
    impl Drop for EmbeddingE2e {
        fn drop(&mut self) {
            let (url, schema) = self.schema.clone();
            if schema.is_empty() {
                return;
            }
            // The test runtime is busy in `block_on`; drop on a fresh one.
            let dropped = std::thread::spawn(move || {
                tokio::runtime::Runtime::new().unwrap().block_on(async {
                    let admin = sqlx::postgres::PgPoolOptions::new()
                        .max_connections(1)
                        .connect(&url)
                        .await?;
                    sqlx::query(sqlx::AssertSqlSafe(format!(
                        "DROP SCHEMA {} CASCADE",
                        quote_postgres_identifier(&schema)
                    )))
                    .execute(&admin)
                    .await?;
                    admin.close().await;
                    Ok::<_, sqlx::Error>(())
                })
            })
            .join();
            if !matches!(dropped, Ok(Ok(()))) {
                eprintln!("warning: could not drop the E2E schema");
            }
        }
    }

    /// A running writer and another changing command are both refused
    /// before anything changes; `inspect` and `plan` still answer and
    /// change nothing.
    #[test]
    #[ignore = "requires fixed Atlas artifact and MEMORIES_DB_MIGRATE_E2E_BINARY release binary (and TEST_POSTGRES_URL for postgres)"]
    fn embedding_e2e_writers_and_concurrent_commands_are_refused() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            let e = EmbeddingE2e::new().await;
            e.add_thread("topic").await;
            let before = e.user_data().await;
            let (_, line) = e.run(&["embedding", "inspect"]).await;
            let space = EmbeddingE2e::field(&line, "space").to_string();

            let writer = e.hold_writer().await;
            let (code, line) = e.switch(&space, false).await;
            assert_eq!(code, Some(1));
            assert!(
                line.contains("stage=preflight error_code=writer_active resolution=retry"),
                "{line}"
            );
            let (code, line) = e.run(&["embedding", "inspect"]).await;
            assert_eq!(code, Some(0), "{line}");
            assert_eq!(EmbeddingE2e::field(&line, "state"), "incomplete");
            let (code, line) = e
                .run(&[
                    "embedding",
                    "plan",
                    "--model-id",
                    "target-model",
                    "--dimension",
                    "8",
                ])
                .await;
            assert_eq!(code, Some(0), "{line}");
            drop(writer);
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;

            // Another command holding the operation lock (another Job).
            let pool = e.pool().await;
            let held = grpc_admin::db_migrate::embedding::lock::acquire(&pool, &e.state_dir)
                .await
                .ok()
                .unwrap();
            let (code, line) = e.switch(&space, false).await;
            assert_eq!(code, Some(1));
            assert!(
                line.contains("error_code=operation_in_progress resolution=retry"),
                "{line}"
            );
            let (_, line) = e.run(&["embedding", "inspect"]).await;
            assert_eq!(EmbeddingE2e::field(&line, "next_action"), "wait", "{line}");
            drop(held);
            pool.close().await;
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;

            assert_eq!(e.user_data().await, before);
            let (code, line) = e.switch(&space, false).await;
            assert_eq!(code, Some(0), "nothing was left behind: {line}");
        });
    }

    /// `release apply` on PostgreSQL: refused while a memories instance
    /// holds the writer key and while an unfinished embedding migration
    /// cannot take the work; applied once the migration has ended.
    #[cfg(feature = "postgres")]
    #[test]
    #[ignore = "requires TEST_POSTGRES_URL, fixed Atlas artifact, and MEMORIES_DB_MIGRATE_E2E_BINARY release binary"]
    fn release_e2e_respects_writers_and_an_unfinished_embedding_migration() {
        use grpc_admin::db_migrate::embedding::attempt::Stage;
        use infra::infra::embedding_space::MarkerState;
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            let e = EmbeddingE2e::new_previous_release().await;
            let release = ["release", "apply", "--maintenance-window-ack"];
            e.embedding_attempt(Some(MarkerState::Pending), Stage::RebuildPending)
                .await;
            let (code, _, line) = e.run_full(&release).await;
            assert_ne!(code, Some(0));
            assert!(
                line.contains(
                    "refused error_code=embedding_migration_in_progress resolution=continue_or_restore \
                     attempt=a1 attempt_stage=rebuild_pending backup_mode=backup backup=/embedding-backup"
                ),
                "{line}"
            );
            let pool = e.pool().await;
            assert!(
                !super::table_exists(&pool, "memories_storage_identity")
                    .await
                    .unwrap(),
                "nothing was applied"
            );
            pool.close().await;

            e.embedding_attempt(None, Stage::Backup).await;
            let writer = e.hold_writer().await;
            let (code, _, line) = e.run_full(&release).await;
            assert_ne!(code, Some(0));
            assert!(line.contains("refused error_code=writer_active"), "{line}");
            drop(writer);
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;

            let (code, _, line) = e.run_full(&release).await;
            assert_eq!(code, Some(0), "{line}");
            let pool = e.pool().await;
            assert!(
                super::table_exists(&pool, "memories_storage_identity")
                    .await
                    .unwrap()
            );
            pool.close().await;
        });
    }

    /// Verification refuses incomplete rebuilds and unaccepted failures,
    /// then the commit ends the attempt in the target space.
    #[test]
    #[ignore = "requires fixed Atlas artifact and MEMORIES_DB_MIGRATE_E2E_BINARY release binary (and TEST_POSTGRES_URL for postgres)"]
    fn embedding_e2e_finalize_verifies_and_accepts_failures() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            let e = EmbeddingE2e::new().await;
            let thread = e.add_thread("topic").await;
            let before = e.user_data().await;
            let (_, line) = e.run(&["embedding", "inspect"]).await;
            let space = EmbeddingE2e::field(&line, "space").to_string();
            let (code, line) = e.switch(&space, false).await;
            assert_eq!(code, Some(0), "{line}");
            let attempt = EmbeddingE2e::field(&line, "attempt").to_string();
            let target = EmbeddingE2e::field(&line, "target_space").to_string();

            let finalize = async |accept: Option<&str>| {
                let mut args = vec!["embedding", "finalize", "--attempt", attempt.as_str()];
                if let Some(n) = accept {
                    args.extend(["--accept-failed", n]);
                }
                e.run(&args).await
            };
            let (code, line) = finalize(None).await;
            assert_eq!(code, Some(1));
            assert!(
                line.contains("stage=verify error_code=rebuild_incomplete resolution=resume_rebuild"),
                "{line}"
            );
            assert_eq!(EmbeddingE2e::field(&line, "attempt"), attempt);
            assert_eq!(EmbeddingE2e::field(&line, "missing_thread"), "1");
            assert_eq!(EmbeddingE2e::field(&line, "orphan"), "0");
            assert_eq!(
                e.marker().await.map(|m| m.state),
                Some(infra::infra::embedding_space::MarkerState::Pending),
                "a failed verification changes nothing"
            );

            e.fail_thread(thread, "topic", &target).await;
            let (code, line) = finalize(None).await;
            assert_eq!(code, Some(1));
            assert!(
                line.contains("error_code=rebuild_has_failures resolution=resolve_failures"),
                "{line}"
            );
            assert_eq!(EmbeddingE2e::field(&line, "failed_thread"), "1");
            let (code, line) = finalize(Some("2")).await;
            assert_eq!(code, Some(1), "a different count is not accepted: {line}");

            let (code, line) = finalize(Some("1")).await;
            assert_eq!(code, Some(0), "{line}");
            assert_eq!(
                line,
                format!(
                    "embedding_finalize status=completed outcome=finalized attempt={attempt} space={target} accepted_failed=1 next_action=apply"
                )
            );
            assert_eq!(e.marker().await, None);
            assert_eq!(e.user_data().await, before, "switch and finalize keep user data");
            let (code, line) = finalize(None).await;
            assert_eq!(code, Some(0), "{line}");
            assert_eq!(EmbeddingE2e::field(&line, "outcome"), "already_completed");
            assert_eq!(EmbeddingE2e::field(&line, "accepted_failed"), "1");

            let (_, line) = e.run(&["embedding", "inspect"]).await;
            assert_eq!(EmbeddingE2e::field(&line, "state"), "failed", "{line}");
            assert_eq!(EmbeddingE2e::field(&line, "space"), target);
            assert_eq!(EmbeddingE2e::field(&line, "next_action"), "resolve_failed");
            for cmd in [
                vec!["embedding", "abandon", "--attempt", attempt.as_str()],
                vec!["embedding", "restore", "--attempt", attempt.as_str()],
                vec!["embedding", "switch", "--resume", attempt.as_str()],
            ] {
                let (code, line) = e.run(&cmd).await;
                assert_eq!(code, Some(1));
                assert!(
                    line.contains("error_code=attempt_finished resolution=plan_required"),
                    "{line}"
                );
            }
        });
    }

    /// A commit interrupted after `committing` is completed without
    /// verifying again; one interrupted after `completed` only clears
    /// the markers.
    #[test]
    #[ignore = "requires fixed Atlas artifact and MEMORIES_DB_MIGRATE_E2E_BINARY release binary (and TEST_POSTGRES_URL for postgres)"]
    fn embedding_e2e_interrupted_finalize_completes_on_rerun() {
        use grpc_admin::db_migrate::embedding::attempt::{self, Stage, Status};
        use infra::infra::embedding_space::MarkerState;
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            let e = EmbeddingE2e::new().await;
            e.add_thread("never embedded").await;
            let (_, line) = e.run(&["embedding", "inspect"]).await;
            let space = EmbeddingE2e::field(&line, "space").to_string();
            let (_, line) = e.switch(&space, false).await;
            let attempt = EmbeddingE2e::field(&line, "attempt").to_string();
            let finalize = ["embedding", "finalize", "--attempt", attempt.as_str()];

            // After the last `committing` marker, before `completed`; the
            // server had pinned the chunking settings meanwhile.
            infra::infra::embedding_space::record::write_rebuild_chunking(
                &e.table().await,
                Some("{}"),
            )
            .await
            .unwrap();
            e.set_marker(MarkerState::Committing, &attempt).await;
            let mut record = attempt::load(&e.state_dir).unwrap().unwrap();
            record.stage = Stage::Commit;
            attempt::save(&e.state_dir, &record).unwrap();
            let (code, line) = e.run(&["embedding", "inspect"]).await;
            assert_eq!(code, Some(0));
            assert_eq!(
                EmbeddingE2e::field(&line, "attempt_stage"),
                "commit",
                "{line}"
            );
            assert_eq!(EmbeddingE2e::field(&line, "next_action"), "finalize");
            // The stage table answers before the ordinary checks: a running
            // writer does not change it.
            let writer = e.hold_writer().await;
            let (code, line) = e.run(&["embedding", "switch", "--resume", &attempt]).await;
            assert_eq!(code, Some(1));
            assert!(
                line.contains("error_code=finalize_in_progress resolution=finalize_required"),
                "{line}"
            );
            let (code, line) = e.run(&finalize).await;
            assert_eq!(code, Some(1));
            assert!(line.contains("error_code=writer_active"), "{line}");
            drop(writer);
            // Released asynchronously (connection close / rollback).
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;
            let (code, line) = e.run(&finalize).await;
            assert_eq!(
                code,
                Some(0),
                "the missing thread is not verified again: {line}"
            );
            assert_eq!(EmbeddingE2e::field(&line, "outcome"), "finalized");
            assert_eq!(e.marker().await, None);
            let record = infra::infra::embedding_space::record::read_table_record(&e.table().await)
                .await
                .unwrap();
            assert_eq!(
                record.rebuild_chunking, None,
                "the pin ends with the rebuild"
            );

            // After `completed`, before the markers were cleared.
            e.set_marker(MarkerState::Committing, &attempt).await;
            assert_eq!(
                attempt::load(&e.state_dir).unwrap().unwrap().status,
                Status::Completed
            );
            let (code, line) = e.run(&finalize).await;
            assert_eq!(code, Some(0), "{line}");
            assert_eq!(EmbeddingE2e::field(&line, "outcome"), "already_completed");
            assert_eq!(e.marker().await, None);
        });
    }

    /// `restore` puts the backed-up tables and index back, removes what
    /// the RDB no longer has, and ends the attempt; reruns and
    /// interrupted runs complete idempotently.
    #[test]
    #[ignore = "requires fixed Atlas artifact and MEMORIES_DB_MIGRATE_E2E_BINARY release binary (and TEST_POSTGRES_URL for postgres)"]
    fn embedding_e2e_restore_puts_the_backup_back() {
        use grpc_admin::db_migrate::embedding::attempt::{self, CancelOperation, Status};
        use infra::infra::embedding_space::MarkerState;
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            let e = EmbeddingE2e::new().await;
            let kept = e.add_thread("kept").await;
            let gone = e.add_thread("gone").await;
            let space = e.record_source_space().await;
            let (_, line) = e.run(&["embedding", "inspect"]).await;
            assert_eq!(EmbeddingE2e::field(&line, "space"), space, "{line}");
            // Source-space index entries end up in the backup.
            e.fail_thread(kept, "kept", &space).await;
            e.fail_thread(gone, "gone", &space).await;
            let (code, line) = e.switch(&space, true).await;
            assert_eq!(code, Some(0), "{line}");
            let attempt = EmbeddingE2e::field(&line, "attempt").to_string();
            let backup = attempt::load(&e.state_dir)
                .unwrap()
                .unwrap()
                .backup_path
                .unwrap();
            assert_eq!(e.index_entries().await, 0, "switch emptied the index");
            e.delete_thread(gone).await;
            let before = e.user_data().await;

            let restore = ["embedding", "restore", "--attempt", attempt.as_str()];
            let (code, line) = e.run(&restore).await;
            assert_eq!(code, Some(0), "{line}");
            assert_eq!(
                line,
                format!(
                    "embedding_restore status=completed outcome=restored attempt={attempt} space={space} next_action=reconcile"
                )
            );
            assert_eq!(e.marker().await, None);
            assert_eq!(e.user_data().await, before, "restore keeps user data");
            assert_eq!(e.index_entries().await, 1, "the deleted thread's entry is gone");
            let (_, line) = e.run(&["embedding", "inspect"]).await;
            assert_eq!(EmbeddingE2e::field(&line, "space"), space, "{line}");
            assert_eq!(EmbeddingE2e::field(&line, "orphan"), "0");
            assert_eq!(EmbeddingE2e::field(&line, "failed_permanent"), "1");
            assert!(std::path::Path::new(&backup).exists());

            let (code, line) = e.run(&restore).await;
            assert_eq!(code, Some(0), "{line}");
            assert_eq!(EmbeddingE2e::field(&line, "outcome"), "already_restored");
            let (code, line) = e
                .run(&["embedding", "finalize", "--attempt", &attempt])
                .await;
            assert_eq!(code, Some(1));
            assert!(
                line.contains("error_code=rebuild_not_pending resolution=plan_required"),
                "{line}"
            );

            // Interrupted after `restored` was recorded, markers left.
            e.set_marker(MarkerState::Restoring, &attempt).await;
            let (code, line) = e
                .run(&["embedding", "finalize", "--attempt", &attempt])
                .await;
            assert_eq!(code, Some(1));
            assert!(line.contains("error_code=cancel_in_progress"), "{line}");
            assert_eq!(EmbeddingE2e::field(&line, "cancel_operation"), "restore");
            let (code, line) = e.run(&restore).await;
            assert_eq!(code, Some(0), "{line}");
            assert_eq!(EmbeddingE2e::field(&line, "outcome"), "already_restored");
            assert_eq!(e.marker().await, None);

            // Interrupted in the middle: the restore runs again in full.
            let mut record = attempt::load(&e.state_dir).unwrap().unwrap();
            record.status = Status::Running;
            record.cancel_operation = Some(CancelOperation::Restore);
            attempt::save(&e.state_dir, &record).unwrap();
            e.set_marker(MarkerState::Restoring, &attempt).await;
            let (code, line) = e.run(&restore).await;
            assert_eq!(code, Some(0), "{line}");
            assert_eq!(EmbeddingE2e::field(&line, "outcome"), "restored");
            assert_eq!(e.marker().await, None);
        });
    }

    /// A broken backup manifest is reported before anything changes.
    #[test]
    #[ignore = "requires fixed Atlas artifact and MEMORIES_DB_MIGRATE_E2E_BINARY release binary (and TEST_POSTGRES_URL for postgres)"]
    fn embedding_e2e_restore_refuses_a_corrupt_backup() {
        use infra::infra::embedding_space::MarkerState;
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            let e = EmbeddingE2e::new().await;
            e.add_thread("topic").await;
            let (_, line) = e.run(&["embedding", "inspect"]).await;
            let space = EmbeddingE2e::field(&line, "space").to_string();
            let (_, line) = e.switch(&space, true).await;
            let attempt = EmbeddingE2e::field(&line, "attempt").to_string();
            let backup = grpc_admin::db_migrate::embedding::attempt::load(&e.state_dir)
                .unwrap()
                .unwrap()
                .backup_path
                .unwrap();
            std::fs::write(std::path::Path::new(&backup).join("manifest.json"), b"{").unwrap();
            let (code, line) = e
                .run(&["embedding", "restore", "--attempt", &attempt])
                .await;
            assert_eq!(code, Some(1));
            assert!(
                line.contains(
                    "stage=validate error_code=resource_corrupt resolution=manual_recovery"
                ),
                "{line}"
            );
            assert_eq!(
                e.marker().await.map(|m| m.state),
                Some(MarkerState::Pending),
                "nothing changed"
            );
        });
    }

    #[test]
    #[ignore = "requires fixed Atlas artifact and MEMORIES_DB_MIGRATE_E2E_BINARY release binary (and TEST_POSTGRES_URL for postgres)"]
    fn embedding_e2e_switch_resume_and_discard_without_backup() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            let e = EmbeddingE2e::new().await;
            e.add_thread("topic").await;
            let (code, line) = e.run(&["embedding", "inspect"]).await;
            assert_eq!(code, Some(0), "{line}");
            assert_eq!(EmbeddingE2e::field(&line, "state"), "incomplete");
            let space = EmbeddingE2e::field(&line, "space").to_string();

            let target = ["--model-id", "target-model", "--dimension", "8"];
            let (code, line) = e
                .run(&[&["embedding", "plan"][..], &target[..]].concat())
                .await;
            assert_eq!(code, Some(0), "{line}");
            assert_eq!(EmbeddingE2e::field(&line, "decision"), "reembed_required");
            assert_eq!(EmbeddingE2e::field(&line, "thread"), "1");

            // Neither or both backup options: nothing changes.
            let (code, line) = e
                .run(
                    &[
                        &[
                            "embedding",
                            "switch",
                            "--expected-space",
                            &space,
                            "--maintenance-window-ack",
                        ][..],
                        &target[..],
                    ]
                    .concat(),
                )
                .await;
            assert_eq!(code, Some(1));
            assert!(line.contains("error_code=backup_option_required"), "{line}");
            // A stale expectation of the current space.
            let (code, line) = e
                .run(
                    &[
                        &[
                            "embedding",
                            "switch",
                            "--expected-space",
                            "unknown",
                            "--maintenance-window-ack",
                            "--no-backup-unsafe",
                        ][..],
                        &target[..],
                    ]
                    .concat(),
                )
                .await;
            assert_eq!(code, Some(1));
            assert!(
                line.contains("error_code=space_changed resolution=plan_required"),
                "{line}"
            );

            let (code, line) = e
                .run(
                    &[
                        &[
                            "embedding",
                            "switch",
                            "--expected-space",
                            &space,
                            "--maintenance-window-ack",
                            "--no-backup-unsafe",
                        ][..],
                        &target[..],
                    ]
                    .concat(),
                )
                .await;
            assert_eq!(code, Some(0), "{line}");
            assert_eq!(EmbeddingE2e::field(&line, "outcome"), "switched");
            assert_eq!(EmbeddingE2e::field(&line, "backup"), "none");
            let attempt = EmbeddingE2e::field(&line, "attempt").to_string();

            let (_, line) = e.run(&["embedding", "inspect"]).await;
            assert_eq!(EmbeddingE2e::field(&line, "state"), "migrating");
            assert_eq!(
                EmbeddingE2e::field(&line, "attempt_stage"),
                "rebuild_pending"
            );
            assert_eq!(EmbeddingE2e::field(&line, "next_action"), "start_rebuild");

            let (code, line) = e.run(&["embedding", "switch", "--resume", &attempt]).await;
            assert_eq!(code, Some(0), "{line}");
            assert_eq!(EmbeddingE2e::field(&line, "outcome"), "already_switched");

            // A new switch while this one is unfinished is refused with
            // the stage-specific resolution and the attempt.
            let (code, line) = e
                .run(
                    &[
                        &[
                            "embedding",
                            "switch",
                            "--expected-space",
                            &space,
                            "--maintenance-window-ack",
                            "--no-backup-unsafe",
                        ][..],
                        &target[..],
                    ]
                    .concat(),
                )
                .await;
            assert_eq!(code, Some(1));
            assert!(
                line.contains(
                    "error_code=embedding_switch_in_progress resolution=continue_or_abandon"
                ),
                "{line}"
            );
            assert_eq!(EmbeddingE2e::field(&line, "attempt"), attempt);

            let (code, line) = e
                .run(&["embedding", "abandon", "--attempt", &attempt])
                .await;
            assert_eq!(code, Some(0), "{line}");
            assert_eq!(EmbeddingE2e::field(&line, "outcome"), "discarded");
            assert_eq!(EmbeddingE2e::field(&line, "next_action"), "reconcile");
            let (_, line) = e
                .run(&["embedding", "abandon", "--attempt", &attempt])
                .await;
            assert_eq!(EmbeddingE2e::field(&line, "outcome"), "already_abandoned");
            let (_, line) = e.run(&["embedding", "inspect"]).await;
            assert_eq!(EmbeddingE2e::field(&line, "state"), "incomplete", "{line}");
            assert_eq!(EmbeddingE2e::field(&line, "space"), "none");

            let (code, line) = e.run(&["embedding", "switch", "--resume", &attempt]).await;
            assert_eq!(code, Some(1));
            assert!(
                line.contains("error_code=attempt_finished resolution=plan_required"),
                "{line}"
            );
        });
    }

    /// Regression: a configured store that holds rows but no storage
    /// identifier (for example another environment's store picked up from
    /// configuration) is refused by changing commands and left untouched.
    #[test]
    #[ignore = "requires fixed Atlas artifact and MEMORIES_DB_MIGRATE_E2E_BINARY release binary (and TEST_POSTGRES_URL for postgres)"]
    fn embedding_e2e_changes_never_touch_an_unidentified_store() {
        use infra::infra::memory_vector::config::{
            DistanceType, FtsConfig, VectorDBConfig, VectorIndexConfig,
        };
        use infra::infra::memory_vector::record::MemoryVectorRecord;
        use infra::infra::memory_vector::repository::MemoryVectorRepositoryImpl;
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            let e = EmbeddingE2e::new().await;
            e.add_thread("topic").await;
            let foreign = e.root().join("foreign.lancedb");
            let repo = MemoryVectorRepositoryImpl::new(VectorDBConfig {
                uri: foreign.to_string_lossy().into_owned(),
                table_name: "memories".into(),
                vector_size: 4,
                distance_type: DistanceType::Cosine,
                fts: FtsConfig::default(),
                vector_index: VectorIndexConfig::default(),
            })
            .await
            .unwrap();
            let data = protobuf::llm_memory::data::MemoryData {
                content: "x".into(),
                ..Default::default()
            };
            let row = MemoryVectorRecord::from_chunk_with_content(
                1,
                &data,
                &[0.5; 4],
                Some("m"),
                "text",
                0,
                0,
                1,
                "x".into(),
            );
            repo.replace_kinds_upsert(1, &["text"], vec![row])
                .await
                .unwrap();
            let version = repo.table_handle().version().await.unwrap();

            let output = e
                .command()
                .args([
                    "embedding",
                    "switch",
                    "--expected-space",
                    "unknown",
                    "--maintenance-window-ack",
                    "--no-backup-unsafe",
                    "--model-id",
                    "target-model",
                    "--dimension",
                    "8",
                ])
                .env("MEMORY_VECTOR_ENABLED", "true")
                .env("MEMORY_LANCEDB_URI", &foreign)
                .env("MEMORY_VECTOR_SIZE", "4")
                .env("MEMORY_WORKERS_YAML", &e.workers_yaml)
                .env("MEMORY_EMBEDDING_STATE_DIR", &e.state_dir)
                .output()
                .await
                .unwrap();
            let line = String::from_utf8_lossy(&output.stdout)
                .lines()
                .last()
                .unwrap_or_default()
                .to_string();
            assert_eq!(output.status.code(), Some(1), "{line}");
            assert!(
                line.contains("error_code=storage_mismatch resolution=check_environment"),
                "{line}"
            );
            let latest = repo.table_handle();
            latest.checkout_latest().await.unwrap();
            assert_eq!(
                latest.version().await.unwrap(),
                version,
                "the store is untouched"
            );
            assert!(!e.state_dir.join("attempt.json").exists());
        });
    }

    /// An attempt interrupted right after its record was written (stage
    /// `backup`, no marker yet) is resumed to completion, and one stopped
    /// there can instead be abandoned without changing anything.
    #[test]
    #[ignore = "requires fixed Atlas artifact and MEMORIES_DB_MIGRATE_E2E_BINARY release binary (and TEST_POSTGRES_URL for postgres)"]
    fn embedding_e2e_interrupted_switch_resumes_or_abandons() {
        use grpc_admin::db_migrate::embedding::attempt::{
            self, AttemptRecord, FORMAT_VERSION, Method, Stage, Status,
        };
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            let e = EmbeddingE2e::new().await;
            e.add_thread("topic").await;
            let target = infra::infra::embedding_space::SpaceComponents {
                model_id: "target-model".into(),
                tokenizer_model_id: String::new(),
                revision: "unversioned".into(),
                dimension: 8,
                distance: "cosine".into(),
            };
            let interrupted = |id: &str| AttemptRecord {
                format_version: FORMAT_VERSION,
                attempt_id: id.into(),
                method: Method::NoBackup,
                source_space_id: None,
                target_space_id: target.space_id().to_string(),
                target_space: target.clone(),
                backup_path: None,
                backup_complete: false,
                stage: Stage::Backup,
                status: Status::Running,
                cancel_operation: None,
                discarded: false,
                accepted_failed: None,
                backup_keep: None,
                started_at: 0,
            };
            attempt::save(&e.state_dir, &interrupted("a1")).unwrap();
            let (_, line) = e.run(&["embedding", "inspect"]).await;
            assert_eq!(EmbeddingE2e::field(&line, "attempt_stage"), "backup");
            assert_eq!(
                EmbeddingE2e::field(&line, "next_action"),
                "resume_or_abandon"
            );
            let (code, line) = e.run(&["embedding", "abandon", "--attempt", "a1"]).await;
            assert_eq!(code, Some(0), "{line}");
            assert_eq!(EmbeddingE2e::field(&line, "outcome"), "abandoned");
            assert_eq!(EmbeddingE2e::field(&line, "next_action"), "none");

            attempt::save(&e.state_dir, &interrupted("a2")).unwrap();
            let (code, line) = e.run(&["embedding", "switch", "--resume", "a2"]).await;
            assert_eq!(code, Some(0), "{line}");
            assert_eq!(EmbeddingE2e::field(&line, "outcome"), "switched");
            let (_, line) = e.run(&["embedding", "inspect"]).await;
            assert_eq!(
                EmbeddingE2e::field(&line, "attempt_stage"),
                "rebuild_pending"
            );
        });
    }

    #[test]
    #[ignore = "requires fixed Atlas artifact and MEMORIES_DB_MIGRATE_E2E_BINARY release binary (and TEST_POSTGRES_URL for postgres)"]
    fn embedding_e2e_switch_with_backup_and_abandon_before_changes() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            let e = EmbeddingE2e::new().await;
            e.add_thread("topic").await;
            let backups = e.backups.to_string_lossy().to_string();
            let (_, line) = e.run(&["embedding", "inspect"]).await;
            let space = EmbeddingE2e::field(&line, "space").to_string();
            let target = ["--model-id", "target-model", "--dimension", "8"];
            let (code, line) = e
                .run(
                    &[
                        &[
                            "embedding",
                            "switch",
                            "--expected-space",
                            &space,
                            "--maintenance-window-ack",
                            "--backup-dir",
                            &backups,
                        ][..],
                        &target[..],
                    ]
                    .concat(),
                )
                .await;
            assert_eq!(code, Some(0), "{line}");
            let attempt = EmbeddingE2e::field(&line, "attempt").to_string();
            let backup = EmbeddingE2e::field(&line, "backup").to_string();
            assert_ne!(backup, "none");
            assert!(
                e.backups
                    .join(format!("embedding-{attempt}/manifest.json"))
                    .is_file()
            );

            // A backup attempt that replaced the tables cannot be abandoned.
            let (code, line) = e
                .run(&["embedding", "abandon", "--attempt", &attempt])
                .await;
            assert_eq!(code, Some(1));
            assert!(
                line.contains("error_code=abandon_not_allowed resolution=continue_or_restore"),
                "{line}"
            );
            assert!(line.contains("backup="), "{line}");
            let (code, line) = e.run(&["embedding", "abandon", "--attempt", "other"]).await;
            assert_eq!(code, Some(1));
            assert!(line.contains("error_code=attempt_not_found"), "{line}");
        });
    }

    /// A database of the release before embedding storage identifiers
    /// gains its RDB identifier through the ordinary `local apply`.
    #[cfg(not(feature = "postgres"))]
    #[test]
    #[ignore = "requires fixed Atlas artifact and MEMORIES_DB_MIGRATE_E2E_BINARY release binary"]
    fn local_apply_e2e_adds_the_storage_identifier_to_a_previous_release_database() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            let local = LocalE2e::new();
            let previous = atlas_migration_versions().len() - 1;
            let applied = std::process::Command::new(
                std::path::Path::new(&local.artifact_root).join("bin/atlas"),
            )
            .current_dir(&local.artifact_root)
            .args(["migrate", "apply", &previous.to_string()])
            .args(["--config", "file://migrate.hcl", "--env", "sqlite"])
            .env(
                "MEMORIES_ATLAS_DATABASE_URL",
                atlas_database_url(&local.database_url).unwrap(),
            )
            .output()
            .unwrap();
            assert!(
                applied.status.success(),
                "{}",
                String::from_utf8_lossy(&applied.stderr)
            );
            let pool = local.pool().await;
            assert!(
                !super::table_exists(&pool, "memories_storage_identity")
                    .await
                    .unwrap()
            );
            pool.close().await;

            let (code, line) = local.apply().await;
            assert_eq!(code, Some(0), "{line}");
            assert!(
                line.starts_with("local_apply status=completed outcome=migrated"),
                "{line}"
            );
            let pool = local.pool().await;
            let id: String = sqlx::query_scalar(
                "SELECT storage_id FROM memories_storage_identity WHERE identity_key = 'rdb'",
            )
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(id.len(), 32);
            pool.close().await;
            assert_eq!(
                local.apply().await.1,
                "local_apply status=completed outcome=no_op"
            );
        });
    }

    #[cfg(not(feature = "postgres"))]
    #[test]
    #[ignore = "requires fixed Atlas artifact and MEMORIES_DB_MIGRATE_E2E_BINARY release binary"]
    fn local_apply_e2e_requires_an_explicit_backup_choice_and_no_other_connection() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            let local = LocalE2e::new();
            assert_eq!(
                local.local(&["local", "apply", "--maintenance-window-ack"]).await,
                (
                    Some(1),
                    "local_apply status=failed stage=preflight error_code=backup_option_required resolution=tool_update_required"
                        .to_string()
                )
            );
            assert_eq!(
                local
                    .local(&["local", "apply", "--maintenance-window-ack", "--no-backup-unsafe"])
                    .await
                    .1,
                "local_apply status=completed outcome=migrated"
            );

            // Memories switches its database to WAL on startup; an idle server
            // connection then stays visible through the WAL-index lock.
            let pool = local.pool().await;
            for statement in ["PRAGMA journal_mode = WAL", "SELECT COUNT(*) FROM thread"] {
                sqlx::query(statement).execute(&pool).await.unwrap();
            }
            assert_eq!(
                local.apply().await,
                (
                    Some(1),
                    "local_apply status=failed stage=writer_check error_code=writer_active resolution=retry"
                        .to_string()
                )
            );
            pool.close().await;
            assert_eq!(local.apply().await.0, Some(0));
        });
    }

    #[cfg(not(feature = "postgres"))]
    #[test]
    #[ignore = "requires fixed Atlas artifact and MEMORIES_DB_MIGRATE_E2E_BINARY release binary"]
    fn local_apply_e2e_refuses_a_database_of_a_newer_release_without_recording_a_restore() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            const NEWER: &str = "20991231000000";
            let latest = atlas_migration_versions().last().unwrap().clone();
            let local = LocalE2e::new();
            assert_eq!(local.apply().await.0, Some(0));
            let record_before = std::fs::read(local.attempt_record()).ok();
            let pool = local.pool().await;
            {
                // A newer release records its revision like any applied one;
                // the temporary table lives on this one connection.
                let mut connection = pool.acquire().await.unwrap();
                for (statement, value) in [
                    (
                        "CREATE TEMP TABLE newer AS SELECT * FROM atlas_schema_revisions WHERE version = ?",
                        latest.as_str(),
                    ),
                    ("UPDATE newer SET version = ?", NEWER),
                    ("UPDATE memories_schema_contract SET version = ?", NEWER),
                ] {
                    sqlx::query(statement)
                        .bind(value)
                        .execute(&mut *connection)
                        .await
                        .unwrap();
                }
                sqlx::query("INSERT INTO atlas_schema_revisions SELECT * FROM newer")
                    .execute(&mut *connection)
                    .await
                    .unwrap();
            }
            pool.close().await;
            assert_eq!(
                local.apply().await,
                (
                    Some(1),
                    "local_apply status=failed stage=plan error_code=db_newer_than_tool resolution=tool_update_required"
                        .to_string()
                )
            );
            assert_eq!(std::fs::read(local.attempt_record()).ok(), record_before);

            // With a tool that knows the newer release, the database is usable.
            let pool = local.pool().await;
            sqlx::query("DELETE FROM atlas_schema_revisions WHERE version = ?")
                .bind(NEWER)
                .execute(&pool)
                .await
                .unwrap();
            sqlx::query("UPDATE memories_schema_contract SET version = ?")
                .bind(&latest)
                .execute(&pool)
                .await
                .unwrap();
            pool.close().await;
            assert_eq!(
                local.apply().await.1,
                "local_apply status=completed outcome=no_op"
            );
        });
    }

    #[cfg(not(feature = "postgres"))]
    #[test]
    #[ignore = "requires fixed Atlas artifact and MEMORIES_DB_MIGRATE_E2E_BINARY release binary"]
    fn local_apply_e2e_ignores_the_record_of_a_discarded_database() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            let local = LocalE2e::new();
            assert_eq!(local.apply().await.0, Some(0));
            local.record_restore_required(None);
            std::fs::remove_file(local.database()).unwrap();
            assert_eq!(
                local.apply().await,
                (
                    Some(0),
                    "local_apply status=completed outcome=migrated".to_string()
                )
            );
        });
    }

    #[cfg(not(feature = "postgres"))]
    #[test]
    #[ignore = "requires fixed Atlas artifact and MEMORIES_DB_MIGRATE_E2E_BINARY release binary"]
    fn local_apply_e2e_leaves_a_database_from_before_memory_kind_untouched() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            let legacy = LocalE2e::new();
            let pool = legacy.pool().await;
            sqlx::raw_sql(
                "CREATE TABLE memory (id BIGINT PRIMARY KEY, content TEXT); \
                 CREATE TABLE thread (id BIGINT PRIMARY KEY);",
            )
            .execute(&pool)
            .await
            .unwrap();
            pool.close().await;
            let before = std::fs::read(legacy.database()).unwrap();
            assert_eq!(
                legacy.apply().await,
                (
                    Some(1),
                    "local_apply status=failed stage=preflight error_code=legacy_schema_unsupported resolution=legacy_upgrade_required"
                        .to_string()
                )
            );
            assert_eq!(std::fs::read(legacy.database()).unwrap(), before);
            assert!(!legacy.backups.exists());
        });
    }

    #[cfg(not(feature = "postgres"))]
    #[test]
    #[ignore = "requires fixed Atlas artifact and MEMORIES_DB_MIGRATE_E2E_BINARY release binary"]
    fn local_restore_e2e_returns_to_the_backup_and_unblocks_a_required_restore() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            let local = LocalE2e::new();
            local.command().run(&["schema", "apply"]).await.unwrap();
            let (code, line) = local.apply().await;
            assert_eq!(code, Some(0), "{line}");
            let backup = LocalE2e::backup_from(&line);
            let backup_arg = backup.to_str().unwrap().to_string();

            // The previous attempt is recorded as needing a restore.
            local.record_restore_required(Some(backup.clone()));
            let record = local.attempt_record();
            let (code, refused) = local.apply().await;
            assert_eq!(code, Some(1));
            assert!(
                refused.starts_with(
                    "local_apply status=failed stage=preflight error_code=restore_required resolution=restore_required backup="
                ),
                "{refused}"
            );

            assert_eq!(
                local
                    .local(&[
                        "local",
                        "restore",
                        "--maintenance-window-ack",
                        "--backup",
                        &backup_arg
                    ])
                    .await,
                (
                    Some(0),
                    "local_restore status=completed next_action=apply".to_string()
                )
            );
            // The restored database is the pre-task state again.
            let pool = local.pool().await;
            let completed: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM memories_data_migration_task_state WHERE state = 'completed'",
            )
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(completed, 0);
            pool.close().await;
            assert!(!record.exists());

            let (code, line) = local.apply().await;
            assert_eq!(code, Some(0), "{line}");
            assert!(line.starts_with("local_apply status=completed outcome=migrated"));
        });
    }

    #[cfg(not(feature = "postgres"))]
    #[test]
    #[ignore = "requires fixed Atlas artifact and MEMORIES_DB_MIGRATE_E2E_BINARY release binary"]
    fn sqlite_migration_e2e_with_fixed_atlas_artifact() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            let artifact_root = fixed_e2e_atlas_artifact_root().unwrap();
            let binary = fixed_e2e_release_binary().unwrap();
            let temporary = tempfile::tempdir().unwrap();
            let (database_url, vector_uri) = sqlite_e2e_target_paths(temporary.path());
            let _environment =
                ScopedE2eEnvironment::configure(&artifact_root, &database_url, &vector_uri);
            let command = MigrationE2eCommand {
                binary: &binary,
                artifact_root: &artifact_root,
                database_url: &database_url,
                vector_uri: &vector_uri,
            };
            run_thread_message_times_migration_e2e(&command, &database_url, None)
                .await
                .unwrap();
        });
    }

    #[cfg(not(feature = "postgres"))]
    #[test]
    #[ignore = "requires fixed Atlas artifact and MEMORIES_DB_MIGRATE_E2E_BINARY release binary"]
    fn sqlite_adoption_baseline_e2e_with_fixed_atlas_artifact() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            let artifact_root = fixed_e2e_atlas_artifact_root().unwrap();
            let binary = fixed_e2e_release_binary().unwrap();
            for candidate_version in adoption_baseline_versions().iter().rev() {
                let temporary = tempfile::tempdir().unwrap();
                let (database_url, vector_uri) = sqlite_e2e_target_paths(temporary.path());
                let _environment =
                    ScopedE2eEnvironment::configure(&artifact_root, &database_url, &vector_uri);
                let command = MigrationE2eCommand {
                    binary: &binary,
                    artifact_root: &artifact_root,
                    database_url: &database_url,
                    vector_uri: &vector_uri,
                };
                run_adoption_baseline_e2e(&command, &database_url, candidate_version)
                    .await
                    .unwrap();
            }
        });
    }

    #[cfg(not(feature = "postgres"))]
    #[test]
    fn sqlite_sqlx_target_url_with_percent_encoded_absolute_path_opens_the_database() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            let temporary = tempfile::tempdir().unwrap();
            let (database_url, _) = sqlite_e2e_target_paths(temporary.path());
            let pool = sqlx::Pool::<Rdb>::connect(&database_url).await.unwrap();

            sqlx::query("CREATE TABLE url_contract_test (id INTEGER PRIMARY KEY)")
                .execute(&pool)
                .await
                .unwrap();
        });
    }

    #[cfg(not(feature = "postgres"))]
    #[test]
    #[ignore = "requires fixed Atlas artifact"]
    fn sqlite_adoption_baseline_in_process_with_fixed_atlas_artifact() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            let artifact_root = fixed_e2e_atlas_artifact_root().unwrap();
            for candidate_version in adoption_baseline_versions().iter().rev() {
                let temporary = tempfile::tempdir().unwrap();
                let (database_url, vector_uri) = sqlite_e2e_target_paths(temporary.path());
                run_adoption_baseline_in_process_e2e(
                    &artifact_root,
                    &database_url,
                    &vector_uri,
                    candidate_version,
                )
                .await
                .unwrap();
            }
        });
    }

    #[cfg(feature = "postgres")]
    #[test]
    #[ignore = "requires TEST_POSTGRES_URL, fixed Atlas artifact, and MEMORIES_DB_MIGRATE_E2E_BINARY release binary"]
    fn postgres_dedicated_history_schema_e2e_with_fixed_atlas_artifact() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            use sqlx::postgres::PgPoolOptions;

            let artifact_root = fixed_e2e_atlas_artifact_root().unwrap();
            let binary = fixed_e2e_release_binary().unwrap();
            let service_url = std::env::var("TEST_POSTGRES_URL")
                .expect("TEST_POSTGRES_URL must be set for the PostgreSQL E2E test");
            let database_name = format!(
                "memories_db_migrate_history_{}_{}",
                std::process::id(),
                command_utils::util::datetime::now_millis()
            );
            let mut url = url::Url::parse(&service_url).unwrap();
            assert!(
                !url.query_pairs()
                    .any(|(key, _)| key == "search_path" || key == "options[search_path]"),
                "dedicated-history E2E requires an unscoped PostgreSQL URL"
            );
            url.set_path(&format!("/{database_name}"));
            let database_url = url.to_string();
            let admin_pool = PgPoolOptions::new()
                .max_connections(1)
                .connect(&service_url)
                .await
                .unwrap();
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "CREATE DATABASE {}",
                quote_postgres_identifier(&database_name)
            )))
            .execute(&admin_pool)
            .await
            .unwrap();
            let temporary = tempfile::tempdir().unwrap();
            let vector_uri = temporary.path().join("threads.lancedb");
            let result: Result<()> = async {
                let _environment =
                    ScopedE2eEnvironment::configure(&artifact_root, &database_url, &vector_uri);
                let command = MigrationE2eCommand {
                    binary: &binary,
                    artifact_root: &artifact_root,
                    database_url: &database_url,
                    vector_uri: &vector_uri,
                };
                let status = command.run(&["schema", "status"]).await?;
                assert!(status.contains("schema_status status=uninitialized"));
                command.run(&["schema", "apply", "--dry-run"]).await?;
                let status = command.run(&["schema", "status"]).await?;
                assert!(status.contains("schema_status status=uninitialized"));
                let pool = PgPoolOptions::new().connect(&database_url).await?;
                let schema_left_by_dry_run: bool = sqlx::query_scalar(
                    "SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'atlas_schema_revisions')",
                )
                .fetch_one(&pool)
                .await?;
                assert!(!schema_left_by_dry_run);
                sqlx::query("CREATE SCHEMA atlas_schema_revisions")
                    .execute(&pool)
                    .await?;
                drop(pool);
                command.run(&["schema", "apply", "--dry-run"]).await?;
                let pool = PgPoolOptions::new().connect(&database_url).await?;
                let existing_schema_preserved: bool = sqlx::query_scalar(
                    "SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'atlas_schema_revisions')",
                )
                .fetch_one(&pool)
                .await?;
                assert!(existing_schema_preserved);
                drop(pool);
                command.run(&["schema", "apply"]).await?;
                let pool = PgPoolOptions::new().connect(&database_url).await?;
                let history_in_dedicated_schema: bool = sqlx::query_scalar(
                    "SELECT to_regclass('atlas_schema_revisions.atlas_schema_revisions') IS NOT NULL",
                )
                .fetch_one(&pool)
                .await?;
                assert!(history_in_dedicated_schema);
                let history_in_public: bool = sqlx::query_scalar(
                    "SELECT to_regclass('public.atlas_schema_revisions') IS NOT NULL",
                )
                .fetch_one(&pool)
                .await?;
                assert!(!history_in_public);
                assert!(super::table_exists(&pool, "memories_schema_contract").await?);
                assert!(super::table_exists(&pool, "memories_data_migration_task_state").await?);
                drop(pool);
                let status = command.run(&["schema", "status"]).await?;
                assert!(status.contains("schema_status status=managed pending_count=0"));
                command.run(&["schema", "apply"]).await?;
                let status = command.run(&["schema", "status"]).await?;
                assert!(status.contains("schema_status status=managed pending_count=0"));
                Ok(())
            }
            .await;
            let cleanup = sqlx::query(sqlx::AssertSqlSafe(format!(
                "DROP DATABASE {} WITH (FORCE)",
                quote_postgres_identifier(&database_name)
            )))
            .execute(&admin_pool)
            .await;
            match (result, cleanup) {
                (Ok(()), Ok(_)) => {}
                (Err(error), Ok(_)) => panic!("dedicated-history E2E failed: {error:#}"),
                (Ok(()), Err(error)) => panic!("dedicated-history E2E cleanup failed: {error:#}"),
                (Err(error), Err(cleanup_error)) => panic!(
                    "dedicated-history E2E failed: {error:#}; cleanup also failed: {cleanup_error:#}"
                ),
            }
        });
    }

    #[cfg(feature = "postgres")]
    #[test]
    #[ignore = "requires TEST_POSTGRES_URL, fixed Atlas artifact, and MEMORIES_DB_MIGRATE_E2E_BINARY release binary"]
    fn postgres_migration_e2e_with_fixed_atlas_artifact() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            use sqlx::postgres::PgPoolOptions;

            let artifact_root = fixed_e2e_atlas_artifact_root().unwrap();
            let binary = fixed_e2e_release_binary().unwrap();
            let service_url = std::env::var("TEST_POSTGRES_URL")
                .expect("TEST_POSTGRES_URL must be set for the PostgreSQL E2E test");
            let admin_pool = PgPoolOptions::new()
                .max_connections(1)
                .connect(&service_url)
                .await
                .unwrap();
            for (index, excluded_option) in [None, Some("search_path"), Some("options[search_path]")]
                .into_iter()
                .enumerate()
            {
                let temporary = tempfile::tempdir().unwrap();
                let schema = format!(
                    "memories_db_migrate_e2e_{}_{}_{}",
                    std::process::id(),
                    command_utils::util::datetime::now_millis(),
                    index,
                );
                let mut url = url::Url::parse(&postgres_schema_url(&service_url, &schema).unwrap()).unwrap();
                if let Some(excluded_option) = excluded_option {
                    let pairs = url.query_pairs()
                        .filter(|(key, _)| key != excluded_option)
                        .map(|(key, value)| (key.into_owned(), value.into_owned()))
                        .collect::<Vec<_>>();
                    url.query_pairs_mut().clear().extend_pairs(pairs);
                }
                let database_url = url.to_string();
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "CREATE SCHEMA {}",
                    quote_postgres_identifier(&schema)
                )))
                .execute(&admin_pool)
                .await
                .unwrap();
                let vector_uri = temporary.path().join("threads.lancedb");
                let result = {
                    let _environment =
                        ScopedE2eEnvironment::configure(&artifact_root, &database_url, &vector_uri);
                    let command = MigrationE2eCommand {
                        binary: &binary,
                        artifact_root: &artifact_root,
                        database_url: &database_url,
                        vector_uri: &vector_uri,
                    };
                    run_thread_message_times_migration_e2e(&command, &database_url, Some(&schema)).await
                };
                let cleanup = sqlx::query(sqlx::AssertSqlSafe(format!(
                    "DROP SCHEMA {} CASCADE",
                    quote_postgres_identifier(&schema)
                )))
                .execute(&admin_pool)
                .await;
                match (result, cleanup) {
                    (Ok(()), Ok(_)) => {}
                    (Err(error), Ok(_)) => panic!("PostgreSQL migration E2E failed: {error:#}"),
                    (Ok(()), Err(error)) => panic!("PostgreSQL E2E schema cleanup failed: {error:#}"),
                    (Err(run_error), Err(cleanup_error)) => panic!(
                        "PostgreSQL migration E2E failed: {run_error:#}; schema cleanup also failed: {cleanup_error:#}"
                    ),
                }
            }
        });
    }

    #[cfg(feature = "postgres")]
    #[test]
    #[ignore = "requires TEST_POSTGRES_URL, fixed Atlas artifact, and MEMORIES_DB_MIGRATE_E2E_BINARY release binary"]
    fn postgres_adoption_baseline_e2e_with_fixed_atlas_artifact() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            use sqlx::postgres::PgPoolOptions;

            let artifact_root = fixed_e2e_atlas_artifact_root().unwrap();
            let binary = fixed_e2e_release_binary().unwrap();
            let service_url = std::env::var("TEST_POSTGRES_URL")
                .expect("TEST_POSTGRES_URL must be set for the PostgreSQL E2E test");
            let admin_pool = PgPoolOptions::new()
                .max_connections(1)
                .connect(&service_url)
                .await
                .unwrap();
            for (candidate_index, candidate_version) in adoption_baseline_versions().iter().rev().enumerate() {
                let temporary = tempfile::tempdir().unwrap();
                let schema = format!(
                    "memories_db_migrate_adoption_{}_{}_{}",
                    std::process::id(),
                    command_utils::util::datetime::now_millis(),
                    candidate_index,
                );
                let database_url = postgres_schema_url(&service_url, &schema).unwrap();
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "CREATE SCHEMA {}",
                    quote_postgres_identifier(&schema)
                )))
                .execute(&admin_pool)
                .await
                .unwrap();
                let vector_uri = temporary.path().join("threads.lancedb");
                let result = {
                    let _environment =
                        ScopedE2eEnvironment::configure(&artifact_root, &database_url, &vector_uri);
                    let command = MigrationE2eCommand {
                        binary: &binary,
                        artifact_root: &artifact_root,
                        database_url: &database_url,
                        vector_uri: &vector_uri,
                    };
                    run_adoption_baseline_e2e(&command, &database_url, candidate_version).await
                };
                let cleanup = sqlx::query(sqlx::AssertSqlSafe(format!(
                    "DROP SCHEMA {} CASCADE",
                    quote_postgres_identifier(&schema)
                )))
                .execute(&admin_pool)
                .await;
                match (result, cleanup) {
                    (Ok(()), Ok(_)) => {}
                    (Err(error), Ok(_)) => panic!("PostgreSQL adoption baseline E2E failed: {error:#}"),
                    (Ok(()), Err(error)) => panic!("PostgreSQL adoption E2E schema cleanup failed: {error:#}"),
                    (Err(run_error), Err(cleanup_error)) => panic!(
                        "PostgreSQL adoption baseline E2E failed: {run_error:#}; schema cleanup also failed: {cleanup_error:#}"
                    ),
                }
            }
        });
    }

    #[test]
    fn schema_diff_uses_config_environment_references_not_database_urls() {
        let args = schema_diff_args("postgres", "20260803000003").unwrap();
        assert!(args.windows(2).any(|pair| pair == ["--from", "env://url"]));
        assert!(
            args.windows(2)
                .any(|pair| pair == ["--exclude", "atlas_schema_revisions"])
        );
        assert!(args.iter().any(|arg| arg == "--to"));
        assert!(args.iter().all(|arg| !arg.starts_with("postgres:")));
        assert!(args.iter().all(|arg| !arg.starts_with("postgresql:")));
    }

    #[test]
    fn verify_configuration_accepts_only_the_adapter_internal_dev_url() {
        assert!(verify_config_uses_internal_dev_url(&atlas_artifact_root().unwrap()).unwrap());
    }

    #[cfg(not(feature = "postgres"))]
    #[test]
    fn post_migrate_status_reports_unavailable_only_when_both_control_tables_are_absent() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            use sqlx::sqlite::SqlitePoolOptions;

            let pool = SqlitePoolOptions::new()
                .max_connections(1)
                .connect("sqlite::memory:")
                .await
                .unwrap();
            assert!(post_migration_state_unavailable(&pool).await.unwrap());
            sqlx::raw_sql(
                "CREATE TABLE memories_schema_contract (contract_key TEXT PRIMARY KEY, version TEXT NOT NULL);",
            )
            .execute(&pool)
            .await
            .unwrap();
            assert!(post_migration_state_unavailable(&pool).await.is_err());
        });
    }

    #[cfg(not(feature = "postgres"))]
    #[test]
    fn schema_state_distinguishes_a_database_migrated_by_a_newer_release() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            use sqlx::sqlite::SqlitePoolOptions;

            const NEWER: &str = "20991231000000";
            async fn database(extra: &[&str], contract: &str) -> RdbPool {
                let pool = SqlitePoolOptions::new()
                    .max_connections(1)
                    .connect("sqlite::memory:")
                    .await
                    .unwrap();
                sqlx::raw_sql(
                    "CREATE TABLE thread (id BIGINT PRIMARY KEY); \
                     CREATE TABLE atlas_schema_revisions (version TEXT PRIMARY KEY, type BIGINT NOT NULL); \
                     CREATE TABLE memories_schema_contract (contract_key TEXT PRIMARY KEY, version TEXT NOT NULL); \
                     CREATE TABLE memories_data_migration_task_state (task_identity TEXT PRIMARY KEY);",
                )
                .execute(&pool)
                .await
                .unwrap();
                let known = atlas_migration_versions();
                let versions = known.iter().map(String::as_str).chain(extra.iter().copied());
                for version in versions {
                    sqlx::query("INSERT INTO atlas_schema_revisions (version, type) VALUES (?, ?)")
                        .bind(version)
                        .bind(ATLAS_APPLIED_REVISION_TYPE)
                        .execute(&pool)
                        .await
                        .unwrap();
                }
                sqlx::query(
                    "INSERT INTO memories_schema_contract (contract_key, version) VALUES ('rdb_schema', ?)",
                )
                .bind(contract)
                .execute(&pool)
                .await
                .unwrap();
                pool
            }

            let newer = database(&[NEWER], NEWER).await;
            assert_eq!(
                schema_state(&newer).await.unwrap(),
                SchemaState::NewerThanTool
            );
            assert_eq!(
                pending_count_for_schema_state(SchemaState::NewerThanTool, 6),
                None
            );
            // An unknown version below the tool's latest is not a newer release.
            let gap = database(&["20260801000000"], NEWER).await;
            assert_eq!(schema_state(&gap).await.unwrap(), SchemaState::SchemaCorrupt);
            // The contract must follow the newer history it claims.
            let stale_contract = database(&[NEWER], "20260930000001").await;
            assert_eq!(
                schema_state(&stale_contract).await.unwrap(),
                SchemaState::SchemaCorrupt
            );
        });
    }

    #[cfg(not(feature = "postgres"))]
    #[test]
    fn schema_state_accepts_a_valid_history_prefix_as_pending() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            use sqlx::sqlite::SqlitePoolOptions;

            let pool = SqlitePoolOptions::new()
                .max_connections(1)
                .connect("sqlite::memory:")
                .await
                .unwrap();
            sqlx::raw_sql(
                "CREATE TABLE thread (id BIGINT PRIMARY KEY); \
                 CREATE TABLE atlas_schema_revisions (version TEXT PRIMARY KEY, type BIGINT NOT NULL);",
            )
            .execute(&pool)
            .await
            .unwrap();
            sqlx::query("INSERT INTO atlas_schema_revisions (version, type) VALUES (?, ?)")
                .bind("20260803000001")
                .bind(ATLAS_BASELINE_REVISION_TYPE)
                .execute(&pool)
                .await
                .unwrap();

            assert_eq!(
                schema_state(&pool).await.unwrap(),
                SchemaState::Pending { applied_count: 1 }
            );
            assert_eq!(
                pending_count_for_schema_state(
                    SchemaState::Pending { applied_count: 1 },
                    atlas_migration_versions().len()
                ),
                Some(6)
            );

            sqlx::query("INSERT INTO atlas_schema_revisions (version, type) VALUES (?, ?)")
                .bind("20260803000002")
                .bind(ATLAS_APPLIED_REVISION_TYPE)
                .execute(&pool)
                .await
                .unwrap();
            assert_eq!(
                schema_state(&pool).await.unwrap(),
                SchemaState::Pending { applied_count: 2 }
            );

            sqlx::raw_sql(
                "CREATE TABLE memories_schema_contract (contract_key TEXT PRIMARY KEY, version TEXT NOT NULL); \
                 CREATE TABLE memories_data_migration_task_state (task_identity TEXT PRIMARY KEY);",
            )
            .execute(&pool)
            .await
            .unwrap();
            sqlx::query(
                "INSERT INTO memories_schema_contract (contract_key, version) VALUES ('rdb_schema', ?)",
            )
            .bind("20260803000003")
            .execute(&pool)
            .await
            .unwrap();
            sqlx::query("INSERT INTO atlas_schema_revisions (version, type) VALUES (?, ?)")
                .bind("20260803000003")
                .bind(ATLAS_APPLIED_REVISION_TYPE)
                .execute(&pool)
                .await
                .unwrap();
            assert_eq!(
                schema_state(&pool).await.unwrap(),
                SchemaState::Pending { applied_count: 3 }
            );
            sqlx::query("UPDATE memories_schema_contract SET version = ? WHERE contract_key = 'rdb_schema'")
                .bind("20260920000001")
                .execute(&pool)
                .await
                .unwrap();
            sqlx::query("INSERT INTO atlas_schema_revisions (version, type) VALUES (?, ?)")
                .bind("20260920000001")
                .bind(ATLAS_APPLIED_REVISION_TYPE)
                .execute(&pool)
                .await
                .unwrap();
            assert_eq!(
                schema_state(&pool).await.unwrap(),
                SchemaState::Pending { applied_count: 4 }
            );
            sqlx::query("UPDATE memories_schema_contract SET version = ? WHERE contract_key = 'rdb_schema'")
                .bind("20260926000001")
                .execute(&pool)
                .await
                .unwrap();
            sqlx::query("INSERT INTO atlas_schema_revisions (version, type) VALUES (?, ?)")
                .bind("20260926000001")
                .bind(ATLAS_APPLIED_REVISION_TYPE)
                .execute(&pool)
                .await
                .unwrap();
            assert_eq!(
                schema_state(&pool).await.unwrap(),
                SchemaState::Pending { applied_count: 5 }
            );
            sqlx::query("UPDATE memories_schema_contract SET version = ? WHERE contract_key = 'rdb_schema'")
                .bind("20260930000001")
                .execute(&pool)
                .await
                .unwrap();
            sqlx::query("INSERT INTO atlas_schema_revisions (version, type) VALUES (?, ?)")
                .bind("20260930000001")
                .bind(ATLAS_APPLIED_REVISION_TYPE)
                .execute(&pool)
                .await
                .unwrap();
            assert_eq!(
                schema_state(&pool).await.unwrap(),
                SchemaState::Pending { applied_count: 6 }
            );
            sqlx::query("UPDATE memories_schema_contract SET version = ? WHERE contract_key = 'rdb_schema'")
                .bind("20261009000001")
                .execute(&pool)
                .await
                .unwrap();
            sqlx::query("INSERT INTO atlas_schema_revisions (version, type) VALUES (?, ?)")
                .bind("20261009000001")
                .bind(ATLAS_APPLIED_REVISION_TYPE)
                .execute(&pool)
                .await
                .unwrap();
            assert_eq!(schema_state(&pool).await.unwrap(), SchemaState::Managed);
        });
    }

    #[cfg(not(feature = "postgres"))]
    #[test]
    fn schema_state_accepts_a_fresh_database_history_prefix() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            use sqlx::sqlite::SqlitePoolOptions;

            let pool = SqlitePoolOptions::new()
                .max_connections(1)
                .connect("sqlite::memory:")
                .await
                .unwrap();
            sqlx::raw_sql(
                "CREATE TABLE thread (id BIGINT PRIMARY KEY); \
                 CREATE TABLE atlas_schema_revisions (version TEXT PRIMARY KEY, type BIGINT NOT NULL);",
            )
            .execute(&pool)
            .await
            .unwrap();
            sqlx::query("INSERT INTO atlas_schema_revisions (version, type) VALUES (?, ?)")
                .bind("20260803000001")
                .bind(ATLAS_APPLIED_REVISION_TYPE)
                .execute(&pool)
                .await
                .unwrap();

            assert_eq!(
                schema_state(&pool).await.unwrap(),
                SchemaState::Pending { applied_count: 1 }
            );
        });
    }

    #[cfg(not(feature = "postgres"))]
    #[test]
    fn schema_state_accepts_a_second_adoption_candidate_baseline_history() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            use sqlx::sqlite::SqlitePoolOptions;

            let pool = SqlitePoolOptions::new()
                .max_connections(1)
                .connect("sqlite::memory:")
                .await
                .unwrap();
            sqlx::raw_sql(
                "CREATE TABLE thread (id BIGINT PRIMARY KEY); \
                 CREATE TABLE atlas_schema_revisions (version TEXT PRIMARY KEY, type BIGINT NOT NULL); \
                 CREATE TABLE memories_schema_contract (contract_key TEXT PRIMARY KEY, version TEXT NOT NULL); \
                 CREATE TABLE memories_data_migration_task_state (task_identity TEXT PRIMARY KEY);",
            )
            .execute(&pool)
            .await
            .unwrap();
            sqlx::query("INSERT INTO atlas_schema_revisions (version, type) VALUES (?, ?)")
                .bind("20260803000002")
                .bind(ATLAS_BASELINE_REVISION_TYPE)
                .execute(&pool)
                .await
                .unwrap();
            sqlx::query("INSERT INTO atlas_schema_revisions (version, type) VALUES (?, ?)")
                .bind("20260803000003")
                .bind(ATLAS_APPLIED_REVISION_TYPE)
                .execute(&pool)
                .await
                .unwrap();
            sqlx::query(
                "INSERT INTO memories_schema_contract (contract_key, version) VALUES ('rdb_schema', ?)",
            )
            .bind("20260803000003")
            .execute(&pool)
            .await
            .unwrap();
            sqlx::query("INSERT INTO atlas_schema_revisions (version, type) VALUES (?, ?)")
                .bind("20260920000001")
                .bind(ATLAS_APPLIED_REVISION_TYPE)
                .execute(&pool)
                .await
                .unwrap();
            sqlx::query("UPDATE memories_schema_contract SET version = ?")
                .bind("20260920000001")
                .execute(&pool)
                .await
                .unwrap();

            assert_eq!(schema_state(&pool).await.unwrap(), SchemaState::Pending { applied_count: 4 });
            sqlx::query("INSERT INTO atlas_schema_revisions (version, type) VALUES (?, ?)")
                .bind("20260926000001")
                .bind(ATLAS_APPLIED_REVISION_TYPE)
                .execute(&pool)
                .await
                .unwrap();
            sqlx::query("UPDATE memories_schema_contract SET version = ?")
                .bind("20260926000001")
                .execute(&pool)
                .await
                .unwrap();
            assert_eq!(
                schema_state(&pool).await.unwrap(),
                SchemaState::Pending { applied_count: 5 }
            );
            sqlx::query("INSERT INTO atlas_schema_revisions (version, type) VALUES (?, ?)")
                .bind("20260930000001")
                .bind(ATLAS_APPLIED_REVISION_TYPE)
                .execute(&pool)
                .await
                .unwrap();
            sqlx::query("UPDATE memories_schema_contract SET version = ?")
                .bind("20260930000001")
                .execute(&pool)
                .await
                .unwrap();
            assert_eq!(
                schema_state(&pool).await.unwrap(),
                SchemaState::Pending { applied_count: 6 }
            );
            sqlx::query("INSERT INTO atlas_schema_revisions (version, type) VALUES (?, ?)")
                .bind("20261009000001")
                .bind(ATLAS_APPLIED_REVISION_TYPE)
                .execute(&pool)
                .await
                .unwrap();
            sqlx::query("UPDATE memories_schema_contract SET version = ?")
                .bind("20261009000001")
                .execute(&pool)
                .await
                .unwrap();
            assert_eq!(schema_state(&pool).await.unwrap(), SchemaState::Managed);
        });
    }

    #[cfg(not(feature = "postgres"))]
    #[test]
    fn schema_state_rejects_empty_gapped_unknown_and_contract_mismatched_atlas_history() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            use sqlx::sqlite::SqlitePoolOptions;

            let pool = SqlitePoolOptions::new()
                .max_connections(1)
                .connect("sqlite::memory:")
                .await
                .unwrap();
            assert_eq!(schema_state(&pool).await.unwrap(), SchemaState::Uninitialized);

            sqlx::raw_sql("CREATE TABLE thread (id BIGINT PRIMARY KEY);")
                .execute(&pool)
                .await
                .unwrap();
            assert_eq!(
                schema_state(&pool).await.unwrap(),
                SchemaState::BaselineRequired
            );

            sqlx::raw_sql("CREATE TABLE atlas_schema_revisions (version TEXT PRIMARY KEY, type BIGINT NOT NULL); \
                CREATE TABLE memories_schema_contract (contract_key TEXT PRIMARY KEY, version TEXT NOT NULL);")
            .execute(&pool)
            .await
            .unwrap();
            assert_eq!(schema_state(&pool).await.unwrap(), SchemaState::SchemaCorrupt);

            sqlx::raw_sql(
                "CREATE TABLE memories_data_migration_task_state (task_identity TEXT PRIMARY KEY);",
            )
            .execute(&pool)
            .await
            .unwrap();
            assert_eq!(schema_state(&pool).await.unwrap(), SchemaState::SchemaCorrupt);

            sqlx::query("INSERT INTO atlas_schema_revisions (version, type) VALUES (?, ?)")
                .bind("20260803000001")
                .bind(ATLAS_BASELINE_REVISION_TYPE)
                .execute(&pool)
                .await
                .unwrap();
            sqlx::query(
                "INSERT INTO memories_schema_contract (contract_key, version) VALUES ('rdb_schema', ?)",
            )
            .bind("20260803000002")
            .execute(&pool)
            .await
            .unwrap();
            assert_eq!(schema_state(&pool).await.unwrap(), SchemaState::SchemaCorrupt);

            sqlx::query("INSERT INTO atlas_schema_revisions (version, type) VALUES (?, ?)")
                .bind("20260803000002")
                .bind(ATLAS_APPLIED_REVISION_TYPE)
                .execute(&pool)
                .await
                .unwrap();
            sqlx::query("UPDATE memories_schema_contract SET version = ?")
                .bind("20260803000002")
                .execute(&pool)
                .await
                .unwrap();
            assert_eq!(schema_state(&pool).await.unwrap(), SchemaState::SchemaCorrupt);

            sqlx::query("INSERT INTO atlas_schema_revisions (version, type) VALUES (?, ?)")
                .bind("20260803000003")
                .bind(ATLAS_APPLIED_REVISION_TYPE)
                .execute(&pool)
                .await
                .unwrap();
            sqlx::query("UPDATE memories_schema_contract SET version = ?")
                .bind("20260920000001")
                .execute(&pool)
                .await
                .unwrap();
            sqlx::query("INSERT INTO atlas_schema_revisions (version, type) VALUES (?, ?)")
                .bind("20260920000001")
                .bind(ATLAS_APPLIED_REVISION_TYPE)
                .execute(&pool)
                .await
                .unwrap();
            assert_eq!(schema_state(&pool).await.unwrap(), SchemaState::Pending { applied_count: 4 });
            sqlx::query("INSERT INTO atlas_schema_revisions (version, type) VALUES (?, ?)")
                .bind("20260926000001")
                .bind(ATLAS_APPLIED_REVISION_TYPE)
                .execute(&pool)
                .await
                .unwrap();
            sqlx::query("UPDATE memories_schema_contract SET version = ?")
                .bind("20260926000001")
                .execute(&pool)
                .await
                .unwrap();
            assert_eq!(
                schema_state(&pool).await.unwrap(),
                SchemaState::Pending { applied_count: 5 }
            );
            sqlx::query("INSERT INTO atlas_schema_revisions (version, type) VALUES (?, ?)")
                .bind("20260930000001")
                .bind(ATLAS_APPLIED_REVISION_TYPE)
                .execute(&pool)
                .await
                .unwrap();
            sqlx::query("UPDATE memories_schema_contract SET version = ?")
                .bind("20260930000001")
                .execute(&pool)
                .await
                .unwrap();
            assert_eq!(
                schema_state(&pool).await.unwrap(),
                SchemaState::Pending { applied_count: 6 }
            );
            sqlx::query("INSERT INTO atlas_schema_revisions (version, type) VALUES (?, ?)")
                .bind("20261009000001")
                .bind(ATLAS_APPLIED_REVISION_TYPE)
                .execute(&pool)
                .await
                .unwrap();
            sqlx::query("UPDATE memories_schema_contract SET version = ?")
                .bind("20261009000001")
                .execute(&pool)
                .await
                .unwrap();
            assert_eq!(schema_state(&pool).await.unwrap(), SchemaState::Managed);

            let invalid_pool = SqlitePoolOptions::new()
                .max_connections(1)
                .connect("sqlite::memory:")
                .await
                .unwrap();
            sqlx::raw_sql(
                "CREATE TABLE thread (id BIGINT PRIMARY KEY); \
                 CREATE TABLE atlas_schema_revisions (version TEXT PRIMARY KEY, type BIGINT NOT NULL); \
                 CREATE TABLE memories_schema_contract (contract_key TEXT PRIMARY KEY, version TEXT NOT NULL); \
                 CREATE TABLE memories_data_migration_task_state (task_identity TEXT PRIMARY KEY);",
            )
            .execute(&invalid_pool)
            .await
            .unwrap();
            sqlx::query("INSERT INTO atlas_schema_revisions (version, type) VALUES (?, ?)")
                .bind("20260803000002")
                .bind(ATLAS_APPLIED_REVISION_TYPE)
                .execute(&invalid_pool)
                .await
                .unwrap();
            sqlx::query(
                "INSERT INTO memories_schema_contract (contract_key, version) VALUES ('rdb_schema', ?)",
            )
            .bind("20260803000002")
            .execute(&invalid_pool)
            .await
            .unwrap();
            assert_eq!(
                schema_state(&invalid_pool).await.unwrap(),
                SchemaState::SchemaCorrupt,
                "a history prefix cannot skip the adoption baseline"
            );

            sqlx::query("DELETE FROM atlas_schema_revisions")
                .execute(&invalid_pool)
                .await
                .unwrap();
            sqlx::query("UPDATE memories_schema_contract SET version = ?")
                .bind("20260803000003")
                .execute(&invalid_pool)
                .await
                .unwrap();
            sqlx::query("INSERT INTO atlas_schema_revisions (version, type) VALUES (?, ?)")
                .bind("20260803000003")
                .bind(ATLAS_APPLIED_REVISION_TYPE)
                .execute(&invalid_pool)
                .await
                .unwrap();
            assert_eq!(
                schema_state(&invalid_pool).await.unwrap(),
                SchemaState::SchemaCorrupt,
                "an unknown history version must not be treated as a pending prefix"
            );
        });
    }
}
