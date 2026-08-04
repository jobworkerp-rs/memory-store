//! Release schema-migration adapter and post-schema task coordinator.

use anyhow::{Context, Result, bail};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use clap::{Args, Parser, Subcommand};
use grpc_admin::db_migrate::{
    catalog::{self},
    state::{self, TaskStateKind},
    task_from_catalog,
};
use infra_utils::infra::rdb::{Rdb, RdbPool};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::fs;
use std::process::Stdio;
use url::Url;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SchemaState {
    Uninitialized,
    BaselineRequired,
    Pending { applied_count: usize },
    Managed,
    SchemaCorrupt,
}

impl SchemaState {
    fn as_str(self) -> &'static str {
        match self {
            Self::Uninitialized => "uninitialized",
            Self::BaselineRequired => "baseline_required",
            Self::Pending { .. } => "pending",
            Self::Managed => "managed",
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
            SchemaCommand::Validate => {
                run_atlas(&["migrate", "validate"]).await?;
                println!("schema_validate status=valid");
                Ok(())
            }
            SchemaCommand::Status => run_status().await,
            SchemaCommand::Apply(args) => run_apply(args.dry_run).await,
            SchemaCommand::Verify(verify_args) => run_verify(verify_args.to_version).await,
            SchemaCommand::Baseline => run_baseline().await,
        },
        Command::PostMigrate { command } => run_post_migrate(command).await,
    }
}

async fn run_status() -> Result<()> {
    let pool = open_target_pool().await?;
    let state = schema_state(&pool).await?;
    if state == SchemaState::SchemaCorrupt {
        bail!(
            "schema_corrupt: Atlas history, schema contract, and task state must be introduced together"
        );
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
        SchemaState::BaselineRequired | SchemaState::SchemaCorrupt => None,
    }
}

async fn run_apply(dry_run: bool) -> Result<()> {
    let pool = open_target_pool().await?;
    let state = schema_state(&pool).await?;
    match (state, dry_run) {
        (SchemaState::SchemaCorrupt, _) => bail!(
            "schema_corrupt: Atlas history, schema contract, and task state must be introduced together"
        ),
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

async fn run_uninitialized_dry_run() -> Result<()> {
    let atlas_result = run_atlas(&["migrate", "apply", "--dry-run"]).await;
    let cleanup_result = cleanup_uninitialized_dry_run_history().await;
    match (atlas_result, cleanup_result) {
        (Ok(()), Ok(())) => Ok(()),
        (Ok(()), Err(error)) => Err(error),
        (Err(error), Ok(())) => Err(error),
        (Err(error), Err(cleanup_error)) => Err(error.context(format!(
            "Atlas dry-run also left an unsafe migration-control state: {cleanup_error}"
        ))),
    }
}

async fn cleanup_uninitialized_dry_run_history() -> Result<()> {
    let pool = open_target_pool().await?;
    let has_application_table = table_exists(&pool, "thread").await?;
    let has_history = table_exists(&pool, "atlas_schema_revisions").await?;
    let has_contract = table_exists(&pool, "memories_schema_contract").await?;
    let has_task_state = table_exists(&pool, "memories_data_migration_task_state").await?;
    if !has_history {
        return Ok(());
    }
    if has_application_table || has_contract || has_task_state {
        bail!("Atlas dry-run changed an uninitialized target beyond its empty revision history");
    }

    let revision_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM atlas_schema_revisions")
        .fetch_one(&pool)
        .await
        .context("checking Atlas dry-run revision history")?;
    if revision_count != 0 {
        bail!("Atlas dry-run left a non-empty revision history on an uninitialized target");
    }
    // Atlas creates its revision table before rendering a dry-run plan. An
    // empty table is not migration state, so remove it to preserve dry-run.
    sqlx::query("DROP TABLE atlas_schema_revisions")
        .execute(&pool)
        .await
        .context("restoring uninitialized target after Atlas dry-run")?;
    if schema_state(&pool).await? != SchemaState::Uninitialized {
        bail!("Atlas dry-run cleanup did not restore the uninitialized target state");
    }
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
    sqlx::Pool::<Rdb>::connect(&url)
        .await
        .context("connecting to migration target database")
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
    {
        let mut query = url.query_pairs_mut();
        query.clear();
        query.extend_pairs(
            pairs
                .iter()
                .map(|(key, value)| (key.as_str(), value.as_str())),
        );
    }
    Ok(url.into())
}

async fn ensure_schema_prerequisites(pool: &RdbPool, minimum_version: &str) -> Result<()> {
    validate_schema_version(minimum_version)?;
    match schema_state(pool).await? {
        SchemaState::Managed => {}
        SchemaState::SchemaCorrupt => bail!(
            "schema_corrupt: Atlas history, schema contract, and task state must be introduced together"
        ),
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
        SchemaState::SchemaCorrupt => {
            bail!(
                "schema_corrupt: Atlas history, schema contract, and task state must be introduced together"
            )
        }
    }
}

async fn schema_state(pool: &RdbPool) -> Result<SchemaState> {
    let has_application_table = table_exists(pool, "thread").await?;
    let has_history = table_exists(pool, "atlas_schema_revisions").await?;
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
    let Some(applied_count) = valid_schema_history_prefix_len(pool).await? else {
        return Ok(SchemaState::SchemaCorrupt);
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

/// Atlas history may stop at a fixed-catalog boundary. Fresh databases start
/// at v1 with a normal applied revision; adopted schemas require a baseline.
async fn valid_schema_history_prefix_len(pool: &RdbPool) -> Result<Option<usize>> {
    let history: Vec<(String, i64)> =
        sqlx::query_as("SELECT version, type FROM atlas_schema_revisions ORDER BY version ASC")
            .fetch_all(pool)
            .await
            .context("reading Atlas schema revision history")?;
    let expected = atlas_migration_versions();
    let Some((first_version, first_type)) = history.first() else {
        return Ok(None);
    };
    let Some(start_index) = expected.iter().position(|version| version == first_version) else {
        return Ok(None);
    };
    let first_revision_is_valid = if start_index == 0 {
        *first_type == ATLAS_BASELINE_REVISION_TYPE || *first_type == ATLAS_APPLIED_REVISION_TYPE
    } else {
        adoption_baseline_versions().contains(&first_version.as_str())
            && *first_type == ATLAS_BASELINE_REVISION_TYPE
    };
    if !first_revision_is_valid || history.len() > expected.len() - start_index {
        return Ok(None);
    }
    for (offset, (version, revision_type)) in history.iter().enumerate() {
        if version != &expected[start_index + offset]
            || (offset > 0 && *revision_type != ATLAS_APPLIED_REVISION_TYPE)
        {
            return Ok(None);
        }
    }
    Ok(Some(start_index + history.len()))
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
    let contracts: Vec<(String, String)> = sqlx::query_as(
        "SELECT contract_key, version FROM memories_schema_contract ORDER BY contract_key ASC",
    )
    .fetch_all(pool)
    .await
    .context("reading schema contract history alignment")?;
    Ok(contracts
        == vec![(
            "rdb_schema".to_string(),
            expected[applied_count - 1].clone(),
        )])
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
        .envs(child_env.iter().map(|(name, value)| (name, value)))
        .env("MEMORIES_ATLAS_DATABASE_URL", database_url)
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
    let digest = Sha256::digest(
        fs::read(path).with_context(|| format!("reading fixed Atlas binary {}", path.display()))?,
    );
    Ok(digest.iter().map(|byte| format!("{byte:02x}")).collect())
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
        [] => bail!(
            "baseline_schema_mismatch: target does not match a supported adoption schema; candidates={}",
            mismatches.join(" | ")
        ),
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
    let schema = format!(
        "memories_atlas_verify_{}_{}",
        std::process::id(),
        command_utils::util::datetime::now_millis()
    );
    let create = format!("CREATE SCHEMA {}", quote_postgres_identifier(&schema));
    sqlx::query(sqlx::AssertSqlSafe(create))
        .execute(&pool)
        .await
        .context("creating temporary PostgreSQL schema for Atlas verification")?;
    let target_url = migration_database_url()?;
    let dev_url = atlas_database_url(&postgres_schema_url(&target_url, &schema)?)?;
    let result = run_atlas_capture(
        args,
        "verify.hcl",
        &[("MEMORIES_ATLAS_INTERNAL_DEV_URL".to_string(), dev_url)],
    )
    .await;
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
    use super::{
        ADOPTION_BASELINE_VERSION, adoption_baseline_versions, atlas_artifact_root,
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
    use super::{SchemaState, postgres_schema_url, quote_postgres_identifier, schema_state};
    use anyhow::{Context, Result, bail};
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
                .envs(E2E_FIXED_SEARCH_ENVIRONMENT)
                .stdin(Stdio::null())
                .kill_on_drop(true);
            let output = tokio::time::timeout(Duration::from_secs(120), process.output())
                .await
                .context("memories-db-migrate release binary timed out")?
                .context("starting memories-db-migrate release binary")?;
            let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
            if output.status.code() != Some(0) {
                bail!(
                    "memories-db-migrate {:?} exited with {}; stdout={stdout:?}; stderr={}",
                    arguments,
                    output.status,
                    String::from_utf8_lossy(&output.stderr),
                );
            }
            Ok(stdout)
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

        let pool = sqlx::Pool::<Rdb>::connect(database_url).await?;
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
        assert!(output.contains("verify status=verified version=20260803000003"));
        let output = command.run(&["post-migrate", "verify"]).await?;
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
        assert!(output.contains("verify status=verified version=20260803000003"));

        let pool = sqlx::Pool::<Rdb>::connect(database_url).await?;
        assert_eq!(schema_state(&pool).await?, SchemaState::Managed);
        let contract: String = sqlx::query_scalar(
            "SELECT version FROM memories_schema_contract WHERE contract_key = 'rdb_schema'",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(contract, "20260803000003");
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
            2
        );
        assert_eq!(
            remaining_migration_count_after_baseline("20260803000002").unwrap(),
            1
        );
        assert!(remaining_migration_count_after_baseline("20260803000003").is_err());
    }

    #[test]
    fn schema_version_rejects_non_canonical_values() {
        assert!(validate_schema_version("20260803").is_err());
        assert!(validate_schema_version("2026080300000x").is_err());
    }

    #[test]
    fn schema_status_pending_count_is_deterministic_for_safe_states() {
        assert_eq!(
            pending_count_for_schema_state(SchemaState::Uninitialized, 3),
            Some(3)
        );
        assert_eq!(
            pending_count_for_schema_state(SchemaState::Managed, 3),
            Some(0)
        );
        assert_eq!(
            pending_count_for_schema_state(SchemaState::Pending { applied_count: 1 }, 3),
            Some(2)
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
    fn docker_smoke_migration_uses_public_schema_subcommands() {
        const DOCKERFILE: &str = include_str!("../../../Dockerfile");

        for command in [
            "schema validate",
            "schema status",
            "schema apply --dry-run",
            "schema apply",
            "schema verify",
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

        let post_migrate_verify = DOCKERFILE
            .find("memories-db-migrate post-migrate verify")
            .expect("Docker smoke migration must verify post-migration tasks");
        let final_status = DOCKERFILE[post_migrate_verify..]
            .find("memories-db-migrate schema status")
            .map(|offset| post_migrate_verify + offset)
            .expect(
                "Docker smoke migration must check schema status after post-migrate verification",
            );
        assert!(
            final_status > post_migrate_verify,
            "the managed schema status check must follow post-migrate verification"
        );
        assert!(
            DOCKERFILE[final_status..]
                .contains("grep -Fx 'schema_status status=managed pending_count=0'"),
            "Docker smoke migration must reject a final schema status other than managed with no pending migrations"
        );
        assert!(
            DOCKERFILE[post_migrate_verify..].contains(
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
    fn postgres_migration_e2e_with_fixed_atlas_artifact() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            use sqlx::postgres::PgPoolOptions;

            let artifact_root = fixed_e2e_atlas_artifact_root().unwrap();
            let binary = fixed_e2e_release_binary().unwrap();
            let service_url = std::env::var("TEST_POSTGRES_URL")
                .expect("TEST_POSTGRES_URL must be set for the PostgreSQL E2E test");
            let temporary = tempfile::tempdir().unwrap();
            let schema = format!(
                "memories_db_migrate_e2e_{}_{}",
                std::process::id(),
                command_utils::util::datetime::now_millis()
            );
            let database_url = postgres_schema_url(&service_url, &schema).unwrap();
            let admin_pool = PgPoolOptions::new()
                .max_connections(1)
                .connect(&service_url)
                .await
                .unwrap();
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
                Some(2)
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
                .bind("20260803000003")
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
