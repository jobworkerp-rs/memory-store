use anyhow::{Context, Result, bail};
use app::app::REFLECTION_USER_ID;
use app::app::reflection::build_memory_data::build_reflection_search_document_from_metadata;
use clap::{Parser, Subcommand};
use infra::infra::memory::rdb::{MemoryRepository, MemoryRepositoryImpl};
use infra::infra::memory_vector::dispatcher::{EmbeddingDispatch, EmbeddingJobDispatcher};
use infra::infra::module::RepositoryModule;
use infra_utils::infra::rdb::UseRdbPool;
use infra_utils::infra::rdb::{Rdb, RdbPool};
use protobuf::llm_memory::data::{MemoryId, MessageRole};

#[cfg(feature = "postgres")]
const METADATA_TEXT_EXPR: &str = "m.metadata::TEXT";
#[cfg(not(feature = "postgres"))]
const METADATA_TEXT_EXPR: &str = "m.metadata";
#[cfg(feature = "postgres")]
const P1: &str = "$1";
#[cfg(not(feature = "postgres"))]
const P1: &str = "?";
#[cfg(feature = "postgres")]
const P2: &str = "$2";
#[cfg(not(feature = "postgres"))]
const P2: &str = "?";
#[cfg(feature = "postgres")]
const P3: &str = "$3";
#[cfg(not(feature = "postgres"))]
const P3: &str = "?";
#[cfg(feature = "postgres")]
const P4: &str = "$4";
#[cfg(not(feature = "postgres"))]
const P4: &str = "?";

#[derive(Parser, Debug)]
#[command(name = "memories-admin", about = "Operational maintenance commands")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    Reflection {
        #[command(subcommand)]
        command: ReflectionCommand,
    },
}

#[derive(Subcommand, Debug)]
enum ReflectionCommand {
    BackfillSearchDocument(BackfillArgs),
}

#[derive(Parser, Debug)]
struct BackfillArgs {
    #[arg(long, default_value_t = 500)]
    batch_size: i64,
    #[arg(long)]
    origin_user_id: Option<i64>,
    #[arg(long)]
    origin_thread_id: Option<i64>,
    #[arg(long)]
    created_after: Option<i64>,
    #[arg(long)]
    created_before: Option<i64>,
    #[arg(long)]
    redispatch_text: bool,
    #[arg(long)]
    redispatch_existing: bool,
    #[arg(long)]
    dry_run: bool,
}

#[derive(Debug, Default)]
struct Stats {
    scanned: u64,
    matched: u64,
    updated: u64,
    unchanged: u64,
    skipped: u64,
    dispatched: u64,
    dispatch_failed: u64,
}

#[derive(Debug, sqlx::FromRow)]
struct ReflectionBackfillRow {
    id: i64,
    content: String,
    metadata: Option<String>,
    created_at: i64,
    origin_user_id: i64,
    origin_thread_id: i64,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    use command_utils::util::tracing::{load_tracing_config_from_env, tracing_init};
    tracing_init(load_tracing_config_from_env().unwrap_or_default()).await?;

    let cli = Cli::parse();
    match cli.command {
        Command::Reflection { command } => match command {
            ReflectionCommand::BackfillSearchDocument(args) => run_backfill(args).await,
        },
    }
}

async fn run_backfill(args: BackfillArgs) -> Result<()> {
    if args.batch_size <= 0 {
        bail!("--batch-size must be positive");
    }
    if args.redispatch_existing && !args.redispatch_text {
        bail!("--redispatch-existing requires --redispatch-text");
    }

    let repositories = RepositoryModule::new_by_env().await;
    let pool = repositories.pool();
    let memory_repo = repositories.create_memory_repository();
    let dispatcher = if args.redispatch_text && !args.dry_run {
        let dispatcher = EmbeddingJobDispatcher::from_env()
            .context("initializing generic embedding dispatcher")?;
        dispatcher
            .ensure_initialized()
            .await
            .context("initializing generic embedding workers")?;
        Some(dispatcher)
    } else {
        None
    };

    let mut stats = Stats::default();
    let mut after_id = 0_i64;
    loop {
        let page = fetch_page(pool, args.batch_size, after_id).await?;
        if page.is_empty() {
            break;
        }
        for row in page {
            after_id = after_id.max(row.id);
            stats.scanned += 1;
            if !matches_filter(&row, &args) {
                continue;
            }
            stats.matched += 1;

            let Some(metadata) = row.metadata.as_deref() else {
                stats.skipped += 1;
                continue;
            };
            let generated = match build_reflection_search_document_from_metadata(metadata) {
                Ok(doc) if !doc.trim().is_empty() => doc,
                Ok(_) => {
                    stats.skipped += 1;
                    continue;
                }
                Err(e) => {
                    stats.skipped += 1;
                    tracing::warn!("skip reflection memory_id={}: {e:#}", row.id);
                    continue;
                }
            };

            let changed = generated != row.content;
            if changed {
                stats.updated += 1;
                if !args.dry_run {
                    update_content(&memory_repo, row.id, &generated).await?;
                }
            } else {
                stats.unchanged += 1;
            }

            let should_dispatch = args.redispatch_text && (changed || args.redispatch_existing);
            if should_dispatch
                && !args.dry_run
                && let Some(dispatcher) = &dispatcher
            {
                match dispatcher.dispatch(row.id, &generated).await {
                    Ok(_) => stats.dispatched += 1,
                    Err(e) => {
                        stats.dispatch_failed += 1;
                        tracing::warn!("dispatch failed for memory_id={}: {e:?}", row.id);
                    }
                }
            }
        }
    }

    println!(
        "reflection backfill-search-document done: scanned={} matched={} updated={} unchanged={} skipped={} dispatched={} dispatch_failed={} dry_run={}",
        stats.scanned,
        stats.matched,
        stats.updated,
        stats.unchanged,
        stats.skipped,
        stats.dispatched,
        stats.dispatch_failed,
        args.dry_run,
    );
    Ok(())
}

async fn fetch_page(
    pool: &'static RdbPool,
    batch_size: i64,
    after_id: i64,
) -> Result<Vec<ReflectionBackfillRow>> {
    let sql = format!(
        "SELECT m.id AS id, m.content AS content, {METADATA_TEXT_EXPR} AS metadata, \
         m.created_at AS created_at, tri.origin_user_id AS origin_user_id, \
         tri.origin_thread_id AS origin_thread_id \
         FROM memory m \
         JOIN thread_reflection_index tri ON tri.memory_id = m.id \
         WHERE m.id > {P1} AND m.user_id = {P2} AND m.role = {P3} \
         ORDER BY m.id ASC LIMIT {P4}"
    );
    sqlx::query_as::<Rdb, ReflectionBackfillRow>(sqlx::AssertSqlSafe(sql))
        .bind(after_id)
        .bind(REFLECTION_USER_ID)
        .bind(MessageRole::RoleReflection as i32)
        .bind(batch_size)
        .fetch_all(pool)
        .await
        .context("fetching reflection memory page")
}

fn matches_filter(row: &ReflectionBackfillRow, args: &BackfillArgs) -> bool {
    if args.origin_user_id.is_some_and(|v| row.origin_user_id != v) {
        return false;
    }
    if args
        .origin_thread_id
        .is_some_and(|v| row.origin_thread_id != v)
    {
        return false;
    }
    if args.created_after.is_some_and(|v| row.created_at < v) {
        return false;
    }
    if args.created_before.is_some_and(|v| row.created_at > v) {
        return false;
    }
    true
}

async fn update_content(repo: &MemoryRepositoryImpl, memory_id: i64, content: &str) -> Result<()> {
    let mut tx = repo.db_pool().begin().await?;
    repo.update_content_only(&mut *tx, &MemoryId { value: memory_id }, content)
        .await?;
    tx.commit().await?;
    Ok(())
}
