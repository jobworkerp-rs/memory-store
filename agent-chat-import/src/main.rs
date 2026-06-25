mod cli;
mod client;
mod common;
mod generation_workers;
mod parser;
#[cfg(feature = "personality-after")]
mod personality;
mod source;
#[cfg(feature = "summarize-after")]
mod summarize;

use anyhow::{Result, anyhow};
use clap::Parser;
use cli::{Cli, CodexArgs, DEFAULT_PLAIN_SOURCE_NAME, GlobalArgs, PlainArgs, Subcmd};
use client::{ImportClient, LiveGrpcImportClient, LiveGrpcImportClientConfig, RetryPolicy};
use common::importer::{
    CanonicalSessionResult, ChunkLimits, run_all, run_all_with_entry_collector,
};
use source::claude_code::ClaudeCodeSource;
use source::codex::CodexSource;
use source::plain::PlainSource;
use source::plain::prune::{self, PruneConfig, PruneOutcome, PruneSkipReason, PruneSummary};
use std::cell::RefCell;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();

    let cli = Cli::parse();
    let user_id = cli.validate_user_id().unwrap_or_else(|e| e.exit());

    init_tracing(cli.global.verbose).await?;

    match cli.command {
        Subcmd::UpsertGenerationWorkers(args) => {
            run_upsert_generation_workers(args).await?;
        }
        Subcmd::ClaudeCode(args) => {
            run_claude_code(&cli.global, args, user_id).await?;
        }
        Subcmd::Codex(args) => {
            run_codex(&cli.global, args, user_id).await?;
        }
        Subcmd::Plain(args) => {
            run_plain(&cli.global, args, user_id).await?;
        }
    }

    Ok(())
}

async fn run_upsert_generation_workers(args: cli::UpsertGenerationWorkersArgs) -> Result<()> {
    let features = generation_workers::parse_feature_selection(&args.feature)?;
    let languages = generation_workers::parse_language_selection(&args.language)?;
    let repo_root = args
        .repo_root
        .unwrap_or_else(generation_workers::resolve_repo_root);
    let registered = generation_workers::upsert_generation_workers(
        generation_workers::UpsertGenerationWorkersArgs {
            repo_root,
            channel: args.channel,
            timeout_sec: args.timeout_sec,
            features,
            languages,
        },
    )
    .await?;
    for name in registered {
        println!("upserted generation worker: {name}");
    }
    Ok(())
}

async fn init_tracing(verbose: bool) -> Result<()> {
    let log_filename =
        command_utils::util::tracing::create_filename_with_ip_postfix("memories-import", "log");
    let mut conf = command_utils::util::tracing::load_tracing_config_from_env().unwrap_or_default();
    if verbose && conf.level.is_none() {
        conf.level = Some("debug".to_string());
    }
    conf.file_name = Some(log_filename);
    command_utils::util::tracing::tracing_init(conf).await?;
    Ok(())
}

/// Build the live gRPC client. `--dry-run` returns `None` so the
/// importer skips every RPC and reports the "no thread / no memory
/// written" reality in its summary.
async fn build_import_client(global: &GlobalArgs) -> Result<Option<Arc<dyn ImportClient>>> {
    if global.dry_run {
        return Ok(None);
    }
    let server_url = global.server_url.clone().ok_or_else(|| {
        anyhow!("--server-url is required (use --dry-run to skip the live import)")
    })?;
    let cfg = LiveGrpcImportClientConfig {
        server_url,
        timeout: Duration::from_secs(global.server_timeout_sec),
        tls_ca_path: global.server_tls_ca.clone(),
        auth_token: global.auth_token.clone(),
        retry: if global.no_retry {
            RetryPolicy::no_retry()
        } else {
            RetryPolicy {
                max_attempts: global.server_retry_max,
                base_delay_ms: global.server_retry_base_ms,
                max_delay_ms: global.server_retry_cap_ms,
                jitter_ratio: global.server_retry_jitter_ratio,
            }
        },
    };
    let live = LiveGrpcImportClient::connect(cfg).await?;
    Ok(Some(Arc::new(live)))
}

/// Build the importer's `ChunkLimits` from CLI overrides. Lives here
/// (not in `common::importer`) so the importer crate stays unaware of
/// `GlobalArgs` / `clap`.
fn chunk_limits_from_global(global: &GlobalArgs) -> ChunkLimits {
    ChunkLimits {
        max_entries: global.chunk_max_entries,
        max_bytes: global.chunk_max_bytes,
    }
}

async fn run_claude_code(
    global: &GlobalArgs,
    args: cli::ClaudeCodeArgs,
    user_id: i64,
) -> Result<()> {
    args.attachment_subtypes_policy()
        .map_err(|e| anyhow::anyhow!("--attachment-subtypes: {e}"))?;
    run_canonical_source(global, user_id, "claude_code", ClaudeCodeSource::new(args)).await
}

#[cfg(feature = "summarize-after")]
async fn dispatch_summarize_after(
    global: &GlobalArgs,
    template: Option<serde_json::Value>,
    import_errors: usize,
    memories_imported: usize,
    user_id: i64,
) -> Result<()> {
    let Some(template) = template else {
        return Ok(());
    };
    if let Some(reason) = summarize::skip_reason(import_errors, memories_imported) {
        eprintln!("Skipping thread-summary-batch dispatch: {reason}.");
        return Ok(());
    }
    let workflow_path = global
        .summarize_workflow
        .as_deref()
        .expect("clap requires SUMMARIZE_WORKFLOW_ARG when SUMMARIZE_INPUT_GROUP is set");
    println!(
        "\nDispatching thread-summary-batch workflow ({})...",
        workflow_path.display()
    );
    match summarize::run_summarize_after(
        template,
        workflow_path,
        global.summarize_channel.as_deref(),
        user_id,
        global.since_millis()?,
        &common::language::resolve_output_language(global.output_language.as_deref())?,
        global.summarize_timeout_sec,
    )
    .await
    {
        Ok(result) => println!("thread-summary-batch result: {result}"),
        Err(e) => eprintln!("Warning: thread-summary-batch dispatch failed: {e}"),
    }
    Ok(())
}

#[cfg(feature = "personality-after")]
async fn dispatch_personality_after(
    global: &GlobalArgs,
    template: Option<serde_json::Value>,
    import_errors: usize,
    memories_imported: usize,
    user_id: i64,
) -> Result<()> {
    let Some(template) = template else {
        return Ok(());
    };
    if let Some(reason) = personality::skip_reason(import_errors, memories_imported) {
        eprintln!("Skipping thread-personality-batch dispatch: {reason}.");
        return Ok(());
    }
    let workflow_path = global
        .personality_workflow
        .as_deref()
        .expect("clap requires PERSONALITY_WORKFLOW_ARG when PERSONALITY_INPUT_GROUP is set");
    println!(
        "\nDispatching thread-personality-batch workflow ({})...",
        workflow_path.display()
    );
    match personality::run_personality_after(
        template,
        workflow_path,
        global.personality_channel.as_deref(),
        user_id,
        global.since_millis()?,
        &common::language::resolve_output_language(global.output_language.as_deref())?,
        global.personality_timeout_sec,
    )
    .await
    {
        Ok(result) => println!("thread-personality-batch result: {result}"),
        Err(e) => eprintln!("Warning: thread-personality-batch dispatch failed: {e}"),
    }
    Ok(())
}

async fn run_codex(global: &GlobalArgs, args: CodexArgs, user_id: i64) -> Result<()> {
    run_canonical_source(global, user_id, "codex", CodexSource::new(args)).await
}

async fn run_plain(global: &GlobalArgs, args: PlainArgs, user_id: i64) -> Result<()> {
    if args.source_name == DEFAULT_PLAIN_SOURCE_NAME {
        eprintln!(
            "WARNING: --source-name is the default ('plain'). Importing more than one vault \
             under this name will let identical relative paths collide on the same thread. \
             Pick a unique name per vault (e.g. --source-name obsidian-private)."
        );
    }
    let prune_requested = args.prune_missing;
    let no_interactive = args.no_interactive;
    let orphan_threads = args.effective_prune_orphan_threads();
    let source_name = args.source_name.clone();
    let source = PlainSource::new(args);

    let since_millis = global.since_millis()?;
    let since_millis_with_margin = global.since_millis_with_margin()?;
    let extra_labels = global.extra_labels();
    #[cfg(feature = "summarize-after")]
    let summarize_template = match global.summarize_after_raw()? {
        Some(raw) => Some(summarize::parse_template(&raw)?),
        None => None,
    };
    #[cfg(feature = "personality-after")]
    let personality_template = match global.extract_personality_after_raw()? {
        Some(raw) => Some(personality::parse_template(&raw)?),
        None => None,
    };
    let client = build_import_client(global).await?;
    let display_label = if global.dry_run {
        "[dry-run] plain".to_string()
    } else {
        "plain".to_string()
    };

    // RefCell so the closure can mutate the collected sets while the
    // borrow checker still sees `&dyn FnMut` as `&mut`. The collector
    // is only invoked from inside `run_all_with_entry_collector`, so
    // the borrow window is closed before we read the sets back.
    let d_external_id: RefCell<HashSet<String>> = RefCell::new(HashSet::new());
    let d_path: RefCell<HashSet<PathBuf>> = RefCell::new(HashSet::new());

    let results = run_all_with_entry_collector(
        &source,
        client.as_deref(),
        since_millis,
        since_millis_with_margin,
        user_id,
        &extra_labels,
        |entries| {
            let (eid, paths) = prune::extract_d_sets(entries);
            d_external_id.borrow_mut().extend(eid);
            d_path.borrow_mut().extend(paths);
        },
    )
    .await?;
    print_canonical_summary(&display_label, &results);

    let errors: usize = results.iter().filter(|r| r.error.is_some()).count();

    // Phase A `--prune-missing`: only runs when explicitly requested,
    // not under dry-run, and the import had no errors. Anything else
    // surfaces as a Skip in the summary.
    let prune_outcome = if !prune_requested {
        PruneOutcome::Skipped(PruneSkipReason::NotRequested)
    } else if global.dry_run {
        eprintln!(
            "WARNING: --prune-missing is ignored under --dry-run; \
             rerun without --dry-run to compute prune candidates."
        );
        PruneOutcome::Skipped(PruneSkipReason::DryRun)
    } else if errors > 0 {
        eprintln!(
            "WARNING: --prune-missing skipped because the import had {errors} session error(s)."
        );
        PruneOutcome::Skipped(PruneSkipReason::ImportHadErrors)
    } else {
        // Live run with no errors: ImportClient is non-None.
        let live_client = client
            .as_deref()
            .ok_or_else(|| anyhow!("internal: prune live path without client"))?;
        // Reuse the source's cached canonical root (already resolved
        // during discover()) so we don't re-canonicalize.
        let canonical_root = source.canonical_root()?.to_path_buf();
        run_prune_for_plain(
            live_client,
            &PruneConfig {
                source_name,
                user_id,
                canonical_root,
                orphan_threads,
                no_interactive,
            },
            d_external_id.into_inner(),
            d_path.into_inner(),
        )
        .await?
    };
    print_prune_summary(&prune_outcome);

    #[cfg(any(feature = "summarize-after", feature = "personality-after"))]
    let memories_imported: usize = results.iter().map(|r| r.memories_imported).sum();
    dispatch_post_import_workflows(
        global,
        #[cfg(feature = "summarize-after")]
        summarize_template,
        #[cfg(feature = "personality-after")]
        personality_template,
        errors,
        #[cfg(any(feature = "summarize-after", feature = "personality-after"))]
        memories_imported,
        user_id,
    )
    .await?;

    let prune_errors = match &prune_outcome {
        PruneOutcome::Ran(s) => s.errors,
        _ => 0,
    };
    let prune_aborted = matches!(
        prune_outcome,
        PruneOutcome::Skipped(PruneSkipReason::NoInteractiveRequired)
    );
    if errors > 0 || prune_errors > 0 || prune_aborted {
        std::process::exit(1);
    }
    Ok(())
}

async fn run_prune_for_plain(
    client: &dyn ImportClient,
    cfg: &PruneConfig,
    d_external_id: HashSet<String>,
    d_path: HashSet<PathBuf>,
) -> Result<PruneOutcome> {
    let prefix = format!("{}:", cfg.source_name);
    let server_memories = client
        .find_memories_by_external_id_prefix(prefix, cfg.user_id)
        .await?;
    let m_prime: Vec<prune::PruneCandidate> = server_memories
        .iter()
        .filter_map(prune::extract_candidate)
        .filter(|c| !d_external_id.contains(&c.external_id))
        .collect();
    let initial = PruneSummary {
        candidates_considered: m_prime.len(),
        ..Default::default()
    };
    let canonical_root = cfg.canonical_root.clone();
    let (m, filter_stats) = prune::filter_candidates(m_prime, &d_path, |p| {
        prune::fs_path_exists(&canonical_root, p)
    });
    let stats = PruneSummary {
        excluded_path_missing: filter_stats.excluded_path_missing,
        excluded_still_on_fs: filter_stats.excluded_still_on_fs,
        ..initial
    };
    prune::execute_prune(client, cfg, m, stats).await
}

fn print_prune_summary(outcome: &PruneOutcome) {
    match outcome {
        PruneOutcome::Skipped(reason) => {
            let label = match reason {
                // NotRequested is the no-prune-flag default — stay silent.
                PruneSkipReason::NotRequested => return,
                PruneSkipReason::DryRun => "skipped (dry-run)",
                PruneSkipReason::ImportHadErrors => "skipped (import had errors)",
                PruneSkipReason::NoInteractiveRequired => {
                    "skipped (non-TTY without --no-interactive)"
                }
                PruneSkipReason::NothingToPrune => "skipped (no candidates)",
                PruneSkipReason::UserAborted => "skipped (operator declined)",
            };
            println!("  Prune (--prune-missing): {label}");
        }
        PruneOutcome::Ran(s) => {
            println!("  Prune (--prune-missing):");
            println!("    Candidates considered: {}", s.candidates_considered);
            println!("    Excluded (path missing): {}", s.excluded_path_missing);
            println!("    Excluded (still on fs): {}", s.excluded_still_on_fs);
            println!("    Memories deleted: {}", s.memories_deleted);
            println!("    Threads deleted (orphan): {}", s.threads_deleted);
            println!("    Errors: {}", s.errors);
        }
    }
}

/// Shared dispatch path for canonical-trait sources. Builds the
/// `ImportClient` once and hands it through `run_all`.
async fn run_canonical_source<S>(
    global: &GlobalArgs,
    user_id: i64,
    label: &str,
    source: S,
) -> Result<()>
where
    S: source::ChatSource,
{
    let since_millis = global.since_millis()?;
    let since_millis_with_margin = global.since_millis_with_margin()?;
    let extra_labels = global.extra_labels();

    #[cfg(feature = "summarize-after")]
    let summarize_template = match global.summarize_after_raw()? {
        Some(raw) => Some(summarize::parse_template(&raw)?),
        None => None,
    };
    #[cfg(feature = "personality-after")]
    let personality_template = match global.extract_personality_after_raw()? {
        Some(raw) => Some(personality::parse_template(&raw)?),
        None => None,
    };

    let client = build_import_client(global).await?;
    let display_label = if global.dry_run {
        format!("[dry-run] {label}")
    } else {
        label.to_string()
    };
    let results = run_all(
        &source,
        client.as_deref(),
        since_millis,
        since_millis_with_margin,
        user_id,
        &extra_labels,
        chunk_limits_from_global(global),
    )
    .await?;
    print_canonical_summary(&display_label, &results);

    let errors: usize = results.iter().filter(|r| r.error.is_some()).count();
    #[cfg(any(feature = "summarize-after", feature = "personality-after"))]
    let memories_imported: usize = results.iter().map(|r| r.memories_imported).sum();

    dispatch_post_import_workflows(
        global,
        #[cfg(feature = "summarize-after")]
        summarize_template,
        #[cfg(feature = "personality-after")]
        personality_template,
        errors,
        #[cfg(any(feature = "summarize-after", feature = "personality-after"))]
        memories_imported,
        user_id,
    )
    .await?;

    if errors > 0 {
        std::process::exit(1);
    }
    Ok(())
}

// Run the summary and personality dispatches concurrently. Both are
// independent (disjoint owner ID spaces) and each absorbs its own
// runtime errors as warnings, so one failure must not block the other.
async fn dispatch_post_import_workflows(
    global: &GlobalArgs,
    #[cfg(feature = "summarize-after")] summarize_template: Option<serde_json::Value>,
    #[cfg(feature = "personality-after")] personality_template: Option<serde_json::Value>,
    errors: usize,
    #[cfg(any(feature = "summarize-after", feature = "personality-after"))]
    memories_imported: usize,
    user_id: i64,
) -> Result<()> {
    #[cfg(feature = "summarize-after")]
    let summary_fut = async {
        if global.dry_run && summarize_template.is_some() {
            println!("[dry-run] Skipping thread-summary-batch workflow execution");
            Ok(())
        } else {
            dispatch_summarize_after(
                global,
                summarize_template,
                errors,
                memories_imported,
                user_id,
            )
            .await
        }
    };
    #[cfg(feature = "personality-after")]
    let personality_fut = async {
        if global.dry_run && personality_template.is_some() {
            println!("[dry-run] Skipping thread-personality-batch workflow execution");
            Ok(())
        } else {
            dispatch_personality_after(
                global,
                personality_template,
                errors,
                memories_imported,
                user_id,
            )
            .await
        }
    };

    #[cfg(all(feature = "summarize-after", feature = "personality-after"))]
    {
        tokio::try_join!(summary_fut, personality_fut)?;
    }
    #[cfg(all(feature = "summarize-after", not(feature = "personality-after")))]
    {
        summary_fut.await?;
    }
    #[cfg(all(not(feature = "summarize-after"), feature = "personality-after"))]
    {
        personality_fut.await?;
    }
    #[cfg(not(any(feature = "summarize-after", feature = "personality-after")))]
    {
        let _ = (global, errors, user_id);
    }
    Ok(())
}

fn print_canonical_summary(label: &str, results: &[CanonicalSessionResult]) {
    let sessions = results.len();
    let threads_created = results.iter().filter(|r| r.thread_created).count();
    let imported: usize = results.iter().map(|r| r.memories_imported).sum();
    let dup: usize = results.iter().map(|r| r.memories_skipped_duplicate).sum();
    let filtered: usize = results.iter().map(|r| r.memories_skipped_filtered).sum();
    let rewired: usize = results.iter().map(|r| r.memories_rewired).sum();
    let errors: usize = results.iter().filter(|r| r.error.is_some()).count();
    let skipped_results: Vec<&CanonicalSessionResult> =
        results.iter().filter(|r| r.skip_reason.is_some()).collect();
    println!("\n{label} summary:");
    println!("  Sessions processed: {sessions}");
    println!("  Threads created: {threads_created}");
    println!("  Memories imported: {imported}");
    println!("  Memories skipped (duplicate): {dup}");
    println!("  Memories skipped (filtered): {filtered}");
    if rewired > 0 {
        println!("  Memories rewired: {rewired}");
    }
    if !skipped_results.is_empty() {
        let mut by_reason: std::collections::BTreeMap<&str, usize> =
            std::collections::BTreeMap::new();
        for r in &skipped_results {
            *by_reason
                .entry(r.skip_reason.as_deref().unwrap_or("unknown"))
                .or_default() += 1;
        }
        println!("  Sessions skipped: {}", skipped_results.len());
        for (reason, count) in &by_reason {
            println!("    - {reason}: {count}");
        }
    }
    println!("  Errors: {errors}");
    for r in results.iter().filter(|r| r.error.is_some()) {
        eprintln!("  ! {}: {}", r.session_id, r.error.as_deref().unwrap_or(""));
    }
    for r in &skipped_results {
        eprintln!(
            "  ~ skipped {}: {}",
            r.session_id,
            r.skip_reason.as_deref().unwrap_or("")
        );
    }
}
