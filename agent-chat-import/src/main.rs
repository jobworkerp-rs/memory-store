mod cli;
mod client;
mod common;
mod events;
mod generation_workers;
mod parser;
#[cfg(feature = "personality-after")]
mod personality;
mod source;
#[cfg(feature = "summarize-after")]
mod summarize;

use anyhow::{Result, anyhow};
use clap::Parser;
use cli::{Cli, CodexArgs, DEFAULT_PLAIN_SOURCE_NAME, GlobalArgs, OpenCodeArgs, PlainArgs, Subcmd};
use client::{ImportClient, LiveGrpcImportClient, LiveGrpcImportClientConfig, RetryPolicy};
use common::importer::{
    CanonicalSessionResult, ChunkLimits, run_all_with_entry_collector_and_event_sink,
    run_all_with_event_sink,
};
use events::{EventOutput, ImportCompletedReport, ImportCompletedSession, ImportSessionError};
use source::claude_code::ClaudeCodeSource;
use source::codex::CodexSource;
use source::opencode::OpenCodeSource;
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
    common::importer::set_thread_group_writes_enabled(!cli.global.no_thread_group_writes);
    if cli.global.dry_run_connect {
        report_connected_dry_run(&cli.global).await?;
    }

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
        Subcmd::OpenCode(args) => {
            run_opencode(&cli.global, args, user_id).await?;
        }
        Subcmd::Plain(args) => {
            run_plain(&cli.global, args, user_id).await?;
        }
    }

    if cli.global.dry_run_connect
        && let Some(report) = client::preview_report()
    {
        println!(
            "[dry-run-connect] planned sessions={} memories={} observations={} relations={} \
             pending={} suppressed_sessions={} conflict_sessions={}",
            report.sessions,
            report.planned_memories,
            report.planned_observations,
            report.planned_relations,
            report.pending,
            report.suppressed_sessions,
            report.conflict_sessions,
        );
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
    let cfg = live_client_config(global)?;
    let live = Arc::new(LiveGrpcImportClient::connect(cfg).await?);
    if global.dry_run_connect {
        client::set_preview_client(live.clone());
    }
    Ok(Some(live))
}

fn live_client_config(global: &GlobalArgs) -> Result<LiveGrpcImportClientConfig> {
    let server_url = global.server_url.clone().ok_or_else(|| {
        anyhow!("--server-url is required (use --dry-run to skip the live import)")
    })?;
    Ok(LiveGrpcImportClientConfig {
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
        preview_only: global.dry_run_connect,
    })
}

/// Connected dry-run: prove connectivity and print the server's current
/// ThreadGroup reconciliation snapshot. No import RPC is issued.
async fn report_connected_dry_run(global: &GlobalArgs) -> Result<()> {
    let live = LiveGrpcImportClient::connect(live_client_config(global)?).await?;
    let report = live.find_thread_group_reconciliation_report().await?;
    println!(
        "[dry-run-connect] active_groups={} redirected_groups={} split_groups={} \
         pending_candidates={} ambiguous_candidates={} conflict_candidates={} \
         unsupported_observations={}",
        report.active_groups,
        report.redirected_groups,
        report.split_groups,
        report.pending_candidates,
        report.ambiguous_candidates,
        report.conflict_candidates,
        report.unsupported_observations,
    );
    Ok(())
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
    run_canonical_source(global, user_id, "claude-code", ClaudeCodeSource::new(args)).await
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

async fn run_opencode(global: &GlobalArgs, args: OpenCodeArgs, user_id: i64) -> Result<()> {
    let since = global.since_millis()?;
    let source = OpenCodeSource::new(args, since)?;
    run_canonical_source(global, user_id, "opencode", source).await
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

    let event_output = EventOutput::new(global.events_jsonl, "plain", std::io::stdout());
    let results = run_all_with_entry_collector_and_event_sink(
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
        Some(&event_output),
    )
    .await?;
    print_canonical_summary(&display_label, &results);

    let errors: usize = results.iter().filter(|r| r.error.is_some()).count();

    let prune_outcome = emit_import_completed_before_prune(&event_output, &results, || async {
        // Phase A `--prune-missing`: only runs when explicitly requested,
        // not under dry-run, and the import had no errors. Anything else
        // surfaces as a Skip in the summary.
        if !prune_requested {
            Ok(PruneOutcome::Skipped(PruneSkipReason::NotRequested))
        } else if global.dry_run {
            eprintln!(
                "WARNING: --prune-missing is ignored under --dry-run; \
                 rerun without --dry-run to compute prune candidates."
            );
            Ok(PruneOutcome::Skipped(PruneSkipReason::DryRun))
        } else if errors > 0 {
            eprintln!(
                "WARNING: --prune-missing skipped because the import had {errors} session error(s)."
            );
            Ok(PruneOutcome::Skipped(PruneSkipReason::ImportHadErrors))
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
            .await
        }
    })
    .await?;
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
    let creator_prefix = format!("{}:{}:", cfg.source_name, cfg.user_id);
    let server_memories = client
        .find_memories_by_external_id_prefix(creator_prefix)
        .await?;
    let d_external_id = d_external_id
        .into_iter()
        .map(|external_id| {
            crate::common::importer::namespace_external_id(
                &cfg.source_name,
                cfg.user_id,
                &external_id,
            )
        })
        .collect::<HashSet<_>>();
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
    label: &'static str,
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
    let event_output = EventOutput::new(global.events_jsonl, label, std::io::stdout());
    let results = run_all_with_event_sink(
        &source,
        client.as_deref(),
        since_millis,
        since_millis_with_margin,
        user_id,
        &extra_labels,
        chunk_limits_from_global(global),
        Some(&event_output),
    )
    .await?;
    print_canonical_summary(&display_label, &results);

    let errors: usize = results.iter().filter(|r| r.error.is_some()).count();
    #[cfg(any(feature = "summarize-after", feature = "personality-after"))]
    let memories_imported: usize = results.iter().map(|r| r.memories_imported).sum();
    emit_import_completed(&event_output, &results)?;

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

#[derive(Debug, Default, Clone, Copy)]
struct ImportSummaryAggregate {
    sessions_processed: usize,
    threads_created: usize,
    memories_imported: usize,
    memories_skipped_duplicate: usize,
    memories_skipped_filtered: usize,
    memories_deferred: usize,
    memories_skipped_ignored: usize,
    memories_skipped_warning: usize,
    memories_rewired: usize,
    errors_count: usize,
}

fn aggregate_import_results(results: &[CanonicalSessionResult]) -> ImportSummaryAggregate {
    ImportSummaryAggregate {
        sessions_processed: results.len(),
        threads_created: results
            .iter()
            .filter(|result| result.thread_created)
            .count(),
        memories_imported: results.iter().map(|result| result.memories_imported).sum(),
        memories_skipped_duplicate: results
            .iter()
            .map(|result| result.memories_skipped_duplicate)
            .sum(),
        memories_skipped_filtered: results
            .iter()
            .map(|result| result.memories_skipped_filtered)
            .sum(),
        memories_deferred: results.iter().map(|result| result.memories_deferred).sum(),
        memories_skipped_ignored: results
            .iter()
            .map(|result| result.memories_skipped_ignored)
            .sum(),
        memories_skipped_warning: results
            .iter()
            .map(|result| result.memories_skipped_warning)
            .sum(),
        memories_rewired: results.iter().map(|result| result.memories_rewired).sum(),
        errors_count: results
            .iter()
            .filter(|result| result.error.is_some())
            .count(),
    }
}

fn print_canonical_summary(label: &str, results: &[CanonicalSessionResult]) {
    let summary = aggregate_import_results(results);
    let sessions = summary.sessions_processed;
    let threads_created = summary.threads_created;
    let imported = summary.memories_imported;
    let dup = summary.memories_skipped_duplicate;
    let filtered = summary.memories_skipped_filtered;
    let deferred = summary.memories_deferred;
    let ignored = summary.memories_skipped_ignored;
    let warning_skipped = summary.memories_skipped_warning;
    let rewired = summary.memories_rewired;
    let errors = summary.errors_count;
    let skipped_results: Vec<&CanonicalSessionResult> =
        results.iter().filter(|r| r.skip_reason.is_some()).collect();
    println!("\n{label} summary:");
    println!("  Sessions processed: {sessions}");
    println!("  Threads created: {threads_created}");
    println!("  Memories imported: {imported}");
    println!("  Memories skipped (duplicate): {dup}");
    println!("  Memories skipped (filtered): {filtered}");
    if deferred > 0 {
        println!("  Memories deferred: {deferred}");
    }
    if ignored > 0 {
        println!("  Memories skipped (ignored): {ignored}");
    }
    if warning_skipped > 0 {
        println!("  Memories skipped (warning): {warning_skipped}");
    }
    print_reason_summary("Ignored", results.iter().map(|r| &r.ignored_by_reason));
    print_reason_summary(
        "Warning exclusions",
        results.iter().map(|r| &r.warning_exclusions_by_reason),
    );
    print_reason_summary("Warnings", results.iter().map(|r| &r.warnings_by_reason));
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

/// Build the final event without changing the existing human-readable summary.
/// The event is informational for now; exit status remains governed by the
/// existing error checks until `import_completed` becomes the primary decision
/// path.
fn build_import_completed_report(results: &[CanonicalSessionResult]) -> ImportCompletedReport {
    let summary = aggregate_import_results(results);
    ImportCompletedReport {
        sessions_processed: summary.sessions_processed,
        threads_created: summary.threads_created,
        memories_imported: summary.memories_imported,
        memories_skipped_duplicate: summary.memories_skipped_duplicate,
        memories_skipped_ignored: summary.memories_skipped_ignored,
        errors_count: summary.errors_count,
        sessions: results
            .iter()
            .map(|result| ImportCompletedSession {
                session_key: result.session_key.clone(),
                status: if result.error.is_some() {
                    "failed".to_string()
                } else {
                    "completed".to_string()
                },
                imported_count: result.cumulative_imported_count(),
                error: result.error.as_ref().map(|message| ImportSessionError {
                    code: None,
                    message: message.clone(),
                }),
            })
            .collect(),
    }
}

fn emit_import_completed<W: std::io::Write>(
    event_output: &EventOutput<W>,
    results: &[CanonicalSessionResult],
) -> Result<()> {
    event_output
        .import_completed(&build_import_completed_report(results))
        .map_err(|error| anyhow!("failed to write import_completed event: {error}"))
}

async fn emit_import_completed_before_prune<W, F, Fut>(
    event_output: &EventOutput<W>,
    results: &[CanonicalSessionResult],
    prune: F,
) -> Result<PruneOutcome>
where
    W: std::io::Write,
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = Result<PruneOutcome>>,
{
    emit_import_completed(event_output, results)?;
    prune().await
}

fn print_reason_summary<'a, I>(label: &str, maps: I)
where
    I: Iterator<Item = &'a std::collections::BTreeMap<String, usize>>,
{
    let mut totals = std::collections::BTreeMap::<String, usize>::new();
    for map in maps {
        for (reason, count) in map {
            *totals.entry(reason.clone()).or_default() += count;
        }
    }
    if totals.is_empty() {
        return;
    }
    println!("  {label} by reason:");
    for (reason, count) in totals {
        println!("    - {reason}: {count}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn import_completed_report_aggregates_results_and_keeps_parse_errors() {
        let results = vec![
            CanonicalSessionResult {
                session_id: "ok".into(),
                session_key: Some("codex:ok".into()),
                thread_created: true,
                memories_imported: 2,
                memories_skipped_duplicate: 1,
                memories_skipped_ignored: 3,
                ..Default::default()
            },
            CanonicalSessionResult {
                session_id: "parse-error".into(),
                error: Some("invalid JSON".into()),
                ..Default::default()
            },
        ];

        let report = build_import_completed_report(&results);

        assert_eq!(report.sessions_processed, 2);
        assert_eq!(report.threads_created, 1);
        assert_eq!(report.memories_imported, 2);
        assert_eq!(report.memories_skipped_duplicate, 1);
        assert_eq!(report.memories_skipped_ignored, 3);
        assert_eq!(report.errors_count, 1);
        assert_eq!(report.sessions[0].status, "completed");
        assert_eq!(report.sessions[0].imported_count, 3);
        assert_eq!(report.sessions[1].session_key, None);
        assert_eq!(report.sessions[1].status, "failed");
        assert_eq!(
            report.sessions[1]
                .error
                .as_ref()
                .map(|error| error.message.as_str()),
            Some("invalid JSON")
        );
    }

    #[test]
    fn import_summary_and_completion_report_keep_aggregate_counts_in_parity() {
        let results = vec![
            CanonicalSessionResult {
                thread_created: true,
                memories_imported: 4,
                memories_skipped_duplicate: 2,
                memories_skipped_ignored: 5,
                ..Default::default()
            },
            CanonicalSessionResult {
                thread_created: true,
                error: Some("chunk failed".into()),
                ..Default::default()
            },
            CanonicalSessionResult {
                ..Default::default()
            },
        ];

        let summary_aggregates = (
            results.len(),
            results
                .iter()
                .filter(|result| result.thread_created)
                .count(),
            results.iter().map(|result| result.memories_imported).sum(),
            results
                .iter()
                .map(|result| result.memories_skipped_duplicate)
                .sum(),
            results
                .iter()
                .map(|result| result.memories_skipped_ignored)
                .sum(),
            results
                .iter()
                .filter(|result| result.error.is_some())
                .count(),
        );
        let report = build_import_completed_report(&results);
        let report_aggregates = (
            report.sessions_processed,
            report.threads_created,
            report.memories_imported,
            report.memories_skipped_duplicate,
            report.memories_skipped_ignored,
            report.errors_count,
        );

        assert_eq!(summary_aggregates, (3, 2, 4, 2, 5, 1));
        assert_eq!(report_aggregates, summary_aggregates);
    }

    #[test]
    fn parse_error_execution_emits_import_completed_with_null_session_key() {
        let output = EventOutput::new(true, "codex", Vec::new());
        let results = vec![CanonicalSessionResult {
            session_id: "unreadable.jsonl".into(),
            error: Some("failed to parse session".into()),
            ..Default::default()
        }];

        emit_import_completed(&output, &results).unwrap();

        let event: serde_json::Value =
            serde_json::from_str(String::from_utf8(output.into_inner()).unwrap().trim()).unwrap();
        assert_eq!(event["event"], "import_completed");
        assert_eq!(event["errors_count"], 1);
        assert_eq!(event["success"], false);
        assert_eq!(event["sessions"][0]["session_key"], serde_json::Value::Null);
        assert_eq!(event["sessions"][0]["status"], "failed");
    }

    #[tokio::test]
    async fn plain_import_completed_event_is_emitted_before_prune_starts() {
        use std::io::Write;
        use std::sync::{Arc, Mutex};

        #[derive(Clone)]
        struct SharedWriter(Arc<Mutex<Vec<u8>>>);

        impl Write for SharedWriter {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(bytes);
                Ok(bytes.len())
            }

            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let bytes = Arc::new(Mutex::new(Vec::new()));
        let output = EventOutput::new(true, "plain", SharedWriter(bytes.clone()));

        emit_import_completed_before_prune(&output, &[], || async {
            let output = String::from_utf8(bytes.lock().unwrap().clone()).unwrap();
            assert!(output.contains("\"event\":\"import_completed\""));
            Ok(PruneOutcome::Skipped(PruneSkipReason::NothingToPrune))
        })
        .await
        .unwrap();
    }
}
