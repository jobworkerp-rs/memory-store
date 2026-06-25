//! 3-phase commit for `finalize_generated_reflection`.
//!
//! Phase 1 (read-only, no tx)  — fetch origin thread / labels, decide
//!                                is_recurrence, locate previous active
//!                                reflection.
//! Phase 2 (idempotent, short tx) — retrieve-or-create the aggregate
//!                                reflection thread keyed on
//!                                (REFLECTION_USER_ID, sha256(labels)).
//! Phase 3 (main tx)              — insert memory + thread_memory junction
//!                                + sidecar + child rows + derived stats.
//!                                F-G3 idempotency rides on the
//!                                `memory.external_id` UNIQUE index: a
//!                                conflict rolls back this tx and we
//!                                surface the existing reflection_id.
//! Phase 4 (post-commit, fire-and-forget) — dispatch summary / intent
//!                                embedding workflows. Failures are
//!                                logged but never propagated; the
//!                                workflow eventually marks the embedding
//!                                status, and Redispatch* picks up
//!                                anything still in PENDING.

use anyhow::Result;
use protobuf::llm_memory::data::{ReflectionId, ThreadData, UserId};
use protobuf::llm_memory::service::FinalizeReflectionRequest;
use sha2::{Digest, Sha256};

use infra::error::LlmMemoryError;
use infra::infra::memory::rdb::MemoryRepository;
use infra::infra::reflection::aggregate_thread::ThreadAggregateKeyRepository;
use infra::infra::reflection::fact::ReflectionFactRepository;
use infra::infra::reflection::failure_mode::ReflectionFailureModeRepository;
use infra::infra::reflection::failure_mode_convert;
use infra::infra::reflection::rdb::ThreadReflectionIndexRepository;
use infra::infra::reflection::rows::ThreadReflectionIndexRow;
use infra::infra::reflection::stats::ReflectionStatsRepository;
use infra::infra::reflection::tool::ReflectionToolRepository;
use infra::infra::reflection::tool_outcome::ReflectionToolOutcomeRepository;
use infra::infra::thread::rdb::ThreadRepository;
use infra::infra::thread_label::rdb::ThreadLabelRepository;
use infra::infra::thread_memory::rdb::ThreadMemoryRepository;

use crate::app::REFLECTION_USER_ID;
use crate::app::reflection::ReflectionAppImpl;
use crate::app::reflection::build_memory_data::{ReflectionMemoryParts, build_parts};
use crate::app::reflection::validate;

/// Default 30-day window for F-G recurrence detection (spec §4.1).
/// Operators can override via `REFLECTION_RECURRENCE_WINDOW_DAYS`.
const DEFAULT_RECURRENCE_WINDOW_DAYS: i64 = 30;

const DAY_MILLIS: i64 = 86_400_000;

/// Aggregate-thread label that distinguishes reflection containers
/// from ordinary user threads sharing the same label set.
const REFLECTION_LABEL: &str = "reflection";

pub async fn finalize(
    app: &ReflectionAppImpl,
    req: &FinalizeReflectionRequest,
) -> Result<ReflectionId> {
    validate::validate_request(req)?;

    let parsed = req
        .parsed_output
        .as_ref()
        .expect("validate_request guards against parsed_output=None");
    let origin_thread_id = req
        .origin_thread_id
        .as_ref()
        .expect("validate_request guards against origin_thread_id=None")
        .value;

    let now = command_utils::util::datetime::now_millis();
    let recurrence_window = recurrence_window_millis();

    // ===== Phase 1: read-only =====
    let origin = app
        .thread_repo
        .find(&protobuf::llm_memory::data::ThreadId {
            value: origin_thread_id,
        })
        .await?
        .ok_or_else(|| {
            LlmMemoryError::NotFound(format!("origin thread {origin_thread_id} not found"))
        })?;
    let origin_data = origin
        .data
        .as_ref()
        .ok_or_else(|| LlmMemoryError::OtherError("origin thread missing data".into()))?;
    let origin_user_id = origin_data.user_id.as_ref().map(|u| u.value).unwrap_or(0);
    let origin_channel = origin_data.channel.clone();

    // The three remaining Phase 1 reads only depend on origin / req,
    // so they can run in parallel against the pool.
    let (origin_labels, is_recurrence, previous) = tokio::try_join!(
        app.thread_label_repo
            .find_labels_by_thread(origin_thread_id),
        app.index_repo.detect_recurrence(
            origin_user_id,
            parsed.task_category,
            &parsed.failure_modes,
            recurrence_window,
            now,
            None,
        ),
        app.index_repo
            .find_active_by_thread_id(origin_thread_id, false),
    )?;
    let previous_reflection_id = previous.as_ref().map(|p| p.memory_id);

    // ===== Phase 2: aggregate-thread retrieve-or-create =====
    let target_labels = compose_aggregate_labels(&origin_labels);
    let labels_hash = sha256_join_pipe(&target_labels);
    let aggregate_thread_id = resolve_or_create_aggregate_thread(
        app,
        REFLECTION_USER_ID,
        &target_labels,
        &labels_hash,
        now,
    )
    .await?;

    // ===== Phase 3: main tx =====
    // Concurrent finalize calls into the same aggregate thread can
    // race on `thread_memory.(thread_id, position)` because the
    // `INSERT ... SELECT MAX(position)+1 ...` runs against the
    // pre-commit snapshot under postgres MVCC: two parallel txs see
    // the same MAX and try to insert the same position. SQLite
    // serialises writers so the race is invisible there. We retry the
    // whole Phase 3 on UNIQUE violation — the second pass observes the
    // committed row from the winner and picks the next position.
    let parts = build_parts(req, parsed, origin_thread_id, now)?;

    const MAX_PHASE3_RETRIES: u32 = 3;
    let mut attempt = 0u32;
    let memory_id = loop {
        attempt += 1;
        let now_attempt = command_utils::util::datetime::now_millis();
        match run_phase3(
            app,
            req,
            parsed,
            &parts,
            aggregate_thread_id,
            origin_thread_id,
            origin_user_id,
            origin_channel.as_deref(),
            previous_reflection_id,
            is_recurrence,
            now_attempt,
        )
        .await
        {
            Ok(Phase3Outcome::Inserted(id)) => break id,
            Ok(Phase3Outcome::Existing(id)) => break id,
            Err(e) if is_unique_violation(&e) && attempt < MAX_PHASE3_RETRIES => {
                tracing::warn!(
                    target = "reflection.finalize",
                    attempt,
                    "phase 3 hit a UNIQUE violation (likely thread_memory.(thread_id, position) race); retrying",
                );
                continue;
            }
            Err(e) => return Err(e),
        }
    };

    // ===== Phase 4: fire-and-forget dispatch =====
    dispatch_embeddings(app, memory_id, &parsed.summary, &parsed.task_intent).await;

    Ok(ReflectionId { value: memory_id })
}

enum Phase3Outcome {
    Inserted(i64),
    Existing(i64),
}

#[allow(clippy::too_many_arguments)]
async fn run_phase3(
    app: &ReflectionAppImpl,
    req: &FinalizeReflectionRequest,
    parsed: &protobuf::llm_memory::data::ReflectionLlmOutput,
    parts: &ReflectionMemoryParts,
    aggregate_thread_id: i64,
    origin_thread_id: i64,
    origin_user_id: i64,
    origin_channel: Option<&str>,
    previous_reflection_id: Option<i64>,
    is_recurrence: bool,
    now: i64,
) -> Result<Phase3Outcome> {
    let ReflectionMemoryParts {
        memory_data,
        external_id,
        heuristic_score,
        score_self,
        score_resolved,
        ..
    } = parts;

    let mut tx = app.pool.begin().await?;

    // 3-a. Memory insert with external_id UNIQUE idempotency (F-G3).
    let memory_id = match app.memory_repo.create(&mut *tx, memory_data).await {
        Ok(id) => id.value,
        Err(e) => {
            // Roll back as soon as we know we won't proceed; sqlx state
            // would otherwise prevent further reads on the same conn.
            tx.rollback().await.ok();
            if is_unique_violation(&e) {
                let existing = app
                    .memory_repo
                    .find_by_external_id(external_id)
                    .await?
                    .ok_or_else(|| {
                        LlmMemoryError::OtherError(format!(
                            "external_id {external_id} reported UNIQUE violation but row \
                             not found in fallback select"
                        ))
                    })?;
                let id = existing
                    .id
                    .as_ref()
                    .ok_or_else(|| LlmMemoryError::OtherError("memory.id missing".into()))?
                    .value;
                return Ok(Phase3Outcome::Existing(id));
            }
            return Err(e);
        }
    };

    // 3-b. Aggregate-thread junction. The auto-position SELECT can race
    // under postgres (see retry note in `finalize`); the outer loop
    // re-runs Phase 3 on UNIQUE violation.
    if let Err(e) = app
        .thread_memory_repo
        .insert_auto_position_tx(&mut *tx, aggregate_thread_id, memory_id, now)
        .await
    {
        tx.rollback().await.ok();
        return Err(e);
    }

    // 3-c. Sidecar.
    let index_row = build_index_row(IndexRowInput {
        memory_id,
        aggregate_thread_id,
        origin_thread_id,
        origin_user_id,
        origin_channel,
        parsed,
        req,
        score_self: *score_self,
        heuristic_score: *heuristic_score,
        score_resolved: *score_resolved,
        previous_reflection_id,
        is_recurrence,
        now_millis: now,
    });
    if let Err(e) = app.index_repo.insert_index_tx(&mut *tx, &index_row).await {
        tx.rollback().await.ok();
        return Err(e);
    }

    // 3-d. Child rows. Dedup defends the `(memory_id, mode)` PRIMARY
    // KEY locally so a duplicate enum value fails fast here rather than
    // surfacing as an opaque DB error mid-transaction.
    let mut seen_modes = std::collections::HashSet::new();
    for mode_val in &parsed.failure_modes {
        let Some(db_name) = failure_mode_convert::db_name_from_i32(*mode_val) else {
            continue;
        };
        if !seen_modes.insert(db_name) {
            continue;
        }
        if let Err(e) = app
            .failure_mode_repo
            .insert_mode_tx(&mut *tx, memory_id, db_name)
            .await
        {
            tx.rollback().await.ok();
            return Err(e);
        }
    }
    for tool in &parsed.tools_used {
        if tool.is_empty() {
            continue;
        }
        if let Err(e) = app
            .tool_repo
            .insert_tool_tx(&mut *tx, memory_id, tool)
            .await
        {
            tx.rollback().await.ok();
            return Err(e);
        }
    }
    // tool_outcome_stats grain (spec §3.3.1 / F-A6 rebuild SQL):
    // grouped by `(origin_user_id, tool, outcome)` over the unique
    // `tools_used` set, not over `tool_outcomes`. We mirror that grain
    // here so `GetToolOutcomeStats` is invariant across a rebuild
    // boundary. tool_contribution_stats stays per-`tool_outcomes` row
    // because the rebuild SQL groups over `reflection_tool_outcome`.
    let mut counted_tools = std::collections::HashSet::with_capacity(parsed.tools_used.len());
    for tool in &parsed.tools_used {
        if tool.is_empty() {
            continue;
        }
        if counted_tools.insert(tool.as_str())
            && let Err(e) = app
                .stats_repo
                .upsert_tool_outcome_tx(&mut *tx, origin_user_id, tool, parsed.outcome, 1, now)
                .await
        {
            tx.rollback().await.ok();
            return Err(e);
        }
    }
    for outcome in &parsed.tool_outcomes {
        let error_kind = outcome.error_kind.as_deref().unwrap_or("");
        if let Err(e) = app
            .tool_outcome_repo
            .insert_outcome_tx(
                &mut *tx,
                memory_id,
                &outcome.tool,
                outcome.contribution,
                outcome.error_kind.as_deref(),
            )
            .await
        {
            tx.rollback().await.ok();
            return Err(e);
        }
        if let Err(e) = app
            .stats_repo
            .upsert_tool_contribution_tx(
                &mut *tx,
                origin_user_id,
                &outcome.tool,
                outcome.contribution,
                error_kind,
                1,
                now,
            )
            .await
        {
            tx.rollback().await.ok();
            return Err(e);
        }
    }
    for fact in &parsed.facts {
        // Validate already ensured anchor_memory_id is set.
        let anchor = fact.anchor_memory_id.as_ref().expect("validate guarantees");
        let links_json = if fact.links.is_empty() {
            None
        } else {
            Some(serialize_fact_links(&fact.links)?)
        };
        if let Err(e) = app
            .fact_repo
            .insert_fact_tx(
                &mut *tx,
                memory_id,
                anchor.value,
                fact.kind,
                fact.turn_index as i32,
                fact.weight.map(|w| w as f64),
                fact.note.as_deref(),
                links_json.as_deref(),
            )
            .await
        {
            tx.rollback().await.ok();
            return Err(e);
        }
    }

    tx.commit().await?;
    Ok(Phase3Outcome::Inserted(memory_id))
}

fn recurrence_window_millis() -> i64 {
    let days = std::env::var("REFLECTION_RECURRENCE_WINDOW_DAYS")
        .ok()
        .and_then(|s| s.trim().parse::<i64>().ok())
        .filter(|d| *d > 0)
        .unwrap_or(DEFAULT_RECURRENCE_WINDOW_DAYS);
    days.saturating_mul(DAY_MILLIS)
}

fn compose_aggregate_labels(origin_labels: &[String]) -> Vec<String> {
    let mut out: Vec<String> = origin_labels.to_vec();
    if !out.iter().any(|l| l == REFLECTION_LABEL) {
        out.push(REFLECTION_LABEL.to_string());
    }
    out.sort();
    out.dedup();
    out
}

fn sha256_join_pipe(labels: &[String]) -> String {
    use std::fmt::Write;
    let digest = Sha256::digest(labels.join("|").as_bytes());
    digest.iter().fold(String::with_capacity(64), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    })
}

/// Phase 2 implementation. Tries to find an existing aggregate thread
/// for `(user_id, labels_hash)`; on miss, creates a new thread + the
/// `thread_aggregate_key` mapping inside a short tx. UNIQUE collision
/// from a concurrent finalize falls back to SELECT — this is the
/// idempotency path.
async fn resolve_or_create_aggregate_thread(
    app: &ReflectionAppImpl,
    user_id: i64,
    target_labels: &[String],
    labels_hash: &str,
    now: i64,
) -> Result<i64> {
    if let Some(row) = app.aggregate_key_repo.find(user_id, labels_hash).await? {
        return Ok(row.thread_id);
    }

    // Create a fresh aggregate thread. ThreadData.labels is a hint for
    // higher-level APIs that surface the thread; the authoritative
    // label rows live in `thread_label`, populated below via
    // `add_labels_tx`.
    let thread_data = ThreadData {
        default_system_memory_id: None,
        user_id: Some(UserId { value: user_id }),
        description: Some("aggregate reflection thread".to_string()),
        channel: None,
        embedding: None,
        embedding_dim: None,
        created_at: now,
        updated_at: now,
        metadata: None,
        labels: target_labels.to_vec(),
    };

    let mut tx = app.pool.begin().await?;
    let new_thread_id = app.thread_repo.create(&mut *tx, &thread_data).await?;
    for label in target_labels {
        app.thread_label_repo
            .add_labels_tx(&mut *tx, new_thread_id.value, label, now)
            .await?;
    }
    let insert_res = app
        .aggregate_key_repo
        .insert_tx(&mut *tx, user_id, labels_hash, new_thread_id.value, now)
        .await;
    match insert_res {
        Ok(()) => {
            tx.commit().await?;
            Ok(new_thread_id.value)
        }
        Err(e) => {
            // Roll back the freshly-created thread; the existing entry
            // (winner of the race) stays untouched. Spec §4.2.2.1 calls
            // the rolled-back thread an orphan — but we always roll it
            // back here so search filters never surface a duplicate.
            tx.rollback().await.ok();
            if is_unique_violation(&e) {
                let row = app
                    .aggregate_key_repo
                    .find(user_id, labels_hash)
                    .await?
                    .ok_or_else(|| {
                        LlmMemoryError::OtherError(format!(
                            "aggregate_key UNIQUE violation but no row visible to fallback \
                             select (user_id={user_id})"
                        ))
                    })?;
                Ok(row.thread_id)
            } else {
                Err(e)
            }
        }
    }
}

struct IndexRowInput<'a> {
    memory_id: i64,
    aggregate_thread_id: i64,
    origin_thread_id: i64,
    origin_user_id: i64,
    origin_channel: Option<&'a str>,
    parsed: &'a protobuf::llm_memory::data::ReflectionLlmOutput,
    req: &'a FinalizeReflectionRequest,
    score_self: f32,
    heuristic_score: f32,
    score_resolved: f32,
    previous_reflection_id: Option<i64>,
    is_recurrence: bool,
    now_millis: i64,
}

fn build_index_row(input: IndexRowInput<'_>) -> ThreadReflectionIndexRow {
    use protobuf::llm_memory::data::{DatasetQuality, EmbeddingStatus};
    let IndexRowInput {
        memory_id,
        aggregate_thread_id,
        origin_thread_id,
        origin_user_id,
        origin_channel,
        parsed,
        req,
        score_self,
        heuristic_score,
        score_resolved,
        previous_reflection_id,
        is_recurrence,
        now_millis,
    } = input;
    ThreadReflectionIndexRow {
        memory_id,
        thread_id: aggregate_thread_id,
        origin_thread_id,
        origin_user_id,
        origin_channel: origin_channel.map(str::to_string),
        outcome: parsed.outcome,
        score: score_resolved as f64,
        score_self: score_self as f64,
        score_heuristic: heuristic_score as f64,
        task_category: parsed.task_category,
        reflection_aspect: parsed.reflection_aspect,
        dataset_quality: DatasetQuality::Auto as i32,
        summary_embedding_status: EmbeddingStatus::Pending as i32,
        summary_embedding_error: None,
        intent_embedding_status: EmbeddingStatus::Pending as i32,
        intent_embedding_error: None,
        prompt_version: req.prompt_version.clone(),
        target_model_version: req.target_model_version.clone(),
        experiment_id: req.experiment_id.clone(),
        experiment_variant: req.experiment_variant.clone(),
        previous_reflection_id,
        pinned: false,
        is_recurrence,
        // Workflow records the fingerprint via UpsertMitigationApplied
        // post-finalize; the sidecar leaves it null at insert time.
        mitigation_fingerprint: None,
        created_at: now_millis,
        updated_at: now_millis,
    }
}

fn serialize_fact_links(links: &[protobuf::llm_memory::data::FactLink]) -> Result<String> {
    use protobuf::llm_memory::data::FactLinkField;
    let arr: Vec<serde_json::Value> = links
        .iter()
        .map(|l| {
            let field = FactLinkField::try_from(l.field)
                .map(|f| f.as_str_name())
                .unwrap_or("FACT_LINK_FIELD_UNSPECIFIED");
            serde_json::json!({ "field": field, "index": l.index })
        })
        .collect();
    serde_json::to_string(&serde_json::Value::Array(arr))
        .map_err(|e| LlmMemoryError::OtherError(format!("links serialize: {e}")).into())
}

fn is_unique_violation(err: &anyhow::Error) -> bool {
    if matches!(
        err.downcast_ref::<LlmMemoryError>(),
        Some(LlmMemoryError::AlreadyExists(_))
    ) {
        return true;
    }
    if let Some(LlmMemoryError::DBError(db_err)) = err.downcast_ref::<LlmMemoryError>()
        && let sqlx::Error::Database(db) = db_err
        && matches!(db.kind(), sqlx::error::ErrorKind::UniqueViolation)
    {
        return true;
    }
    false
}

async fn dispatch_embeddings(
    app: &ReflectionAppImpl,
    memory_id: i64,
    summary: &str,
    task_intent: &str,
) {
    if let Some(d) = &app.summary_dispatcher {
        match d.dispatch(memory_id, summary).await {
            Ok(_) => {}
            Err(e) => tracing::warn!(
                "reflection summary dispatch failed for memory_id={memory_id}: {e:?}"
            ),
        }
    }
    if let Some(d) = &app.intent_dispatcher
        && !task_intent.is_empty()
    {
        match d.dispatch(memory_id, task_intent).await {
            Ok(_) => {}
            Err(e) => {
                tracing::warn!("reflection intent dispatch failed for memory_id={memory_id}: {e:?}")
            }
        }
    }
}
