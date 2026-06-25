//! F-G11 — bulk re-dispatch of reflection embeddings.
//!
//! `kind=SUMMARY` re-runs the summary dispatcher (memory_vector
//! upsert), `kind=INTENT` re-runs the intent dispatcher
//! (reflection_intent_vector upsert), `kind=BOTH` runs both. The
//! filter narrows by origin_user_id / origin_thread_id /
//! prompt_version / *_embedding_status / period so operators can
//! pick a slice without touching healthy rows.
//!
//! Dispatch failures are counted but never propagated as errors —
//! the workflow YAML is responsible for marking
//! `MarkReflectionEmbeddingStatus(FAILED, …)` when its own retry
//! exhausts. This wrapper just kicks the dispatch back into the
//! pipeline.

use anyhow::Result;
use infra::error::LlmMemoryError;
use infra::infra::memory::rdb::MemoryRepository;
use infra::infra::reflection::rdb::ThreadReflectionIndexRepository;
use infra::infra::reflection::rows::{ReflectionSortKey, ResolvedReflectionSearchFilter};
use protobuf::llm_memory::data::{EmbeddingKind, ReflectionSearchFilter};
use protobuf::llm_memory::service::RedispatchReflectionEmbeddingsResponse;

use crate::app::reflection::ReflectionAppImpl;

const DEFAULT_BATCH: u32 = 100;
const MAX_BATCH: u32 = 1000;

pub async fn redispatch(
    app: &ReflectionAppImpl,
    kind: EmbeddingKind,
    filter: Option<&ReflectionSearchFilter>,
    batch_size: Option<u32>,
) -> Result<RedispatchReflectionEmbeddingsResponse> {
    if matches!(kind, EmbeddingKind::Unspecified) {
        return Err(LlmMemoryError::InvalidArgument(
            "RedispatchReflectionEmbeddings.kind must be SUMMARY / INTENT / BOTH".into(),
        )
        .into());
    }

    let started = std::time::Instant::now();
    let resolved = filter
        .map(super::search::resolve_filter)
        .unwrap_or_default();
    let limit = batch_size.unwrap_or(DEFAULT_BATCH).clamp(1, MAX_BATCH) as i64;

    let mut dispatched = 0u32;
    let mut skipped = 0u32;
    let mut failed = 0u32;

    // Without an explicit filter, narrow to non-OK rows so a careless
    // call does not re-dispatch every reflection in the system. Callers
    // may override by setting the corresponding filter fields. For
    // `kind=BOTH` with no status filter we run two queries (summary
    // PENDING / intent PENDING) and union the results so an
    // intent-only-pending row is not silently dropped.
    let rows = collect_rows(app, &resolved, kind, limit).await?;

    for row in rows {
        let memory = app
            .memory_repo
            .find(
                &protobuf::llm_memory::data::MemoryId {
                    value: row.memory_id,
                },
                false,
            )
            .await?;
        let Some(memory) = memory else {
            skipped += 1;
            continue;
        };
        let Some(memory_data) = memory.data.as_ref() else {
            skipped += 1;
            continue;
        };
        let summary = memory_data.content.as_str();
        let task_intent = extract_task_intent(memory_data.metadata.as_deref());

        let want_summary =
            matches!(kind, EmbeddingKind::Summary | EmbeddingKind::Both) && !summary.is_empty();
        let want_intent =
            matches!(kind, EmbeddingKind::Intent | EmbeddingKind::Both) && !task_intent.is_empty();

        match dispatch_one(
            app,
            row.memory_id,
            summary,
            &task_intent,
            want_summary,
            want_intent,
        )
        .await
        {
            Ok(true) => dispatched += 1,
            Ok(false) => skipped += 1,
            Err(_) => failed += 1,
        }
    }

    Ok(RedispatchReflectionEmbeddingsResponse {
        dispatched_count: dispatched,
        skipped_count: skipped,
        failed_count: failed,
        duration_ms: started.elapsed().as_millis() as i64,
    })
}

async fn dispatch_one(
    app: &ReflectionAppImpl,
    memory_id: i64,
    summary: &str,
    task_intent: &str,
    want_summary: bool,
    want_intent: bool,
) -> Result<bool> {
    let mut any = false;
    if want_summary && let Some(d) = &app.summary_dispatcher {
        match d.dispatch(memory_id, summary).await {
            Ok(_) => any = true,
            Err(e) => {
                tracing::warn!("redispatch summary failed for memory_id={memory_id}: {e:?}");
                return Err(
                    LlmMemoryError::OtherError(format!("summary dispatch failed: {e:?}")).into(),
                );
            }
        }
    }
    if want_intent && let Some(d) = &app.intent_dispatcher {
        match d.dispatch(memory_id, task_intent).await {
            Ok(_) => any = true,
            Err(e) => {
                tracing::warn!("redispatch intent failed for memory_id={memory_id}: {e:?}");
                return Err(
                    LlmMemoryError::OtherError(format!("intent dispatch failed: {e:?}")).into(),
                );
            }
        }
    }
    Ok(any)
}

/// Resolve the rows to redispatch with default narrowing applied.
///
/// SUMMARY / INTENT collapse to a single search with the matching
/// `*_embedding_status = PENDING` predicate added when the caller has
/// not pinned a status. BOTH with no status filter on either side runs
/// two queries (summary PENDING / intent PENDING) and unions the rows
/// so an intent-only-pending reflection is not dropped just because
/// summary already moved to OK. An explicit override (e.g. caller
/// pinned `summary_embedding_status=OK` to backfill for a new model)
/// short-circuits the union and goes through the single-query path
/// untouched.
async fn collect_rows(
    app: &ReflectionAppImpl,
    resolved: &ResolvedReflectionSearchFilter,
    kind: EmbeddingKind,
    limit: i64,
) -> Result<Vec<infra::infra::reflection::rows::ThreadReflectionIndexRow>> {
    use protobuf::llm_memory::data::EmbeddingStatus;
    let pending = EmbeddingStatus::Pending as i32;

    let needs_union = matches!(kind, EmbeddingKind::Both)
        && resolved.summary_embedding_status.is_none()
        && resolved.intent_embedding_status.is_none();

    if !needs_union {
        let mut filter = resolved.clone();
        match kind {
            EmbeddingKind::Summary if filter.summary_embedding_status.is_none() => {
                filter.summary_embedding_status = Some(pending);
            }
            EmbeddingKind::Intent if filter.intent_embedding_status.is_none() => {
                filter.intent_embedding_status = Some(pending);
            }
            _ => {}
        }
        return app
            .index_repo
            .search_index(&filter, ReflectionSortKey::CreatedAtDesc, limit, 0)
            .await;
    }

    let mut summary_filter = resolved.clone();
    summary_filter.summary_embedding_status = Some(pending);
    let mut intent_filter = resolved.clone();
    intent_filter.intent_embedding_status = Some(pending);

    let summary_rows = app
        .index_repo
        .search_index(&summary_filter, ReflectionSortKey::CreatedAtDesc, limit, 0)
        .await?;
    let intent_rows = app
        .index_repo
        .search_index(&intent_filter, ReflectionSortKey::CreatedAtDesc, limit, 0)
        .await?;

    // Union by memory_id. Each per-side query is already capped at
    // `limit` rows of newest-first results, so the union after sorting
    // and re-capping retains the newest `limit` distinct rows that hit
    // either pending side. Older rows past the cap surface in the next
    // batch run.
    let mut seen = std::collections::HashSet::with_capacity(summary_rows.len() + intent_rows.len());
    let mut merged = Vec::with_capacity(summary_rows.len() + intent_rows.len());
    for row in summary_rows.into_iter().chain(intent_rows) {
        if seen.insert(row.memory_id) {
            merged.push(row);
        }
    }
    merged.sort_by_key(|r| std::cmp::Reverse(r.created_at));
    if merged.len() as i64 > limit {
        merged.truncate(limit as usize);
    }
    Ok(merged)
}

fn extract_task_intent(metadata: Option<&str>) -> String {
    let Some(s) = metadata else {
        return String::new();
    };
    let Ok(v) = serde_json::from_str::<serde_json::Value>(s) else {
        return String::new();
    };
    v.pointer("/eval/task_intent")
        .and_then(|x| x.as_str())
        .unwrap_or("")
        .to_string()
}
