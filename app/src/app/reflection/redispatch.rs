//! F-G11 — bulk re-dispatch of reflection embeddings.
//!
//! `kind=SUMMARY` is a deprecated compatibility path that applies the
//! reflection sidecar filter, then enqueues the generic memory text
//! embedding workflow. `kind=INTENT` remains the dedicated
//! reflection-intent path. `kind=BOTH` combines both only when the
//! caller supplies a non-empty sidecar filter.
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
use infra::infra::reflection::rows::{
    ReflectionSortKey, ResolvedReflectionSearchFilter, ThreadReflectionIndexRow,
};
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
    validate_filter_presence(kind, filter)?;
    let resolved = filter
        .map(super::search::resolve_filter)
        .unwrap_or_default();
    let limit = batch_size.unwrap_or(DEFAULT_BATCH).clamp(1, MAX_BATCH) as i64;

    let mut dispatched = 0u32;
    let mut skipped = 0u32;
    let mut failed = 0u32;

    let work_items = collect_work_items(app, &resolved, kind, limit).await?;

    for item in work_items {
        let memory = app
            .memory_repo
            .find(
                &protobuf::llm_memory::data::MemoryId {
                    value: item.row.memory_id,
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
        let search_document = memory_data.content.as_str();
        let task_intent = extract_task_intent(memory_data.metadata.as_deref());

        let want_summary = item.want_summary && !search_document.is_empty();
        let want_intent = item.want_intent && !task_intent.is_empty();

        match dispatch_one(
            app,
            item.row.memory_id,
            search_document,
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

#[derive(Debug)]
struct WorkItem {
    row: ThreadReflectionIndexRow,
    want_summary: bool,
    want_intent: bool,
}

async fn dispatch_one(
    app: &ReflectionAppImpl,
    memory_id: i64,
    search_document: &str,
    task_intent: &str,
    want_summary: bool,
    want_intent: bool,
) -> Result<bool> {
    let mut any = false;
    if want_summary && let Some(d) = &app.memory_embedding_dispatcher {
        match d.dispatch(memory_id, search_document).await {
            Ok(_) => any = true,
            Err(e) => {
                tracing::warn!(
                    "redispatch reflection search-document failed for memory_id={memory_id}: {e:?}"
                );
                return Err(
                    LlmMemoryError::OtherError(format!("text dispatch failed: {e:?}")).into(),
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

async fn collect_work_items(
    app: &ReflectionAppImpl,
    resolved: &ResolvedReflectionSearchFilter,
    kind: EmbeddingKind,
    limit: i64,
) -> Result<Vec<WorkItem>> {
    match kind {
        EmbeddingKind::Summary => Ok(scan_all_pages(app, resolved, limit)
            .await?
            .into_iter()
            .map(|row| WorkItem {
                row,
                want_summary: true,
                want_intent: false,
            })
            .collect()),
        EmbeddingKind::Intent => Ok(collect_intent_rows(app, resolved, limit)
            .await?
            .into_iter()
            .map(|row| WorkItem {
                row,
                want_summary: false,
                want_intent: true,
            })
            .collect()),
        EmbeddingKind::Both => collect_both_work_items(app, resolved, limit).await,
        EmbeddingKind::Unspecified => unreachable!("validated by caller"),
    }
}

async fn collect_both_work_items(
    app: &ReflectionAppImpl,
    resolved: &ResolvedReflectionSearchFilter,
    page_size: i64,
) -> Result<Vec<WorkItem>> {
    let summary_rows = scan_all_pages(app, resolved, page_size).await?;

    // When the caller pins `intent_embedding_status`, the intent scan
    // applies the exact same filter as the summary scan and returns an
    // identical row set. Reuse the summary rows instead of paging the
    // whole filtered set a second time.
    if resolved.intent_embedding_status.is_some() {
        let mut out: Vec<_> = summary_rows
            .into_iter()
            .map(|row| WorkItem {
                row,
                want_summary: true,
                want_intent: true,
            })
            .collect();
        out.sort_by_key(|item| std::cmp::Reverse(item.row.memory_id));
        return Ok(out);
    }

    let intent_rows = collect_intent_rows_all_pages(app, resolved, page_size).await?;

    let mut by_id: std::collections::BTreeMap<i64, WorkItem> = std::collections::BTreeMap::new();
    for row in summary_rows {
        by_id.insert(
            row.memory_id,
            WorkItem {
                row,
                want_summary: true,
                want_intent: false,
            },
        );
    }
    for row in intent_rows {
        by_id
            .entry(row.memory_id)
            .and_modify(|item| item.want_intent = true)
            .or_insert(WorkItem {
                row,
                want_summary: false,
                want_intent: true,
            });
    }
    let mut out: Vec<_> = by_id.into_values().collect();
    out.sort_by_key(|item| std::cmp::Reverse(item.row.memory_id));
    Ok(out)
}

async fn collect_intent_rows(
    app: &ReflectionAppImpl,
    resolved: &ResolvedReflectionSearchFilter,
    limit: i64,
) -> Result<Vec<ThreadReflectionIndexRow>> {
    let mut rows = collect_intent_rows_for_page(app, resolved, limit, None).await?;
    if rows.len() as i64 > limit {
        rows.truncate(limit as usize);
    }
    Ok(rows)
}

async fn collect_intent_rows_all_pages(
    app: &ReflectionAppImpl,
    resolved: &ResolvedReflectionSearchFilter,
    page_size: i64,
) -> Result<Vec<ThreadReflectionIndexRow>> {
    // A pinned `intent_embedding_status` uses the same filter as the
    // summary scan, so page it once. The sole caller already handles this
    // case, but keep the branch as a defensive fallback rather than a
    // panic so a future caller cannot trigger a redundant double scan.
    if resolved.intent_embedding_status.is_some() {
        return scan_all_pages(app, resolved, page_size).await;
    }

    let mut out = Vec::new();
    let mut cursor = None;
    loop {
        let page = collect_intent_rows_for_page(app, resolved, page_size, cursor).await?;
        if page.is_empty() {
            break;
        }
        cursor = page.last().map(|r| r.memory_id);
        out.extend(page);
    }
    Ok(out)
}

async fn collect_intent_rows_for_page(
    app: &ReflectionAppImpl,
    resolved: &ResolvedReflectionSearchFilter,
    limit: i64,
    cursor: Option<i64>,
) -> Result<Vec<ThreadReflectionIndexRow>> {
    use protobuf::llm_memory::data::EmbeddingStatus;

    if resolved.intent_embedding_status.is_some() {
        let mut filter = resolved.clone();
        filter.memory_id_lt = cursor;
        return app
            .index_repo
            .search_index(&filter, ReflectionSortKey::MemoryIdDesc, limit, 0)
            .await;
    }

    let mut pending_filter = resolved.clone();
    pending_filter.intent_embedding_status = Some(EmbeddingStatus::Pending as i32);
    pending_filter.memory_id_lt = cursor;
    let mut failed_filter = resolved.clone();
    failed_filter.intent_embedding_status = Some(EmbeddingStatus::Failed as i32);
    failed_filter.memory_id_lt = cursor;

    let pending_rows = app
        .index_repo
        .search_index(&pending_filter, ReflectionSortKey::MemoryIdDesc, limit, 0)
        .await?;
    let failed_rows = app
        .index_repo
        .search_index(&failed_filter, ReflectionSortKey::MemoryIdDesc, limit, 0)
        .await?;
    let mut by_id = std::collections::BTreeMap::new();
    for row in pending_rows.into_iter().chain(failed_rows) {
        by_id.insert(row.memory_id, row);
    }
    let mut merged: Vec<_> = by_id.into_values().collect();
    merged.sort_by_key(|row| std::cmp::Reverse(row.memory_id));
    if merged.len() as i64 > limit {
        merged.truncate(limit as usize);
    }
    Ok(merged)
}

async fn scan_all_pages(
    app: &ReflectionAppImpl,
    resolved: &ResolvedReflectionSearchFilter,
    page_size: i64,
) -> Result<Vec<ThreadReflectionIndexRow>> {
    let mut out = Vec::new();
    let mut cursor = None;
    loop {
        let mut filter = resolved.clone();
        filter.memory_id_lt = cursor;
        let page = app
            .index_repo
            .search_index(&filter, ReflectionSortKey::MemoryIdDesc, page_size, 0)
            .await?;
        if page.is_empty() {
            break;
        }
        cursor = page.last().map(|r| r.memory_id);
        out.extend(page);
    }
    Ok(out)
}

fn validate_filter_presence(
    kind: EmbeddingKind,
    filter: Option<&ReflectionSearchFilter>,
) -> Result<()> {
    if matches!(kind, EmbeddingKind::Summary | EmbeddingKind::Both)
        && filter.is_none_or(is_empty_filter)
    {
        return Err(LlmMemoryError::InvalidArgument(
            "RedispatchReflectionEmbeddings(kind=SUMMARY/BOTH) requires a non-empty reflection \
             filter (filter absent or empty filter is rejected); use \
             MemoryVectorService.RedispatchEmbeddings(user_id=300000, kinds=[TEXT]) for all \
             reflection search-document embeddings"
                .into(),
        )
        .into());
    }
    Ok(())
}

fn is_empty_filter(filter: &ReflectionSearchFilter) -> bool {
    filter.origin_user_id.is_none()
        && filter.origin_channel.as_deref().is_none_or(str::is_empty)
        && filter.outcomes.is_empty()
        && filter.score_min.is_none()
        && filter.score_max.is_none()
        && filter.task_categories.is_empty()
        && filter.reflection_aspect.is_none()
        && filter.failure_modes.is_empty()
        && filter.failure_modes_match_any.is_empty()
        && !has_non_empty_string(&filter.tools_used)
        && !has_non_empty_string(&filter.tools_used_match_any)
        && filter.prompt_version.as_deref().is_none_or(str::is_empty)
        && filter
            .target_model_version
            .as_deref()
            .is_none_or(str::is_empty)
        && filter.experiment_id.as_deref().is_none_or(str::is_empty)
        && filter
            .experiment_variant
            .as_deref()
            .is_none_or(str::is_empty)
        && filter.pinned.is_none()
        && filter.dataset_quality.is_none()
        && filter.summary_embedding_status.is_none()
        && filter.intent_embedding_status.is_none()
        && filter.created_after.is_none()
        && filter.created_before.is_none()
        && filter.origin_thread_id.is_none()
}

fn has_non_empty_string(values: &[String]) -> bool {
    values.iter().any(|v| !v.is_empty())
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
