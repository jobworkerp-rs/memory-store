//! Runtime-state mutators for reflection sidecars.
//!
//! Spec §4.1 / §3.7: F-F2 (RecordAppliedTarget), F-F5 (RecordFewShotUsage),
//! F-F6 (UpsertMitigationApplied) plus their list-by-memory_id read
//! companions. F-F1 (Pin) and F-F8 (MarkEmbeddingStatus) live on the
//! trait root because they only need a single sidecar UPDATE; the rest
//! touch the dedicated child tables and benefit from being grouped here.

use anyhow::Result;
use infra::error::LlmMemoryError;
use infra::infra::reflection::applied_target::ReflectionAppliedTargetRepository;
use infra::infra::reflection::few_shot_usage::ReflectionFewShotUsageRepository;
use infra::infra::reflection::rdb::ThreadReflectionIndexRepository;
use infra::infra::reflection::rows::{ReflectionAppliedTargetRow, ReflectionFewShotUsageRow};
use protobuf::llm_memory::data::ReflectionId;

use crate::app::reflection::ReflectionAppImpl;

/// Existence guard shared by the F-F* runtime-state mutators. The
/// schema is FK-free per project policy, so the app layer must check
/// `thread_reflection_index` before INSERT-or-IGNORE-style child
/// writes — otherwise an unknown `reflection_id` silently produces
/// orphan rows in `reflection_applied_target` /
/// `reflection_few_shot_usage`. `mark_embedding_status` already gets
/// this for free via UPDATE affected-rows; the INSERT-side flows
/// need an explicit lookup.
async fn ensure_reflection_exists(app: &ReflectionAppImpl, id: &ReflectionId) -> Result<()> {
    use infra::infra::reflection::rdb::ThreadReflectionIndexRepository;
    if app.index_repo.find_by_memory_id(id.value).await?.is_none() {
        return Err(LlmMemoryError::NotFound(format!("reflection {} not found", id.value)).into());
    }
    Ok(())
}

/// F-F2 — `INSERT OR IGNORE` semantics: returns `true` when a new row
/// was added, `false` when the (memory_id, target) pair already existed.
pub async fn record_applied_target(
    app: &ReflectionAppImpl,
    id: &ReflectionId,
    target: String,
    fingerprint: Option<String>,
) -> Result<bool> {
    if target.is_empty() {
        return Err(LlmMemoryError::InvalidArgument("target must not be empty".into()).into());
    }
    ensure_reflection_exists(app, id).await?;
    let now = command_utils::util::datetime::now_millis();
    let mut tx = app.pool.begin().await?;
    let inserted = app
        .applied_target_repo
        .record_target_tx(&mut *tx, id.value, &target, fingerprint.as_deref(), now)
        .await?;
    tx.commit().await?;
    Ok(inserted)
}

/// F-F5 — record that this reflection was used as a few-shot example
/// inside `used_in_thread_id`. Idempotent on repeat calls.
pub async fn record_few_shot_usage(
    app: &ReflectionAppImpl,
    id: &ReflectionId,
    used_in_thread_id: i64,
) -> Result<bool> {
    ensure_reflection_exists(app, id).await?;
    let now = command_utils::util::datetime::now_millis();
    let mut tx = app.pool.begin().await?;
    let inserted = app
        .few_shot_usage_repo
        .record_usage_tx(&mut *tx, id.value, used_in_thread_id, now)
        .await?;
    tx.commit().await?;
    Ok(inserted)
}

/// F-F6 — fingerprint-aware upsert. Returns `true` on insert or
/// fingerprint change, `false` when the existing row already carries
/// the same fingerprint (= duplicate apply).
pub async fn upsert_mitigation_applied(
    app: &ReflectionAppImpl,
    id: &ReflectionId,
    target: String,
    fingerprint: String,
) -> Result<bool> {
    if target.is_empty() || fingerprint.is_empty() {
        return Err(LlmMemoryError::InvalidArgument(
            "target / fingerprint must not be empty".into(),
        )
        .into());
    }
    ensure_reflection_exists(app, id).await?;
    let now = command_utils::util::datetime::now_millis();
    let mut tx = app.pool.begin().await?;
    let applied = app
        .applied_target_repo
        .upsert_mitigation_tx(&mut *tx, id.value, &target, &fingerprint, now)
        .await?;
    // Mirror the fingerprint onto the sidecar so search/read paths
    // surface the apply. We only mirror when the child write actually
    // changed state (`applied=true`); a duplicate fingerprint must
    // leave the sidecar untouched so its `updated_at` does not bump
    // for no-ops. Spec §3.3.2 / §3.7.
    if applied {
        app.index_repo
            .update_mitigation_fingerprint_tx(&mut *tx, id.value, &fingerprint, now)
            .await?;
    }
    tx.commit().await?;
    Ok(applied)
}

pub async fn list_applied_targets(
    app: &ReflectionAppImpl,
    id: &ReflectionId,
) -> Result<Vec<ReflectionAppliedTargetRow>> {
    app.applied_target_repo.list_by_memory_id(id.value).await
}

pub async fn list_few_shot_usage(
    app: &ReflectionAppImpl,
    id: &ReflectionId,
) -> Result<Vec<ReflectionFewShotUsageRow>> {
    app.few_shot_usage_repo.list_by_memory_id(id.value).await
}
