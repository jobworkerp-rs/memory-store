//! Cache invariant: `ReflectionAppImpl` carries a shared
//! `memory_cache` handle (same Stretto instance `MemoryAppImpl`
//! holds) so a same-process `MemoryApp::find_memory` against a
//! deleted reflection cannot read the row back through the 30s TTL
//! window. The rest of `MemoryAppImpl::delete_memory`'s side effects
//! (advisory lock on `default_system_memory_id`, `memory_rating`
//! delete, `thread_memory` junction delete, `thread` detach) do not
//! apply to reflections (reflections are never default-system memos
//! and have no ratings), so the explicit cascade in this file plus
//! the shared cache invalidation gives the same end state as
//! routing through `MemoryApp::delete_memory` without entangling
//! `ReflectionAppImpl` with the larger `MemoryAppImpl` API.

use anyhow::Result;
use infra::error::LlmMemoryError;
use infra::infra::memory::rdb::MemoryRepository;
use infra::infra::reflection::applied_target::ReflectionAppliedTargetRepository;
use infra::infra::reflection::fact::ReflectionFactRepository;
use infra::infra::reflection::failure_mode::ReflectionFailureModeRepository;
use infra::infra::reflection::few_shot_usage::ReflectionFewShotUsageRepository;
use infra::infra::reflection::rdb::ThreadReflectionIndexRepository;
use infra::infra::reflection::tool::ReflectionToolRepository;
use infra::infra::reflection::tool_outcome::ReflectionToolOutcomeRepository;
use infra::infra::thread_memory::rdb::ThreadMemoryRepository;
use protobuf::llm_memory::data::{MemoryId, Reflection, ReflectionId};

use crate::app::reflection::ReflectionAppImpl;
use crate::app::reflection::search::hydrate_rows;

/// A memory row without a sidecar is treated as not-found here — the
/// caller asked for a reflection, not a raw memory.
pub async fn find(app: &ReflectionAppImpl, id: &ReflectionId) -> Result<Option<Reflection>> {
    let row = app.index_repo.find_by_memory_id(id.value).await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let mut hits = hydrate_rows(app, vec![row]).await?;
    Ok(hits.pop().and_then(|h| h.reflection))
}

/// `pinned` is the only writable field today; the other operational
/// flags have dedicated RPCs so they can carry per-write metadata
/// (mitigation_fingerprint, used_in_thread_id, embedding error_reason).
pub async fn update(
    app: &ReflectionAppImpl,
    id: &ReflectionId,
    pinned: Option<bool>,
) -> Result<()> {
    if let Some(p) = pinned {
        crate::app::reflection::ReflectionApp::pin(app, id, p).await?;
    }
    Ok(())
}

/// Cascade delete for one reflection.
///
/// The project intentionally omits FK constraints on every table
/// (`infra/sql/sqlite/003_reflection_schema.sql` header), so a bare
/// `memory.delete` would leave the sidecar, all six child tables, and
/// the `thread_memory` junction dangling. Search / aggregate would
/// then read those stale rows through `hydrate_rows` and either panic
/// on the missing memory or silently surface empty envelopes. To stay
/// consistent with the Phase D finalize commit (which inserts child
/// rows inside a single transaction), the cascade runs in one tx and
/// only commits once every owning table has been cleared.
///
/// Derived stats (`tool_outcome_stats`, `tool_contribution_stats`) are
/// `origin_user_id`-keyed and remain after delete on purpose: they are
/// rebuilt by `ReflectionApp::rebuild_derived_stats` (F-A6). The drift
/// is bounded — `count` is overstated by exactly the rows attributable
/// to the deleted reflection — and never produces a NotFound.
pub async fn delete(app: &ReflectionAppImpl, id: &ReflectionId) -> Result<()> {
    // Sidecar guard: `ReflectionId` and `MemoryId` are wire-compatible
    // (`{value: i64}`), so a request carrying a plain-memory id would
    // otherwise reach `memory_repo.delete` and destroy unrelated user
    // data. Refusing ids without a `thread_reflection_index` row keeps
    // this RPC scoped to reflections. The pre-tx lookup is sufficient
    // because reflection ids come from the snowflake generator and
    // cannot collide with an in-flight insert for the same id.
    if app.index_repo.find_by_memory_id(id.value).await?.is_none() {
        return Err(LlmMemoryError::NotFound(format!("reflection {} not found", id.value)).into());
    }

    let memory_id = id.value;
    let mut tx = app.pool.begin().await?;

    // Child tables first (they reference memory_id only, no ordering
    // between them is required, but we issue them in spec §3.2 order
    // for readability).
    app.failure_mode_repo
        .delete_by_memory_id_tx(&mut *tx, memory_id)
        .await?;
    app.tool_repo
        .delete_by_memory_id_tx(&mut *tx, memory_id)
        .await?;
    app.tool_outcome_repo
        .delete_by_memory_id_tx(&mut *tx, memory_id)
        .await?;
    app.fact_repo
        .delete_by_memory_id_tx(&mut *tx, memory_id)
        .await?;
    app.applied_target_repo
        .delete_by_memory_id_tx(&mut *tx, memory_id)
        .await?;
    app.few_shot_usage_repo
        .delete_by_memory_id_tx(&mut *tx, memory_id)
        .await?;

    // Sidecar (search / aggregate authoritative source).
    app.index_repo
        .delete_by_memory_id_tx(&mut *tx, memory_id)
        .await?;

    // Detach from the aggregate reflection thread; the thread row
    // itself stays so subsequent reflections under the same labels
    // reuse it via `thread_aggregate_key`.
    app.thread_memory_repo
        .delete_by_memory_tx(&mut *tx, memory_id)
        .await?;

    // Memory body last. By this point every dependent row is gone,
    // so a successful commit yields a fully consistent state.
    let mem_id = MemoryId { value: memory_id };
    let deleted = app.memory_repo.delete_tx(&mut *tx, &mem_id).await?;
    if !deleted {
        // The sidecar guard above passed, so the memory row should
        // exist. A missing row here means someone else deleted it
        // concurrently; treat the cascade as no-op and roll back the
        // child / sidecar / junction deletes to keep them in sync.
        tx.rollback().await?;
        return Err(LlmMemoryError::NotFound(format!("reflection {} not found", id.value)).into());
    }

    tx.commit().await?;

    // Invalidate the shared `memory_id:<id>` Stretto cache so a
    // same-process `MemoryApp::find_memory` against this id cannot
    // read the row back through the 30s TTL window. The handle is
    // optional (test wiring passes `None`) and `try_remove` is
    // best-effort: failures fall back to TTL expiry rather than
    // surfacing to the caller, because the underlying delete is
    // already committed.
    if let Some(cache) = app.memory_cache.as_ref() {
        let key = std::sync::Arc::new(super::super::memory_cache_key(&memory_id));
        if let Err(e) = cache.try_remove(&key).await {
            tracing::warn!(
                "memory cache invalidation failed for deleted reflection {}: {e}",
                memory_id
            );
        }
    }

    // LanceDB cascade for the intent embedding (best-effort, after the
    // RDB commit). The summary embedding lives in the shared
    // `memory_vector` table and is cleaned by the grpc handler the
    // same way `MemoryGrpcImpl::delete` does it — keeping that here
    // would force `ReflectionAppImpl` to carry a `MemoryVectorAppImpl`
    // field, which Phase E intentionally avoids. RDB is the source of
    // truth for both tracks, so an orphan LanceDB record is recovered
    // by the next `RebuildIntentIndex` / `rebuild_index` run.
    if let Some(intent_repo) = app.intent_vector_repo.as_ref()
        && let Err(e) = intent_repo.delete_by_memory_id(memory_id).await
    {
        tracing::error!(
            "LanceDB intent-vector cascade delete failed for reflection memory_id={}: {e}",
            memory_id
        );
    }

    Ok(())
}
