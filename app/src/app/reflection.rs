//! `ReflectionApp` — application layer for the thread-reflection
//! subsystem. The trait surface is split across submodules so each
//! responsibility (finalize / search / aggregate / runtime_flags /
//! redispatch / export / distance) can be reviewed in isolation. This
//! file owns the trait and the `ReflectionAppImpl` struct only.

pub mod aggregate;
pub mod build_memory_data;
pub mod crud;
pub mod distance;
pub mod export;
pub mod finalize;
pub mod generate;
pub mod redispatch;
pub mod runtime_flags;
pub mod search;
pub mod validate;

#[cfg(test)]
mod tests;

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;
use infra::infra::reflection::rows::{ReflectionAppliedTargetRow, ReflectionFewShotUsageRow};
use protobuf::llm_memory::data::{
    EmbeddingKind, EmbeddingStatus, EmbeddingVector, FailureSignatureIndicators,
    HybridSearchOptions, Memory, Reflection, ReflectionId, ReflectionSearchFilter,
    ReflectionSearchResult, ThreadId,
};
use protobuf::llm_memory::service::{
    AggregateFailureModesResponse, AggregateLessonsResponse, AggregateScoresGroupBy,
    AggregateScoresResponse, AggregateToolContributionsResponse, FinalizeReflectionRequest,
    GenerateForThreadRequest, GenerateForThreadResponse, RebuildDerivedStatsResponse,
    RedispatchReflectionEmbeddingsResponse, ReflectionListSort, ToolContributionStatsResponse,
    ToolOutcomeStatsResponse,
};
use stretto::AsyncCache;

use infra::infra::memory::rdb::MemoryRepositoryImpl;
use infra::infra::reflection::aggregate_thread::ThreadAggregateKeyRepositoryImpl;
use infra::infra::reflection::applied_target::ReflectionAppliedTargetRepositoryImpl;
use infra::infra::reflection::dictionary::FailureModeDictionaryRepositoryImpl;
use infra::infra::reflection::fact::ReflectionFactRepositoryImpl;
use infra::infra::reflection::failure_mode::ReflectionFailureModeRepositoryImpl;
use infra::infra::reflection::few_shot_usage::ReflectionFewShotUsageRepositoryImpl;
use infra::infra::reflection::rdb::ThreadReflectionIndexRepositoryImpl;
use infra::infra::reflection::signature_norm::FailureSignatureIndicatorNormRepositoryImpl;
use infra::infra::reflection::stats::ReflectionStatsRepositoryImpl;
use infra::infra::reflection::tool::ReflectionToolRepositoryImpl;
use infra::infra::reflection::tool_outcome::ReflectionToolOutcomeRepositoryImpl;
use infra::infra::thread::rdb::ThreadRepositoryImpl;
use infra::infra::thread_label::rdb::ThreadLabelRepositoryImpl;
use infra::infra::thread_memory::rdb::ThreadMemoryRepositoryImpl;
use infra_utils::infra::rdb::RdbPool;

use infra::infra::reflection_intent_dispatch::ReflectionIntentDispatcher;
use infra::infra::reflection_intent_vector::repository::ReflectionIntentVectorRepository;
use infra::infra::reflection_summary_dispatch::ReflectionSummaryDispatcher;
use jobworkerp_client::client::wrapper::JobworkerpClientWrapper;

use self::distance::NormTable;

/// Application-layer trait. Implementations must be `Send + Sync` so
/// gRPC handlers can hold them inside `Arc`. Methods take `&self` so
/// they can be invoked concurrently from multiple tonic worker tasks.
#[async_trait]
pub trait ReflectionApp: Send + Sync {
    // ===== Generate / Finalize =====

    /// F-G1 — enqueue or run the thread-reflection workflow via
    /// jobworkerp. Returns `Unimplemented` when the dispatch kill
    /// switch (`MEMORY_REFLECTION_DISPATCH_ENABLED`) is off.
    async fn generate(&self, req: &GenerateForThreadRequest) -> Result<GenerateForThreadResponse>;

    /// Spec §4.2.2 / §9.1 fixpoint #1: persist a finalized reflection
    /// produced by the thread-reflection workflow. See
    /// `finalize.rs` for the 3-phase commit detail.
    async fn finalize_generated_reflection(
        &self,
        req: &FinalizeReflectionRequest,
    ) -> Result<ReflectionId>;

    // ===== CRUD =====

    /// Find one reflection by id. Returns `None` when no sidecar row
    /// exists for that memory_id.
    async fn find(&self, id: &ReflectionId) -> Result<Option<Reflection>>;

    /// Update writable operational fields. Currently only `pinned` is
    /// writable through this surface; other fields (applied_target,
    /// few_shot_usage, embedding_status) use dedicated RPCs.
    async fn update(&self, id: &ReflectionId, pinned: Option<bool>) -> Result<()>;

    /// Delete the reflection memory; cascades to sidecar + child tables.
    async fn delete(&self, id: &ReflectionId) -> Result<()>;

    /// F-F1 — toggle the `pinned` operational flag on the sidecar.
    async fn pin(&self, id: &ReflectionId, pinned: bool) -> Result<()>;

    /// F-F8 — update the SUMMARY or INTENT embedding status. BOTH is
    /// rejected (BOTH is a redispatch-only selector).
    async fn mark_embedding_status(
        &self,
        id: &ReflectionId,
        kind: EmbeddingKind,
        status: EmbeddingStatus,
        error_reason: Option<String>,
    ) -> Result<()>;

    // ===== Search =====

    async fn search(
        &self,
        filter: Option<&ReflectionSearchFilter>,
        sort: ReflectionListSort,
        limit: Option<u32>,
        offset: Option<i64>,
    ) -> Result<Vec<ReflectionSearchResult>>;

    /// F-S1 — hybrid search wrapper. At least one of `query_text` or
    /// `query_vectors` must be non-empty; for the filter-only case
    /// callers go through `search` directly. `boost_pinned_high_score`
    /// applies the F-S6 post-rank lift; `hybrid_options.strategy` is
    /// honoured for `RRF` / `WEIGHTED`, while the `*_THEN_*` variants
    /// return `Unimplemented` because reflection has no FTS index.
    #[allow(clippy::too_many_arguments)]
    async fn search_hybrid(
        &self,
        filter: Option<&ReflectionSearchFilter>,
        query_text: Option<&str>,
        query_vectors: &[EmbeddingVector],
        hybrid_options: Option<&HybridSearchOptions>,
        boost_pinned_high_score: bool,
        sort: ReflectionListSort,
        limit: Option<u32>,
    ) -> Result<Vec<ReflectionSearchResult>>;

    async fn find_by_thread(
        &self,
        thread_id: &ThreadId,
        include_history: bool,
    ) -> Result<Vec<Reflection>>;

    /// F-S7 — failure-signature distance ranking. Returns
    /// `(hits, is_truncated, scanned_count)` so the RPC adapter can
    /// surface the spec §4.2.4 truncation signal through the unary
    /// `MatchSignaturesResponse`.
    async fn match_failure_signatures(
        &self,
        indicators: &FailureSignatureIndicators,
        pattern_type: Option<i32>,
        top_k: u32,
        filter: Option<&ReflectionSearchFilter>,
    ) -> Result<(Vec<ReflectionSearchResult>, bool, u32)>;

    /// F-S3 — find reflections whose intent embedding is similar to
    /// the embedding of a reference thread or reference reflection.
    /// Returns `Unimplemented` when the intent vector repo is not
    /// configured (set `MEMORY_VECTOR_ENABLED=true` and
    /// `REFLECTION_LANCEDB_URI`).
    async fn find_similar_trajectories(
        &self,
        reference: TrajectoryReference,
        top_k: u32,
        filter: Option<&ReflectionSearchFilter>,
    ) -> Result<Vec<ReflectionSearchResult>>;

    /// F-S8 — same surface as F-S3 but the seed is a free-form intent
    /// text; the call must synchronously embed the text before issuing
    /// the LanceDB query.
    async fn find_similar_by_intent_text(
        &self,
        intent_text: &str,
        top_k: u32,
        filter: Option<&ReflectionSearchFilter>,
    ) -> Result<Vec<ReflectionSearchResult>>;

    // ===== Aggregate =====

    async fn aggregate_failure_modes(
        &self,
        filter: Option<&ReflectionSearchFilter>,
        include_co_occurrence: bool,
    ) -> Result<AggregateFailureModesResponse>;

    /// F-A2 — score aggregates with dynamic GROUP BY across 11 axes
    /// (incl. TIME_BUCKET). Returns count / avg / min / max / p50 /
    /// p95 per bucket; the percentile path is dual-backend (Postgres
    /// uses `percentile_cont`, SQLite picks the nearest-rank in app
    /// code).
    async fn aggregate_scores(
        &self,
        filter: Option<&ReflectionSearchFilter>,
        group_by: &[AggregateScoresGroupBy],
        time_bucket_seconds: Option<u32>,
    ) -> Result<AggregateScoresResponse>;

    /// F-A3 — token-frequency histogram over `metadata.eval.lessons`.
    async fn aggregate_lessons(
        &self,
        filter: Option<&ReflectionSearchFilter>,
        top_n: Option<u32>,
    ) -> Result<AggregateLessonsResponse>;

    /// Tool-contribution pivot over `tool_contribution_stats`.
    async fn aggregate_tool_contributions(
        &self,
        filter: Option<&ReflectionSearchFilter>,
        group_by_tool: bool,
        group_by_contribution: bool,
        group_by_error_kind: bool,
        group_by_origin_user: bool,
    ) -> Result<AggregateToolContributionsResponse>;

    async fn get_tool_outcome_stats(
        &self,
        origin_user_id: i64,
        tool: &str,
    ) -> Result<ToolOutcomeStatsResponse>;

    async fn get_tool_contribution_stats(
        &self,
        origin_user_id: i64,
        tool: &str,
    ) -> Result<ToolContributionStatsResponse>;

    async fn rebuild_derived_stats(
        &self,
        origin_user_id: Option<i64>,
        rebuild_tool_outcome: bool,
        rebuild_tool_contribution: bool,
    ) -> Result<RebuildDerivedStatsResponse>;

    // ===== Runtime flags (F-F2 / F-F5 / F-F6 + read companions) =====

    async fn record_applied_target(
        &self,
        id: &ReflectionId,
        target: String,
        fingerprint: Option<String>,
    ) -> Result<bool>;

    async fn record_few_shot_usage(
        &self,
        id: &ReflectionId,
        used_in_thread_id: i64,
    ) -> Result<bool>;

    async fn upsert_mitigation_applied(
        &self,
        id: &ReflectionId,
        target: String,
        fingerprint: String,
    ) -> Result<bool>;

    async fn list_applied_targets(
        &self,
        id: &ReflectionId,
    ) -> Result<Vec<ReflectionAppliedTargetRow>>;

    async fn list_few_shot_usage(
        &self,
        id: &ReflectionId,
    ) -> Result<Vec<ReflectionFewShotUsageRow>>;

    // ===== Export =====

    async fn export(
        &self,
        filter: Option<&ReflectionSearchFilter>,
        cursor_after_memory_id: Option<i64>,
        batch_size: Option<u32>,
    ) -> Result<Vec<Reflection>>;

    // ===== Vector ops (intent track) =====

    /// N-row upsert of a reflection's intent embedding chunks into the
    /// reflection intent LanceDB table. Replaces the reflection's rows for
    /// `replace_kinds` (text) then inserts the new chunk rows. An empty
    /// `rows` set is a valid stale-delete. Returns `(success_count,
    /// failure_count, errors)`; `Unimplemented` when the intent vector
    /// repo is not configured.
    async fn batch_upsert_intent_embeddings_rows(
        &self,
        id: &ReflectionId,
        embedding_model: Option<&str>,
        replace_kinds: &[String],
        rows: Vec<(String, i32, i32, i32, String, Vec<f32>)>,
    ) -> Result<(u32, u32, Vec<String>)>;

    // ===== Redispatch (F-G11) =====

    async fn redispatch_reflection_embeddings(
        &self,
        kind: EmbeddingKind,
        filter: Option<&ReflectionSearchFilter>,
        batch_size: Option<u32>,
    ) -> Result<RedispatchReflectionEmbeddingsResponse>;
}

/// Reference for F-S3 (`FindSimilarTrajectories`): either a thread or
/// an existing reflection. Defined here so the proto `oneof` can be
/// flattened into the app-layer surface without leaking the protobuf
/// `Reference` enum (which lives inside `FindSimilarRequest`).
#[derive(Debug, Clone)]
pub enum TrajectoryReference {
    Thread(ThreadId),
    Reflection(ReflectionId),
}

/// DI struct held by `AppModule`. Owns one repository handle per table
/// touched by `ReflectionApp`. The dispatcher fields are `Option` because
/// they depend on `MEMORY_VECTOR_ENABLED` /
/// `MEMORY_REFLECTION_DISPATCH_ENABLED` env knobs; the rest of the app
/// surface stays usable against an RDB-only deployment.
pub struct ReflectionAppImpl {
    pub(crate) pool: &'static RdbPool,
    pub(crate) memory_repo: MemoryRepositoryImpl,
    pub(crate) thread_repo: ThreadRepositoryImpl,
    pub(crate) thread_memory_repo: ThreadMemoryRepositoryImpl,
    pub(crate) thread_label_repo: ThreadLabelRepositoryImpl,
    pub(crate) aggregate_key_repo: ThreadAggregateKeyRepositoryImpl,
    pub(crate) index_repo: ThreadReflectionIndexRepositoryImpl,
    pub(crate) failure_mode_repo: ReflectionFailureModeRepositoryImpl,
    pub(crate) tool_repo: ReflectionToolRepositoryImpl,
    pub(crate) tool_outcome_repo: ReflectionToolOutcomeRepositoryImpl,
    pub(crate) fact_repo: ReflectionFactRepositoryImpl,
    pub(crate) applied_target_repo: ReflectionAppliedTargetRepositoryImpl,
    pub(crate) few_shot_usage_repo: ReflectionFewShotUsageRepositoryImpl,
    pub(crate) stats_repo: ReflectionStatsRepositoryImpl,
    /// failure_mode_dictionary is referenced at workflow-validate time
    /// only; kept here so wiring stays in one place when that path
    /// lands.
    #[allow(dead_code)]
    pub(crate) dictionary_repo: FailureModeDictionaryRepositoryImpl,
    /// Failure-signature norms loaded once at construction time. The
    /// repo handle is retained for a future live-reload helper (spec
    /// §9.3); current callers read through `norm_table`.
    #[allow(dead_code)]
    pub(crate) signature_norm_repo: FailureSignatureIndicatorNormRepositoryImpl,
    pub(crate) norm_table: NormTable,

    /// LanceDB intent-vector store consumed by F-S3 / F-S8 and
    /// `upsert_intent_embedding`.
    pub(crate) intent_vector_repo: Option<ReflectionIntentVectorRepository>,
    pub(crate) summary_dispatcher: Option<Arc<ReflectionSummaryDispatcher>>,
    pub(crate) intent_dispatcher: Option<Arc<ReflectionIntentDispatcher>>,
    /// jobworkerp client used by F-G1 `Generate` (enqueue the
    /// language-specific `memories-thread-reflection-single-<lang>`
    /// workflow) and F-S8
    /// `FindSimilarByIntentText` (synchronously embed the query text
    /// through the shared embedding worker). `None` is the
    /// kill-switched state (`MEMORY_REFLECTION_DISPATCH_ENABLED=false`):
    /// both call sites detect it and return `Unimplemented`,
    /// mirroring the dispatcher `Option` fields above.
    pub(crate) jobworkerp_client: Option<Arc<JobworkerpClientWrapper>>,

    /// Shared `memory_id:<id>` Stretto cache (the same instance
    /// `MemoryAppImpl` owns). `ReflectionAppImpl::delete` invalidates
    /// the entry for the deleted reflection memory_id so a same-process
    /// `MemoryApp::find_memory` cannot read the row back from the 30s
    /// TTL window. Optional so test wiring that does not exercise
    /// cross-app cache invariants can pass `None`.
    pub(crate) memory_cache: Option<AsyncCache<Arc<String>, Memory>>,
}

impl ReflectionAppImpl {
    /// The signature-norm HashMap is loaded eagerly via the supplied
    /// `signature_norm_repo` so the F-S7 distance hot path stays
    /// allocation-free. A failed load surfaces as `tracing::warn!` and
    /// leaves the table empty; F-S7 then returns 0 distance for every
    /// indicator until the row set is repopulated and the process is
    /// restarted.
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        pool: &'static RdbPool,
        memory_repo: MemoryRepositoryImpl,
        thread_repo: ThreadRepositoryImpl,
        thread_memory_repo: ThreadMemoryRepositoryImpl,
        thread_label_repo: ThreadLabelRepositoryImpl,
        aggregate_key_repo: ThreadAggregateKeyRepositoryImpl,
        index_repo: ThreadReflectionIndexRepositoryImpl,
        failure_mode_repo: ReflectionFailureModeRepositoryImpl,
        tool_repo: ReflectionToolRepositoryImpl,
        tool_outcome_repo: ReflectionToolOutcomeRepositoryImpl,
        fact_repo: ReflectionFactRepositoryImpl,
        applied_target_repo: ReflectionAppliedTargetRepositoryImpl,
        few_shot_usage_repo: ReflectionFewShotUsageRepositoryImpl,
        stats_repo: ReflectionStatsRepositoryImpl,
        dictionary_repo: FailureModeDictionaryRepositoryImpl,
        signature_norm_repo: FailureSignatureIndicatorNormRepositoryImpl,
        intent_vector_repo: Option<ReflectionIntentVectorRepository>,
        summary_dispatcher: Option<Arc<ReflectionSummaryDispatcher>>,
        intent_dispatcher: Option<Arc<ReflectionIntentDispatcher>>,
        jobworkerp_client: Option<Arc<JobworkerpClientWrapper>>,
        memory_cache: Option<AsyncCache<Arc<String>, Memory>>,
    ) -> Self {
        let norm_table = load_norm_table(&signature_norm_repo).await;
        Self {
            pool,
            memory_repo,
            thread_repo,
            thread_memory_repo,
            thread_label_repo,
            aggregate_key_repo,
            index_repo,
            failure_mode_repo,
            tool_repo,
            tool_outcome_repo,
            fact_repo,
            applied_target_repo,
            few_shot_usage_repo,
            stats_repo,
            dictionary_repo,
            signature_norm_repo,
            norm_table,
            intent_vector_repo,
            summary_dispatcher,
            intent_dispatcher,
            jobworkerp_client,
            memory_cache,
        }
    }
}

async fn load_norm_table(repo: &FailureSignatureIndicatorNormRepositoryImpl) -> NormTable {
    use infra::infra::reflection::signature_norm::FailureSignatureIndicatorNormRepository;
    match repo.list_all().await {
        Ok(rows) => {
            let mut map: NormTable = NormTable::with_capacity(rows.len());
            for row in rows {
                map.insert(
                    row.indicator_name,
                    (row.max_value as f32, row.weight as f32),
                );
            }
            map
        }
        Err(e) => {
            tracing::warn!(
                "failure_signature_indicator_norm load failed; F-S7 distance will return 0 \
                 for every indicator until restart: {e:#}"
            );
            NormTable::new()
        }
    }
}

#[async_trait]
impl ReflectionApp for ReflectionAppImpl {
    async fn generate(&self, req: &GenerateForThreadRequest) -> Result<GenerateForThreadResponse> {
        generate::generate(self, req).await
    }

    async fn finalize_generated_reflection(
        &self,
        req: &FinalizeReflectionRequest,
    ) -> Result<ReflectionId> {
        finalize::finalize(self, req).await
    }

    async fn find(&self, id: &ReflectionId) -> Result<Option<Reflection>> {
        crud::find(self, id).await
    }

    async fn update(&self, id: &ReflectionId, pinned: Option<bool>) -> Result<()> {
        crud::update(self, id, pinned).await
    }

    async fn delete(&self, id: &ReflectionId) -> Result<()> {
        crud::delete(self, id).await
    }

    async fn pin(&self, id: &ReflectionId, pinned: bool) -> Result<()> {
        use infra::infra::reflection::rdb::ThreadReflectionIndexRepository;
        let mut tx = self.pool.begin().await?;
        let now = command_utils::util::datetime::now_millis();
        let updated = self
            .index_repo
            .update_pinned_tx(&mut *tx, id.value, pinned, now)
            .await?;
        tx.commit().await?;
        if !updated {
            return Err(infra::error::LlmMemoryError::NotFound(format!(
                "reflection {} not found",
                id.value
            ))
            .into());
        }
        Ok(())
    }

    async fn search(
        &self,
        filter: Option<&ReflectionSearchFilter>,
        sort: ReflectionListSort,
        limit: Option<u32>,
        offset: Option<i64>,
    ) -> Result<Vec<ReflectionSearchResult>> {
        search::search(self, filter, sort, limit, offset).await
    }

    async fn search_hybrid(
        &self,
        filter: Option<&ReflectionSearchFilter>,
        query_text: Option<&str>,
        query_vectors: &[EmbeddingVector],
        hybrid_options: Option<&HybridSearchOptions>,
        boost_pinned_high_score: bool,
        sort: ReflectionListSort,
        limit: Option<u32>,
    ) -> Result<Vec<ReflectionSearchResult>> {
        search::search_hybrid(
            self,
            filter,
            query_text,
            query_vectors,
            hybrid_options,
            boost_pinned_high_score,
            sort,
            limit,
        )
        .await
    }

    async fn find_by_thread(
        &self,
        thread_id: &ThreadId,
        include_history: bool,
    ) -> Result<Vec<Reflection>> {
        search::find_by_thread(self, thread_id, include_history).await
    }

    async fn match_failure_signatures(
        &self,
        indicators: &FailureSignatureIndicators,
        pattern_type: Option<i32>,
        top_k: u32,
        filter: Option<&ReflectionSearchFilter>,
    ) -> Result<(Vec<ReflectionSearchResult>, bool, u32)> {
        search::match_failure_signatures(self, indicators, pattern_type, top_k, filter).await
    }

    async fn find_similar_trajectories(
        &self,
        reference: TrajectoryReference,
        top_k: u32,
        filter: Option<&ReflectionSearchFilter>,
    ) -> Result<Vec<ReflectionSearchResult>> {
        search::find_similar_trajectories(self, reference, top_k, filter).await
    }

    async fn find_similar_by_intent_text(
        &self,
        intent_text: &str,
        top_k: u32,
        filter: Option<&ReflectionSearchFilter>,
    ) -> Result<Vec<ReflectionSearchResult>> {
        search::find_similar_by_intent_text(self, intent_text, top_k, filter).await
    }

    async fn aggregate_failure_modes(
        &self,
        filter: Option<&ReflectionSearchFilter>,
        include_co_occurrence: bool,
    ) -> Result<AggregateFailureModesResponse> {
        aggregate::aggregate_failure_modes(self, filter, include_co_occurrence).await
    }

    async fn aggregate_scores(
        &self,
        filter: Option<&ReflectionSearchFilter>,
        group_by: &[AggregateScoresGroupBy],
        time_bucket_seconds: Option<u32>,
    ) -> Result<AggregateScoresResponse> {
        aggregate::aggregate_scores(self, filter, group_by, time_bucket_seconds).await
    }

    async fn aggregate_lessons(
        &self,
        filter: Option<&ReflectionSearchFilter>,
        top_n: Option<u32>,
    ) -> Result<AggregateLessonsResponse> {
        aggregate::aggregate_lessons(self, filter, top_n).await
    }

    async fn aggregate_tool_contributions(
        &self,
        filter: Option<&ReflectionSearchFilter>,
        group_by_tool: bool,
        group_by_contribution: bool,
        group_by_error_kind: bool,
        group_by_origin_user: bool,
    ) -> Result<AggregateToolContributionsResponse> {
        aggregate::aggregate_tool_contributions(
            self,
            filter,
            group_by_tool,
            group_by_contribution,
            group_by_error_kind,
            group_by_origin_user,
        )
        .await
    }

    async fn get_tool_outcome_stats(
        &self,
        origin_user_id: i64,
        tool: &str,
    ) -> Result<ToolOutcomeStatsResponse> {
        aggregate::get_tool_outcome_stats(self, origin_user_id, tool).await
    }

    async fn get_tool_contribution_stats(
        &self,
        origin_user_id: i64,
        tool: &str,
    ) -> Result<ToolContributionStatsResponse> {
        aggregate::get_tool_contribution_stats(self, origin_user_id, tool).await
    }

    async fn rebuild_derived_stats(
        &self,
        origin_user_id: Option<i64>,
        rebuild_tool_outcome: bool,
        rebuild_tool_contribution: bool,
    ) -> Result<RebuildDerivedStatsResponse> {
        aggregate::rebuild_derived_stats(
            self,
            origin_user_id,
            rebuild_tool_outcome,
            rebuild_tool_contribution,
        )
        .await
    }

    async fn record_applied_target(
        &self,
        id: &ReflectionId,
        target: String,
        fingerprint: Option<String>,
    ) -> Result<bool> {
        runtime_flags::record_applied_target(self, id, target, fingerprint).await
    }

    async fn record_few_shot_usage(
        &self,
        id: &ReflectionId,
        used_in_thread_id: i64,
    ) -> Result<bool> {
        runtime_flags::record_few_shot_usage(self, id, used_in_thread_id).await
    }

    async fn upsert_mitigation_applied(
        &self,
        id: &ReflectionId,
        target: String,
        fingerprint: String,
    ) -> Result<bool> {
        runtime_flags::upsert_mitigation_applied(self, id, target, fingerprint).await
    }

    async fn list_applied_targets(
        &self,
        id: &ReflectionId,
    ) -> Result<Vec<ReflectionAppliedTargetRow>> {
        runtime_flags::list_applied_targets(self, id).await
    }

    async fn list_few_shot_usage(
        &self,
        id: &ReflectionId,
    ) -> Result<Vec<ReflectionFewShotUsageRow>> {
        runtime_flags::list_few_shot_usage(self, id).await
    }

    async fn export(
        &self,
        filter: Option<&ReflectionSearchFilter>,
        cursor_after_memory_id: Option<i64>,
        batch_size: Option<u32>,
    ) -> Result<Vec<Reflection>> {
        export::export(self, filter, cursor_after_memory_id, batch_size).await
    }

    async fn redispatch_reflection_embeddings(
        &self,
        kind: EmbeddingKind,
        filter: Option<&ReflectionSearchFilter>,
        batch_size: Option<u32>,
    ) -> Result<RedispatchReflectionEmbeddingsResponse> {
        redispatch::redispatch(self, kind, filter, batch_size).await
    }

    async fn batch_upsert_intent_embeddings_rows(
        &self,
        id: &ReflectionId,
        embedding_model: Option<&str>,
        replace_kinds: &[String],
        rows: Vec<(String, i32, i32, i32, String, Vec<f32>)>,
    ) -> Result<(u32, u32, Vec<String>)> {
        use infra::infra::reflection::rdb::ThreadReflectionIndexRepository;
        use infra::infra::reflection_intent_vector::record::{
            ReflectionIntentFilterContext, ReflectionIntentVectorRecord,
        };

        if replace_kinds.is_empty() {
            return Err(infra::error::LlmMemoryError::InvalidArgument(
                "batch_upsert_intent_embeddings_rows: replace_kinds must not be empty".to_string(),
            )
            .into());
        }

        let Some(repo) = self.intent_vector_repo.as_ref() else {
            return Err(infra::error::LlmMemoryError::Unimplemented(
                "BatchUpsertIntentEmbeddings requires the reflection intent vector store to be \
                 configured (set MEMORY_VECTOR_ENABLED=true and REFLECTION_LANCEDB_URI)."
                    .into(),
            )
            .into());
        };

        // The rows need filter-column context (origin_user_id /
        // task_category / reflection_aspect / outcome / channel /
        // created_at) so F-S3 / F-S8 can prune candidates without
        // crossing back into RDB. The sidecar is the source of truth for
        // these columns — read once here and stamp onto every chunk row.
        let row = self
            .index_repo
            .find_by_memory_id(id.value)
            .await?
            .ok_or_else(|| {
                infra::error::LlmMemoryError::NotFound(format!(
                    "reflection {} not found (no sidecar row); BatchUpsertIntentEmbeddings \
                     requires a finalized reflection",
                    id.value
                ))
            })?;

        let ctx = ReflectionIntentFilterContext {
            origin_user_id: row.origin_user_id,
            origin_channel: row.origin_channel.clone(),
            task_category: row.task_category,
            reflection_aspect: row.reflection_aspect,
            outcome: row.outcome,
            created_at: row.created_at,
        };

        let mut records = Vec::with_capacity(rows.len());
        for (vector_kind, chunk_index, begin, end, content, embedding) in rows {
            records.push(ReflectionIntentVectorRecord::from_chunk_with_content(
                id.value,
                &ctx,
                &embedding,
                embedding_model,
                &vector_kind,
                chunk_index,
                begin,
                end,
                content,
            ));
        }

        let replace_refs: Vec<&str> = replace_kinds.iter().map(String::as_str).collect();
        let success = repo
            .replace_kinds_upsert(id.value, &replace_refs, records)
            .await
            .map_err(|e| {
                infra::error::LlmMemoryError::OtherError(format!(
                    "reflection intent vector upsert failed: {e:#}"
                ))
            })? as u32;
        Ok((success, 0, Vec::new()))
    }

    async fn mark_embedding_status(
        &self,
        id: &ReflectionId,
        kind: EmbeddingKind,
        status: EmbeddingStatus,
        error_reason: Option<String>,
    ) -> Result<()> {
        use infra::infra::reflection::rdb::{EmbeddingTrack, ThreadReflectionIndexRepository};
        let track = match kind {
            EmbeddingKind::Summary => EmbeddingTrack::Summary,
            EmbeddingKind::Intent => EmbeddingTrack::Intent,
            // BOTH / UNSPECIFIED rejected explicitly: this RPC is the
            // single-track status mutator. Bulk re-status flows live
            // behind RedispatchReflectionEmbeddings (Phase D commit 4).
            EmbeddingKind::Both | EmbeddingKind::Unspecified => {
                return Err(infra::error::LlmMemoryError::InvalidArgument(
                    "MarkReflectionEmbeddingStatus requires kind=SUMMARY or INTENT".into(),
                )
                .into());
            }
        };
        if matches!(
            status,
            EmbeddingStatus::Pending | EmbeddingStatus::Unspecified
        ) {
            return Err(infra::error::LlmMemoryError::InvalidArgument(
                "MarkReflectionEmbeddingStatus rejects status=PENDING/UNSPECIFIED".into(),
            )
            .into());
        }
        let mut tx = self.pool.begin().await?;
        let now = command_utils::util::datetime::now_millis();
        let updated = self
            .index_repo
            .update_embedding_status_tx(
                &mut *tx,
                id.value,
                track,
                status as i32,
                error_reason.as_deref(),
                now,
            )
            .await?;
        tx.commit().await?;
        if !updated {
            return Err(infra::error::LlmMemoryError::NotFound(format!(
                "reflection {} not found",
                id.value
            ))
            .into());
        }
        Ok(())
    }
}
