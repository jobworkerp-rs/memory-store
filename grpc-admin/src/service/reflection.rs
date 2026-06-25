//! `ReflectionService` server adapter (Phase E).
//!
//! Reflection proto code lives in the shared `protobuf` crate only —
//! grpc-admin does *not* regenerate it (see `grpc-admin/build.rs`).
//! This file therefore imports request / response / server types from
//! `protobuf::llm_memory::service::*` so the trait method signatures
//! line up with `ReflectionApp` without an intermediate conversion.

use app::app::reflection::{ReflectionApp, ReflectionAppImpl, TrajectoryReference};
use async_stream::stream;
use command_utils::trace::Tracing;
use futures::stream::BoxStream;
use protobuf::llm_memory::data::{Reflection, ReflectionId, ReflectionSearchResult};
use protobuf::llm_memory::service::reflection_service_server::ReflectionService;
use protobuf::llm_memory::service::{
    AggregateFailureModesRequest, AggregateFailureModesResponse, AggregateLessonsRequest,
    AggregateLessonsResponse, AggregateScoresGroupBy, AggregateScoresRequest,
    AggregateScoresResponse, AggregateToolContributionsRequest, AggregateToolContributionsResponse,
    DeleteReflectionRequest, ExportReflectionsRequest, FinalizeReflectionRequest,
    FindReflectionRequest, FindReflectionsByThreadIdRequest, FindSimilarByTextRequest,
    FindSimilarRequest, GenerateForThreadRequest, GenerateForThreadResponse,
    GetToolContributionStatsRequest, GetToolOutcomeStatsRequest, MarkEmbeddingStatusRequest,
    MatchSignaturesRequest, MatchSignaturesResponse, OptionalReflectionResponse,
    PinReflectionRequest, RebuildDerivedStatsRequest, RebuildDerivedStatsResponse,
    RecordAppliedTargetRequest, RecordFewShotUsageRequest, ReflectionListSort,
    SearchReflectionsRequest, SuccessResponse, ToolContributionStatsResponse,
    ToolOutcomeStatsResponse, UpdateReflectionRequest, UpsertMitigationAppliedRequest,
    UpsertMitigationAppliedResponse, find_similar_request,
};
use tonic::Response;

use crate::service::error_handle::handle_error;

#[derive(Clone, DebugStub)]
pub(crate) struct ReflectionGrpcImpl {
    #[debug_stub = "Arc<ReflectionAppImpl>"]
    reflection_app: std::sync::Arc<ReflectionAppImpl>,
    // Used at `Delete` time to drop the summary embedding row from the
    // shared `memory_vector` LanceDB table; intent embeddings live in
    // their own table and are cascaded inside `crud.rs::delete`. Held
    // here (not on `ReflectionAppImpl`) to mirror the `MemoryGrpcImpl`
    // wiring and keep the app crate free of `MemoryVectorAppImpl`.
    #[debug_stub = "Option<Arc<MemoryVectorAppImpl>>"]
    vector_app: Option<std::sync::Arc<app::app::memory_vector::MemoryVectorAppImpl>>,
}

impl ReflectionGrpcImpl {
    pub fn new(
        reflection_app: std::sync::Arc<ReflectionAppImpl>,
        vector_app: Option<std::sync::Arc<app::app::memory_vector::MemoryVectorAppImpl>>,
    ) -> Self {
        Self {
            reflection_app,
            vector_app,
        }
    }

    fn app(&self) -> &ReflectionAppImpl {
        &self.reflection_app
    }
}

impl Tracing for ReflectionGrpcImpl {}

#[tonic::async_trait]
impl ReflectionService for ReflectionGrpcImpl {
    // ===== Generate / Finalize =====

    async fn generate(
        &self,
        request: tonic::Request<GenerateForThreadRequest>,
    ) -> Result<tonic::Response<GenerateForThreadResponse>, tonic::Status> {
        let _s = Self::trace_request("reflection", "generate", &request);
        match self.app().generate(request.get_ref()).await {
            Ok(resp) => Ok(Response::new(resp)),
            Err(e) => Err(handle_error(&e)),
        }
    }

    async fn finalize_reflection(
        &self,
        request: tonic::Request<FinalizeReflectionRequest>,
    ) -> Result<tonic::Response<ReflectionId>, tonic::Status> {
        let _s = Self::trace_request("reflection", "finalize_reflection", &request);
        match self
            .app()
            .finalize_generated_reflection(request.get_ref())
            .await
        {
            Ok(id) => Ok(Response::new(id)),
            Err(e) => Err(handle_error(&e)),
        }
    }

    // ===== CRUD =====

    async fn find(
        &self,
        request: tonic::Request<FindReflectionRequest>,
    ) -> Result<tonic::Response<OptionalReflectionResponse>, tonic::Status> {
        let _s = Self::trace_request("reflection", "find", &request);
        let id = request
            .get_ref()
            .id
            .as_ref()
            .ok_or_else(|| tonic::Status::invalid_argument("id is required"))?;
        match self.app().find(id).await {
            Ok(reflection) => Ok(Response::new(OptionalReflectionResponse { reflection })),
            Err(e) => Err(handle_error(&e)),
        }
    }

    type FindByThreadStream = BoxStream<'static, Result<Reflection, tonic::Status>>;
    async fn find_by_thread(
        &self,
        request: tonic::Request<FindReflectionsByThreadIdRequest>,
    ) -> Result<tonic::Response<Self::FindByThreadStream>, tonic::Status> {
        let _s = Self::trace_request("reflection", "find_by_thread", &request);
        let req = request.get_ref();
        let thread_id = req
            .thread_id
            .as_ref()
            .ok_or_else(|| tonic::Status::invalid_argument("thread_id is required"))?;
        match self
            .app()
            .find_by_thread(thread_id, req.include_history)
            .await
        {
            Ok(list) => Ok(Response::new(Box::pin(stream! {
                for r in list {
                    yield Ok(r);
                }
            }))),
            Err(e) => Err(handle_error(&e)),
        }
    }

    async fn update(
        &self,
        request: tonic::Request<UpdateReflectionRequest>,
    ) -> Result<tonic::Response<SuccessResponse>, tonic::Status> {
        let _s = Self::trace_request("reflection", "update", &request);
        let req = request.get_ref();
        let id = req
            .id
            .as_ref()
            .ok_or_else(|| tonic::Status::invalid_argument("id is required"))?;
        // proto §UpdateReflectionRequest: `update_mask` enumerates the
        // fields the caller wants written. Today only `pinned` is
        // writable (proto comment + spec §3.7), and forwarding the
        // request without inspecting the mask would let a caller send
        // mask=["dataset_quality"] (or an empty mask) and still get a
        // "success" with no observable change. Reject anything that
        // doesn't ask for `pinned`, and require the matching `pinned`
        // value so a missing `pinned` field cannot silently no-op.
        let mask_paths: &[String] = req
            .update_mask
            .as_ref()
            .map(|m| m.paths.as_slice())
            .unwrap_or(&[]);
        if mask_paths.is_empty() {
            return Err(tonic::Status::invalid_argument(
                "update_mask must list at least one writable field (currently only `pinned`)",
            ));
        }
        for path in mask_paths {
            if path != "pinned" {
                return Err(tonic::Status::invalid_argument(format!(
                    "update_mask path `{path}` is not writable; only `pinned` is supported \
                     (other fields use dedicated RPCs — RecordAppliedTarget, \
                     RecordFewShotUsage, MarkReflectionEmbeddingStatus)"
                )));
            }
        }
        if req.pinned.is_none() {
            return Err(tonic::Status::invalid_argument(
                "update_mask requested `pinned` but the `pinned` field is unset",
            ));
        }
        match self.app().update(id, req.pinned).await {
            Ok(()) => Ok(Response::new(SuccessResponse { is_success: true })),
            Err(e) => Err(handle_error(&e)),
        }
    }

    async fn delete(
        &self,
        request: tonic::Request<DeleteReflectionRequest>,
    ) -> Result<tonic::Response<SuccessResponse>, tonic::Status> {
        let _s = Self::trace_request("reflection", "delete", &request);
        let id = request
            .get_ref()
            .id
            .as_ref()
            .ok_or_else(|| tonic::Status::invalid_argument("id is required"))?;
        match self.app().delete(id).await {
            Ok(()) => {
                // Cascade the summary embedding from LanceDB. Best-effort
                // and ordered after the RDB delete to keep RDB as the
                // source of truth — same contract as
                // `MemoryGrpcImpl::delete`. The intent-vector cascade
                // already ran inside `ReflectionApp::delete`.
                if let Some(va) = self.vector_app.as_ref()
                    && let Err(e) = va.delete_vector(id.value).await
                {
                    tracing::error!(
                        "LanceDB summary-vector cascade delete failed for reflection \
                         memory_id={}: {e}",
                        id.value
                    );
                }
                Ok(Response::new(SuccessResponse { is_success: true }))
            }
            Err(e) => Err(handle_error(&e)),
        }
    }

    // ===== Search =====

    type SearchStream = BoxStream<'static, Result<ReflectionSearchResult, tonic::Status>>;
    async fn search(
        &self,
        request: tonic::Request<SearchReflectionsRequest>,
    ) -> Result<tonic::Response<Self::SearchStream>, tonic::Status> {
        let _s = Self::trace_request("reflection", "search", &request);
        let req = request.get_ref();
        let sort = match req.sort {
            Some(s) => ReflectionListSort::try_from(s).unwrap_or(ReflectionListSort::Unspecified),
            None => ReflectionListSort::Unspecified,
        };

        // Detect any relevance signal: query_text (non-empty after trim)
        // or query_vectors (any non-empty vector). When present, route
        // through `search_hybrid` (F-S1 hybrid wrapper); otherwise the
        // request is a plain filter-only listing.
        let text_present = req
            .query_text
            .as_deref()
            .map(|s| !s.trim().is_empty())
            .unwrap_or(false);
        let vectors_present = req.query_vectors.iter().any(|v| !v.values.is_empty());
        let hybrid_signals_present = text_present
            || vectors_present
            || req.hybrid_options.is_some()
            || req.boost_pinned_high_score == Some(true);

        if hybrid_signals_present {
            // Hybrid path does not consume `cursor_after_memory_id`:
            // pagination over relevance-ranked results would need a
            // compound cursor we do not support yet. Reject loudly
            // rather than silently drop the cursor.
            if req.cursor_after_memory_id.is_some() {
                return Err(tonic::Status::invalid_argument(
                    "cursor_after_memory_id is not supported for hybrid search; remove the \
                     cursor or drop the relevance signals to get a filter-only listing",
                ));
            }
            match self
                .app()
                .search_hybrid(
                    req.filter.as_ref(),
                    req.query_text.as_deref(),
                    &req.query_vectors,
                    req.hybrid_options.as_ref(),
                    req.boost_pinned_high_score.unwrap_or(false),
                    sort,
                    req.limit,
                )
                .await
            {
                Ok(list) => {
                    return Ok(Response::new(Box::pin(stream! {
                        for r in list {
                            yield Ok(r);
                        }
                    })));
                }
                Err(e) => return Err(handle_error(&e)),
            }
        }

        // `cursor_after_memory_id` is a single-i64 keyset (`memory_id <
        // cursor`). It is only stable when the result set is ordered
        // by `memory_id DESC` end-to-end — mixing it with
        // `CREATED_DESC` (or any other secondary key) lets a row with
        // a smaller `memory_id` but a later `created_at` jump ahead of
        // the cursor, so page 1 returns id A while page 2 with
        // cursor=A's-id silently drops B because B happens to have an
        // even smaller `memory_id`. We accept the cursor only with
        // `UNSPECIFIED` (which the app layer maps to the
        // memory_id-desc keyset path); other sorts plus a cursor are
        // rejected so callers cannot construct the silently-broken
        // pagination shape.
        if req.cursor_after_memory_id.is_some() && !matches!(sort, ReflectionListSort::Unspecified)
        {
            return Err(tonic::Status::invalid_argument(
                "cursor_after_memory_id requires sort = REFLECTION_LIST_SORT_UNSPECIFIED; \
                 other sort keys (score / created_at / failure_first) need OFFSET-based \
                 paging or proto support for a compound cursor",
            ));
        }
        match self
            .app()
            .search(
                req.filter.as_ref(),
                sort,
                req.limit,
                req.cursor_after_memory_id,
            )
            .await
        {
            Ok(list) => Ok(Response::new(Box::pin(stream! {
                for r in list {
                    yield Ok(r);
                }
            }))),
            Err(e) => Err(handle_error(&e)),
        }
    }

    type FindSimilarTrajectoriesStream =
        BoxStream<'static, Result<ReflectionSearchResult, tonic::Status>>;
    async fn find_similar_trajectories(
        &self,
        request: tonic::Request<FindSimilarRequest>,
    ) -> Result<tonic::Response<Self::FindSimilarTrajectoriesStream>, tonic::Status> {
        let _s = Self::trace_request("reflection", "find_similar_trajectories", &request);
        let req = request.get_ref();
        let reference = match req.reference.as_ref() {
            Some(find_similar_request::Reference::ReferenceThreadId(t)) => {
                TrajectoryReference::Thread(*t)
            }
            Some(find_similar_request::Reference::ReferenceReflectionId(r)) => {
                TrajectoryReference::Reflection(*r)
            }
            None => {
                return Err(tonic::Status::invalid_argument(
                    "reference (thread_id or reflection_id) is required",
                ));
            }
        };
        match self
            .app()
            .find_similar_trajectories(reference, req.top_k, req.filter.as_ref())
            .await
        {
            Ok(list) => Ok(Response::new(Box::pin(stream! {
                for r in list {
                    yield Ok(r);
                }
            }))),
            Err(e) => Err(handle_error(&e)),
        }
    }

    type FindSimilarByIntentTextStream =
        BoxStream<'static, Result<ReflectionSearchResult, tonic::Status>>;
    async fn find_similar_by_intent_text(
        &self,
        request: tonic::Request<FindSimilarByTextRequest>,
    ) -> Result<tonic::Response<Self::FindSimilarByIntentTextStream>, tonic::Status> {
        let _s = Self::trace_request("reflection", "find_similar_by_intent_text", &request);
        let req = request.get_ref();
        match self
            .app()
            .find_similar_by_intent_text(&req.intent_text, req.top_k, req.filter.as_ref())
            .await
        {
            Ok(list) => Ok(Response::new(Box::pin(stream! {
                for r in list {
                    yield Ok(r);
                }
            }))),
            Err(e) => Err(handle_error(&e)),
        }
    }

    async fn match_failure_signatures(
        &self,
        request: tonic::Request<MatchSignaturesRequest>,
    ) -> Result<tonic::Response<MatchSignaturesResponse>, tonic::Status> {
        let _s = Self::trace_request("reflection", "match_failure_signatures", &request);
        let req = request.get_ref();
        let indicators = req
            .current_indicators
            .as_ref()
            .ok_or_else(|| tonic::Status::invalid_argument("current_indicators is required"))?;
        match self
            .app()
            .match_failure_signatures(
                indicators,
                req.current_pattern_type,
                req.top_k,
                req.filter.as_ref(),
            )
            .await
        {
            Ok((hits, is_truncated, scanned_count)) => Ok(Response::new(MatchSignaturesResponse {
                hits,
                is_truncated,
                scanned_count,
            })),
            Err(e) => Err(handle_error(&e)),
        }
    }

    // ===== Export =====

    type ExportStream = BoxStream<'static, Result<Reflection, tonic::Status>>;
    async fn export(
        &self,
        request: tonic::Request<ExportReflectionsRequest>,
    ) -> Result<tonic::Response<Self::ExportStream>, tonic::Status> {
        let _s = Self::trace_request("reflection", "export", &request);
        let req = request.get_ref();
        // proto §ExportReflectionsRequest lets callers exclude facts /
        // tool_outcomes to keep the export payload small. The app layer
        // always hydrates the full envelope (same path as search), so
        // the include flags are honored here by clearing the unwanted
        // vectors before yielding. This is a grpc-layer trim, not a
        // real optimization — Phase F's bulk-hydrate refactor can
        // teach the app layer to skip the child fetches entirely when
        // these flags are false.
        let include_facts = req.include_facts;
        let include_tool_outcomes = req.include_tool_outcomes;
        match self
            .app()
            .export(
                req.filter.as_ref(),
                req.cursor_after_memory_id,
                req.batch_size,
            )
            .await
        {
            Ok(list) => Ok(Response::new(Box::pin(stream! {
                for mut r in list {
                    if let Some(data) = r.data.as_mut() {
                        if !include_facts {
                            data.facts.clear();
                        }
                        if !include_tool_outcomes {
                            data.tool_outcomes.clear();
                        }
                    }
                    yield Ok(r);
                }
            }))),
            Err(e) => Err(handle_error(&e)),
        }
    }

    // ===== Aggregate =====

    async fn aggregate_failure_modes(
        &self,
        request: tonic::Request<AggregateFailureModesRequest>,
    ) -> Result<tonic::Response<AggregateFailureModesResponse>, tonic::Status> {
        let _s = Self::trace_request("reflection", "aggregate_failure_modes", &request);
        let req = request.get_ref();
        // `top_n` is `optional uint32`. Reject `Some(0)` explicitly:
        // truncating to 0 would return an empty `entries[]` and look
        // indistinguishable from "no failure modes matched the filter",
        // silently hiding real data. `None` (omitted) keeps the
        // "return everything" semantics.
        if req.top_n == Some(0) {
            return Err(tonic::Status::invalid_argument(
                "AggregateFailureModes: top_n must be omitted or >= 1 (top_n=0 is rejected so an \
                 empty response cannot be confused with `no matches`)",
            ));
        }
        // `include_co_occurrence` is honoured: callers that only need
        // per-mode counts pay the cost of a single GROUP BY instead of
        // the self-join over `reflection_failure_mode`. `top_n` is
        // trimmed post-hoc because the dictionary already bounds the
        // output cardinality.
        match self
            .app()
            .aggregate_failure_modes(req.filter.as_ref(), req.include_co_occurrence)
            .await
        {
            Ok(mut resp) => {
                if let Some(top_n) = req.top_n
                    && (top_n as usize) < resp.entries.len()
                {
                    resp.entries.truncate(top_n as usize);
                }
                Ok(Response::new(resp))
            }
            Err(e) => Err(handle_error(&e)),
        }
    }

    async fn aggregate_scores(
        &self,
        request: tonic::Request<AggregateScoresRequest>,
    ) -> Result<tonic::Response<AggregateScoresResponse>, tonic::Status> {
        let _s = Self::trace_request("reflection", "aggregate_scores", &request);
        let req = request.get_ref();
        let group_by: Vec<_> = req
            .group_by
            .iter()
            .filter_map(|g| AggregateScoresGroupBy::try_from(*g).ok())
            .collect();
        match self
            .app()
            .aggregate_scores(req.filter.as_ref(), &group_by, req.time_bucket_seconds)
            .await
        {
            Ok(resp) => Ok(Response::new(resp)),
            Err(e) => Err(handle_error(&e)),
        }
    }

    async fn aggregate_lessons(
        &self,
        request: tonic::Request<AggregateLessonsRequest>,
    ) -> Result<tonic::Response<AggregateLessonsResponse>, tonic::Status> {
        let _s = Self::trace_request("reflection", "aggregate_lessons", &request);
        let req = request.get_ref();
        match self
            .app()
            .aggregate_lessons(req.filter.as_ref(), req.top_n)
            .await
        {
            Ok(resp) => Ok(Response::new(resp)),
            Err(e) => Err(handle_error(&e)),
        }
    }

    async fn aggregate_tool_contributions(
        &self,
        request: tonic::Request<AggregateToolContributionsRequest>,
    ) -> Result<tonic::Response<AggregateToolContributionsResponse>, tonic::Status> {
        let _s = Self::trace_request("reflection", "aggregate_tool_contributions", &request);
        let req = request.get_ref();
        match self
            .app()
            .aggregate_tool_contributions(
                req.filter.as_ref(),
                req.group_by_tool,
                req.group_by_contribution,
                req.group_by_error_kind,
                req.group_by_origin_user,
            )
            .await
        {
            Ok(resp) => Ok(Response::new(resp)),
            Err(e) => Err(handle_error(&e)),
        }
    }

    // ===== Tool stats =====

    async fn get_tool_outcome_stats(
        &self,
        request: tonic::Request<GetToolOutcomeStatsRequest>,
    ) -> Result<tonic::Response<ToolOutcomeStatsResponse>, tonic::Status> {
        let _s = Self::trace_request("reflection", "get_tool_outcome_stats", &request);
        let req = request.get_ref();
        match self
            .app()
            .get_tool_outcome_stats(req.origin_user_id, &req.tool)
            .await
        {
            Ok(resp) => Ok(Response::new(resp)),
            Err(e) => Err(handle_error(&e)),
        }
    }

    async fn get_tool_contribution_stats(
        &self,
        request: tonic::Request<GetToolContributionStatsRequest>,
    ) -> Result<tonic::Response<ToolContributionStatsResponse>, tonic::Status> {
        let _s = Self::trace_request("reflection", "get_tool_contribution_stats", &request);
        let req = request.get_ref();
        match self
            .app()
            .get_tool_contribution_stats(req.origin_user_id, &req.tool)
            .await
        {
            Ok(resp) => Ok(Response::new(resp)),
            Err(e) => Err(handle_error(&e)),
        }
    }

    async fn rebuild_derived_stats(
        &self,
        request: tonic::Request<RebuildDerivedStatsRequest>,
    ) -> Result<tonic::Response<RebuildDerivedStatsResponse>, tonic::Status> {
        let _s = Self::trace_request("reflection", "rebuild_derived_stats", &request);
        let req = request.get_ref();
        match self
            .app()
            .rebuild_derived_stats(
                req.origin_user_id,
                req.rebuild_tool_outcome_stats,
                req.rebuild_tool_contribution_stats,
            )
            .await
        {
            Ok(resp) => Ok(Response::new(resp)),
            Err(e) => Err(handle_error(&e)),
        }
    }

    // ===== Operational state =====

    async fn pin(
        &self,
        request: tonic::Request<PinReflectionRequest>,
    ) -> Result<tonic::Response<SuccessResponse>, tonic::Status> {
        let _s = Self::trace_request("reflection", "pin", &request);
        let req = request.get_ref();
        let id = req
            .id
            .as_ref()
            .ok_or_else(|| tonic::Status::invalid_argument("id is required"))?;
        match self.app().pin(id, req.pinned).await {
            Ok(()) => Ok(Response::new(SuccessResponse { is_success: true })),
            Err(e) => Err(handle_error(&e)),
        }
    }

    async fn record_applied_target(
        &self,
        request: tonic::Request<RecordAppliedTargetRequest>,
    ) -> Result<tonic::Response<SuccessResponse>, tonic::Status> {
        let _s = Self::trace_request("reflection", "record_applied_target", &request);
        let req = request.get_ref();
        let id = req
            .id
            .as_ref()
            .ok_or_else(|| tonic::Status::invalid_argument("id is required"))?;
        match self
            .app()
            .record_applied_target(id, req.target.clone(), req.mitigation_fingerprint.clone())
            .await
        {
            Ok(applied) => Ok(Response::new(SuccessResponse {
                is_success: applied,
            })),
            Err(e) => Err(handle_error(&e)),
        }
    }

    async fn record_few_shot_usage(
        &self,
        request: tonic::Request<RecordFewShotUsageRequest>,
    ) -> Result<tonic::Response<SuccessResponse>, tonic::Status> {
        let _s = Self::trace_request("reflection", "record_few_shot_usage", &request);
        let req = request.get_ref();
        let id = req
            .id
            .as_ref()
            .ok_or_else(|| tonic::Status::invalid_argument("id is required"))?;
        match self
            .app()
            .record_few_shot_usage(id, req.used_in_thread_id)
            .await
        {
            Ok(applied) => Ok(Response::new(SuccessResponse {
                is_success: applied,
            })),
            Err(e) => Err(handle_error(&e)),
        }
    }

    async fn upsert_mitigation_applied(
        &self,
        request: tonic::Request<UpsertMitigationAppliedRequest>,
    ) -> Result<tonic::Response<UpsertMitigationAppliedResponse>, tonic::Status> {
        let _s = Self::trace_request("reflection", "upsert_mitigation_applied", &request);
        let req = request.get_ref();
        let id = req
            .id
            .as_ref()
            .ok_or_else(|| tonic::Status::invalid_argument("id is required"))?;
        match self
            .app()
            .upsert_mitigation_applied(id, req.target.clone(), req.fingerprint.clone())
            .await
        {
            Ok(applied) => Ok(Response::new(UpsertMitigationAppliedResponse { applied })),
            Err(e) => Err(handle_error(&e)),
        }
    }

    async fn mark_reflection_embedding_status(
        &self,
        request: tonic::Request<MarkEmbeddingStatusRequest>,
    ) -> Result<tonic::Response<SuccessResponse>, tonic::Status> {
        let _s = Self::trace_request("reflection", "mark_reflection_embedding_status", &request);
        let req = request.get_ref();
        let id = req
            .reflection_id
            .as_ref()
            .ok_or_else(|| tonic::Status::invalid_argument("reflection_id is required"))?;
        let kind = protobuf::llm_memory::data::EmbeddingKind::try_from(req.kind)
            .map_err(|_| tonic::Status::invalid_argument("invalid kind"))?;
        let status = protobuf::llm_memory::data::EmbeddingStatus::try_from(req.status)
            .map_err(|_| tonic::Status::invalid_argument("invalid status"))?;
        match self
            .app()
            .mark_embedding_status(id, kind, status, req.error_reason.clone())
            .await
        {
            Ok(()) => Ok(Response::new(SuccessResponse { is_success: true })),
            Err(e) => Err(handle_error(&e)),
        }
    }
}
