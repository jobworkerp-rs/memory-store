use std::fmt::Debug;

use crate::protobuf::llm_memory::service::memory_vector_service_server::MemoryVectorService;
use crate::protobuf::llm_memory::service::{
    BatchUpsertEmbeddingsRequest, BatchUpsertEmbeddingsResponse, GetIndexStatsRequest,
    HybridSearchRequest, IndexStatsResponse, MediaSearchRequest, MemorySearchCountRequest,
    MemorySearchCountResponse, MemorySearchResult, RebuildIndexRequest, RebuildIndexResponse,
    RedispatchEmbeddingsRequest, RedispatchEmbeddingsResponse, SemanticTextSearchRequest,
    SurroundingMemoriesRequest, SurroundingMemoriesResponse, TextSearchRequest,
    UpsertEmbeddingRequest, UpsertEmbeddingResponse, VectorSearchRequest,
};
use crate::service::error_handle::handle_error;
use crate::service::memory::enrich_memory_media;
use app::app::memory_vector::{CountMode, CountSearchInput, MemoryVectorAppImpl};
use async_stream::stream;
use command_utils::trace::Tracing;
use futures::stream::BoxStream;
use infra::infra::memory_vector::dispatcher::DispatchKind as InfraDispatchKind;
use infra::infra::memory_vector::repository::{AggregationStrategy, HybridOptions, HybridStrategy};
use infra::infra::memory_vector::safe_filter::SafeFilter;
use tonic::Response;

pub trait MemoryVectorGrpc {
    fn app(&self) -> &MemoryVectorAppImpl;
    /// `MediaApp` for (a) SearchByMedia resolving the query media to a
    /// fetchable URL before embedding it, and (b) presigning the
    /// `Memory.media` of every search hit just before responding (same
    /// `enrich_memory_media` the Find path uses). `None` when the media
    /// subsystem is not wired — SearchByMedia then returns
    /// FailedPrecondition and hits carry no media (backward compatible).
    fn media_app(&self) -> Option<&std::sync::Arc<app::app::media::MediaAppImpl>> {
        None
    }
}

/// Map an app-layer search hit to the wire result. Centralised so the
/// three streaming search RPCs (SearchByVector / SearchByText /
/// HybridSearch) cannot drift — image memory Phase 4 added five
/// `matched_*` / `score_source` fields and the previous per-RPC
/// duplication was both copy-paste and a dead-state bug (the app layer
/// fills `matched_*` and the query-origin `score_source` in
/// `enrich_hits`, so the handler must forward them, not hardcode None /
/// a static enum). The orphan rule blocks a `From` impl here (both
/// types are foreign), hence a free fn.
fn to_proto_result(item: app::app::memory_vector::MemorySearchResultItem) -> MemorySearchResult {
    MemorySearchResult {
        memory: Some(item.memory),
        score: item.score,
        distance: item.distance,
        score_source: item.score_source,
        position: item.position,
        thread_total: item.thread_total,
        thread_id: item.thread_id,
        thread_owner_user_id: item.thread_owner_user_id,
        thread_description: item.thread_description,
        highlights: item.highlights,
        matched_vector_kind: item.matched_vector_kind,
        matched_begin_position: item.matched_begin_position,
        matched_end_position: item.matched_end_position,
        matched_content: item.matched_content,
    }
}

#[tonic::async_trait]
impl<T: MemoryVectorGrpc + Tracing + Send + Debug + Sync + 'static> MemoryVectorService for T {
    // ===== Embedding management =====

    async fn upsert_embedding(
        &self,
        request: tonic::Request<UpsertEmbeddingRequest>,
    ) -> Result<tonic::Response<UpsertEmbeddingResponse>, tonic::Status> {
        let _s = Self::trace_request("memory_vector", "upsert_embedding", &request);
        let validated = match validate_upsert_embedding_request(request.get_ref())? {
            Some(v) => v,
            // Empty embedding ⇒ no-op skip. Upstream models routinely
            // refuse very short inputs and return an empty vector; the
            // record stays out of LanceDB by design.
            None => {
                return Ok(Response::new(UpsertEmbeddingResponse {
                    success: true,
                    skipped: true,
                }));
            }
        };
        match self
            .app()
            .upsert_embedding(
                validated.memory_id,
                validated.values,
                validated.embedding_model,
            )
            .await
        {
            Ok(()) => Ok(Response::new(UpsertEmbeddingResponse {
                success: true,
                skipped: false,
            })),
            Err(e) => Err(handle_error(&e)),
        }
    }

    async fn batch_upsert_embeddings(
        &self,
        request: tonic::Request<BatchUpsertEmbeddingsRequest>,
    ) -> Result<tonic::Response<BatchUpsertEmbeddingsResponse>, tonic::Status> {
        let _s = Self::trace_request("memory_vector", "batch_upsert_embeddings", &request);
        let req = request.get_ref();
        // `rows` (N-row path) and `items` (legacy single-embedding path)
        // are mutually exclusive request shapes. Routing the four cases
        // explicitly avoids the silent `success_count=0` data-loss trap a
        // `rows` client would hit if it fell through the items-only loop.
        let has_rows = !req.rows.is_empty();
        let has_items = !req.items.is_empty();
        match (has_rows, has_items) {
            // Mixing the two request shapes is ambiguous.
            (true, true) => {
                return Err(tonic::Status::invalid_argument(
                    "BatchUpsertEmbeddings: `rows` and `items` are mutually \
                     exclusive",
                ));
            }
            // rows path: memory_id + replace_kinds
            // required; each row carries its chunk's kind/offset/content.
            (true, false) => {
                let Some(memory_id) = req.memory_id else {
                    return Err(tonic::Status::invalid_argument(
                        "BatchUpsertEmbeddings: `memory_id` is required on \
                         the rows path",
                    ));
                };
                if req.replace_kinds.is_empty() {
                    return Err(tonic::Status::invalid_argument(
                        "BatchUpsertEmbeddings: `replace_kinds` is required \
                         on the rows path (which vector_kinds to replace)",
                    ));
                }
                let mut rows = Vec::with_capacity(req.rows.len());
                for r in &req.rows {
                    let emb = r
                        .embedding
                        .as_ref()
                        .filter(|e| !e.values.is_empty())
                        .ok_or_else(|| {
                            tonic::Status::invalid_argument(format!(
                                "BatchUpsertEmbeddings: row (vector_kind={}, \
                                 chunk_index={}) has a missing or empty \
                                 embedding",
                                r.vector_kind, r.chunk_index
                            ))
                        })?;
                    rows.push((
                        r.vector_kind.clone(),
                        r.chunk_index,
                        r.begin_position,
                        r.end_position,
                        r.content.clone(),
                        emb.values.clone(),
                    ));
                }
                return match self
                    .app()
                    .batch_upsert_embeddings_rows(
                        memory_id,
                        req.embedding_model.as_deref(),
                        &req.replace_kinds,
                        rows,
                    )
                    .await
                {
                    Ok((s, f, errors)) => Ok(Response::new(BatchUpsertEmbeddingsResponse {
                        success_count: s,
                        failure_count: f,
                        errors,
                    })),
                    Err(e) => Err(handle_error(&e)),
                };
            }
            // Neither `rows` nor `items`. Two sub-cases:
            //
            // (a) `memory_id` + `replace_kinds` present, `rows` empty:
            //     a deliberate "stale delete only" request. The embedding
            //     runner can legitimately yield 0 chunks (short / empty
            //     caption) yet still needs to drop the memory's existing
            //     rows of those kinds. `batch_upsert_embeddings_rows`
            //     (→ `replace_kinds_upsert` with 0 records) treats empty
            //     records as a stale-delete, so this MUST reach the app —
            //     otherwise the old text/caption rows leak and stale
            //     search hits persist (review P2 #2). Same validation
            //     contract as the rows path: `memory_id` required,
            //     `replace_kinds` required.
            //
            // (b) truly empty request (no memory_id, no replace_kinds):
            //     backward-compatible no-op success (callers may send an
            //     empty set).
            (false, false) => {
                if req.memory_id.is_some() || !req.replace_kinds.is_empty() {
                    let Some(memory_id) = req.memory_id else {
                        return Err(tonic::Status::invalid_argument(
                            "BatchUpsertEmbeddings: `memory_id` is required \
                             when `replace_kinds` is set (stale-delete on \
                             the rows path)",
                        ));
                    };
                    if req.replace_kinds.is_empty() {
                        return Err(tonic::Status::invalid_argument(
                            "BatchUpsertEmbeddings: `replace_kinds` is \
                             required when `memory_id` is set with no rows \
                             (which vector_kinds to stale-delete)",
                        ));
                    }
                    return match self
                        .app()
                        .batch_upsert_embeddings_rows(
                            memory_id,
                            req.embedding_model.as_deref(),
                            &req.replace_kinds,
                            Vec::new(),
                        )
                        .await
                    {
                        Ok((s, f, errors)) => Ok(Response::new(BatchUpsertEmbeddingsResponse {
                            success_count: s,
                            failure_count: f,
                            errors,
                        })),
                        Err(e) => Err(handle_error(&e)),
                    };
                }
                return Ok(Response::new(BatchUpsertEmbeddingsResponse {
                    success_count: 0,
                    failure_count: 0,
                    errors: Vec::new(),
                }));
            }
            // Case 2: legacy items path — unchanged behaviour.
            (false, true) => {}
        }

        let total_count = req.items.len() as u32;
        let mut skipped_errors = Vec::new();
        let items: Vec<_> = req
            .items
            .iter()
            .filter_map(
                |item| match item.embedding.as_ref().filter(|e| !e.values.is_empty()) {
                    Some(emb) => Some((
                        item.memory_id,
                        emb.values.clone(),
                        item.embedding_model.clone(),
                    )),
                    None => {
                        skipped_errors.push(format!(
                            "memory {} skipped: missing or empty embedding",
                            item.memory_id
                        ));
                        None
                    }
                },
            )
            .collect();
        let skipped_count = total_count - items.len() as u32;
        match self.app().batch_upsert_embeddings(items).await {
            Ok((s, f, mut errors)) => {
                errors.extend(skipped_errors);
                Ok(Response::new(BatchUpsertEmbeddingsResponse {
                    success_count: s,
                    failure_count: f + skipped_count,
                    errors,
                }))
            }
            Err(e) => Err(handle_error(&e)),
        }
    }

    // ===== Search RPCs =====

    type SearchByVectorStream = BoxStream<'static, Result<MemorySearchResult, tonic::Status>>;

    async fn search_by_vector(
        &self,
        request: tonic::Request<VectorSearchRequest>,
    ) -> Result<tonic::Response<Self::SearchByVectorStream>, tonic::Status> {
        let _s = Self::trace_request("memory_vector", "search_by_vector", &request);
        let req = request.get_ref();
        let vectors: Vec<Vec<f32>> = req.query_vectors.iter().map(|v| v.values.clone()).collect();
        if vectors.is_empty() {
            return Err(tonic::Status::invalid_argument("query_vectors is required"));
        }
        let options = req.options.as_ref();
        let limit = options.map_or(10, |o| o.limit as usize).max(1);
        let include_content = options.and_then(|o| o.include_content).unwrap_or(true);
        let proto_filter = options.and_then(|o| o.filter.as_ref());
        let memory_user_id = proto_filter.and_then(|f| f.user_id);
        let thread_filter = proto_filter.and_then(|f| f.thread_filter.as_ref());
        let filter = proto_filter.and_then(SafeFilter::from_proto_filter);
        let aggregation = options
            .and_then(|o| o.aggregation_strategy)
            .and_then(|v| protobuf::llm_memory::data::AggregationStrategy::try_from(v).ok())
            .map(|s| match s {
                protobuf::llm_memory::data::AggregationStrategy::Sum => AggregationStrategy::Sum,
                protobuf::llm_memory::data::AggregationStrategy::Average => {
                    AggregationStrategy::Average
                }
                protobuf::llm_memory::data::AggregationStrategy::Max => AggregationStrategy::Max,
                protobuf::llm_memory::data::AggregationStrategy::WeightedByPosition => {
                    AggregationStrategy::WeightedByPosition
                }
                protobuf::llm_memory::data::AggregationStrategy::RankFusion => {
                    AggregationStrategy::RankFusion
                }
            });

        let results = self
            .app()
            .search_by_vector(
                &vectors,
                filter.as_ref(),
                thread_filter,
                memory_user_id,
                limit,
                include_content,
                aggregation,
            )
            .await
            .map_err(|e| handle_error(&e))?;

        let media_app = self.media_app().cloned();
        let stream = stream! {
            for item in results {
                let mut wire = to_proto_result(item);
                if let Some(m) = wire.memory.as_mut() {
                    enrich_memory_media(media_app.as_ref(), m).await;
                }
                yield Ok(wire);
            }
        };
        Ok(Response::new(Box::pin(stream)))
    }

    type SearchByTextStream = BoxStream<'static, Result<MemorySearchResult, tonic::Status>>;

    async fn search_by_text(
        &self,
        request: tonic::Request<TextSearchRequest>,
    ) -> Result<tonic::Response<Self::SearchByTextStream>, tonic::Status> {
        let _s = Self::trace_request("memory_vector", "search_by_text", &request);
        let req = request.get_ref();
        if req.query_text.is_empty() {
            return Err(tonic::Status::invalid_argument("query_text is required"));
        }
        let options = req.options.as_ref();
        let limit = options.map_or(10, |o| o.limit as usize).max(1);
        let include_content = options.and_then(|o| o.include_content).unwrap_or(true);
        let proto_filter = options.and_then(|o| o.filter.as_ref());
        let memory_user_id = proto_filter.and_then(|f| f.user_id);
        let thread_filter = proto_filter.and_then(|f| f.thread_filter.as_ref());
        let filter = proto_filter.and_then(SafeFilter::from_proto_filter);

        let results = self
            .app()
            .search_by_text(
                &req.query_text,
                filter.as_ref(),
                thread_filter,
                memory_user_id,
                limit,
                include_content,
            )
            .await
            .map_err(|e| handle_error(&e))?;

        let media_app = self.media_app().cloned();
        let stream = stream! {
            for item in results {
                let mut wire = to_proto_result(item);
                if let Some(m) = wire.memory.as_mut() {
                    enrich_memory_media(media_app.as_ref(), m).await;
                }
                yield Ok(wire);
            }
        };
        Ok(Response::new(Box::pin(stream)))
    }

    type HybridSearchStream = BoxStream<'static, Result<MemorySearchResult, tonic::Status>>;

    async fn hybrid_search(
        &self,
        request: tonic::Request<HybridSearchRequest>,
    ) -> Result<tonic::Response<Self::HybridSearchStream>, tonic::Status> {
        let _s = Self::trace_request("memory_vector", "hybrid_search", &request);
        let req = request.get_ref();
        let vectors: Vec<Vec<f32>> = req.query_vectors.iter().map(|v| v.values.clone()).collect();
        if vectors.is_empty() {
            return Err(tonic::Status::invalid_argument("query_vectors is required"));
        }
        if vectors.len() > 1 {
            return Err(tonic::Status::unimplemented(
                "hybrid search does not support multiple query vectors; \
                 provide exactly one vector, or use vector-only search for multi-vector aggregation",
            ));
        }
        if req.query_text.is_empty() {
            return Err(tonic::Status::invalid_argument("query_text is required"));
        }

        let options = req.options.as_ref();
        let limit = options.map_or(10, |o| o.limit as usize).max(1);
        let include_content = options.and_then(|o| o.include_content).unwrap_or(true);
        let proto_filter = options.and_then(|o| o.filter.as_ref());
        let memory_user_id = proto_filter.and_then(|f| f.user_id);
        let thread_filter = proto_filter.and_then(|f| f.thread_filter.as_ref());
        let filter = proto_filter.and_then(SafeFilter::from_proto_filter);

        let hybrid_opts = req.hybrid_options.as_ref();
        let hybrid_options = HybridOptions {
            strategy: match hybrid_opts
                .and_then(|h| protobuf::llm_memory::data::HybridStrategy::try_from(h.strategy).ok())
            {
                Some(protobuf::llm_memory::data::HybridStrategy::Weighted) => {
                    HybridStrategy::Weighted
                }
                Some(protobuf::llm_memory::data::HybridStrategy::VectorThenFts) => {
                    HybridStrategy::VectorThenFts
                }
                Some(protobuf::llm_memory::data::HybridStrategy::FtsThenVector) => {
                    HybridStrategy::FtsThenVector
                }
                _ => HybridStrategy::Rrf,
            },
            vector_weight: hybrid_opts.and_then(|h| h.vector_weight),
            rrf_k: hybrid_opts.and_then(|h| h.rrf_k),
        };

        let results = self
            .app()
            .hybrid_search(
                &vectors,
                &req.query_text,
                filter.as_ref(),
                thread_filter,
                memory_user_id,
                limit,
                &hybrid_options,
                include_content,
            )
            .await
            .map_err(|e| handle_error(&e))?;

        let media_app = self.media_app().cloned();
        let stream = stream! {
            for item in results {
                let mut wire = to_proto_result(item);
                if let Some(m) = wire.memory.as_mut() {
                    enrich_memory_media(media_app.as_ref(), m).await;
                }
                yield Ok(wire);
            }
        };
        Ok(Response::new(Box::pin(stream)))
    }

    // ===== Semantic / media search =====
    //
    // Both server-embed the query in the stored-vector model space
    // (embed_text / embed_image) and run the same de-dup'd N-row ANN as
    // SearchByVector, but tag results with a kind-specific score_source
    // because the query came from the same model (unlike the
    // client-supplied vector of SearchByVector).

    type SearchSemanticStream = BoxStream<'static, Result<MemorySearchResult, tonic::Status>>;

    async fn search_semantic(
        &self,
        request: tonic::Request<SemanticTextSearchRequest>,
    ) -> Result<tonic::Response<Self::SearchSemanticStream>, tonic::Status> {
        let _s = Self::trace_request("memory_vector", "search_semantic", &request);
        let req = request.get_ref();
        if req.query_text.trim().is_empty() {
            return Err(tonic::Status::invalid_argument("query_text is required"));
        }
        let options = req.options.as_ref();
        let limit = options.map_or(10, |o| o.limit as usize).max(1);
        let include_content = options.and_then(|o| o.include_content).unwrap_or(true);
        let proto_filter = options.and_then(|o| o.filter.as_ref());
        let memory_user_id = proto_filter.and_then(|f| f.user_id);
        let thread_filter = proto_filter.and_then(|f| f.thread_filter.as_ref());
        let filter = proto_filter.and_then(SafeFilter::from_proto_filter);

        let results = self
            .app()
            .search_semantic(
                &req.query_text,
                filter.as_ref(),
                thread_filter,
                memory_user_id,
                limit,
                include_content,
            )
            .await
            .map_err(|e| handle_error(&e))?;
        let media_app = self.media_app().cloned();
        let stream = stream! {
            for item in results {
                let mut wire = to_proto_result(item);
                if let Some(m) = wire.memory.as_mut() {
                    enrich_memory_media(media_app.as_ref(), m).await;
                }
                yield Ok(wire);
            }
        };
        Ok(Response::new(Box::pin(stream)))
    }

    type SearchByMediaStream = BoxStream<'static, Result<MemorySearchResult, tonic::Status>>;

    async fn search_by_media(
        &self,
        request: tonic::Request<MediaSearchRequest>,
    ) -> Result<tonic::Response<Self::SearchByMediaStream>, tonic::Status> {
        let _s = Self::trace_request("memory_vector", "search_by_media", &request);
        let req = request.get_ref();
        let media_object_id = req
            .media_object_id
            .as_ref()
            .map(|m| m.value)
            .ok_or_else(|| tonic::Status::invalid_argument("media_object_id is required"))?;
        let options = req.options.as_ref();
        let limit = options.map_or(10, |o| o.limit as usize).max(1);
        let include_content = options.and_then(|o| o.include_content).unwrap_or(true);
        let proto_filter = options.and_then(|o| o.filter.as_ref());
        let memory_user_id = proto_filter.and_then(|f| f.user_id);
        let thread_filter = proto_filter.and_then(|f| f.thread_filter.as_ref());
        let filter = proto_filter.and_then(SafeFilter::from_proto_filter);

        // Resolve the query media to a fetchable URL. No media subsystem
        // → the image pipeline is not deployed here.
        let media_app = self.media_app().ok_or_else(|| {
            tonic::Status::failed_precondition(
                "SearchByMedia is unavailable: media subsystem not configured",
            )
        })?;
        let resolved = media_app
            .resolve(media_object_id, None)
            .await
            .map_err(|e| handle_error(&e))?;
        // unresolved (no body: storage_backend=unresolvable / gc_state
        // {3,4}) or a missing URL → cannot embed_image. Fail fast rather
        // than passing an empty URL to the runner.
        if resolved.unresolved || resolved.url.as_deref().unwrap_or("").is_empty() {
            return Err(tonic::Status::failed_precondition(
                "query media is unresolved (no body); upload the image \
                 bytes first, then retry",
            ));
        }
        let image_url = resolved.url.expect("checked non-empty above");

        let results = self
            .app()
            .search_by_media_url(
                &image_url,
                filter.as_ref(),
                thread_filter,
                memory_user_id,
                limit,
                include_content,
            )
            .await
            .map_err(|e| handle_error(&e))?;
        let media_app = self.media_app().cloned();
        let stream = stream! {
            for item in results {
                let mut wire = to_proto_result(item);
                if let Some(m) = wire.memory.as_mut() {
                    enrich_memory_media(media_app.as_ref(), m).await;
                }
                yield Ok(wire);
            }
        };
        Ok(Response::new(Box::pin(stream)))
    }

    // ===== Surrounding memories =====

    async fn get_surrounding_memories(
        &self,
        request: tonic::Request<SurroundingMemoriesRequest>,
    ) -> Result<tonic::Response<SurroundingMemoriesResponse>, tonic::Status> {
        let _s = Self::trace_request("memory_vector", "get_surrounding_memories", &request);
        let req = request.get_ref();
        let before_count = if req.before_count == 0 {
            5
        } else {
            req.before_count as usize
        };
        let after_count = if req.after_count == 0 {
            5
        } else {
            req.after_count as usize
        };

        // Backward compat: `thread_id` is a new field added in Phase 4, so
        // legacy clients that only know the pre-Phase-4 wire format will
        // send the default value 0. Rather than reject every legacy
        // request with `invalid_argument`, try to resolve the pivot
        // memory's thread through the junction table: if it belongs to
        // exactly one thread we use that, otherwise we fail loudly
        // (shared memories like ROLE_SYSTEM prompts would give ambiguous
        // answers under the old single-thread contract).
        let resolved_thread_id = if req.thread_id == 0 {
            match self
                .app()
                .resolve_single_thread_for_memory(req.memory_id)
                .await
            {
                Ok(tid) => tid,
                Err(e) => return Err(handle_error(&e)),
            }
        } else {
            req.thread_id
        };

        match self
            .app()
            .get_surrounding_memories(req.memory_id, resolved_thread_id, before_count, after_count)
            .await
        {
            Ok(result) => Ok(Response::new(SurroundingMemoriesResponse {
                before: result.before,
                after: result.after,
                target: Some(result.target),
                has_more_before: result.has_more_before,
                has_more_after: result.has_more_after,
            })),
            Err(e) => Err(handle_error(&e)),
        }
    }

    // ===== Index management =====

    async fn rebuild_index(
        &self,
        request: tonic::Request<RebuildIndexRequest>,
    ) -> Result<tonic::Response<RebuildIndexResponse>, tonic::Status> {
        let _s = Self::trace_request("memory_vector", "rebuild_index", &request);
        let req = request.get_ref();
        // TODO: implement filtered rebuild using user_id/thread_id
        if req.user_id.is_some() || req.thread_id.is_some() {
            tracing::warn!(
                "rebuild_index: user_id/thread_id filters are not yet implemented, rebuilding all"
            );
        }
        match self.app().rebuild_index().await {
            Ok((indexed, skipped, duration_ms)) => Ok(Response::new(RebuildIndexResponse {
                indexed_count: indexed,
                skipped_count: skipped,
                duration_ms,
            })),
            Err(e) => Err(handle_error(&e)),
        }
    }

    async fn get_index_stats(
        &self,
        request: tonic::Request<GetIndexStatsRequest>,
    ) -> Result<tonic::Response<IndexStatsResponse>, tonic::Status> {
        let _s = Self::trace_request("memory_vector", "get_index_stats", &request);
        match self.app().get_stats().await {
            Ok(stats) => {
                let wire = stats.to_wire();
                Ok(Response::new(IndexStatsResponse {
                    total_records: stats.total_records,
                    // All LanceDB records have embeddings by design;
                    // these fields are correct for the current
                    // architecture where only embedded records are
                    // stored in the vector index.
                    records_with_embedding: stats.total_records,
                    records_without_embedding: 0,
                    vector_dimension: wire.vector_dimension,
                    distance_type: wire.distance_type,
                    last_optimized_at: wire.last_optimized_at,
                    fts_tokenizer: wire.fts_tokenizer,
                    fts_ngram_min: wire.fts_ngram_min,
                    fts_ngram_max: wire.fts_ngram_max,
                }))
            }
            Err(e) => Err(handle_error(&e)),
        }
    }

    // P2 (Phase 5-1 + 5-2): FILTER_ONLY / TEXT count exactly, VECTOR /
    // HYBRID count up to `MEMORY_COUNT_VECTOR_HARD_CAP`. Errors flow
    // through `error_handle::handle_error` to the appropriate tonic
    // Status (InvalidArgument / FailedPrecondition / Internal).
    async fn count_search_matches(
        &self,
        request: tonic::Request<MemorySearchCountRequest>,
    ) -> Result<tonic::Response<MemorySearchCountResponse>, tonic::Status> {
        let _s = Self::trace_request("memory_vector", "count_search_matches", &request);
        let req = request.get_ref();

        // UNSPECIFIED (and any unknown enum tag) is rejected explicitly
        // so a hybrid request with a missing `mode` is not silently
        // demoted to TEXT (which would ignore `query_vectors`). See
        // proto comment on CountSearchMode.
        let mode = CountMode::from_proto_i32(req.mode).ok_or_else(|| {
            tonic::Status::invalid_argument(
                "CountSearchMatches: `mode` must be set explicitly \
                 (received UNSPECIFIED). Pass FILTER_ONLY / TEXT / VECTOR / HYBRID.",
            )
        })?;

        // VECTOR / HYBRID inputs. FILTER_ONLY and TEXT ignore them;
        // decoding here keeps the handler shape uniform.
        let query_vectors: Vec<Vec<f32>> =
            req.query_vectors.iter().map(|v| v.values.clone()).collect();
        let hybrid_options =
            super::vector_decode::decode_hybrid_options(req.hybrid_options.as_ref());

        let input = CountSearchInput {
            mode,
            filter: req.filter.as_ref(),
            query_text: req.query_text.as_deref(),
            query_vectors: &query_vectors,
            // Aggregation strategy is not on the proto Count request
            // (per `MemorySearchCountRequest` in
            // `memory_vector.proto`); the app layer always treats
            // multi-vector counts as a union, so the field is
            // hard-coded `None` here.
            aggregation: None,
            hybrid_options: hybrid_options.as_ref(),
        };

        let output = self
            .app()
            .count_search_matches(input)
            .await
            .map_err(|e| handle_error(&e))?;

        Ok(tonic::Response::new(MemorySearchCountResponse {
            total: output.total,
            is_truncated: output.is_truncated,
            mode: output.mode.to_proto_i32(),
        }))
    }

    async fn redispatch_embeddings(
        &self,
        request: tonic::Request<RedispatchEmbeddingsRequest>,
    ) -> Result<tonic::Response<RedispatchEmbeddingsResponse>, tonic::Status> {
        let _s = Self::trace_request("memory_vector", "redispatch_embeddings", &request);
        let req = request.get_ref();
        let kinds = validate_redispatch_kinds(&req.kinds)?;
        match self
            .app()
            .redispatch_embeddings(req.user_id, req.thread_id, req.batch_size, &kinds)
            .await
        {
            Ok((dispatched, skipped, failed, duration_ms)) => {
                Ok(Response::new(RedispatchEmbeddingsResponse {
                    dispatched_count: dispatched,
                    skipped_count: skipped,
                    duration_ms,
                    failed_count: failed,
                }))
            }
            Err(e) => Err(handle_error(&e)),
        }
    }
}

#[derive(DebugStub)]
pub(crate) struct MemoryVectorGrpcImpl {
    #[debug_stub = "Arc<MemoryVectorAppImpl>"]
    app: std::sync::Arc<MemoryVectorAppImpl>,
    #[debug_stub = "Option<Arc<MediaAppImpl>>"]
    media_app: Option<std::sync::Arc<app::app::media::MediaAppImpl>>,
}

impl MemoryVectorGrpcImpl {
    pub fn new(app: std::sync::Arc<MemoryVectorAppImpl>) -> Self {
        Self {
            app,
            media_app: None,
        }
    }

    /// Share the one `MediaApp` Arc (same instance as MediaService /
    /// MemoryService) so SearchByMedia can Resolve the query media
    /// without a second storage backend.
    pub fn with_media_app(
        mut self,
        media_app: std::sync::Arc<app::app::media::MediaAppImpl>,
    ) -> Self {
        self.media_app = Some(media_app);
        self
    }
}

impl MemoryVectorGrpc for MemoryVectorGrpcImpl {
    fn app(&self) -> &MemoryVectorAppImpl {
        &self.app
    }

    fn media_app(&self) -> Option<&std::sync::Arc<app::app::media::MediaAppImpl>> {
        self.media_app.as_ref()
    }
}

impl Tracing for MemoryVectorGrpcImpl {}

/// Borrowed view of an `UpsertEmbeddingRequest` after structural
/// validation. Lifetimes tie back to the original request so we avoid
/// cloning the embedding vector in the hot path.
#[derive(Debug)]
pub(crate) struct ValidatedUpsertEmbedding<'a> {
    pub memory_id: i64,
    pub values: &'a [f32],
    pub embedding_model: Option<&'a str>,
}

/// Pure validation for `UpsertEmbedding` requests.
///
/// - `Err(Status)` — request is malformed (missing `embedding` message).
/// - `Ok(None)` — request is well-formed but carries an empty vector.
///   Treated as a no-op skip by the caller; logged at DEBUG so
///   short-input cases (where the upstream embedding model declines
///   to embed) don't spam WARN.
/// - `Ok(Some(_))` — proceed with upsert.
pub(crate) fn validate_upsert_embedding_request(
    req: &UpsertEmbeddingRequest,
) -> Result<Option<ValidatedUpsertEmbedding<'_>>, tonic::Status> {
    let embedding = req
        .embedding
        .as_ref()
        .ok_or_else(|| tonic::Status::invalid_argument("embedding is required"))?;
    if embedding.values.is_empty() {
        tracing::debug!(
            memory_id = req.memory_id,
            "upsert_embedding skipped: empty vector (input likely below the embedding model's minimum length)"
        );
        return Ok(None);
    }
    Ok(Some(ValidatedUpsertEmbedding {
        memory_id: req.memory_id,
        values: &embedding.values,
        embedding_model: req.embedding_model.as_deref(),
    }))
}

/// Validate `RedispatchEmbeddingsRequest.kinds`.
///
/// Proto contract: empty = all kinds; a non-empty list is a FILTER on
/// which dispatch pipelines to (re)run. Map the wire `DispatchKind`
/// discriminants to the internal 2-value enum.
///
/// - `Err(invalid_argument)` — contains `DISPATCH_KIND_UNSPECIFIED` or an
///   out-of-range value (caller must send explicit, known kinds).
/// - `Ok(vec)` — the resolved filter (empty vec = all kinds: proceed
///   without filtering). Pure (no app / I/O) so it is unit-tested.
fn validate_redispatch_kinds(kinds: &[i32]) -> Result<Vec<InfraDispatchKind>, tonic::Status> {
    let mut out = Vec::with_capacity(kinds.len());
    for &k in kinds {
        match InfraDispatchKind::try_from(k) {
            Ok(kind) => out.push(kind),
            // UNSPECIFIED(0) / out-of-range: reject rather than silently
            // running an all-kinds redispatch for a partial request.
            Err(()) => {
                return Err(tonic::Status::invalid_argument(
                    "RedispatchEmbeddings: `kinds` must contain only \
                     DISPATCH_KIND_TEXT or DISPATCH_KIND_MEDIA",
                ));
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protobuf::llm_memory::data::EmbeddingVector;
    // The wire enum (UNSPECIFIED/TEXT/MEDIA) — used here to build request
    // discriminants; production code maps it via `InfraDispatchKind`.
    use crate::protobuf::llm_memory::service::DispatchKind;

    fn req(
        memory_id: i64,
        embedding: Option<EmbeddingVector>,
        model: Option<&str>,
    ) -> UpsertEmbeddingRequest {
        UpsertEmbeddingRequest {
            memory_id,
            embedding,
            embedding_model: model.map(str::to_owned),
        }
    }

    #[test]
    fn rejects_missing_embedding_with_invalid_argument() {
        let r = req(1, None, None);
        let err = validate_upsert_embedding_request(&r).expect_err("should error");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(err.message().contains("embedding is required"));
    }

    #[test]
    fn skips_empty_vector_as_none() {
        let r = req(
            42,
            Some(EmbeddingVector { values: vec![] }),
            Some("test-model"),
        );
        let out = validate_upsert_embedding_request(&r).expect("ok");
        assert!(
            out.is_none(),
            "empty vector must short-circuit to skip, not reach the app layer"
        );
    }

    #[test]
    fn passes_through_non_empty_vector() {
        let r = req(
            7,
            Some(EmbeddingVector {
                values: vec![0.1, 0.2, 0.3],
            }),
            Some("nomic-embed-text"),
        );
        let v = validate_upsert_embedding_request(&r)
            .expect("ok")
            .expect("some");
        assert_eq!(v.memory_id, 7);
        assert_eq!(v.values, &[0.1, 0.2, 0.3]);
        assert_eq!(v.embedding_model, Some("nomic-embed-text"));
    }

    #[test]
    fn forwards_missing_model_as_none() {
        let r = req(
            8,
            Some(EmbeddingVector {
                values: vec![1.0, 2.0],
            }),
            None,
        );
        let v = validate_upsert_embedding_request(&r)
            .expect("ok")
            .expect("some");
        assert_eq!(v.embedding_model, None);
    }

    // ===== Image memory Phase 4: RedispatchEmbeddings.kinds =====

    #[test]
    fn redispatch_kinds_empty_is_all_kinds() {
        let resolved =
            validate_redispatch_kinds(&[]).expect("empty kinds = all kinds (back-compat)");
        assert!(resolved.is_empty(), "empty wire kinds → no filter");
    }

    #[test]
    fn redispatch_kinds_unspecified_is_invalid_argument() {
        let unspec = DispatchKind::Unspecified as i32;
        let err = validate_redispatch_kinds(&[unspec]).expect_err("must reject UNSPECIFIED");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        // Mixed-in UNSPECIFIED is rejected even alongside a valid kind.
        let text = DispatchKind::Text as i32;
        let err = validate_redispatch_kinds(&[text, unspec]).expect_err("must reject");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        // Out-of-range discriminant is rejected too.
        let err = validate_redispatch_kinds(&[99]).expect_err("must reject out-of-range");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    /// The Phase 5 filter: a non-empty `kinds` list now resolves to the
    /// internal enum (no longer Unimplemented). `[MEDIA]` / `[TEXT]` /
    /// both map 1:1 and are applied as a filter by `redispatch_embeddings`.
    #[test]
    fn redispatch_nonempty_kinds_resolves_to_filter() {
        let text = DispatchKind::Text as i32;
        let media = DispatchKind::Media as i32;
        assert_eq!(
            validate_redispatch_kinds(&[text]).unwrap(),
            vec![InfraDispatchKind::Text]
        );
        assert_eq!(
            validate_redispatch_kinds(&[media]).unwrap(),
            vec![InfraDispatchKind::Media]
        );
        assert_eq!(
            validate_redispatch_kinds(&[text, media]).unwrap(),
            vec![InfraDispatchKind::Text, InfraDispatchKind::Media]
        );
    }
}
