use std::fmt::Debug;

// ThreadSearchOptions and Thread are used via generated code, not directly imported
use crate::protobuf::llm_memory::service::thread_vector_service_server::ThreadVectorService;
use crate::protobuf::llm_memory::service::{
    BatchUpsertThreadEmbeddingsResponse, BatchUpsertThreadEmbeddingsRowsRequest,
    ThreadGetIndexStatsRequest, ThreadHybridSearchRequest, ThreadIndexStatsResponse,
    ThreadRebuildIndexRequest, ThreadRebuildIndexResponse, ThreadRedispatchEmbeddingsRequest,
    ThreadRedispatchEmbeddingsResponse, ThreadSearchCountRequest, ThreadSearchCountResponse,
    ThreadSearchResult, ThreadTextSearchRequest, ThreadVectorSearchRequest,
};
use crate::service::error_handle::handle_error;
use crate::service::memory_kind::normalize_thread_search_filter;
use app::app::memory_vector::CountMode;
use app::app::thread_vector::ThreadVectorAppImpl;
use async_stream::stream;
use command_utils::trace::Tracing;
use futures::stream::BoxStream;
use infra::infra::memory_vector::repository::{AggregationStrategy, HybridOptions, HybridStrategy};
use infra::infra::thread_vector::safe_filter::ThreadSafeFilter;
use tonic::Response;

pub trait ThreadVectorGrpc {
    fn app(&self) -> &ThreadVectorAppImpl;
}

#[tonic::async_trait]
impl<T: ThreadVectorGrpc + Tracing + Send + Debug + Sync + 'static> ThreadVectorService for T {
    type SearchByVectorStream = BoxStream<'static, Result<ThreadSearchResult, tonic::Status>>;

    #[tracing::instrument]
    async fn search_by_vector(
        &self,
        request: tonic::Request<ThreadVectorSearchRequest>,
    ) -> Result<Response<Self::SearchByVectorStream>, tonic::Status> {
        let req = request.into_inner();
        let query_vectors: Vec<Vec<f32>> =
            req.query_vectors.into_iter().map(|v| v.values).collect();
        if query_vectors.is_empty() {
            return Err(tonic::Status::invalid_argument(
                "query_vectors must not be empty",
            ));
        }
        let mut options = req.options.unwrap_or_default();
        if let Some(filter) = options.filter.as_mut() {
            normalize_thread_search_filter(filter)?;
        }
        if options.distance_type.is_some() {
            tracing::warn!(
                "distance_type in ThreadSearchOptions is ignored; distance type is configured per-table"
            );
        }
        let limit = if options.limit == 0 {
            10
        } else {
            options.limit as usize
        };
        let include_description = options.include_description.unwrap_or(true);
        let filter = options
            .filter
            .as_ref()
            .and_then(ThreadSafeFilter::from_proto_filter);
        let aggregation = options
            .aggregation_strategy
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

        match self
            .app()
            .search_by_vector(
                &query_vectors,
                limit,
                filter.as_ref(),
                include_description,
                aggregation,
            )
            .await
        {
            Ok(results) => {
                let s = stream! {
                    for item in results {
                        yield Ok(ThreadSearchResult {
                            thread: Some(item.thread),
                            score: item.score,
                            distance: item.distance,
                            score_source: protobuf::llm_memory::data::ScoreSource::ScoreVector as i32,
                            highlights: item.highlights,
                        });
                    }
                };
                Ok(Response::new(Box::pin(s) as Self::SearchByVectorStream))
            }
            Err(e) => Err(handle_error(&e)),
        }
    }

    type SearchByTextStream = BoxStream<'static, Result<ThreadSearchResult, tonic::Status>>;

    #[tracing::instrument]
    async fn search_by_text(
        &self,
        request: tonic::Request<ThreadTextSearchRequest>,
    ) -> Result<Response<Self::SearchByTextStream>, tonic::Status> {
        let req = request.into_inner();
        if req.query_text.is_empty() {
            return Err(tonic::Status::invalid_argument(
                "query_text must not be empty",
            ));
        }
        if req.fts_options.is_some() {
            tracing::warn!(
                "fts_options in ThreadTextSearchRequest is ignored; thread FTS uses fixed column 'description'"
            );
        }
        let mut options = req.options.unwrap_or_default();
        if let Some(filter) = options.filter.as_mut() {
            normalize_thread_search_filter(filter)?;
        }
        if options.distance_type.is_some() {
            tracing::warn!(
                "distance_type in ThreadSearchOptions is ignored; distance type is configured per-table"
            );
        }
        let limit = if options.limit == 0 {
            10
        } else {
            options.limit as usize
        };
        let include_description = options.include_description.unwrap_or(true);
        let filter = options
            .filter
            .as_ref()
            .and_then(ThreadSafeFilter::from_proto_filter);

        match self
            .app()
            .search_by_text(&req.query_text, limit, filter.as_ref(), include_description)
            .await
        {
            Ok(results) => {
                let s = stream! {
                    for item in results {
                        yield Ok(ThreadSearchResult {
                            thread: Some(item.thread),
                            score: item.score,
                            distance: item.distance,
                            score_source: protobuf::llm_memory::data::ScoreSource::ScoreText as i32,
                            highlights: item.highlights,
                        });
                    }
                };
                Ok(Response::new(Box::pin(s) as Self::SearchByTextStream))
            }
            Err(e) => Err(handle_error(&e)),
        }
    }

    type HybridSearchStream = BoxStream<'static, Result<ThreadSearchResult, tonic::Status>>;

    #[tracing::instrument]
    async fn hybrid_search(
        &self,
        request: tonic::Request<ThreadHybridSearchRequest>,
    ) -> Result<Response<Self::HybridSearchStream>, tonic::Status> {
        let req = request.into_inner();
        if req.query_vectors.len() != 1 {
            return Err(tonic::Status::invalid_argument(
                "hybrid search requires exactly one query vector",
            ));
        }
        if req.query_text.is_empty() {
            return Err(tonic::Status::invalid_argument(
                "query_text must not be empty",
            ));
        }
        if req.fts_options.is_some() {
            tracing::warn!(
                "fts_options in ThreadHybridSearchRequest is ignored; thread FTS uses fixed column 'description'"
            );
        }
        let query_vector = &req.query_vectors[0].values;
        let mut options = req.options.unwrap_or_default();
        if let Some(filter) = options.filter.as_mut() {
            normalize_thread_search_filter(filter)?;
        }
        if options.distance_type.is_some() {
            tracing::warn!(
                "distance_type in ThreadSearchOptions is ignored; distance type is configured per-table"
            );
        }
        let limit = if options.limit == 0 {
            10
        } else {
            options.limit as usize
        };
        let include_description = options.include_description.unwrap_or(true);
        let filter = options
            .filter
            .as_ref()
            .and_then(ThreadSafeFilter::from_proto_filter);

        let hybrid_options = if let Some(ho) = req.hybrid_options {
            let strategy = match ho.strategy {
                1 => HybridStrategy::Weighted,
                2 => HybridStrategy::VectorThenFts,
                3 => HybridStrategy::FtsThenVector,
                _ => HybridStrategy::Rrf,
            };
            HybridOptions {
                strategy,
                vector_weight: ho.vector_weight,
                rrf_k: ho.rrf_k,
            }
        } else {
            HybridOptions {
                strategy: HybridStrategy::Rrf,
                vector_weight: None,
                rrf_k: None,
            }
        };

        match self
            .app()
            .hybrid_search(
                query_vector,
                &req.query_text,
                limit,
                filter.as_ref(),
                include_description,
                &hybrid_options,
            )
            .await
        {
            Ok(results) => {
                let s = stream! {
                    for item in results {
                        yield Ok(ThreadSearchResult {
                            thread: Some(item.thread),
                            score: item.score,
                            distance: item.distance,
                            score_source: protobuf::llm_memory::data::ScoreSource::ScoreHybrid as i32,
                            highlights: item.highlights,
                        });
                    }
                };
                Ok(Response::new(Box::pin(s) as Self::HybridSearchStream))
            }
            Err(e) => Err(handle_error(&e)),
        }
    }

    #[tracing::instrument]
    async fn batch_upsert_embeddings_rows(
        &self,
        request: tonic::Request<BatchUpsertThreadEmbeddingsRowsRequest>,
    ) -> Result<Response<BatchUpsertThreadEmbeddingsResponse>, tonic::Status> {
        let _s = Self::trace_request("thread_vector", "batch_upsert_embeddings_rows", &request);
        let req = request.into_inner();
        if req.replace_kinds.is_empty() {
            return Err(tonic::Status::invalid_argument(
                "BatchUpsertEmbeddingsRows: `replace_kinds` is required \
                 (which vector_kinds to replace)",
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
                        "BatchUpsertEmbeddingsRows: row (vector_kind={}, \
                         chunk_index={}) has a missing or empty embedding",
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
        match self
            .app()
            .batch_upsert_thread_embeddings_rows(
                req.thread_id,
                req.embedding_model.as_deref(),
                &req.replace_kinds,
                rows,
            )
            .await
        {
            Ok((success_count, failure_count, errors)) => {
                Ok(Response::new(BatchUpsertThreadEmbeddingsResponse {
                    success_count,
                    failure_count,
                    errors,
                }))
            }
            Err(e) => Err(handle_error(&e)),
        }
    }

    #[tracing::instrument]
    async fn redispatch_embeddings(
        &self,
        request: tonic::Request<ThreadRedispatchEmbeddingsRequest>,
    ) -> Result<Response<ThreadRedispatchEmbeddingsResponse>, tonic::Status> {
        let _s = Self::trace_request("thread_vector", "redispatch_embeddings", &request);
        let req = request.into_inner();
        match self
            .app()
            .redispatch_embeddings(req.user_id, req.batch_size)
            .await
        {
            Ok((dispatched_count, skipped_count, failed_count, duration_ms)) => {
                Ok(Response::new(ThreadRedispatchEmbeddingsResponse {
                    dispatched_count,
                    skipped_count,
                    failed_count,
                    duration_ms,
                }))
            }
            Err(e) => Err(handle_error(&e)),
        }
    }

    #[tracing::instrument]
    async fn rebuild_index(
        &self,
        request: tonic::Request<ThreadRebuildIndexRequest>,
    ) -> Result<Response<ThreadRebuildIndexResponse>, tonic::Status> {
        let req = request.into_inner();
        #[allow(deprecated)]
        match self.app().rebuild_index(req.user_id).await {
            Ok((intact, orphaned, missing, duration_ms)) => {
                Ok(Response::new(ThreadRebuildIndexResponse {
                    indexed_count: intact,
                    orphaned_count: orphaned,
                    missing_count: missing,
                    duration_ms,
                }))
            }
            Err(e) => Err(handle_error(&e)),
        }
    }

    #[tracing::instrument]
    async fn get_index_stats(
        &self,
        _request: tonic::Request<ThreadGetIndexStatsRequest>,
    ) -> Result<Response<ThreadIndexStatsResponse>, tonic::Status> {
        match self.app().get_stats().await {
            Ok(stats) => {
                let rdb_total = stats.rdb_total_records.unwrap_or(stats.total_records);
                let without = rdb_total.saturating_sub(stats.total_records);
                let wire = stats.to_wire();
                Ok(Response::new(ThreadIndexStatsResponse {
                    total_records: rdb_total,
                    records_with_embedding: stats.total_records,
                    records_without_embedding: without,
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
    // HYBRID count up to `MEMORY_COUNT_VECTOR_HARD_CAP`.
    async fn count_search_matches(
        &self,
        request: tonic::Request<ThreadSearchCountRequest>,
    ) -> Result<Response<ThreadSearchCountResponse>, tonic::Status> {
        let _s = Self::trace_request("thread_vector", "count_search_matches", &request);
        let mut req = request.into_inner();

        let mode = CountMode::from_proto_i32(req.mode).ok_or_else(|| {
            tonic::Status::invalid_argument(
                "ThreadVectorService::CountSearchMatches: `mode` must be set explicitly \
                 (received UNSPECIFIED). Pass FILTER_ONLY / TEXT / VECTOR / HYBRID.",
            )
        })?;

        if let Some(filter) = req.filter.as_mut() {
            normalize_thread_search_filter(filter)?;
        }
        let filter = req
            .filter
            .as_ref()
            .and_then(ThreadSafeFilter::from_proto_filter);

        let query_vectors: Vec<Vec<f32>> =
            req.query_vectors.iter().map(|v| v.values.clone()).collect();
        let hybrid_options =
            super::vector_decode::decode_hybrid_options(req.hybrid_options.as_ref());

        let output = self
            .app()
            .count_search_matches(
                mode,
                filter.as_ref(),
                req.query_text.as_deref(),
                &query_vectors,
                None,
                hybrid_options.as_ref(),
            )
            .await
            .map_err(|e| handle_error(&e))?;

        Ok(Response::new(ThreadSearchCountResponse {
            total: output.total,
            is_truncated: output.is_truncated,
            mode: output.mode.to_proto_i32(),
        }))
    }
}

#[derive(debug_stub_derive::DebugStub)]
pub(crate) struct ThreadVectorGrpcImpl {
    #[debug_stub = "Arc<ThreadVectorAppImpl>"]
    app: std::sync::Arc<ThreadVectorAppImpl>,
}

impl ThreadVectorGrpcImpl {
    pub fn new(app: std::sync::Arc<ThreadVectorAppImpl>) -> Self {
        Self { app }
    }
}

impl ThreadVectorGrpc for ThreadVectorGrpcImpl {
    fn app(&self) -> &ThreadVectorAppImpl {
        &self.app
    }
}

impl Tracing for ThreadVectorGrpcImpl {}
