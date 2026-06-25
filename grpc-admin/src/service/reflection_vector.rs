//! `ReflectionVectorService` adapter (Phase E).
//!
//! Like `reflection.rs`, the proto code lives in the shared `protobuf`
//! crate only — grpc-admin does not regenerate the service stubs.

use app::app::reflection::{ReflectionApp, ReflectionAppImpl};
use command_utils::trace::Tracing;
use protobuf::llm_memory::data::EmbeddingKind;
use protobuf::llm_memory::service::reflection_vector_service_server::ReflectionVectorService;
use protobuf::llm_memory::service::{
    BatchUpsertEmbeddingsResponse, BatchUpsertIntentEmbeddingsRowsRequest, GetIndexStatsRequest,
    IndexStatsResponse, RebuildIndexRequest, RebuildIndexResponse,
    RedispatchReflectionEmbeddingsRequest, RedispatchReflectionEmbeddingsResponse,
};
use tonic::Response;

use crate::service::error_handle::handle_error;

#[derive(Clone, DebugStub)]
pub(crate) struct ReflectionVectorGrpcImpl {
    #[debug_stub = "ReflectionAppImpl"]
    reflection_app: std::sync::Arc<ReflectionAppImpl>,
}

impl ReflectionVectorGrpcImpl {
    pub fn new(reflection_app: std::sync::Arc<ReflectionAppImpl>) -> Self {
        Self { reflection_app }
    }

    fn app(&self) -> &ReflectionAppImpl {
        &self.reflection_app
    }
}

impl Tracing for ReflectionVectorGrpcImpl {}

#[tonic::async_trait]
impl ReflectionVectorService for ReflectionVectorGrpcImpl {
    async fn batch_upsert_intent_embeddings(
        &self,
        request: tonic::Request<BatchUpsertIntentEmbeddingsRowsRequest>,
    ) -> Result<tonic::Response<BatchUpsertEmbeddingsResponse>, tonic::Status> {
        let _s = Self::trace_request(
            "reflection_vector",
            "batch_upsert_intent_embeddings",
            &request,
        );
        let req = request.into_inner();
        let id = req
            .reflection_id
            .as_ref()
            .ok_or_else(|| tonic::Status::invalid_argument("reflection_id is required"))?;
        if req.replace_kinds.is_empty() {
            return Err(tonic::Status::invalid_argument(
                "BatchUpsertIntentEmbeddings: `replace_kinds` is required \
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
                        "BatchUpsertIntentEmbeddings: row (vector_kind={}, \
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
            .batch_upsert_intent_embeddings_rows(
                id,
                req.embedding_model.as_deref(),
                &req.replace_kinds,
                rows,
            )
            .await
        {
            Ok((success_count, failure_count, errors)) => {
                Ok(Response::new(BatchUpsertEmbeddingsResponse {
                    success_count,
                    failure_count,
                    errors,
                }))
            }
            Err(e) => Err(handle_error(&e)),
        }
    }

    async fn redispatch_reflection_embeddings(
        &self,
        request: tonic::Request<RedispatchReflectionEmbeddingsRequest>,
    ) -> Result<tonic::Response<RedispatchReflectionEmbeddingsResponse>, tonic::Status> {
        let _s = Self::trace_request(
            "reflection_vector",
            "redispatch_reflection_embeddings",
            &request,
        );
        let req = request.get_ref();
        let kind = EmbeddingKind::try_from(req.kind)
            .map_err(|_| tonic::Status::invalid_argument("invalid kind"))?;
        match self
            .app()
            .redispatch_reflection_embeddings(kind, req.filter.as_ref(), req.batch_size)
            .await
        {
            Ok(resp) => Ok(Response::new(resp)),
            Err(e) => Err(handle_error(&e)),
        }
    }

    async fn rebuild_intent_index(
        &self,
        request: tonic::Request<RebuildIndexRequest>,
    ) -> Result<tonic::Response<RebuildIndexResponse>, tonic::Status> {
        let _s = Self::trace_request("reflection_vector", "rebuild_intent_index", &request);
        // Phase E surface: returns Unimplemented until the
        // `ReflectionIntentVectorRepository::rebuild_index` plumbing is
        // wired through the ReflectionApp trait (Phase F).
        let _ = request;
        Err(tonic::Status::unimplemented(
            "RebuildIntentIndex awaiting Phase F intent-vector repo wiring",
        ))
    }

    async fn get_intent_index_stats(
        &self,
        request: tonic::Request<GetIndexStatsRequest>,
    ) -> Result<tonic::Response<IndexStatsResponse>, tonic::Status> {
        let _s = Self::trace_request("reflection_vector", "get_intent_index_stats", &request);
        let _ = request;
        Err(tonic::Status::unimplemented(
            "GetIntentIndexStats awaiting Phase F intent-vector repo wiring",
        ))
    }
}
