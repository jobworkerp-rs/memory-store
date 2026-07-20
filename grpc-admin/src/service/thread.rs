use std::{fmt::Debug, time::Duration};

use crate::protobuf::llm_memory::data::{Memory, Thread, ThreadData, ThreadId};
use crate::protobuf::llm_memory::service::add_memories_batch_request::ThreadTarget as AddMemoriesBatchThreadTarget;
use crate::protobuf::llm_memory::service::thread_service_server::ThreadService;
use crate::protobuf::llm_memory::service::{
    AddLabelsRequest, AddMemoriesBatchRequest, AddMemoriesBatchResponse,
    AddMemoryOutcome as AddMemoryOutcomeProto, AddMemoryRequest, AddMemoryResponse, CountResponse,
    CreateThreadResponse, FindCoOccurringLabelsRequest, FindCoOccurringLabelsResponse,
    FindCondition, FindDistinctLabelsRequest, FindDistinctLabelsResponse, FindListRequest,
    FindMemoriesByThreadIdRequest, FindThreadListByLabelsRequest, FindThreadListByUserIdRequest,
    MemoryWithPosition as MemoryWithPositionProto, OptionalThreadResponse, RemoveLabelsRequest,
    ResolveAncestorClosureRequest, ResolveAncestorClosureResponse, SearchLabelsRequest,
    SearchLabelsResponse, SuccessResponse, UpdateMemoryParentsRequest, UpdateMemoryParentsResponse,
    UpdateMemoryParentsSkipReason,
};
use crate::service::error_handle::handle_error;
use crate::service::memory::enrich_memory_media;
use crate::service::memory_kind::normalize_memory_kinds;
use app::app::thread::{
    AddMemoriesBatchInput, AddMemoryOutcome as AppAddMemoryOutcome, BatchMemoryInput,
    BatchThreadTarget, MemoryWithPosition, ThreadApp, ThreadAppImpl, ThreadListOptions,
    UpdateMemoryParentsOutcome, UpdateParentsSkipReason,
};
use async_stream::stream;
use command_utils::trace::Tracing;
use futures::stream::BoxStream;
use infra::infra::thread::rdb::ThreadSort;
use tonic::Response;

fn validated_memory_kinds(memory_kinds: &[i32]) -> Result<Vec<i32>, tonic::Status> {
    let mut memory_kinds = memory_kinds.to_vec();
    normalize_memory_kinds(&mut memory_kinds)?;
    Ok(memory_kinds)
}

fn thread_list_options_from_user_req(
    req: &FindThreadListByUserIdRequest,
) -> Result<ThreadListOptions, tonic::Status> {
    Ok(ThreadListOptions {
        created_after: req.created_after,
        created_before: req.created_before,
        updated_after: req.updated_after,
        updated_before: req.updated_before,
        sort: req.sort.map(ThreadSort::from).unwrap_or_default(),
        memory_kinds: validated_memory_kinds(&req.memory_kinds)?,
    })
}

pub trait ThreadGrpc {
    fn app(&self) -> &ThreadAppImpl;
    fn vector_app(&self) -> Option<&app::app::memory_vector::MemoryVectorAppImpl>;
    fn thread_vector_app(&self) -> Option<&app::app::thread_vector::ThreadVectorAppImpl>;
    /// `MediaApp` for presigning the `Memory.media` of thread-history
    /// results just before responding (same `enrich_memory_media` the
    /// Find and search paths use). `None` when the media subsystem is
    /// not wired — results carry no media (backward compatible).
    fn media_app(&self) -> Option<&std::sync::Arc<app::app::media::MediaAppImpl>> {
        None
    }
}

const DEFAULT_TTL: Duration = Duration::from_secs(30);
const LIST_TTL: Duration = Duration::from_secs(5);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protobuf::llm_memory::data::MemoryKind;

    #[test]
    fn thread_list_options_accepts_explicit_memory_kinds() {
        let req = FindThreadListByUserIdRequest {
            memory_kinds: vec![MemoryKind::Raw as i32, MemoryKind::ThreadSummary as i32],
            ..Default::default()
        };

        let options = thread_list_options_from_user_req(&req).unwrap();

        assert_eq!(
            options.memory_kinds,
            vec![MemoryKind::Raw as i32, MemoryKind::ThreadSummary as i32,]
        );
    }

    #[test]
    fn thread_list_options_rejects_unspecified_or_unknown_memory_kinds() {
        for memory_kinds in [vec![0], vec![99]] {
            let req = FindThreadListByUserIdRequest {
                memory_kinds,
                ..Default::default()
            };

            let error = thread_list_options_from_user_req(&req).unwrap_err();

            assert_eq!(error.code(), tonic::Code::InvalidArgument);
        }
    }
}

#[tonic::async_trait]
impl<T: ThreadGrpc + Tracing + Send + Debug + Sync + 'static> ThreadService for T {
    #[tracing::instrument]
    async fn create(
        &self,
        request: tonic::Request<ThreadData>,
    ) -> Result<tonic::Response<CreateThreadResponse>, tonic::Status> {
        let _span = Self::trace_request("thread", "create", &request);
        let req = request.get_ref();
        // Phase 2 of system-prompt-as-memory migration:
        // `default_system_memory_id` is now optional (None means "no default
        // system prompt"). Only `user_id` remains required.
        if req.user_id.is_none_or(|id| id.value == 0) {
            return Err(tonic::Status::invalid_argument("user_id is required"));
        }
        match self.app().create_thread(req).await {
            Ok(id) => Ok(Response::new(CreateThreadResponse { id: Some(id) })),
            Err(e) => Err(handle_error(&e)),
        }
    }
    #[tracing::instrument]
    async fn update(
        &self,
        request: tonic::Request<Thread>,
    ) -> Result<tonic::Response<SuccessResponse>, tonic::Status> {
        let _s = Self::trace_request("thread", "update", &request);
        let req = request.get_ref();
        if let Some(data) = &req.data {
            // `default_system_memory_id` is optional; only `user_id` is required.
            if data.user_id.is_none_or(|id| id.value == 0) {
                return Err(tonic::Status::invalid_argument("user_id is required"));
            }
        }
        if let Some(i) = &req.id {
            match self.app().update_thread(i, &req.data).await {
                Ok(res) => Ok(Response::new(SuccessResponse { is_success: res })),
                Err(e) => Err(handle_error(&e)),
            }
        } else {
            tracing::warn!("id not found in updating: {:?}", req);
            Err(tonic::Status::invalid_argument("id is required"))
        }
    }
    #[tracing::instrument]
    async fn delete(
        &self,
        request: tonic::Request<ThreadId>,
    ) -> Result<tonic::Response<SuccessResponse>, tonic::Status> {
        let _s = Self::trace_request("thread", "delete", &request);
        let req = request.get_ref();
        match self.app().delete_thread(req).await {
            #[allow(unused_variables)]
            Ok((r, exclusive_memory_ids)) => {
                // Cascade delete vector entries for exclusive memories (failure is logged only).
                // Intentional ordering: RDB first, then LanceDB. RDB is the source of truth;
                // orphaned LanceDB records are cleaned up by rebuild_index.
                if let Some(va) = self.vector_app()
                    && !exclusive_memory_ids.is_empty()
                    && let Err(e) = va.delete_vectors_by_memory_ids(&exclusive_memory_ids).await
                {
                    tracing::error!(
                        "LanceDB cascade delete failed for thread_id={}: {e}",
                        req.value
                    );
                }
                // Thread vector deletion is handled by ThreadApp::delete_thread
                Ok(Response::new(SuccessResponse { is_success: r }))
            }
            Err(e) => Err(handle_error(&e)),
        }
    }
    #[tracing::instrument]
    async fn find(
        &self,
        request: tonic::Request<ThreadId>,
    ) -> Result<tonic::Response<OptionalThreadResponse>, tonic::Status> {
        let _s = Self::trace_request("thread", "find", &request);
        let req = request.get_ref();
        match self.app().find_thread(req, Some(&DEFAULT_TTL)).await {
            Ok(res) => Ok(Response::new(OptionalThreadResponse { data: res })),
            Err(e) => Err(handle_error(&e)),
        }
    }

    type FindListStream = BoxStream<'static, Result<Thread, tonic::Status>>;
    #[tracing::instrument]
    async fn find_list(
        &self,
        request: tonic::Request<FindListRequest>,
    ) -> Result<tonic::Response<Self::FindListStream>, tonic::Status> {
        let _s = Self::trace_request("thread", "find_list", &request);
        let mut req = request.into_inner();
        normalize_memory_kinds(&mut req.memory_kinds)?;
        let ttl = if req.limit.is_some() {
            LIST_TTL
        } else {
            DEFAULT_TTL
        };
        match self
            .app()
            .find_thread_list(
                req.limit.as_ref(),
                req.offset.as_ref(),
                &req.memory_kinds,
                Some(&ttl),
            )
            .await
        {
            Ok(list) => Ok(Response::new(Box::pin(stream! {
                for s in list {
                    yield Ok(s)
                }
            }))),
            Err(e) => Err(handle_error(&e)),
        }
    }

    type FindThreadListByUserIdStream = BoxStream<'static, Result<Thread, tonic::Status>>;
    #[tracing::instrument]
    async fn find_thread_list_by_user_id(
        &self,
        request: tonic::Request<FindThreadListByUserIdRequest>,
    ) -> Result<tonic::Response<Self::FindThreadListByUserIdStream>, tonic::Status> {
        let _s = Self::trace_request("thread", "find_thread_list_by_user_id", &request);
        let req = request.get_ref();
        let user_id = req
            .user_id
            .ok_or_else(|| tonic::Status::invalid_argument("user_id is required"))?;
        let opts = thread_list_options_from_user_req(req)?;
        match self
            .app()
            .find_thread_list_by_user_id(
                user_id,
                req.limit.as_ref(),
                req.offset.as_ref(),
                opts,
                Some(&LIST_TTL),
            )
            .await
        {
            Ok(list) => Ok(Response::new(Box::pin(stream! {
                for s in list {
                    yield Ok(s)
                }
            }))),
            Err(e) => Err(handle_error(&e)),
        }
    }

    #[tracing::instrument]
    async fn add_memory(
        &self,
        request: tonic::Request<AddMemoryRequest>,
    ) -> Result<tonic::Response<AddMemoryResponse>, tonic::Status> {
        let _s = Self::trace_request("thread", "add_memory", &request);
        let req = request.get_ref();
        let thread_id = req
            .thread_id
            .as_ref()
            .ok_or_else(|| tonic::Status::invalid_argument("thread_id is required"))?;
        let memory = req
            .memory
            .as_ref()
            .ok_or_else(|| tonic::Status::invalid_argument("memory is required"))?;
        match self.app().add_memory(thread_id, memory).await {
            Ok(id) => Ok(Response::new(AddMemoryResponse { id: Some(id) })),
            Err(e) => Err(handle_error(&e)),
        }
    }

    type FindMemoriesByThreadIdStream = BoxStream<'static, Result<Memory, tonic::Status>>;
    #[tracing::instrument]
    async fn find_memories_by_thread_id(
        &self,
        request: tonic::Request<FindMemoriesByThreadIdRequest>,
    ) -> Result<tonic::Response<Self::FindMemoriesByThreadIdStream>, tonic::Status> {
        let _s = Self::trace_request("thread", "find_memories_by_thread_id", &request);
        let req = request.get_ref();
        let thread_id = req
            .thread_id
            .as_ref()
            .ok_or_else(|| tonic::Status::invalid_argument("thread_id is required"))?;
        let media_app = self.media_app().cloned();
        match self
            .app()
            .find_memories_by_thread_id(
                thread_id,
                req.limit.as_ref(),
                req.offset.as_ref(),
                &req.roles,
                &req.content_types,
            )
            .await
        {
            // The app layer hydrated the cacheable media half; add the
            // per-response presigned URL the same way Find/search do.
            Ok(list) => Ok(Response::new(Box::pin(stream! {
                for mut s in list {
                    enrich_memory_media(media_app.as_ref(), &mut s).await;
                    yield Ok(s)
                }
            }))),
            Err(e) => Err(handle_error(&e)),
        }
    }

    #[tracing::instrument]
    async fn count(
        &self,
        request: tonic::Request<FindCondition>,
    ) -> Result<tonic::Response<CountResponse>, tonic::Status> {
        let _s = Self::trace_request("thread", "count", &request);
        match self.app().count().await {
            Ok(res) => Ok(Response::new(CountResponse { total: res })),
            Err(e) => Err(handle_error(&e)),
        }
    }

    #[tracing::instrument]
    async fn resolve_ancestor_closure(
        &self,
        request: tonic::Request<ResolveAncestorClosureRequest>,
    ) -> Result<tonic::Response<ResolveAncestorClosureResponse>, tonic::Status> {
        let _s = Self::trace_request("thread", "resolve_ancestor_closure", &request);
        let req = request.get_ref();
        let thread_id = req
            .thread_id
            .as_ref()
            .ok_or_else(|| tonic::Status::invalid_argument("thread_id is required"))?;
        let start_memory_id = req
            .start_memory_id
            .as_ref()
            .ok_or_else(|| tonic::Status::invalid_argument("start_memory_id is required"))?;
        // `max_depth = Some(0)` is rejected at the app layer, but a `Some(0)`
        // from a buggy client wastes a round-trip; short-circuit it here too.
        if req.max_depth == Some(0) {
            return Err(tonic::Status::invalid_argument(
                "max_depth must be at least 1",
            ));
        }
        match self
            .app()
            .resolve_ancestor_closure(thread_id, start_memory_id, req.max_depth)
            .await
        {
            Ok(closure) => Ok(Response::new(ResolveAncestorClosureResponse {
                ordered_memories: closure
                    .ordered_memories
                    .into_iter()
                    .map(to_proto_memory_with_position)
                    .collect(),
                system_memory: closure.system_memory.map(to_proto_memory_with_position),
            })),
            Err(e) => Err(handle_error(&e)),
        }
    }

    // ===== Label operations =====

    #[tracing::instrument]
    async fn add_labels(
        &self,
        request: tonic::Request<AddLabelsRequest>,
    ) -> Result<Response<SuccessResponse>, tonic::Status> {
        let req = request.into_inner();
        let thread_id = req
            .thread_id
            .ok_or_else(|| tonic::Status::invalid_argument("thread_id is required"))?;
        if req.labels.is_empty() {
            return Err(tonic::Status::invalid_argument("labels must not be empty"));
        }
        match self.app().add_labels(&thread_id, &req.labels).await {
            Ok(()) => {
                // Vector sync is handled by ThreadApp::add_labels
                Ok(Response::new(SuccessResponse { is_success: true }))
            }
            Err(e) => Err(handle_error(&e)),
        }
    }

    #[tracing::instrument]
    async fn remove_labels(
        &self,
        request: tonic::Request<RemoveLabelsRequest>,
    ) -> Result<Response<SuccessResponse>, tonic::Status> {
        let req = request.into_inner();
        let thread_id = req
            .thread_id
            .ok_or_else(|| tonic::Status::invalid_argument("thread_id is required"))?;
        if req.labels.is_empty() {
            return Err(tonic::Status::invalid_argument("labels must not be empty"));
        }
        match self.app().remove_labels(&thread_id, &req.labels).await {
            Ok(()) => {
                // Vector sync is handled by ThreadApp::remove_labels
                Ok(Response::new(SuccessResponse { is_success: true }))
            }
            Err(e) => Err(handle_error(&e)),
        }
    }

    type FindThreadListByLabelsStream = BoxStream<'static, Result<Thread, tonic::Status>>;
    #[tracing::instrument]
    async fn find_thread_list_by_labels(
        &self,
        request: tonic::Request<FindThreadListByLabelsRequest>,
    ) -> Result<Response<Self::FindThreadListByLabelsStream>, tonic::Status> {
        let req = request.into_inner();
        let match_all = req.match_mode
            == Some(crate::protobuf::llm_memory::data::LabelMatchMode::LabelAll as i32);
        let opts = ThreadListOptions {
            created_after: req.created_after,
            created_before: req.created_before,
            updated_after: req.updated_after,
            updated_before: req.updated_before,
            sort: req.sort.map(ThreadSort::from).unwrap_or_default(),
            memory_kinds: validated_memory_kinds(&req.memory_kinds)?,
        };
        match self
            .app()
            .find_threads_by_labels(
                &req.labels,
                match_all,
                req.user_id,
                req.limit,
                req.offset,
                opts,
            )
            .await
        {
            Ok(threads) => {
                let s = stream! {
                    for t in threads {
                        yield Ok(t);
                    }
                };
                Ok(Response::new(
                    Box::pin(s) as Self::FindThreadListByLabelsStream
                ))
            }
            Err(e) => Err(handle_error(&e)),
        }
    }

    #[tracing::instrument]
    async fn find_distinct_labels(
        &self,
        request: tonic::Request<FindDistinctLabelsRequest>,
    ) -> Result<Response<FindDistinctLabelsResponse>, tonic::Status> {
        let req = request.into_inner();
        match self
            .app()
            .find_distinct_labels(
                req.user_id,
                req.limit,
                req.offset,
                req.created_after,
                req.created_before,
                req.updated_after,
                req.updated_before,
            )
            .await
        {
            Ok(rows) => Ok(Response::new(FindDistinctLabelsResponse {
                labels: rows
                    .into_iter()
                    .map(|r| crate::protobuf::llm_memory::service::LabelWithCount {
                        label: r.label,
                        thread_count: r.thread_count,
                    })
                    .collect(),
            })),
            Err(e) => Err(handle_error(&e)),
        }
    }

    #[tracing::instrument]
    async fn search_labels(
        &self,
        request: tonic::Request<SearchLabelsRequest>,
    ) -> Result<Response<SearchLabelsResponse>, tonic::Status> {
        let req = request.into_inner();
        if req.query.is_empty() {
            return Err(tonic::Status::invalid_argument("query must not be empty"));
        }
        match self
            .app()
            .search_labels(
                &req.query,
                req.user_id,
                req.limit,
                req.created_after,
                req.created_before,
                req.updated_after,
                req.updated_before,
            )
            .await
        {
            Ok(rows) => Ok(Response::new(SearchLabelsResponse {
                labels: rows
                    .into_iter()
                    .map(|r| crate::protobuf::llm_memory::service::LabelWithCount {
                        label: r.label,
                        thread_count: r.thread_count,
                    })
                    .collect(),
            })),
            Err(e) => Err(handle_error(&e)),
        }
    }

    #[tracing::instrument]
    async fn add_memories_batch(
        &self,
        request: tonic::Request<AddMemoriesBatchRequest>,
    ) -> Result<Response<AddMemoriesBatchResponse>, tonic::Status> {
        let _s = Self::trace_request("thread", "add_memories_batch", &request);
        let req = request.into_inner();

        // Pre-flight: enforce server-side per-batch size cap so a tonic
        // frame size violation is reported as ResourceExhausted rather
        // than a generic decode error.
        let cap = import_batch_max_items();
        if req.memories.len() > cap {
            return Err(tonic::Status::resource_exhausted(format!(
                "memories ({}) exceeds IMPORT_BATCH_MAX_ITEMS ({cap})",
                req.memories.len()
            )));
        }

        let thread_target = match req.thread_target {
            Some(AddMemoriesBatchThreadTarget::ExistingThreadId(tid)) => {
                BatchThreadTarget::ExistingThreadId(tid)
            }
            Some(AddMemoriesBatchThreadTarget::Upsert(upsert)) => {
                let thread_data = upsert.thread_data.ok_or_else(|| {
                    tonic::Status::invalid_argument("upsert.thread_data is required")
                })?;
                BatchThreadTarget::UpsertByChannel(thread_data)
            }
            None => {
                return Err(tonic::Status::invalid_argument(
                    "thread_target (existing_thread_id or upsert) is required",
                ));
            }
        };

        let memories: Vec<BatchMemoryInput> = req
            .memories
            .into_iter()
            .map(|m| {
                let memory = m.memory.unwrap_or_default();
                BatchMemoryInput {
                    memory,
                    parent_external_ids: m.parent_external_ids,
                }
            })
            .collect();

        let input = AddMemoriesBatchInput {
            thread_target,
            memories,
            upsert_by_external_id: req.upsert_by_external_id,
            thread_updated_at_override: req.thread_updated_at_override,
            labels: req.labels,
        };

        match self.app().add_memories_batch(input).await {
            Ok(out) => {
                // Fire-and-forget embedding dispatch for memories that need
                // (re-)embedding: newly inserted ones and those whose
                // content was overwritten via `upsert_by_external_id`.
                // Mirrors the AddMemory single-shot semantics.
                {
                    let app_ref = self.app();
                    for (mid, mem, media) in &out.new_memories_for_embedding {
                        app_ref.dispatch_embedding_for_batch_item(mid, mem, media.clone());
                    }
                }
                let resp = AddMemoriesBatchResponse {
                    thread_id: Some(out.thread_id),
                    thread_created: out.thread_created,
                    outcomes: out.outcomes.into_iter().map(to_proto_outcome).collect(),
                };
                Ok(Response::new(resp))
            }
            Err(e) => Err(handle_error(&e)),
        }
    }

    #[tracing::instrument]
    async fn update_memory_parents(
        &self,
        request: tonic::Request<UpdateMemoryParentsRequest>,
    ) -> Result<Response<UpdateMemoryParentsResponse>, tonic::Status> {
        let _s = Self::trace_request("thread", "update_memory_parents", &request);
        let req = request.into_inner();
        let thread_id = req
            .thread_id
            .ok_or_else(|| tonic::Status::invalid_argument("thread_id is required"))?;
        let memory_id = req
            .memory_id
            .ok_or_else(|| tonic::Status::invalid_argument("memory_id is required"))?;

        match self
            .app()
            .update_memory_parent_ids_with_guards(
                &thread_id,
                &memory_id,
                &req.parent_ids,
                req.force_overwrite_when_shared,
                req.force_overwrite_when_non_empty,
            )
            .await
        {
            Ok(UpdateMemoryParentsOutcome::Rewired) => {
                use UpdateMemoryParentsSkipReason as Reason;
                Ok(Response::new(UpdateMemoryParentsResponse {
                    rewired: true,
                    skip_reason: Reason::UpdateMemoryParentsSkipUnspecified as i32,
                }))
            }
            Ok(UpdateMemoryParentsOutcome::Skipped(reason)) => {
                use UpdateMemoryParentsSkipReason as Reason;
                let proto_reason = match reason {
                    UpdateParentsSkipReason::SharedMemory => {
                        Reason::UpdateMemoryParentsSkipSharedMemory
                    }
                    UpdateParentsSkipReason::AlreadyHasParents => {
                        Reason::UpdateMemoryParentsSkipAlreadyHasParents
                    }
                };
                Ok(Response::new(UpdateMemoryParentsResponse {
                    rewired: false,
                    skip_reason: proto_reason as i32,
                }))
            }
            Err(e) => Err(handle_error(&e)),
        }
    }

    #[tracing::instrument]
    async fn find_co_occurring_labels(
        &self,
        request: tonic::Request<FindCoOccurringLabelsRequest>,
    ) -> Result<Response<FindCoOccurringLabelsResponse>, tonic::Status> {
        let req = request.into_inner();
        if req.labels.is_empty() {
            return Err(tonic::Status::invalid_argument("labels must not be empty"));
        }
        match self
            .app()
            .find_co_occurring_labels(
                &req.labels,
                req.user_id,
                req.limit,
                req.offset,
                req.created_after,
                req.created_before,
                req.updated_after,
                req.updated_before,
            )
            .await
        {
            Ok(rows) => Ok(Response::new(FindCoOccurringLabelsResponse {
                labels: rows
                    .into_iter()
                    .map(|r| crate::protobuf::llm_memory::service::LabelWithCount {
                        label: r.label,
                        thread_count: r.thread_count,
                    })
                    .collect(),
            })),
            Err(e) => Err(handle_error(&e)),
        }
    }
}

fn to_proto_memory_with_position(m: MemoryWithPosition) -> MemoryWithPositionProto {
    MemoryWithPositionProto {
        memory: Some(m.memory),
        position: m.position,
    }
}

/// `IMPORT_BATCH_MAX_ITEMS` parsed once at first use, defaulting to 1000.
fn import_batch_max_items() -> usize {
    static CAP: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *CAP.get_or_init(|| {
        std::env::var("IMPORT_BATCH_MAX_ITEMS")
            .ok()
            .and_then(|s| s.parse::<usize>().ok())
            .unwrap_or(1000)
    })
}

fn to_proto_outcome(o: AppAddMemoryOutcome) -> AddMemoryOutcomeProto {
    AddMemoryOutcomeProto {
        memory_id: Some(o.memory_id),
        created: o.created,
        position: o.position,
        existing_parent_ids_empty: o.existing_parent_ids_empty,
        resolved_parent_ids: o.resolved_parent_ids,
    }
}

#[derive(DebugStub)]
pub(crate) struct ThreadGrpcImpl {
    #[debug_stub = "ThreadAppImpl"]
    thread_app: ThreadAppImpl,
    #[debug_stub = "Option<Arc<MemoryVectorAppImpl>>"]
    vector_app: Option<std::sync::Arc<app::app::memory_vector::MemoryVectorAppImpl>>,
    #[debug_stub = "Option<Arc<ThreadVectorAppImpl>>"]
    thread_vector_app: Option<std::sync::Arc<app::app::thread_vector::ThreadVectorAppImpl>>,
    #[debug_stub = "Option<Arc<MediaAppImpl>>"]
    media_app: Option<std::sync::Arc<app::app::media::MediaAppImpl>>,
}

impl ThreadGrpcImpl {
    pub fn new(
        thread_app: ThreadAppImpl,
        vector_app: Option<std::sync::Arc<app::app::memory_vector::MemoryVectorAppImpl>>,
        thread_vector_app: Option<std::sync::Arc<app::app::thread_vector::ThreadVectorAppImpl>>,
    ) -> Self {
        ThreadGrpcImpl {
            thread_app,
            vector_app,
            thread_vector_app,
            media_app: None,
        }
    }

    /// Wire the media app so thread-history results carry the presigned
    /// `Memory.media` (the app layer already hydrates the cacheable
    /// half). `None` leaves results without media (backward compatible).
    pub fn with_media_app(
        mut self,
        media_app: std::sync::Arc<app::app::media::MediaAppImpl>,
    ) -> Self {
        self.media_app = Some(media_app);
        self
    }
}
impl ThreadGrpc for ThreadGrpcImpl {
    fn app(&self) -> &ThreadAppImpl {
        &self.thread_app
    }
    fn vector_app(&self) -> Option<&app::app::memory_vector::MemoryVectorAppImpl> {
        self.vector_app.as_deref()
    }
    fn thread_vector_app(&self) -> Option<&app::app::thread_vector::ThreadVectorAppImpl> {
        self.thread_vector_app.as_deref()
    }
    fn media_app(&self) -> Option<&std::sync::Arc<app::app::media::MediaAppImpl>> {
        self.media_app.as_ref()
    }
}

// use tracing
impl Tracing for ThreadGrpcImpl {}
