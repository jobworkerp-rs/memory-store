use std::{fmt::Debug, time::Duration};

use crate::protobuf::llm_memory::data::{Memory, MemoryData, MemoryId};
use crate::protobuf::llm_memory::service::FindRecentListByUserIdRequest;
use crate::protobuf::llm_memory::service::{
    CountResponse, CreateMemoryResponse, FindCondition, FindListRequest, FindMemoryListRequest,
    MemoryCountCondition, MemoryListEntry, OptionalMemoryResponse, SuccessResponse,
    UpdateContentNoDispatchRequest, memory_service_server::MemoryService,
};
use crate::service::error_handle::handle_error;
use crate::service::memory_kind::{normalize_memory_kinds, normalize_thread_search_filter};
use app::app::media::MediaAppImpl;
use app::app::memory::{MemoryApp, MemoryAppImpl, MemoryCondition, MemoryListCondition};
use async_stream::stream;
use command_utils::trace::Tracing;
use futures::stream::BoxStream;
use infra::infra::memory::rdb::MemorySort;
use std::sync::Arc;
use tonic::Response;

pub trait MemoryGrpc {
    fn app(&self) -> &MemoryAppImpl;
    fn vector_app(&self) -> Option<&app::app::memory_vector::MemoryVectorAppImpl>;
    /// Shared `MediaApp` for issuing the short-lived presigned URL just
    /// before responding. `None` for non-media deployments.
    fn media_app(&self) -> Option<&Arc<MediaAppImpl>> {
        None
    }
}

const DEFAULT_TTL: Duration = Duration::from_secs(30);
const LIST_TTL: Duration = Duration::from_secs(5);

/// Fill `Memory.media.presigned_url` just before responding. The app
/// layer already hydrated the cacheable metadata + `unresolved` flag;
/// here we add the short-lived URL (NEVER cached, re-issued every
/// response). Shared by every read path that returns a `Memory`:
/// MemoryService (Find/FindList), MemoryVectorService (the search RPCs),
/// and ThreadService (thread history) — they all hydrate via the app
/// layer then call this for the presign so behaviour cannot drift.
///
/// `unresolved` is checked FIRST: when true the media body is absent
/// (recovering / migration placeholder) so an empty URL is the correct
/// normal state — no panic, no error log. Only `unresolved=false` with
/// a failed/empty resolve is an invariant violation (debug_assert in
/// debug, error log in release).
pub async fn enrich_memory_media(media_app: Option<&Arc<MediaAppImpl>>, memory: &mut Memory) {
    let Some(media_app) = media_app else {
        return;
    };
    let Some(payload) = memory.media.as_mut() else {
        return;
    };
    if payload.unresolved {
        // Normal "recovering" state: leave url/expires_at empty.
        return;
    }
    let Some(mid) = payload
        .metadata
        .as_ref()
        .and_then(|m| m.id.map(|x| x.value))
    else {
        return;
    };
    match media_app.resolve(mid, None).await {
        Ok(out) => {
            if out.unresolved {
                // Raced into unresolvable between hydrate and resolve —
                // still a normal state, mirror it.
                payload.unresolved = true;
            } else if out.url.is_some() {
                payload.presigned_url = out.url;
                payload.expires_at = out.expires_at;
            } else {
                debug_assert!(
                    false,
                    "media {mid} resolve returned no url while unresolved=false"
                );
                tracing::error!("media {mid} resolve returned no url while unresolved=false");
            }
        }
        Err(e) => {
            debug_assert!(false, "media {mid} resolve failed: {e}");
            tracing::error!("media {mid} presign enrich failed: {e}");
        }
    }
}

fn check_external_id_exclusivity(
    exact: Option<&String>,
    prefix: Option<&String>,
) -> Result<(), tonic::Status> {
    if exact.is_some() && prefix.is_some() {
        return Err(tonic::Status::invalid_argument(
            "external_id and external_id_prefix are mutually exclusive",
        ));
    }
    Ok(())
}

#[tonic::async_trait]
impl<T: MemoryGrpc + Tracing + Send + Debug + Sync + 'static> MemoryService for T {
    #[tracing::instrument]
    async fn create(
        &self,
        request: tonic::Request<MemoryData>,
    ) -> Result<tonic::Response<CreateMemoryResponse>, tonic::Status> {
        let _span = Self::trace_request("memory", "create", &request);
        let req = request.get_ref();
        match self.app().create_memory(req).await {
            Ok(id) => Ok(Response::new(CreateMemoryResponse { id: Some(id) })),
            Err(e) => Err(handle_error(&e)),
        }
    }
    #[tracing::instrument]
    async fn update(
        &self,
        request: tonic::Request<Memory>,
    ) -> Result<tonic::Response<SuccessResponse>, tonic::Status> {
        let _s = Self::trace_request("memory", "update", &request);
        let req = request.get_ref();
        if let Some(i) = &req.id {
            match self.app().update_memory(i, &req.data).await {
                Ok(outcome) => {
                    // Detaching an image leaves the memory's old
                    // image/caption LanceDB rows orphaned (the text-only
                    // re-dispatch can't evict them). Cascade-clear them,
                    // best-effort: same "RDB is source of truth, vector
                    // store is cascaded, failure is logged not
                    // propagated" contract as the `delete` handler below.
                    // rebuild_index reconciles any miss.
                    if outcome.stale_image_cleanup
                        && let Some(va) = self.vector_app()
                        && let Err(e) = va.clear_image_vectors(i.value).await
                    {
                        tracing::error!(
                            "LanceDB image/caption cascade-clear failed for \
                             memory_id={} after media detach: {e}",
                            i.value
                        );
                    }
                    Ok(Response::new(SuccessResponse {
                        is_success: outcome.updated,
                    }))
                }
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
        request: tonic::Request<MemoryId>,
    ) -> Result<tonic::Response<SuccessResponse>, tonic::Status> {
        let _s = Self::trace_request("memory", "delete", &request);
        let req = request.get_ref();
        match self.app().delete_memory(req).await {
            Ok(r) => {
                // Cascade delete from LanceDB (failure is logged, not propagated).
                // Intentional ordering: RDB first, then LanceDB. RDB is the source of truth;
                // orphaned LanceDB records are cleaned up by rebuild_index.
                if let Some(va) = self.vector_app()
                    && let Err(e) = va.delete_vector(req.value).await
                {
                    tracing::error!(
                        "LanceDB cascade delete failed for memory_id={}: {e}",
                        req.value
                    );
                }
                Ok(Response::new(SuccessResponse { is_success: r }))
            }
            Err(e) => Err(handle_error(&e)),
        }
    }
    #[tracing::instrument]
    async fn find(
        &self,
        request: tonic::Request<MemoryId>,
    ) -> Result<tonic::Response<OptionalMemoryResponse>, tonic::Status> {
        let _s = Self::trace_request("memory", "find", &request);
        let req = request.get_ref();
        match self.app().find_memory(req, Some(&DEFAULT_TTL)).await {
            Ok(mut res) => {
                if let Some(m) = res.as_mut() {
                    enrich_memory_media(self.media_app(), m).await;
                }
                Ok(Response::new(OptionalMemoryResponse { data: res }))
            }
            Err(e) => Err(handle_error(&e)),
        }
    }

    type FindListStream = BoxStream<'static, Result<Memory, tonic::Status>>;
    #[tracing::instrument]
    async fn find_list(
        &self,
        request: tonic::Request<FindListRequest>,
    ) -> Result<tonic::Response<Self::FindListStream>, tonic::Status> {
        let _s = Self::trace_request("memory", "find_list", &request);
        let mut req = request.into_inner();
        normalize_memory_kinds(&mut req.memory_kinds)?;
        let ttl = if req.limit.is_some() {
            LIST_TTL
        } else {
            DEFAULT_TTL
        };
        let media_app = self.media_app().cloned();
        match self
            .app()
            .find_memory_list(
                req.limit.as_ref(),
                req.offset.as_ref(),
                &req.memory_kinds,
                Some(&ttl),
            )
            .await
        {
            Ok(list) => {
                // TODO streamingのより良いやり方がないか?
                Ok(Response::new(Box::pin(stream! {
                    for mut s in list {
                        enrich_memory_media(media_app.as_ref(), &mut s).await;
                        yield Ok(s)
                    }
                })))
            }
            Err(e) => Err(handle_error(&e)),
        }
    }
    async fn count(
        &self,
        request: tonic::Request<FindCondition>,
    ) -> Result<tonic::Response<CountResponse>, tonic::Status> {
        let _s = Self::trace_request("memory", "count", &request);
        match self.app().count().await {
            Ok(res) => Ok(Response::new(CountResponse { total: res })),
            Err(e) => Err(handle_error(&e)),
        }
    }

    type FindRecentListByUserIdStream = BoxStream<'static, Result<Memory, tonic::Status>>;
    #[tracing::instrument]
    async fn find_recent_list_by_user_id(
        &self,
        request: tonic::Request<FindRecentListByUserIdRequest>,
    ) -> Result<tonic::Response<Self::FindRecentListByUserIdStream>, tonic::Status> {
        let _s = Self::trace_request("memory", "find_recent_list_by_user_id", &request);
        let mut req = request.into_inner();
        if req.user_id.is_none() {
            return Err(tonic::Status::invalid_argument("user_id is required"));
        }
        normalize_memory_kinds(&mut req.memory_kinds)?;
        let media_app = self.media_app().cloned();
        match self
            .app()
            .find_recent_list_by_user_id(
                req.user_id.unwrap(),
                req.limit.as_ref(),
                req.updated_after.as_ref(),
                req.updated_before.as_ref(),
                &req.memory_kinds,
                Some(&LIST_TTL),
            )
            .await
        {
            Ok(list) => Ok(Response::new(Box::pin(stream! {
                for mut s in list {
                    enrich_memory_media(media_app.as_ref(), &mut s).await;
                    yield Ok(s)
                }
            }))),
            Err(e) => Err(handle_error(&e)),
        }
    }

    type FindListByConditionStream = BoxStream<'static, Result<MemoryListEntry, tonic::Status>>;
    #[tracing::instrument]
    async fn find_list_by_condition(
        &self,
        request: tonic::Request<FindMemoryListRequest>,
    ) -> Result<tonic::Response<Self::FindListByConditionStream>, tonic::Status> {
        let _s = Self::trace_request("memory", "find_list_by_condition", &request);
        let mut req = request.into_inner();
        normalize_memory_kinds(&mut req.memory_kinds)?;
        if let Some(filter) = req.thread_filter.as_mut() {
            normalize_thread_search_filter(filter)?;
        }
        check_external_id_exclusivity(req.external_id.as_ref(), req.external_id_prefix.as_ref())?;
        let ttl = if req.limit.is_some() {
            LIST_TTL
        } else {
            DEFAULT_TTL
        };
        let user_id = req.user_id.as_ref().map(|u| u.value);
        let condition = MemoryListCondition {
            limit: req.limit,
            offset: req.offset,
            filter: MemoryCondition {
                roles: req.roles.clone(),
                content_types: req.content_types.clone(),
                memory_kinds: req.memory_kinds.clone(),
                user_id,
                thread_id: req.thread_id,
                updated_at_range: infra::infra::memory::rdb::UpdatedAtRange {
                    updated_after: req.updated_after,
                    updated_before: req.updated_before,
                },
                created_at_range: infra::infra::memory::rdb::CreatedAtRange {
                    created_after: req.created_after,
                    created_before: req.created_before,
                },
                external_id: req.external_id.clone(),
                external_id_prefix: req.external_id_prefix.clone(),
                thread_filter: req.thread_filter.clone(),
            },
            sort: req.sort.map(MemorySort::from).unwrap_or_default(),
        };
        let media_app = self.media_app().cloned();
        match self
            .app()
            .find_memory_list_by_condition(&condition, Some(&ttl))
            .await
        {
            Ok(list) => Ok(Response::new(Box::pin(stream! {
                for (mut memory, info) in list {
                    enrich_memory_media(media_app.as_ref(), &mut memory).await;
                    yield Ok(MemoryListEntry {
                        memory: Some(memory),
                        position: info.position,
                        thread_total: info.thread_total,
                        thread_id: info.thread_id,
                        thread_owner_user_id: info.thread_owner_user_id,
                        thread_description: info.thread_description,
                    })
                }
            }))),
            Err(e) => Err(handle_error(&e)),
        }
    }

    #[tracing::instrument]
    async fn count_by_condition(
        &self,
        request: tonic::Request<MemoryCountCondition>,
    ) -> Result<tonic::Response<CountResponse>, tonic::Status> {
        let _s = Self::trace_request("memory", "count_by_condition", &request);
        let mut req = request.into_inner();
        normalize_memory_kinds(&mut req.memory_kinds)?;
        if let Some(filter) = req.thread_filter.as_mut() {
            normalize_thread_search_filter(filter)?;
        }
        check_external_id_exclusivity(req.external_id.as_ref(), req.external_id_prefix.as_ref())?;
        let user_id = req.user_id.as_ref().map(|u| u.value);
        let condition = MemoryCondition {
            roles: req.roles.clone(),
            content_types: req.content_types.clone(),
            memory_kinds: req.memory_kinds.clone(),
            user_id,
            thread_id: req.thread_id,
            updated_at_range: infra::infra::memory::rdb::UpdatedAtRange {
                updated_after: req.updated_after,
                updated_before: req.updated_before,
            },
            created_at_range: infra::infra::memory::rdb::CreatedAtRange {
                created_after: req.created_after,
                created_before: req.created_before,
            },
            external_id: req.external_id.clone(),
            external_id_prefix: req.external_id_prefix.clone(),
            thread_filter: req.thread_filter.clone(),
        };
        match self.app().count_memory_by_condition(&condition).await {
            Ok(total) => Ok(Response::new(CountResponse { total })),
            Err(e) => Err(handle_error(&e)),
        }
    }

    #[tracing::instrument]
    async fn update_content_no_dispatch(
        &self,
        request: tonic::Request<UpdateContentNoDispatchRequest>,
    ) -> Result<tonic::Response<SuccessResponse>, tonic::Status> {
        let _s = Self::trace_request("memory", "update_content_no_dispatch", &request);
        let req = request.get_ref();
        let Some(id) = req.id.as_ref() else {
            return Err(tonic::Status::invalid_argument("id is required"));
        };
        match self
            .app()
            .update_content_no_dispatch(id, &req.content)
            .await
        {
            Ok(ok) => Ok(Response::new(SuccessResponse { is_success: ok })),
            Err(e) => Err(handle_error(&e)),
        }
    }
}

#[derive(DebugStub)]
pub(crate) struct MemoryGrpcImpl {
    #[debug_stub = "MemoryAppImpl"]
    memory_app: MemoryAppImpl,
    #[debug_stub = "Option<Arc<MemoryVectorAppImpl>>"]
    vector_app: Option<std::sync::Arc<app::app::memory_vector::MemoryVectorAppImpl>>,
    #[debug_stub = "Option<Arc<MediaAppImpl>>"]
    media_app: Option<Arc<MediaAppImpl>>,
}

impl MemoryGrpcImpl {
    pub fn new(
        memory_app: MemoryAppImpl,
        vector_app: Option<std::sync::Arc<app::app::memory_vector::MemoryVectorAppImpl>>,
        media_app: Option<Arc<MediaAppImpl>>,
    ) -> Self {
        MemoryGrpcImpl {
            memory_app,
            vector_app,
            media_app,
        }
    }
}
impl MemoryGrpc for MemoryGrpcImpl {
    fn app(&self) -> &MemoryAppImpl {
        &self.memory_app
    }
    fn vector_app(&self) -> Option<&app::app::memory_vector::MemoryVectorAppImpl> {
        self.vector_app.as_deref()
    }
    fn media_app(&self) -> Option<&Arc<MediaAppImpl>> {
        self.media_app.as_ref()
    }
}

// use tracing
impl Tracing for MemoryGrpcImpl {}
