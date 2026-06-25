use std::{fmt::Debug, time::Duration};

use crate::protobuf::llm_memory::data::{MemoryRating, MemoryRatingData, MemoryRatingId};
use crate::protobuf::llm_memory::service::{
    CountResponse, CreateMemoryRatingResponse, FindCondition,
    FindMemoryRatingByMemoryAndUserRequest, FindMemoryRatingsByMemoryIdRequest,
    FindMemoryRatingsByUserIdRequest, OptionalMemoryRatingResponse, SuccessResponse,
    memory_rating_service_server::MemoryRatingService,
};
use crate::service::error_handle::handle_error;
use app::app::memory_rating::{MemoryRatingApp, MemoryRatingAppImpl};
use async_stream::stream;
use command_utils::trace::Tracing;
use futures::stream::BoxStream;
use tonic::Response;

pub trait MemoryRatingGrpc {
    fn app(&self) -> &MemoryRatingAppImpl;
}

const DEFAULT_TTL: Duration = Duration::from_secs(30);

#[tonic::async_trait]
impl<T: MemoryRatingGrpc + Tracing + Send + Debug + Sync + 'static> MemoryRatingService for T {
    #[tracing::instrument]
    async fn create(
        &self,
        request: tonic::Request<MemoryRatingData>,
    ) -> Result<tonic::Response<CreateMemoryRatingResponse>, tonic::Status> {
        let _span = Self::trace_request("memory_rating", "create", &request);
        let req = request.get_ref();
        match self.app().create_memory_rating(req).await {
            Ok(id) => Ok(Response::new(CreateMemoryRatingResponse { id: Some(id) })),
            Err(e) => Err(handle_error(&e)),
        }
    }

    #[tracing::instrument]
    async fn upsert(
        &self,
        request: tonic::Request<MemoryRatingData>,
    ) -> Result<tonic::Response<CreateMemoryRatingResponse>, tonic::Status> {
        let _span = Self::trace_request("memory_rating", "upsert", &request);
        let req = request.get_ref();
        match self.app().upsert_memory_rating(req).await {
            Ok(id) => Ok(Response::new(CreateMemoryRatingResponse { id: Some(id) })),
            Err(e) => Err(handle_error(&e)),
        }
    }

    #[tracing::instrument]
    async fn update(
        &self,
        request: tonic::Request<MemoryRating>,
    ) -> Result<tonic::Response<SuccessResponse>, tonic::Status> {
        let _s = Self::trace_request("memory_rating", "update", &request);
        let req = request.get_ref();
        if let Some(i) = &req.id {
            // memory_id and user_id are immutable (UNIQUE constraint pair);
            // warn if the client supplies them, as they will be silently ignored.
            if let Some(d) = &req.data
                && (d.memory_id.is_some() || d.user_id.is_some())
            {
                tracing::warn!(
                    id = i.value,
                    "update request contains memory_id/user_id which are ignored; \
                         use delete + create to change the memory/user association"
                );
            }
            match self.app().update_memory_rating(i, &req.data).await {
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
        request: tonic::Request<MemoryRatingId>,
    ) -> Result<tonic::Response<SuccessResponse>, tonic::Status> {
        let _s = Self::trace_request("memory_rating", "delete", &request);
        let req = request.get_ref();
        match self.app().delete_memory_rating(req).await {
            Ok(r) => Ok(Response::new(SuccessResponse { is_success: r })),
            Err(e) => Err(handle_error(&e)),
        }
    }

    #[tracing::instrument]
    async fn find(
        &self,
        request: tonic::Request<MemoryRatingId>,
    ) -> Result<tonic::Response<OptionalMemoryRatingResponse>, tonic::Status> {
        let _s = Self::trace_request("memory_rating", "find", &request);
        let req = request.get_ref();
        match self.app().find_memory_rating(req, Some(&DEFAULT_TTL)).await {
            Ok(res) => Ok(Response::new(OptionalMemoryRatingResponse { data: res })),
            Err(e) => Err(handle_error(&e)),
        }
    }

    type FindByMemoryIdStream = BoxStream<'static, Result<MemoryRating, tonic::Status>>;
    #[tracing::instrument]
    async fn find_by_memory_id(
        &self,
        request: tonic::Request<FindMemoryRatingsByMemoryIdRequest>,
    ) -> Result<tonic::Response<Self::FindByMemoryIdStream>, tonic::Status> {
        let _s = Self::trace_request("memory_rating", "find_by_memory_id", &request);
        let req = request.get_ref();
        let Some(memory_id) = &req.memory_id else {
            return Err(tonic::Status::invalid_argument("memory_id is required"));
        };
        match self.app().find_by_memory_id(memory_id).await {
            Ok(list) => Ok(Response::new(Box::pin(stream! {
                for s in list {
                    yield Ok(s)
                }
            }))),
            Err(e) => Err(handle_error(&e)),
        }
    }

    type FindByUserIdStream = BoxStream<'static, Result<MemoryRating, tonic::Status>>;
    #[tracing::instrument]
    async fn find_by_user_id(
        &self,
        request: tonic::Request<FindMemoryRatingsByUserIdRequest>,
    ) -> Result<tonic::Response<Self::FindByUserIdStream>, tonic::Status> {
        let _s = Self::trace_request("memory_rating", "find_by_user_id", &request);
        let req = request.get_ref();
        let Some(user_id) = &req.user_id else {
            return Err(tonic::Status::invalid_argument("user_id is required"));
        };
        match self
            .app()
            .find_by_user_id(user_id, req.limit.as_ref())
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
    async fn find_by_memory_id_and_user_id(
        &self,
        request: tonic::Request<FindMemoryRatingByMemoryAndUserRequest>,
    ) -> Result<tonic::Response<OptionalMemoryRatingResponse>, tonic::Status> {
        let _s = Self::trace_request("memory_rating", "find_by_memory_id_and_user_id", &request);
        let req = request.get_ref();
        let (Some(memory_id), Some(user_id)) = (&req.memory_id, &req.user_id) else {
            return Err(tonic::Status::invalid_argument(
                "memory_id and user_id are required",
            ));
        };
        match self.app().find_by_memory_and_user(memory_id, user_id).await {
            Ok(res) => Ok(Response::new(OptionalMemoryRatingResponse { data: res })),
            Err(e) => Err(handle_error(&e)),
        }
    }

    async fn count(
        &self,
        request: tonic::Request<FindCondition>,
    ) -> Result<tonic::Response<CountResponse>, tonic::Status> {
        let _s = Self::trace_request("memory_rating", "count", &request);
        match self.app().count().await {
            Ok(res) => Ok(Response::new(CountResponse { total: res })),
            Err(e) => Err(handle_error(&e)),
        }
    }
}

#[derive(DebugStub)]
pub(crate) struct MemoryRatingGrpcImpl {
    #[debug_stub = "MemoryRatingAppImpl"]
    memory_rating_app: MemoryRatingAppImpl,
}

impl MemoryRatingGrpcImpl {
    pub fn new(memory_rating_app: MemoryRatingAppImpl) -> Self {
        MemoryRatingGrpcImpl { memory_rating_app }
    }
}

impl MemoryRatingGrpc for MemoryRatingGrpcImpl {
    fn app(&self) -> &MemoryRatingAppImpl {
        &self.memory_rating_app
    }
}

impl Tracing for MemoryRatingGrpcImpl {}
