//! `MediaService` gRPC surface. The 3-stage Upload / b-1..b-5 conflict
//! logic lives in `app::app::media`; this layer only adapts the tonic
//! streaming request into the app's chunk stream and maps errors.

use crate::protobuf::llm_memory::data::{MediaMetadata, MediaObjectId};
use crate::protobuf::llm_memory::service::{
    DeleteMediaRequest, Empty, FindMediaRequest, RegisterRequest, ResolveRequest, ResolveResponse,
    UploadRequest, UploadResponse, media_service_server::MediaService, upload_request::Payload,
};
use crate::service::error_handle::handle_error;
use app::app::media::{MediaAppImpl, RegisterParams, UploadHeaderMeta};
use bytes::Bytes;
use command_utils::trace::Tracing;
use futures::StreamExt as _;
use infra::infra::media_storage::StorageError;
use std::fmt::Debug;
use tonic::{Response, Status};

pub trait MediaGrpc {
    fn app(&self) -> &MediaAppImpl;
}

#[tonic::async_trait]
impl<T: MediaGrpc + Tracing + Send + Debug + Sync + 'static> MediaService for T {
    #[tracing::instrument]
    async fn upload(
        &self,
        request: tonic::Request<tonic::Streaming<UploadRequest>>,
    ) -> Result<Response<UploadResponse>, Status> {
        let _span = Self::trace_request("media", "upload", &request);
        let mut stream = request.into_inner();

        // First message must be the header.
        let first = stream
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("empty upload stream"))?;
        let header = match first.payload {
            Some(Payload::Header(h)) => UploadHeaderMeta {
                kind: h.kind,
                media_type: h.media_type,
                alt: h.alt,
                width: h.width,
                height: h.height,
            },
            _ => {
                return Err(Status::invalid_argument(
                    "first upload message must be an UploadHeader",
                ));
            }
        };

        // Remaining messages are chunks. A stray header mid-stream is a
        // protocol error surfaced through the stream (the app layer's
        // size guard maps TooLarge -> RESOURCE_EXHAUSTED).
        let chunks = stream.map(|msg| match msg {
            Ok(UploadRequest {
                payload: Some(Payload::Chunk(bytes)),
            }) => Ok(Bytes::from(bytes)),
            Ok(UploadRequest {
                payload: Some(Payload::Header(_)),
            }) => Err(StorageError::Backend(
                "unexpected UploadHeader mid-stream".to_string(),
            )),
            Ok(_) => Err(StorageError::Backend("empty upload message".to_string())),
            Err(status) => Err(StorageError::Io(status.to_string())),
        });

        match self.app().upload(header, chunks).await {
            Ok(out) => Ok(Response::new(UploadResponse {
                media_object_id: Some(MediaObjectId {
                    value: out.media_object_id,
                }),
                deduplicated: out.deduplicated,
            })),
            Err(e) => Err(handle_error(&e)),
        }
    }

    #[tracing::instrument]
    async fn register(
        &self,
        request: tonic::Request<RegisterRequest>,
    ) -> Result<Response<MediaMetadata>, Status> {
        let _span = Self::trace_request("media", "register", &request);
        let r = request.into_inner();
        let params = RegisterParams {
            kind: r.kind,
            media_type: r.media_type,
            storage_uri: r.storage_uri,
            sha256: r.sha256,
            byte_size: r.byte_size,
            width: r.width,
            height: r.height,
            alt: r.alt,
            storage_backend: r.storage_backend,
        };
        match self.app().register(params).await {
            Ok(meta) => Ok(Response::new(meta)),
            Err(e) => Err(handle_error(&e)),
        }
    }

    #[tracing::instrument]
    async fn find(
        &self,
        request: tonic::Request<FindMediaRequest>,
    ) -> Result<Response<MediaMetadata>, Status> {
        let _span = Self::trace_request("media", "find", &request);
        let id = request
            .into_inner()
            .media_object_id
            .ok_or_else(|| Status::invalid_argument("media_object_id is required"))?
            .value;
        match self.app().metadata_of(id).await {
            Ok(meta) => Ok(Response::new(meta)),
            Err(e) => Err(handle_error(&e)),
        }
    }

    #[tracing::instrument]
    async fn resolve(
        &self,
        request: tonic::Request<ResolveRequest>,
    ) -> Result<Response<ResolveResponse>, Status> {
        let _span = Self::trace_request("media", "resolve", &request);
        let r = request.into_inner();
        let id = r
            .media_object_id
            .ok_or_else(|| Status::invalid_argument("media_object_id is required"))?
            .value;
        match self.app().resolve(id, r.ttl_sec).await {
            Ok(out) => Ok(Response::new(ResolveResponse {
                url: out.url,
                expires_at: out.expires_at,
                unresolved: out.unresolved,
            })),
            Err(e) => Err(handle_error(&e)),
        }
    }

    #[tracing::instrument]
    async fn delete(
        &self,
        request: tonic::Request<DeleteMediaRequest>,
    ) -> Result<Response<Empty>, Status> {
        let _span = Self::trace_request("media", "delete", &request);
        let id = request
            .into_inner()
            .media_object_id
            .ok_or_else(|| Status::invalid_argument("media_object_id is required"))?
            .value;
        match self.app().delete(id).await {
            Ok(()) => Ok(Response::new(Empty {})),
            Err(e) => Err(handle_error(&e)),
        }
    }
}

#[derive(debug_stub_derive::DebugStub)]
pub(crate) struct MediaGrpcImpl {
    #[debug_stub = "MediaAppImpl"]
    media_app: std::sync::Arc<MediaAppImpl>,
}

impl MediaGrpcImpl {
    pub fn new(media_app: std::sync::Arc<MediaAppImpl>) -> Self {
        MediaGrpcImpl { media_app }
    }
}

impl MediaGrpc for MediaGrpcImpl {
    fn app(&self) -> &MediaAppImpl {
        &self.media_app
    }
}

impl Tracing for MediaGrpcImpl {}
