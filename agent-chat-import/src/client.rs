//! ImportClient abstraction over the gRPC ThreadService and MemoryService.
//!
//! Wraps the RPCs the server exposes for batch import and prune:
//!
//!   * `AddMemoriesBatch` — bulk memory insertion (server-side embedding
//!     dispatch, server-side parent_external_ids resolution).
//!   * `UpdateMemoryParents` — guarded parent re-wire for existing memories.
//!   * `MemoryService.FindListByCondition` — used with `external_id_prefix`
//!     to enumerate memories belonging to a given source for prune.
//!   * `MemoryService.Delete` / `ThreadService.Delete` — used by prune
//!     to remove vanished memories and orphan threads.
//!   * `MemoryService.CountByCondition` — used to detect when a thread has
//!     no remaining memories after prune.
//!
//! Dry-run mode is handled at the runner level (`run_all` accepts
//! `Option<&dyn ImportClient>`).

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use futures::StreamExt;
use protobuf::llm_memory::data::{ContentType, MemoryId, ThreadId, UserId};
use protobuf::llm_memory::service::media_service_client::MediaServiceClient;
use protobuf::llm_memory::service::memory_service_client::MemoryServiceClient;
use protobuf::llm_memory::service::thread_group_service_client::ThreadGroupServiceClient;
use protobuf::llm_memory::service::thread_service_client::ThreadServiceClient;
use protobuf::llm_memory::service::upload_request::Payload as UploadPayload;
use protobuf::llm_memory::service::{
    AddLabelsRequest, AddMemoriesBatchRequest, AddMemoriesBatchResponse, FindMemoryListRequest,
    FindThreadListByUserIdRequest, MemoryCountCondition, MemoryListEntry,
    PreviewThreadGroupImportRequest, PreviewThreadGroupImportResponse,
    RecordThreadGroupObservationsRequest, RecordThreadGroupObservationsResponse, RegisterRequest,
    ThreadGroupCapabilitiesRequest, ThreadGroupCapabilitiesResponse,
    ThreadGroupReconciliationReport, UpdateMemoryParentsRequest, UpdateMemoryParentsResponse,
    UploadHeader, UploadRequest,
};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;
use tonic::Status;
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

/// Thin DTO for `MediaService.Upload` so callers (importer) never touch
/// the streaming proto. `kind` is a `ContentType` discriminant (IMAGE=2
/// for images).
#[derive(Debug, Clone)]
pub struct UploadMediaHeader {
    pub kind: ContentType,
    pub media_type: String,
    pub alt: Option<String>,
    pub width: Option<u32>,
    pub height: Option<u32>,
}

/// Thin DTO for `MediaService.Register` (url backend only — the import
/// path never registers s3/file keys, those require a server-side PUT).
#[derive(Debug, Clone)]
pub struct RegisterMediaUrl {
    pub kind: ContentType,
    pub media_type: String,
    pub url: String,
    pub alt: Option<String>,
    pub width: Option<u32>,
    pub height: Option<u32>,
}

#[async_trait]
pub trait ImportClient: Send + Sync {
    /// Group owner for observations created during this import. Older and
    /// test clients preserve the historical same-owner default; live imports
    /// can supply an independent configured owner.
    fn group_owner_user_id(&self, source_user_id: i64) -> i64 {
        source_user_id
    }

    async fn add_memories_batch(
        &self,
        request: AddMemoriesBatchRequest,
    ) -> Result<AddMemoriesBatchResponse>;

    async fn add_labels(&self, request: AddLabelsRequest) -> Result<()>;

    /// Stream bytes to `MediaService.Upload` (reservation→copy→confirm,
    /// sha256 dedup, b-1..b-5 conflict handling all server-side). Returns
    /// the resulting `media_object_id`.
    async fn upload_media(&self, header: UploadMediaHeader, bytes: Vec<u8>) -> Result<i64>;

    /// Reference-register an external URL via `MediaService.Register`
    /// (`storage_backend = "url"`: no fetch, sha256 / byte_size NULL).
    /// Returns the resulting `media_object_id`.
    async fn register_media_url(&self, params: RegisterMediaUrl) -> Result<i64>;

    async fn update_memory_parents(
        &self,
        request: UpdateMemoryParentsRequest,
    ) -> Result<UpdateMemoryParentsResponse>;

    /// Enumerate memories whose external_id begins with the creator-scoped
    /// `prefix`. The server stream is fully consumed and collected — for
    /// vault-sized prefixes (10k–100k entries) this fits comfortably in
    /// memory; very-large vaults are out of Phase A scope.
    async fn find_memories_by_external_id_prefix(
        &self,
        prefix: String,
    ) -> Result<Vec<MemoryListEntry>>;

    /// Look up exactly one external ID. The unique external-id index makes
    /// this suitable for a small, caller-provided set without scanning an
    /// entire source or creator namespace.
    async fn find_memory_by_external_id(
        &self,
        external_id: String,
    ) -> Result<Option<MemoryListEntry>>;

    /// Resolve the unique thread for a user/channel pair. `None` means that
    /// no thread exists; multiple matching threads are reported as an error
    /// so callers never guess which thread should receive imported data.
    async fn find_thread_by_channel_and_user_id(
        &self,
        channel: String,
        user_id: i64,
    ) -> Result<Option<ThreadId>>;

    /// Snapshot every `channel -> thread_id` pair for a user with one
    /// thread-list scan. Importers that resolve a thread per session
    /// (e.g. OpenCode `--all-sessions`) use this once per run instead
    /// of re-streaming all user threads per session. The returned map
    /// may be empty; callers fall back to the per-channel lookup for
    /// misses. Channels backed by multiple threads are omitted so the
    /// per-channel lookup reports the ambiguity.
    async fn find_thread_channels_by_user_id(
        &self,
        user_id: i64,
    ) -> Result<HashMap<String, ThreadId>> {
        let _ = user_id;
        Ok(HashMap::new())
    }

    async fn delete_memory(&self, memory_id: MemoryId) -> Result<()>;

    async fn delete_thread(&self, thread_id: ThreadId) -> Result<()>;

    async fn count_memories_in_thread(&self, thread_id: ThreadId) -> Result<i64>;

    /// Record ThreadGroup adapter observations for an imported subject
    /// thread. Default no-op so lightweight / test clients need not
    /// implement the additive ThreadGroup RPC.
    async fn record_thread_group_observations(
        &self,
        request: RecordThreadGroupObservationsRequest,
    ) -> Result<RecordThreadGroupObservationsResponse> {
        let _ = request;
        Ok(RecordThreadGroupObservationsResponse::default())
    }

    /// Read-only ThreadGroup reconciliation snapshot for the connected
    /// dry-run. Default no-op so lightweight clients stay compatible.
    async fn find_thread_group_reconciliation_report(
        &self,
    ) -> Result<protobuf::llm_memory::service::ThreadGroupReconciliationReport> {
        Ok(Default::default())
    }
}

#[derive(Debug, Clone)]
pub struct LiveGrpcImportClientConfig {
    pub server_url: String,
    pub timeout: Duration,
    pub tls_ca_path: Option<std::path::PathBuf>,
    pub auth_token: Option<String>,
    /// Owner used for new ThreadGroups. `None` preserves the source owner's
    /// historical default while the RPC always carries an explicit value.
    pub group_owner_user_id: Option<i64>,
    /// Per-RPC retry policy. `RetryPolicy::no_retry()` issues a single
    /// attempt and gives up on the first failure. The default
    /// (3 attempts, 1s base / 30s cap with 25% jitter) gives cnpg
    /// PostgreSQL room to recover from transient lock waits or
    /// connection-pool exhaustion without failing the session.
    pub retry: RetryPolicy,
    /// Connected dry-run: route the write RPCs to read-only previews /
    /// no-ops and accumulate a planned report instead of importing.
    pub preview_only: bool,
}

/// Planned-report accumulator for `--dry-run-connect`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ThreadGroupImportPreviewReport {
    pub sessions: u64,
    pub planned_memories: u64,
    pub planned_observations: u64,
    pub planned_relations: u64,
    pub pending: u64,
    pub suppressed_sessions: u64,
    pub conflict_sessions: u64,
}

/// Process-wide preview client so the runner can print the accumulated
/// connected dry-run report after the import path completes.
static PREVIEW_CLIENT: std::sync::OnceLock<Arc<LiveGrpcImportClient>> = std::sync::OnceLock::new();

pub fn set_preview_client(client: Arc<LiveGrpcImportClient>) {
    let _ = PREVIEW_CLIENT.set(client);
}

pub fn preview_report() -> Option<ThreadGroupImportPreviewReport> {
    PREVIEW_CLIENT.get().map(|client| client.preview_report())
}

/// Bounded retry-with-backoff policy applied to every RPC issued by
/// `LiveGrpcImportClient`. Retries are gated on `classify_status` so
/// non-transient errors (e.g. `InvalidArgument`) bubble up immediately.
#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub base_delay_ms: u64,
    pub max_delay_ms: u64,
    /// 0.0 disables jitter; 0.25 means each delay is multiplied by a
    /// uniform random value in `[1.0, 1.25)`. Capped at 1.0.
    pub jitter_ratio: f64,
}

impl RetryPolicy {
    pub fn no_retry() -> Self {
        Self {
            max_attempts: 1,
            base_delay_ms: 0,
            max_delay_ms: 0,
            jitter_ratio: 0.0,
        }
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            base_delay_ms: 1000,
            max_delay_ms: 30_000,
            jitter_ratio: 0.25,
        }
    }
}

pub struct LiveGrpcImportClient {
    channel: Channel,
    auth_token: Option<Arc<String>>,
    retry: RetryPolicy,
    group_owner_user_id: Option<i64>,
    /// One-shot snapshot cache of `channel -> thread_id` for a single
    /// user, filled by the first OpenCode thread resolution so
    /// `--all-sessions` does not re-scan all user threads per session.
    thread_channel_cache: tokio::sync::Mutex<Option<(i64, HashMap<String, ThreadId>)>>,
    /// Connected dry-run mode; see `LiveGrpcImportClientConfig`.
    preview_only: bool,
    preview: std::sync::Mutex<ThreadGroupImportPreviewReport>,
    preview_media_counter: std::sync::atomic::AtomicI64,
}

const MAX_DECODING_MESSAGE_SIZE: usize = 16 * 1024 * 1024 - 1;

/// Per-chunk size for streaming `MediaService.Upload`. Keeps a single
/// large image off one gRPC frame; well under MAX_DECODING_MESSAGE_SIZE.
const MEDIA_UPLOAD_CHUNK: usize = 1024 * 1024;

impl LiveGrpcImportClient {
    pub async fn connect(config: LiveGrpcImportClientConfig) -> Result<Self> {
        let mut endpoint =
            Endpoint::from_shared(config.server_url.clone())?.timeout(config.timeout);
        if config.server_url.starts_with("https://") {
            let mut tls = ClientTlsConfig::new();
            if let Some(ca_path) = config.tls_ca_path {
                let ca_bytes = std::fs::read(&ca_path)
                    .map_err(|e| anyhow!("read --server-tls-ca {}: {e}", ca_path.display()))?;
                let cert = tonic::transport::Certificate::from_pem(ca_bytes);
                tls = tls.ca_certificate(cert);
            } else {
                // No custom CA: trust the roots enabled via tonic's tls-*-roots
                // features (webpki). Without this the trust store is empty and
                // public-CA endpoints (e.g. Let's Encrypt) fail to verify.
                tls = tls.with_enabled_roots();
            }
            endpoint = endpoint.tls_config(tls)?;
        }
        let channel = endpoint
            .connect()
            .await
            .map_err(|e| anyhow!("connect to {}: {e}", config.server_url))?;
        Ok(Self {
            channel,
            auth_token: config.auth_token.map(Arc::new),
            retry: config.retry,
            group_owner_user_id: config.group_owner_user_id,
            thread_channel_cache: tokio::sync::Mutex::new(None),
            preview_only: config.preview_only,
            preview: std::sync::Mutex::new(ThreadGroupImportPreviewReport::default()),
            preview_media_counter: std::sync::atomic::AtomicI64::new(0),
        })
    }

    /// Fail before owner-aware import work if the connected server cannot
    /// preserve the independent ThreadGroup owner contract.
    pub async fn ensure_thread_group_owner_capability(&self) -> Result<()> {
        let response = retry_status(&self.retry, "get_thread_group_capabilities", || {
            let mut client = self.build_thread_group_client();
            let request = self.attach_auth(tonic::Request::new(ThreadGroupCapabilitiesRequest {}));
            async move { client.get_thread_group_capabilities(request).await }
        })
        .await
        .map(|response| response.into_inner());

        validate_thread_group_owner_capability(response)
    }

    /// Snapshot of the accumulated connected dry-run report.
    pub fn preview_report(&self) -> ThreadGroupImportPreviewReport {
        self.preview.lock().unwrap().clone()
    }

    fn build_client(&self) -> ThreadServiceClient<Channel> {
        ThreadServiceClient::new(self.channel.clone())
            .max_decoding_message_size(MAX_DECODING_MESSAGE_SIZE)
            .max_encoding_message_size(MAX_DECODING_MESSAGE_SIZE)
    }

    fn build_memory_client(&self) -> MemoryServiceClient<Channel> {
        MemoryServiceClient::new(self.channel.clone())
            .max_decoding_message_size(MAX_DECODING_MESSAGE_SIZE)
            .max_encoding_message_size(MAX_DECODING_MESSAGE_SIZE)
    }

    fn build_media_client(&self) -> MediaServiceClient<Channel> {
        MediaServiceClient::new(self.channel.clone())
            .max_decoding_message_size(MAX_DECODING_MESSAGE_SIZE)
            .max_encoding_message_size(MAX_DECODING_MESSAGE_SIZE)
    }

    fn build_thread_group_client(&self) -> ThreadGroupServiceClient<Channel> {
        ThreadGroupServiceClient::new(self.channel.clone())
            .max_decoding_message_size(MAX_DECODING_MESSAGE_SIZE)
            .max_encoding_message_size(MAX_DECODING_MESSAGE_SIZE)
    }

    /// Read-only planned-import RPC used by the connected dry-run.
    async fn preview_thread_group_import_rpc(
        &self,
        request: &RecordThreadGroupObservationsRequest,
    ) -> Result<PreviewThreadGroupImportResponse> {
        let preview_request = PreviewThreadGroupImportRequest {
            subject: request.subject.clone(),
            observations: request.observations.clone(),
            explicit_override: false,
            group_owner_user_id: request.group_owner_user_id,
        };
        let response = retry_status(&self.retry, "preview_thread_group_import", || {
            let mut client = self.build_thread_group_client();
            let req = self.attach_auth(tonic::Request::new(preview_request.clone()));
            async move { client.preview_thread_group_import(req).await }
        })
        .await
        .map_err(map_status)?;
        Ok(response.into_inner())
    }

    fn attach_auth<T>(&self, mut req: tonic::Request<T>) -> tonic::Request<T> {
        if let Some(token) = &self.auth_token {
            let value = format!("Bearer {token}");
            if let Ok(v) = MetadataValue::try_from(value) {
                req.metadata_mut().insert("authorization", v);
            }
        }
        req
    }
}

#[async_trait]
impl ImportClient for LiveGrpcImportClient {
    fn group_owner_user_id(&self, source_user_id: i64) -> i64 {
        self.group_owner_user_id.unwrap_or(source_user_id)
    }

    async fn add_memories_batch(
        &self,
        request: AddMemoriesBatchRequest,
    ) -> Result<AddMemoriesBatchResponse> {
        if self.preview_only {
            let mut preview = self.preview.lock().unwrap();
            preview.planned_memories += request.memories.len() as u64;
            let outcomes = request
                .memories
                .iter()
                .enumerate()
                .map(
                    |(index, _)| protobuf::llm_memory::service::AddMemoryOutcome {
                        memory_id: Some(protobuf::llm_memory::data::MemoryId {
                            value: index as i64 + 1,
                        }),
                        created: true,
                        position: index as i32,
                        existing_parent_ids_empty: false,
                        resolved_parent_ids: Vec::new(),
                    },
                )
                .collect();
            return Ok(AddMemoriesBatchResponse {
                thread_id: Some(ThreadId { value: 0 }),
                thread_created: false,
                outcomes,
                suppressed: false,
            });
        }
        // The caller selects the external-id policy per source: legacy
        // sources use upsert=true, while OpenCode pre-filters duplicates and
        // sends upsert=false so existing content is never overwritten.
        let response = retry_status(&self.retry, "add_memories_batch", || {
            let mut client = self.build_client();
            let req = self.attach_auth(tonic::Request::new(request.clone()));
            async move { client.add_memories_batch(req).await }
        })
        .await
        .map_err(map_status)?;
        Ok(response.into_inner())
    }

    async fn add_labels(&self, request: AddLabelsRequest) -> Result<()> {
        if self.preview_only || request.labels.is_empty() {
            return Ok(());
        }
        let response = retry_status(&self.retry, "add_labels", || {
            let mut client = self.build_client();
            let req = self.attach_auth(tonic::Request::new(request.clone()));
            async move { client.add_labels(req).await }
        })
        .await
        .map_err(map_status)?
        .into_inner();
        if !response.is_success {
            return Err(anyhow!("ThreadService.AddLabels returned is_success=false"));
        }
        Ok(())
    }

    async fn upload_media(&self, header: UploadMediaHeader, bytes: Vec<u8>) -> Result<i64> {
        if self.preview_only {
            let _ = (header, bytes);
            return Ok(self
                .preview_media_counter
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                + 1);
        }
        let mut client = self.build_media_client();
        // First message = header; subsequent messages = chunks. Splitting
        // into bounded chunks keeps a single huge image off one frame
        // (server caps cumulative size at MEDIA_UPLOAD_MAX_BYTES anyway).
        let header_msg = UploadRequest {
            payload: Some(UploadPayload::Header(UploadHeader {
                kind: header.kind as i32,
                media_type: header.media_type,
                alt: header.alt,
                width: header.width,
                height: header.height,
            })),
        };
        // Lazily slice `bytes` so at most one chunk copy is alive at a
        // time (the proto needs an owned Vec per chunk, but materializing
        // every chunk up front would double the image in memory). Empty
        // input yields zero chunks (header only), as before.
        let n_chunks = bytes.len().div_ceil(MEDIA_UPLOAD_CHUNK);
        let chunks = (0..n_chunks).map(move |i| {
            let start = i * MEDIA_UPLOAD_CHUNK;
            let end = (start + MEDIA_UPLOAD_CHUNK).min(bytes.len());
            UploadRequest {
                payload: Some(UploadPayload::Chunk(bytes[start..end].to_vec())),
            }
        });
        let stream = futures::stream::iter(std::iter::once(header_msg).chain(chunks));
        let response = client
            .upload(self.attach_auth(tonic::Request::new(stream)))
            .await
            .map_err(map_status)?
            .into_inner();
        response
            .media_object_id
            .map(|id| id.value)
            .ok_or_else(|| anyhow!("Upload response missing media_object_id"))
    }

    async fn register_media_url(&self, params: RegisterMediaUrl) -> Result<i64> {
        if self.preview_only {
            let _ = params;
            return Ok(self
                .preview_media_counter
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                + 1);
        }
        let mut client = self.build_media_client();
        let req = RegisterRequest {
            kind: params.kind as i32,
            media_type: params.media_type,
            storage_uri: params.url,
            sha256: None,
            byte_size: None,
            width: params.width,
            height: params.height,
            alt: params.alt,
            storage_backend: "url".to_string(),
        };
        let meta = client
            .register(self.attach_auth(tonic::Request::new(req)))
            .await
            .map_err(map_status)?
            .into_inner();
        meta.id
            .map(|id| id.value)
            .ok_or_else(|| anyhow!("Register response missing media_object id"))
    }

    async fn update_memory_parents(
        &self,
        request: UpdateMemoryParentsRequest,
    ) -> Result<UpdateMemoryParentsResponse> {
        if self.preview_only {
            let _ = request;
            return Ok(UpdateMemoryParentsResponse::default());
        }
        // Re-sending an UpdateMemoryParents that already succeeded gets
        // `rewired: false` back on the second attempt (the server-side
        // guard sees the parents are already set); the importer only
        // counts `rewired: true`, so the retry never inflates the
        // `memories_rewired` summary line.
        let response = retry_status(&self.retry, "update_memory_parents", || {
            let mut client = self.build_client();
            let req = self.attach_auth(tonic::Request::new(request.clone()));
            async move { client.update_memory_parents(req).await }
        })
        .await
        .map_err(map_status)?;
        Ok(response.into_inner())
    }

    async fn find_memories_by_external_id_prefix(
        &self,
        prefix: String,
    ) -> Result<Vec<MemoryListEntry>> {
        // Only the initial RPC dispatch is retried — a mid-stream
        // failure after we've started consuming items would mean
        // restarting the whole prefix scan, which on a vault-sized
        // prefix could be costlier than the single-attempt failure it
        // would have replaced.
        let stream = retry_status(&self.retry, "find_memories_by_external_id_prefix", || {
            let mut client = self.build_memory_client();
            let req = FindMemoryListRequest {
                external_id_prefix: Some(prefix.clone()),
                ..Default::default()
            };
            let auth_req = self.attach_auth(tonic::Request::new(req));
            async move { client.find_list_by_condition(auth_req).await }
        })
        .await
        .map_err(map_status)?;
        let mut out = Vec::new();
        let mut stream = stream.into_inner();
        while let Some(item) = stream.next().await {
            out.push(item.map_err(map_status)?);
        }
        Ok(out)
    }

    async fn find_memory_by_external_id(
        &self,
        external_id: String,
    ) -> Result<Option<MemoryListEntry>> {
        let stream = retry_status(&self.retry, "find_memory_by_external_id", || {
            let mut client = self.build_memory_client();
            let req = FindMemoryListRequest {
                external_id: Some(external_id.clone()),
                limit: Some(1),
                ..Default::default()
            };
            let auth_req = self.attach_auth(tonic::Request::new(req));
            async move { client.find_list_by_condition(auth_req).await }
        })
        .await
        .map_err(map_status)?;
        let mut stream = stream.into_inner();
        let mut first_entry = None;
        while let Some(entry) = stream.next().await {
            match entry {
                Ok(entry) if first_entry.is_none() => first_entry = Some(entry),
                Ok(_) => {}
                Err(status) if first_entry.is_some() => {
                    tracing::warn!(
                        code = ?status.code(),
                        message = status.message(),
                        "exact memory lookup stream failed after the result was received"
                    );
                }
                Err(status) => return Err(map_status(status)),
            }
        }
        Ok(first_entry)
    }

    async fn find_thread_by_channel_and_user_id(
        &self,
        channel: String,
        user_id: i64,
    ) -> Result<Option<ThreadId>> {
        // Only the initial RPC dispatch is retried. Once the stream is being
        // consumed, restarting it would repeat an unbounded user-thread scan.
        let stream = retry_status(&self.retry, "find_thread_by_channel_and_user_id", || {
            let mut client = self.build_client();
            let request = FindThreadListByUserIdRequest {
                user_id: Some(UserId { value: user_id }),
                ..Default::default()
            };
            let auth_request = self.attach_auth(tonic::Request::new(request));
            async move { client.find_thread_list_by_user_id(auth_request).await }
        })
        .await
        .map_err(map_status)?;

        let mut stream = stream.into_inner();
        let mut matching_thread: Option<ThreadId> = None;
        while let Some(thread) = stream.next().await {
            let thread = thread.map_err(map_status)?;
            let Some(data) = thread.data else {
                continue;
            };
            if data.user_id.as_ref().map(|id| id.value) != Some(user_id)
                || data.channel.as_deref() != Some(channel.as_str())
            {
                continue;
            }
            let thread_id = thread
                .id
                .ok_or_else(|| anyhow!("thread list returned a matching thread without id"))?;
            if matching_thread.is_some() {
                return Err(anyhow!(
                    "multiple threads match channel {channel:?} for user {user_id}"
                ));
            }
            matching_thread = Some(thread_id);
        }
        Ok(matching_thread)
    }

    async fn find_thread_channels_by_user_id(
        &self,
        user_id: i64,
    ) -> Result<HashMap<String, ThreadId>> {
        // Reuse the one-shot snapshot for the same user across sessions.
        if let Some((cached_user, cache)) = self.thread_channel_cache.lock().await.as_ref()
            && *cached_user == user_id
        {
            return Ok(cache.clone());
        }
        // Only the initial RPC dispatch is retried, same policy as the
        // per-channel lookup: a mid-stream failure must not restart the
        // full user-thread scan.
        let stream = retry_status(&self.retry, "find_thread_channels_by_user_id", || {
            let mut client = self.build_client();
            let request = FindThreadListByUserIdRequest {
                user_id: Some(UserId { value: user_id }),
                ..Default::default()
            };
            let auth_request = self.attach_auth(tonic::Request::new(request));
            async move { client.find_thread_list_by_user_id(auth_request).await }
        })
        .await
        .map_err(map_status)?;

        let mut stream = stream.into_inner();
        let mut resolved: HashMap<String, ThreadId> = HashMap::new();
        let mut ambiguous: HashSet<String> = HashSet::new();
        while let Some(thread) = stream.next().await {
            let thread = thread.map_err(map_status)?;
            let Some(data) = thread.data else {
                continue;
            };
            if data.user_id.as_ref().map(|id| id.value) != Some(user_id) {
                continue;
            }
            let Some(channel) = data.channel.filter(|c| !c.is_empty()) else {
                continue;
            };
            let thread_id = thread
                .id
                .ok_or_else(|| anyhow!("thread list returned a thread without id"))?;
            // A channel with two or more threads is ambiguous; omit it
            // from the snapshot so the per-channel lookup reports the
            // ambiguity instead of silently picking one winner.
            if resolved.remove(&channel).is_some() {
                ambiguous.insert(channel);
            } else if !ambiguous.contains(&channel) {
                resolved.insert(channel, thread_id);
            }
        }
        let map = resolved;
        *self.thread_channel_cache.lock().await = Some((user_id, map.clone()));
        Ok(map)
    }

    async fn delete_memory(&self, memory_id: MemoryId) -> Result<()> {
        if self.preview_only {
            let _ = memory_id;
            return Ok(());
        }
        let mut client = self.build_memory_client();
        client
            .delete(self.attach_auth(tonic::Request::new(memory_id)))
            .await
            .map_err(map_status)?;
        Ok(())
    }

    async fn delete_thread(&self, thread_id: ThreadId) -> Result<()> {
        if self.preview_only {
            let _ = thread_id;
            return Ok(());
        }
        let mut client = self.build_client();
        client
            .delete(self.attach_auth(tonic::Request::new(thread_id)))
            .await
            .map_err(map_status)?;
        Ok(())
    }

    async fn count_memories_in_thread(&self, thread_id: ThreadId) -> Result<i64> {
        let mut client = self.build_memory_client();
        let req = MemoryCountCondition {
            thread_id: Some(thread_id.value),
            ..Default::default()
        };
        let response = client
            .count_by_condition(self.attach_auth(tonic::Request::new(req)))
            .await
            .map_err(map_status)?;
        Ok(response.into_inner().total)
    }

    async fn record_thread_group_observations(
        &self,
        request: RecordThreadGroupObservationsRequest,
    ) -> Result<RecordThreadGroupObservationsResponse> {
        if self.preview_only {
            let preview = self.preview_thread_group_import_rpc(&request).await?;
            let mut report = self.preview.lock().unwrap();
            report.sessions += 1;
            report.planned_observations += preview.planned_observations.max(0) as u64;
            report.planned_relations += preview.planned_relations.max(0) as u64;
            report.pending += preview.pending.max(0) as u64;
            if preview.suppressed {
                report.suppressed_sessions += 1;
            }
            if preview.conflict {
                report.conflict_sessions += 1;
            }
            return Ok(RecordThreadGroupObservationsResponse::default());
        }
        let response = retry_status(&self.retry, "record_thread_group_observations", || {
            let mut client = self.build_thread_group_client();
            let req = self.attach_auth(tonic::Request::new(request.clone()));
            async move { client.record_thread_group_observations(req).await }
        })
        .await
        .map_err(map_status)?;
        Ok(response.into_inner())
    }

    async fn find_thread_group_reconciliation_report(
        &self,
    ) -> Result<ThreadGroupReconciliationReport> {
        let response = retry_status(
            &self.retry,
            "find_thread_group_reconciliation_report",
            || {
                let mut client = self.build_thread_group_client();
                let req = self.attach_auth(tonic::Request::new(
                    protobuf::llm_memory::service::FindThreadGroupReconciliationReportRequest {},
                ));
                async move { client.find_thread_group_reconciliation_report(req).await }
            },
        )
        .await
        .map_err(map_status)?;
        Ok(response.into_inner())
    }
}

fn map_status(status: Status) -> anyhow::Error {
    anyhow!(
        "gRPC error: code={:?}, message={}",
        status.code(),
        status.message()
    )
}

fn validate_thread_group_owner_capability(
    response: Result<ThreadGroupCapabilitiesResponse, Status>,
) -> Result<()> {
    match response {
        Ok(response) if response.supports_independent_group_owner_scope => Ok(()),
        Ok(_) => Err(anyhow!(
            "connected Memories server does not support independent ThreadGroup group ownership; upgrade the server before importing"
        )),
        Err(status) if status.code() == tonic::Code::Unimplemented => Err(anyhow!(
            "connected Memories server does not support independent ThreadGroup group ownership (UNIMPLEMENTED: {}); upgrade the server before importing",
            status.message()
        )),
        Err(status) => Err(map_status(status)),
    }
}

// PostgreSQL SQLSTATEs we treat as transient when the server wraps
// them into `tonic::Status::internal`. We match against the message
// text because tonic does not surface the SQLSTATE as a typed field;
// the server-side error wrapping (sqlx → anyhow → Status::internal)
// keeps the code as a substring in the human-readable message.
const PG_SQLSTATE_SERIALIZATION_FAILURE: &str = "40001";
const PG_SQLSTATE_DEADLOCK_DETECTED: &str = "40P01";

/// Returns `true` when the given gRPC status describes a transient,
/// retryable condition. The set is deliberately narrow:
///   * `Unavailable` — the server (or anything between us and it) is
///     temporarily unreachable.
///   * `DeadlineExceeded` — RPC deadline hit; usually means the next
///     attempt will succeed if the upstream caught up.
///   * `ResourceExhausted` — connection pool / quota / rate limit.
///   * `Cancelled` — an in-flight RPC can become cancelled after an h2
///     `GOAWAY`, so retrying lets the session recover. Ctrl+C also produces
///     `Cancelled`; retrying it up to `max_attempts` is acceptable because
///     the backoff is short and the practical impact is limited.
///   * `Internal` with a PostgreSQL serialization-failure or
///     deadlock-detected SQLSTATE in the message text.
fn is_retryable_status(status: &Status) -> bool {
    use tonic::Code;
    if matches!(
        status.code(),
        Code::Unavailable | Code::DeadlineExceeded | Code::ResourceExhausted | Code::Cancelled
    ) {
        return true;
    }
    if status.code() == Code::Internal {
        let msg = status.message();
        if msg.contains(PG_SQLSTATE_SERIALIZATION_FAILURE)
            || msg.contains(PG_SQLSTATE_DEADLOCK_DETECTED)
        {
            return true;
        }
    }
    false
}

fn compute_backoff(policy: &RetryPolicy, attempt: u32) -> Duration {
    // attempt is 1-based — first retry uses base_delay, second uses 2x,
    // capped at max_delay.
    let exp = attempt.saturating_sub(1).min(30);
    let raw = policy
        .base_delay_ms
        .saturating_mul(1u64 << exp)
        .min(policy.max_delay_ms);
    if policy.jitter_ratio <= 0.0 {
        return Duration::from_millis(raw);
    }
    // Full-jitter in `[raw, raw * (1 + jitter_ratio))`. `rand` is not
    // a dependency of this crate; use a deterministic-enough source by
    // taking the low bits of the wall clock.
    let clamp = policy.jitter_ratio.clamp(0.0, 1.0);
    let max_jitter = (raw as f64) * clamp;
    let rng_seed = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0);
    let frac = (rng_seed % 1000) as f64 / 1000.0;
    Duration::from_millis(raw.saturating_add((frac * max_jitter) as u64))
}

/// Run an RPC with the configured `RetryPolicy`. The closure returns a
/// `Result<T, Status>`; `Status` lets us classify the error precisely
/// (we lose code information once it's mapped through `map_status` into
/// an `anyhow::Error`). Returns the last error if all attempts fail.
async fn retry_status<F, Fut, T>(
    policy: &RetryPolicy,
    op_name: &str,
    mut op: F,
) -> Result<T, Status>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, Status>>,
{
    let mut attempt: u32 = 1;
    loop {
        match op().await {
            Ok(v) => return Ok(v),
            Err(status) => {
                if attempt >= policy.max_attempts || !is_retryable_status(&status) {
                    return Err(status);
                }
                let delay = compute_backoff(policy, attempt);
                tracing::warn!(
                    op = op_name,
                    attempt = attempt,
                    max_attempts = policy.max_attempts,
                    code = ?status.code(),
                    delay_ms = delay.as_millis() as u64,
                    "RPC failed transient, retrying"
                );
                tokio::time::sleep(delay).await;
                attempt += 1;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use tonic::{Code, Status};

    #[test]
    fn owner_scope_capability_gate_accepts_supported_server() {
        let response = protobuf::llm_memory::service::ThreadGroupCapabilitiesResponse {
            supports_independent_group_owner_scope: true,
        };

        assert!(validate_thread_group_owner_capability(Ok(response)).is_ok());
    }

    #[test]
    fn owner_scope_capability_gate_rejects_legacy_unimplemented_server_explicitly() {
        let error =
            validate_thread_group_owner_capability(Err(Status::unimplemented("unknown RPC")))
                .unwrap_err()
                .to_string();

        assert!(error.contains("does not support independent ThreadGroup group ownership"));
        assert!(error.contains("UNIMPLEMENTED"));
    }

    #[test]
    fn owner_scope_capability_gate_rejects_missing_server_capability() {
        let response = protobuf::llm_memory::service::ThreadGroupCapabilitiesResponse {
            supports_independent_group_owner_scope: false,
        };

        let error = validate_thread_group_owner_capability(Ok(response))
            .unwrap_err()
            .to_string();
        assert!(error.contains("does not support independent ThreadGroup group ownership"));
    }

    #[test]
    fn is_retryable_status_covers_transient_codes() {
        assert!(is_retryable_status(&Status::unavailable("x")));
        assert!(is_retryable_status(&Status::deadline_exceeded("x")));
        assert!(is_retryable_status(&Status::resource_exhausted("x")));
        assert!(is_retryable_status(&Status::cancelled("x")));
        assert!(!is_retryable_status(&Status::invalid_argument("x")));
        assert!(!is_retryable_status(&Status::not_found("x")));
        assert!(!is_retryable_status(&Status::failed_precondition("x")));
    }

    #[test]
    fn is_retryable_status_covers_postgres_sqlstates() {
        // Server-side wrapping is just "Internal: <text>"; we sniff the
        // text for the canonical Postgres serialization/deadlock codes.
        assert!(is_retryable_status(&Status::new(
            Code::Internal,
            "db error: SQLSTATE 40001 serialization_failure"
        )));
        assert!(is_retryable_status(&Status::new(
            Code::Internal,
            "db error: SQLSTATE 40P01 deadlock_detected"
        )));
        // Internal without the SQLSTATE marker is NOT retried — it likely
        // signals a server bug, not a transient race.
        assert!(!is_retryable_status(&Status::new(
            Code::Internal,
            "unexpected null in column"
        )));
    }

    #[test]
    fn compute_backoff_caps_at_max() {
        let policy = RetryPolicy {
            max_attempts: 10,
            base_delay_ms: 100,
            max_delay_ms: 1000,
            jitter_ratio: 0.0,
        };
        // attempt 1 → 100 ms; attempt 4 → 800 ms; attempt 5 → 1000 ms (capped).
        assert_eq!(compute_backoff(&policy, 1).as_millis(), 100);
        assert_eq!(compute_backoff(&policy, 4).as_millis(), 800);
        assert_eq!(compute_backoff(&policy, 5).as_millis(), 1000);
        // Even an absurdly high attempt stays at the cap, doesn't wrap.
        assert_eq!(compute_backoff(&policy, 30).as_millis(), 1000);
    }

    #[tokio::test]
    async fn retry_status_succeeds_after_transient_failures() {
        let policy = RetryPolicy {
            max_attempts: 3,
            base_delay_ms: 1, // sub-millisecond to keep the test fast
            max_delay_ms: 10,
            jitter_ratio: 0.0,
        };
        let attempts = Mutex::new(0u32);
        let r: Result<u32, Status> = retry_status(&policy, "test", || {
            let n = {
                let mut g = attempts.lock().unwrap();
                *g += 1;
                *g
            };
            async move {
                match n {
                    1 => Err(Status::unavailable("transient")),
                    2 => Err(Status::cancelled("transient after GOAWAY")),
                    _ => Ok(n),
                }
            }
        })
        .await;
        assert_eq!(r.unwrap(), 3);
    }

    #[tokio::test]
    async fn retry_status_gives_up_after_max_attempts() {
        let policy = RetryPolicy {
            max_attempts: 2,
            base_delay_ms: 1,
            max_delay_ms: 10,
            jitter_ratio: 0.0,
        };
        let attempts = Mutex::new(0u32);
        let r: Result<u32, Status> = retry_status(&policy, "test", || {
            *attempts.lock().unwrap() += 1;
            async { Err(Status::unavailable("always")) }
        })
        .await;
        assert!(r.is_err());
        assert_eq!(*attempts.lock().unwrap(), 2, "max_attempts respected");
    }

    #[tokio::test]
    async fn retry_status_does_not_retry_non_retryable() {
        let policy = RetryPolicy::default();
        let attempts = Mutex::new(0u32);
        let r: Result<u32, Status> = retry_status(&policy, "test", || {
            *attempts.lock().unwrap() += 1;
            async { Err(Status::invalid_argument("bad")) }
        })
        .await;
        assert!(r.is_err());
        assert_eq!(
            *attempts.lock().unwrap(),
            1,
            "non-retryable status must surface on first attempt"
        );
    }

    struct OneMemoryPerLookup {
        eof_count: std::sync::Arc<std::sync::atomic::AtomicUsize>,
        post_entry_error: bool,
    }

    #[async_trait::async_trait]
    impl protobuf::llm_memory::service::memory_service_server::MemoryService for OneMemoryPerLookup {
        type FindListStream = std::pin::Pin<
            Box<
                dyn futures::Stream<Item = Result<protobuf::llm_memory::data::Memory, Status>>
                    + Send,
            >,
        >;
        type FindRecentListByUserIdStream = std::pin::Pin<
            Box<
                dyn futures::Stream<Item = Result<protobuf::llm_memory::data::Memory, Status>>
                    + Send,
            >,
        >;
        type FindListByConditionStream = std::pin::Pin<
            Box<
                dyn futures::Stream<
                        Item = Result<protobuf::llm_memory::service::MemoryListEntry, Status>,
                    > + Send,
            >,
        >;

        async fn create(
            &self,
            _request: tonic::Request<protobuf::llm_memory::data::MemoryData>,
        ) -> Result<tonic::Response<protobuf::llm_memory::service::CreateMemoryResponse>, Status>
        {
            Err(Status::unimplemented("not used by this test"))
        }

        async fn update(
            &self,
            _request: tonic::Request<protobuf::llm_memory::data::Memory>,
        ) -> Result<tonic::Response<protobuf::llm_memory::service::SuccessResponse>, Status>
        {
            Err(Status::unimplemented("not used by this test"))
        }

        async fn delete(
            &self,
            _request: tonic::Request<protobuf::llm_memory::data::MemoryId>,
        ) -> Result<tonic::Response<protobuf::llm_memory::service::SuccessResponse>, Status>
        {
            Err(Status::unimplemented("not used by this test"))
        }

        async fn find(
            &self,
            _request: tonic::Request<protobuf::llm_memory::data::MemoryId>,
        ) -> Result<tonic::Response<protobuf::llm_memory::service::OptionalMemoryResponse>, Status>
        {
            Err(Status::unimplemented("not used by this test"))
        }

        async fn find_list(
            &self,
            _request: tonic::Request<protobuf::llm_memory::service::FindListRequest>,
        ) -> Result<tonic::Response<Self::FindListStream>, Status> {
            Err(Status::unimplemented("not used by this test"))
        }

        async fn find_recent_list_by_user_id(
            &self,
            _request: tonic::Request<protobuf::llm_memory::service::FindRecentListByUserIdRequest>,
        ) -> Result<tonic::Response<Self::FindRecentListByUserIdStream>, Status> {
            Err(Status::unimplemented("not used by this test"))
        }

        async fn count(
            &self,
            _request: tonic::Request<protobuf::llm_memory::service::FindCondition>,
        ) -> Result<tonic::Response<protobuf::llm_memory::service::CountResponse>, Status> {
            Err(Status::unimplemented("not used by this test"))
        }

        async fn find_list_by_condition(
            &self,
            request: tonic::Request<protobuf::llm_memory::service::FindMemoryListRequest>,
        ) -> Result<tonic::Response<Self::FindListByConditionStream>, Status> {
            let request = request.into_inner();
            assert_eq!(request.limit, Some(1));
            let entry = protobuf::llm_memory::service::MemoryListEntry {
                memory: Some(protobuf::llm_memory::data::Memory {
                    data: Some(protobuf::llm_memory::data::MemoryData {
                        external_id: request.external_id,
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            };
            let eof_count = std::sync::Arc::clone(&self.eof_count);
            let post_entry_error = self.post_entry_error;
            let stream = futures::stream::unfold(
                (Some(entry), post_entry_error),
                move |(entry, emit_error)| {
                    let eof_count = std::sync::Arc::clone(&eof_count);
                    async move {
                        match (entry, emit_error) {
                            (Some(entry), true) => Some((Ok(entry), (None, true))),
                            (None, true) => Some((
                                Err(Status::internal("stream failed after first result")),
                                (None, false),
                            )),
                            (Some(entry), false) => Some((Ok(entry), (None, false))),
                            (None, false) => {
                                for _ in 0..2 {
                                    tokio::task::yield_now().await;
                                }
                                eof_count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                                None
                            }
                        }
                    }
                },
            );
            Ok(tonic::Response::new(Box::pin(stream)))
        }

        async fn count_by_condition(
            &self,
            _request: tonic::Request<protobuf::llm_memory::service::MemoryCountCondition>,
        ) -> Result<tonic::Response<protobuf::llm_memory::service::CountResponse>, Status> {
            Err(Status::unimplemented("not used by this test"))
        }

        async fn update_content_no_dispatch(
            &self,
            _request: tonic::Request<protobuf::llm_memory::service::UpdateContentNoDispatchRequest>,
        ) -> Result<tonic::Response<protobuf::llm_memory::service::SuccessResponse>, Status>
        {
            Err(Status::unimplemented("not used by this test"))
        }
    }

    #[tokio::test]
    async fn exact_lookup_drains_each_server_stream_to_eof() {
        let eof_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        let incoming = futures::stream::unfold(listener, |listener| async {
            match listener.accept().await {
                Ok((stream, _)) => Some((Ok::<_, std::io::Error>(stream), listener)),
                Err(_) => None,
            }
        });
        let server = tonic::transport::Server::builder()
            .add_service(
                protobuf::llm_memory::service::memory_service_server::MemoryServiceServer::new(
                    OneMemoryPerLookup {
                        eof_count: std::sync::Arc::clone(&eof_count),
                        post_entry_error: false,
                    },
                ),
            )
            .serve_with_incoming(incoming);
        let server_task = tokio::spawn(server);

        let client = LiveGrpcImportClient::connect(LiveGrpcImportClientConfig {
            server_url: format!("http://{address}"),
            timeout: Duration::from_secs(5),
            tls_ca_path: None,
            auth_token: None,
            group_owner_user_id: None,
            retry: RetryPolicy::no_retry(),
            preview_only: false,
        })
        .await
        .unwrap();

        for lookup in 0..1100 {
            let result = client
                .find_memory_by_external_id(format!("memory-{lookup}"))
                .await;
            assert!(result.is_ok(), "lookup {lookup} failed: {result:?}");
        }
        assert_eq!(
            eof_count.load(std::sync::atomic::Ordering::SeqCst),
            1100,
            "every server stream must be consumed through EOF"
        );

        server_task.abort();
        let _ = server_task.await;
    }

    #[tokio::test]
    async fn exact_lookup_keeps_first_entry_after_post_entry_stream_error() {
        let eof_count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let listener = tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0))
            .await
            .unwrap();
        let address = listener.local_addr().unwrap();
        let incoming = futures::stream::unfold(listener, |listener| async {
            match listener.accept().await {
                Ok((stream, _)) => Some((Ok::<_, std::io::Error>(stream), listener)),
                Err(_) => None,
            }
        });
        let server = tonic::transport::Server::builder()
            .add_service(
                protobuf::llm_memory::service::memory_service_server::MemoryServiceServer::new(
                    OneMemoryPerLookup {
                        eof_count: std::sync::Arc::clone(&eof_count),
                        post_entry_error: true,
                    },
                ),
            )
            .serve_with_incoming(incoming);
        let server_task = tokio::spawn(server);

        let client = LiveGrpcImportClient::connect(LiveGrpcImportClientConfig {
            server_url: format!("http://{address}"),
            timeout: Duration::from_secs(5),
            tls_ca_path: None,
            auth_token: None,
            group_owner_user_id: None,
            retry: RetryPolicy::no_retry(),
            preview_only: false,
        })
        .await
        .unwrap();

        let result = client
            .find_memory_by_external_id("memory-after-error".to_string())
            .await
            .unwrap();
        let external_id = result
            .and_then(|entry| entry.memory)
            .and_then(|memory| memory.data)
            .and_then(|data| data.external_id);
        assert_eq!(external_id.as_deref(), Some("memory-after-error"));

        server_task.abort();
        let _ = server_task.await;
    }
}
