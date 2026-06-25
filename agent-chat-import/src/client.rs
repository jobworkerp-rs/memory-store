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
use protobuf::llm_memory::data::{ContentType, MemoryId, ThreadId};
use protobuf::llm_memory::service::media_service_client::MediaServiceClient;
use protobuf::llm_memory::service::memory_service_client::MemoryServiceClient;
use protobuf::llm_memory::service::thread_service_client::ThreadServiceClient;
use protobuf::llm_memory::service::upload_request::Payload as UploadPayload;
use protobuf::llm_memory::service::{
    AddMemoriesBatchRequest, AddMemoriesBatchResponse, FindMemoryListRequest, MemoryCountCondition,
    MemoryListEntry, RegisterRequest, UpdateMemoryParentsRequest, UpdateMemoryParentsResponse,
    UploadHeader, UploadRequest,
};
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
    async fn add_memories_batch(
        &self,
        request: AddMemoriesBatchRequest,
    ) -> Result<AddMemoriesBatchResponse>;

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

    /// Enumerate memories whose external_id begins with `prefix`, scoped to
    /// `user_id`. The server stream is fully consumed and collected — for
    /// vault-sized prefixes (10k–100k entries) this fits comfortably in
    /// memory; very-large vaults are out of Phase A scope.
    async fn find_memories_by_external_id_prefix(
        &self,
        prefix: String,
        user_id: i64,
    ) -> Result<Vec<MemoryListEntry>>;

    async fn delete_memory(&self, memory_id: MemoryId) -> Result<()>;

    async fn delete_thread(&self, thread_id: ThreadId) -> Result<()>;

    async fn count_memories_in_thread(&self, thread_id: ThreadId) -> Result<i64>;
}

#[derive(Debug, Clone)]
pub struct LiveGrpcImportClientConfig {
    pub server_url: String,
    pub timeout: Duration,
    pub tls_ca_path: Option<std::path::PathBuf>,
    pub auth_token: Option<String>,
    /// Per-RPC retry policy. `RetryPolicy::no_retry()` issues a single
    /// attempt and gives up on the first failure. The default
    /// (3 attempts, 1s base / 30s cap with 25% jitter) gives cnpg
    /// PostgreSQL room to recover from transient lock waits or
    /// connection-pool exhaustion without failing the session.
    pub retry: RetryPolicy,
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
        })
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
    async fn add_memories_batch(
        &self,
        request: AddMemoriesBatchRequest,
    ) -> Result<AddMemoriesBatchResponse> {
        // `upsert_by_external_id = true` is set unconditionally by the
        // importer, so re-sending the same batch is idempotent on the
        // server side (existing memories are reused, never duplicated).
        let response = retry_status(&self.retry, "add_memories_batch", || {
            let mut client = self.build_client();
            let req = self.attach_auth(tonic::Request::new(request.clone()));
            async move { client.add_memories_batch(req).await }
        })
        .await
        .map_err(map_status)?;
        Ok(response.into_inner())
    }

    async fn upload_media(&self, header: UploadMediaHeader, bytes: Vec<u8>) -> Result<i64> {
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
        user_id: i64,
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
                user_id: Some(protobuf::llm_memory::data::UserId { value: user_id }),
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

    async fn delete_memory(&self, memory_id: MemoryId) -> Result<()> {
        let mut client = self.build_memory_client();
        client
            .delete(self.attach_auth(tonic::Request::new(memory_id)))
            .await
            .map_err(map_status)?;
        Ok(())
    }

    async fn delete_thread(&self, thread_id: ThreadId) -> Result<()> {
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
}

fn map_status(status: Status) -> anyhow::Error {
    anyhow!(
        "gRPC error: code={:?}, message={}",
        status.code(),
        status.message()
    )
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
///   * `Internal` with a PostgreSQL serialization-failure or
///     deadlock-detected SQLSTATE in the message text.
fn is_retryable_status(status: &Status) -> bool {
    use tonic::Code;
    if matches!(
        status.code(),
        Code::Unavailable | Code::DeadlineExceeded | Code::ResourceExhausted
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
    fn is_retryable_status_covers_transient_codes() {
        assert!(is_retryable_status(&Status::unavailable("x")));
        assert!(is_retryable_status(&Status::deadline_exceeded("x")));
        assert!(is_retryable_status(&Status::resource_exhausted("x")));
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
                if n < 3 {
                    Err(Status::unavailable("transient"))
                } else {
                    Ok(n)
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
}
