//! Pluggable media body storage (image memory feature).
//!
//! The DB (`media_object`) holds metadata only; the bytes live behind a
//! `MediaStorage` backend. Design: `ai-docs/image-memory-design-storage.md`
//! §5.
//!
//! object-safety: the backend is selected at runtime via
//! `MEDIA_STORAGE_BACKEND`, so the trait must stay dyn-compatible. A
//! generic `impl Stream` argument would make `put_streaming` non-dispatchable,
//! so the chunk stream is a boxed [`ChunkStream`]. `#[async_trait]` boxes the
//! futures, so the async methods remain object-safe.
//!
//! The concrete holder is an `enum StorageBackend` (not `Arc<dyn>`) because
//! the inline backend has a base64↔`media_object.metadata` side channel
//! (`take_bytes`) that has no place on the generic trait — see §5.3.1. The
//! trait is still kept object-safe for future dynamic swapping / mock-ability.

pub mod file;
pub mod inline;
pub mod s3;

/// Backend-agnostic storage-key validation (rejects absolute paths,
/// `..`, empty keys). Re-exported so the app layer can pre-validate
/// `Register` keys with the same rule the file backend enforces
/// (design §0.1 / spec §4.1.2).
pub use file::validate_storage_key;

use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use std::pin::Pin;

/// Errors a storage backend can surface. Kept small and explicit so the
/// app layer can map them to gRPC status codes without string matching.
#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    /// Cumulative bytes exceeded the per-upload cap.
    #[error("object too large: {0} bytes exceeds limit")]
    TooLarge(u64),
    /// Object/key not found (HEAD / GET on a missing key).
    #[error("object not found: {0}")]
    NotFound(String),
    /// Backend rejected the request (auth, malformed key, etc.).
    #[error("storage backend error: {0}")]
    Backend(String),
    /// I/O failure talking to the backend (network, fs).
    #[error("storage io error: {0}")]
    Io(String),
}

/// Boxed chunk stream so the trait stays object-safe (a generic
/// `impl Stream` argument would make `put_streaming` non-dispatchable).
pub type ChunkStream = Pin<Box<dyn Stream<Item = Result<Bytes, StorageError>> + Send>>;

/// `head` result. Only the size is needed (Register byte_size check).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectStat {
    pub byte_size: u64,
}

/// A short-lived GET URL plus its absolute expiry (epoch millis).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PresignedUrl {
    pub url: String,
    pub expires_at: i64,
}

/// One stale temp object found by `list_temp_older_than`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TempObject {
    pub key: String,
    pub last_modified: i64,
}

#[async_trait]
pub trait MediaStorage: Send + Sync {
    /// Stream-write to `temp_key`. The caller computes sha256 itself; this
    /// returns the cumulative byte count. `chunks` is a boxed stream (not
    /// generic) so the trait stays dyn-compatible. Exceeding `max_bytes`
    /// returns [`StorageError::TooLarge`] and the partial write must be
    /// cleaned up by the caller (delete temp_key).
    async fn put_streaming(
        &self,
        temp_key: &str,
        chunks: ChunkStream,
        max_bytes: u64,
    ) -> Result<u64, StorageError>;

    /// Copy `src_key` → `dst_key`. Atomic rename is NOT assumed (kept
    /// stable across minio/S3): S3 = CopyObject, file = fs::copy.
    async fn copy(&self, src_key: &str, dst_key: &str) -> Result<(), StorageError>;

    /// Delete a single object. Idempotent: a missing key is `Ok(())`.
    async fn delete(&self, key: &str) -> Result<(), StorageError>;

    /// Existence check (HEAD-equivalent). Returns the byte size so
    /// Register can validate the client-declared size.
    async fn head(&self, key: &str) -> Result<Option<ObjectStat>, StorageError>;

    /// Issue a short-lived GET URL (S3 presigned GET, file internal proxy
    /// / file://). The inline backend does NOT implement this — the app
    /// layer reads its base64 directly (see §5.3.1 / §7.3).
    async fn presign_get(&self, key: &str, ttl_sec: u32) -> Result<PresignedUrl, StorageError>;

    /// List `{prefix}` objects older than `age_sec` (temp-prefix GC scan).
    async fn list_temp_older_than(
        &self,
        prefix: &str,
        age_sec: u64,
    ) -> Result<Vec<TempObject>, StorageError>;
}

/// Runtime-selected backend holder. `inline` is test-only and carries a
/// base64↔metadata side channel, so the app layer must `match` on it
/// rather than rely on `Arc<dyn MediaStorage>` alone (design §5.3.1).
pub enum StorageBackend {
    S3(s3::S3MediaStorage),
    File(file::FileMediaStorage),
    Inline(inline::InlineMediaStorage),
}

impl StorageBackend {
    /// Borrow the active backend as the object-safe trait (for the
    /// generic put/copy/delete/head/presign flow). Inline-specific
    /// `take_bytes` is reached via [`StorageBackend::as_inline`].
    pub fn as_dyn(&self) -> &dyn MediaStorage {
        match self {
            StorageBackend::S3(s) => s,
            StorageBackend::File(f) => f,
            StorageBackend::Inline(i) => i,
        }
    }

    /// `Some` only for the inline backend (base64↔metadata side channel).
    pub fn as_inline(&self) -> Option<&inline::InlineMediaStorage> {
        match self {
            StorageBackend::Inline(i) => Some(i),
            _ => None,
        }
    }

    /// The `storage_backend` string persisted in `media_object`.
    pub fn name(&self) -> &'static str {
        match self {
            StorageBackend::S3(_) => "s3",
            StorageBackend::File(_) => "file",
            StorageBackend::Inline(_) => "inline",
        }
    }
}

/// Media subsystem config read from env (spec §4.7). Held by the app
/// layer alongside the backend so Upload/Resolve know the prefix / TTL /
/// size cap without re-reading env per call.
#[derive(Debug, Clone)]
pub struct MediaConfig {
    pub backend: String,
    pub s3_prefix: String,
    pub presign_ttl_sec: u32,
    pub upload_max_bytes: u64,
}

impl MediaConfig {
    /// Read `MEDIA_*` env. Safe defaults (file backend, no s3 prefix,
    /// 15min presign, 20 MiB cap) so a deployment that does not use the
    /// image memory feature starts without extra config.
    pub fn from_env() -> Self {
        fn env_or(key: &str, default: &str) -> String {
            std::env::var(key)
                .ok()
                .filter(|v| !v.is_empty())
                .unwrap_or_else(|| default.to_string())
        }
        let backend = env_or("MEDIA_STORAGE_BACKEND", "file");
        // s3 prefix doubles as the file/inline key prefix so final/temp
        // key derivation is backend-independent.
        let s3_prefix = env_or("MEDIA_STORAGE_S3_PREFIX", "memories/");
        let presign_ttl_sec = env_or("MEDIA_PRESIGN_TTL_SEC", "900")
            .parse()
            .unwrap_or(900);
        let upload_max_bytes = env_or("MEDIA_UPLOAD_MAX_BYTES", "20971520")
            .parse()
            .unwrap_or(20 * 1024 * 1024);
        Self {
            backend,
            s3_prefix,
            presign_ttl_sec,
            upload_max_bytes,
        }
    }
}

impl StorageBackend {
    /// Construct the backend from env (spec §4.7). `url` has no storage
    /// implementation (Resolve returns the registered URL directly), so
    /// it shares the inline holder shape only structurally — callers
    /// must not Upload with backend=url (Register is the url entrypoint).
    pub fn from_env(cfg: &MediaConfig) -> anyhow::Result<Self> {
        match cfg.backend.as_str() {
            "s3" => {
                fn req(key: &str) -> anyhow::Result<String> {
                    std::env::var(key).map_err(|_| {
                        anyhow::anyhow!("{key} is required when MEDIA_STORAGE_BACKEND=s3")
                    })
                }
                let s3cfg = s3::S3Config {
                    bucket: req("MEDIA_STORAGE_S3_BUCKET")?,
                    region: std::env::var("MEDIA_STORAGE_S3_REGION")
                        .unwrap_or_else(|_| "us-east-1".to_string()),
                    endpoint: std::env::var("MEDIA_STORAGE_S3_ENDPOINT").ok(),
                    access_key: req("MEDIA_STORAGE_S3_ACCESS_KEY")?,
                    secret_key: req("MEDIA_STORAGE_S3_SECRET_KEY")?,
                    force_path_style: std::env::var("MEDIA_STORAGE_S3_FORCE_PATH_STYLE")
                        .map(|v| v != "false")
                        .unwrap_or(true),
                };
                Ok(StorageBackend::S3(s3::S3MediaStorage::new(&s3cfg)))
            }
            "file" | "url" => {
                // `url` has no body storage; use the file backend as a
                // harmless holder (Resolve handles url before touching
                // storage, Register is the only url write path).
                let dir = std::env::var("MEDIA_STORAGE_LOCAL_DIR")
                    .unwrap_or_else(|_| "./media".to_string());
                Ok(StorageBackend::File(file::FileMediaStorage::new(dir)))
            }
            "inline" => Ok(StorageBackend::Inline(inline::InlineMediaStorage::new())),
            other => {
                anyhow::bail!(
                    "unknown MEDIA_STORAGE_BACKEND={other:?} (expected s3|file|url|inline)"
                )
            }
        }
    }
}

/// Derive the deterministic final object key from a sha256 hex string.
/// `{prefix}{sha[0..2]}/{sha[2..4]}/{sha}` — no extension (media_type is
/// the DB's truth). Design §5.4.
pub fn final_key(prefix: &str, sha256: &str) -> String {
    // sha256 hex is always 64 chars; callers pass validated values, but
    // guard the slice so a malformed sha cannot panic the server.
    if sha256.len() < 4 {
        return format!("{prefix}{sha256}");
    }
    format!("{prefix}{}/{}/{}", &sha256[0..2], &sha256[2..4], sha256)
}

/// Derive the per-upload temp key. Design §5.4. The upload id is a
/// snowflake; `media_object` does NOT persist it, so reservation GC must
/// rely on the `_tmp/` prefix age scan rather than per-row temp deletes.
pub fn temp_key(prefix: &str, upload_id: i64) -> String {
    format!("{prefix}_tmp/{upload_id}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn final_key_two_level_sharding() {
        let sha = "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789";
        assert_eq!(final_key("memories/", sha), format!("memories/ab/cd/{sha}"));
    }

    #[test]
    fn final_key_short_sha_does_not_panic() {
        // Defensive: callers pass validated 64-char shas, but a malformed
        // value must not panic the server.
        assert_eq!(final_key("p/", "ab"), "p/ab");
    }

    #[test]
    fn temp_key_uses_tmp_prefix() {
        assert_eq!(temp_key("memories/", 42), "memories/_tmp/42");
    }
}
