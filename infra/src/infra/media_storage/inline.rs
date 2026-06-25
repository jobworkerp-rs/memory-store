//! Test-only in-process media backend. Design §5.3.1 / §7.3.
//!
//! The "DB holds metadata only" rule's single exception: bytes are
//! base64-encoded into `media_object.metadata`. The generic
//! `put_streaming`/`copy` cannot reach DB metadata, so this backend keeps
//! a process-local buffer and exposes `take_bytes` as a side channel for
//! the app layer's confirm/promote tx (see [`super::StorageBackend`]).
//!
//! NOT for production / embedding: `presign_get` returns a `data:` URI
//! (UI display tests only); the embedding workflow never gets inline
//! media (twice gated: `dispatch_kinds` excludes inline, and a startup
//! guard rejects `inline ∧ mode != none`).

use super::{ChunkStream, MediaStorage, ObjectStat, PresignedUrl, StorageError, TempObject};
use async_trait::async_trait;
use futures::StreamExt as _;
use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Default)]
pub struct InlineMediaStorage {
    // temp_key -> bytes (during put_streaming) and final_key -> bytes
    // (after copy). A std Mutex is enough: test-only, short critical
    // sections, no .await held across the lock.
    buffers: Mutex<HashMap<String, Vec<u8>>>,
}

impl InlineMediaStorage {
    pub fn new() -> Self {
        Self::default()
    }

    /// Remove and return the bytes stored under `final_key`. Called by
    /// the app layer's confirm/promote tx to base64 them into
    /// `media_object.metadata` (the inline side channel, §5.3.1).
    pub fn take_bytes(&self, final_key: &str) -> Option<Vec<u8>> {
        self.buffers.lock().unwrap().remove(final_key)
    }
}

#[async_trait]
impl MediaStorage for InlineMediaStorage {
    async fn put_streaming(
        &self,
        temp_key: &str,
        mut chunks: ChunkStream,
        max_bytes: u64,
    ) -> Result<u64, StorageError> {
        let mut buf: Vec<u8> = Vec::new();
        let mut total: u64 = 0;
        while let Some(chunk) = chunks.next().await {
            let chunk = chunk?;
            total = total.saturating_add(chunk.len() as u64);
            if total > max_bytes {
                return Err(StorageError::TooLarge(total));
            }
            buf.extend_from_slice(&chunk);
        }
        self.buffers
            .lock()
            .unwrap()
            .insert(temp_key.to_string(), buf);
        Ok(total)
    }

    /// Move the temp buffer to the final key (the in-process equivalent
    /// of an S3/file physical copy). The bytes are not persisted; a
    /// restart drops them, so reservation GC only needs to delete DB rows
    /// for inline (no leaked final to reclaim).
    async fn copy(&self, src_key: &str, dst_key: &str) -> Result<(), StorageError> {
        let mut buffers = self.buffers.lock().unwrap();
        let bytes = buffers
            .get(src_key)
            .cloned()
            .ok_or_else(|| StorageError::NotFound(src_key.to_string()))?;
        buffers.insert(dst_key.to_string(), bytes);
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), StorageError> {
        // Idempotent.
        self.buffers.lock().unwrap().remove(key);
        Ok(())
    }

    async fn head(&self, key: &str) -> Result<Option<ObjectStat>, StorageError> {
        Ok(self.buffers.lock().unwrap().get(key).map(|b| ObjectStat {
            byte_size: b.len() as u64,
        }))
    }

    async fn presign_get(&self, key: &str, ttl_sec: u32) -> Result<PresignedUrl, StorageError> {
        // Inline Resolve normally reads base64 from media_object.metadata
        // (the app layer handles that). This trait method is only used by
        // tests that exercise the storage layer directly; emit a data:
        // URI from the in-process buffer so a round-trip is verifiable.
        use base64::Engine as _;
        let buffers = self.buffers.lock().unwrap();
        let bytes = buffers
            .get(key)
            .ok_or_else(|| StorageError::NotFound(key.to_string()))?;
        let b64 = base64::engine::general_purpose::STANDARD.encode(bytes);
        let expires_at = command_utils::util::datetime::now_millis() + (ttl_sec as i64) * 1000;
        Ok(PresignedUrl {
            // media_type is unknown at this layer; the app layer builds
            // the proper data:{media_type};base64,... from metadata.
            url: format!("data:application/octet-stream;base64,{b64}"),
            expires_at,
        })
    }

    async fn list_temp_older_than(
        &self,
        _prefix: &str,
        _age_sec: u64,
    ) -> Result<Vec<TempObject>, StorageError> {
        // In-process buffers vanish on restart, so there are no stale
        // temp objects to reclaim.
        Ok(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    fn stream(parts: Vec<&'static [u8]>) -> ChunkStream {
        Box::pin(futures::stream::iter(
            parts.into_iter().map(|p| Ok(Bytes::from_static(p))),
        ))
    }

    #[tokio::test]
    async fn put_copy_head_presign_take_roundtrip() {
        let s = InlineMediaStorage::new();
        let n = s
            .put_streaming("t/1", stream(vec![b"hel", b"lo"]), 1024)
            .await
            .unwrap();
        assert_eq!(n, 5);
        s.copy("t/1", "final/abc").await.unwrap();
        assert_eq!(s.head("final/abc").await.unwrap().unwrap().byte_size, 5);
        let url = s.presign_get("final/abc", 60).await.unwrap().url;
        assert!(url.starts_with("data:application/octet-stream;base64,"));
        // take_bytes is the app-layer side channel.
        assert_eq!(s.take_bytes("final/abc"), Some(b"hello".to_vec()));
        assert_eq!(s.take_bytes("final/abc"), None, "take is destructive");
    }

    #[tokio::test]
    async fn put_streaming_enforces_max_bytes() {
        let s = InlineMediaStorage::new();
        let err = s
            .put_streaming("t/2", stream(vec![b"abcdef"]), 3)
            .await
            .unwrap_err();
        assert!(matches!(err, StorageError::TooLarge(6)));
    }

    #[tokio::test]
    async fn delete_is_idempotent() {
        let s = InlineMediaStorage::new();
        s.delete("missing").await.unwrap();
    }
}
