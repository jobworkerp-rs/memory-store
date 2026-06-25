//! Local-filesystem media backend (development). Design §5.3 / O15.
//!
//! `presign_get` returns a `file://` URL: usable for client display tests
//! and only usable for embedding when memories and jobworkerp share a
//! host (the embedding path is s3/url in production).

use super::{ChunkStream, MediaStorage, ObjectStat, PresignedUrl, StorageError, TempObject};
use async_trait::async_trait;
use futures::StreamExt as _;
use std::path::{Path, PathBuf};
use tokio::io::AsyncWriteExt as _;

pub struct FileMediaStorage {
    root: PathBuf,
}

impl FileMediaStorage {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    /// Join a storage key under the root, rejecting path traversal.
    ///
    /// Upload keys are server-generated (sha256 / snowflake), but the
    /// `Register` RPC takes a client-supplied backend-internal key
    /// (`storage_uri`), and that key flows straight into head / presign /
    /// delete. Without this guard a `../` or absolute key would let a
    /// caller HEAD / presign (`file://`) / delete an arbitrary file
    /// outside the media root. This is a filesystem path-traversal
    /// concern and is orthogonal to the project's "no SSRF address block"
    /// stance (that is about network destinations, not local fs paths).
    fn path_for(&self, key: &str) -> Result<PathBuf, StorageError> {
        validate_storage_key(key)?;
        Ok(self.root.join(key))
    }
}

/// Reject keys that could escape the storage root: absolute paths, a
/// Windows prefix/root, any `..` component, or an empty key. Pure /
/// backend-agnostic so the app layer can pre-validate `Register` keys
/// with the same rule (design §0.1 / spec §4.1.2 reverse-feedback).
pub fn validate_storage_key(key: &str) -> Result<(), StorageError> {
    use std::path::Component;
    if key.is_empty() {
        return Err(StorageError::Backend("empty storage key".to_string()));
    }
    let p = Path::new(key);
    for comp in p.components() {
        match comp {
            Component::Normal(_) | Component::CurDir => {}
            Component::ParentDir => {
                return Err(StorageError::Backend(format!(
                    "storage key must not contain '..': {key}"
                )));
            }
            Component::RootDir | Component::Prefix(_) => {
                return Err(StorageError::Backend(format!(
                    "storage key must be relative (no leading '/' / drive): {key}"
                )));
            }
        }
    }
    Ok(())
}

#[async_trait]
impl MediaStorage for FileMediaStorage {
    async fn put_streaming(
        &self,
        temp_key: &str,
        mut chunks: ChunkStream,
        max_bytes: u64,
    ) -> Result<u64, StorageError> {
        let path = self.path_for(temp_key)?;
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| StorageError::Io(e.to_string()))?;
        }
        let mut file = tokio::fs::File::create(&path)
            .await
            .map_err(|e| StorageError::Io(e.to_string()))?;
        let mut total: u64 = 0;
        while let Some(chunk) = chunks.next().await {
            let chunk = chunk?;
            total = total.saturating_add(chunk.len() as u64);
            if total > max_bytes {
                // Best-effort cleanup of the partial temp write; the
                // caller also deletes temp_key on the error path.
                let _ = tokio::fs::remove_file(&path).await;
                return Err(StorageError::TooLarge(total));
            }
            file.write_all(&chunk)
                .await
                .map_err(|e| StorageError::Io(e.to_string()))?;
        }
        file.flush()
            .await
            .map_err(|e| StorageError::Io(e.to_string()))?;
        Ok(total)
    }

    async fn copy(&self, src_key: &str, dst_key: &str) -> Result<(), StorageError> {
        let src = self.path_for(src_key)?;
        let dst = self.path_for(dst_key)?;
        if let Some(parent) = dst.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .map_err(|e| StorageError::Io(e.to_string()))?;
        }
        tokio::fs::copy(&src, &dst).await.map_err(|e| {
            if e.kind() == std::io::ErrorKind::NotFound {
                StorageError::NotFound(src_key.to_string())
            } else {
                StorageError::Io(e.to_string())
            }
        })?;
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), StorageError> {
        match tokio::fs::remove_file(self.path_for(key)?).await {
            Ok(()) => Ok(()),
            // Idempotent: a missing key is success.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(StorageError::Io(e.to_string())),
        }
    }

    async fn head(&self, key: &str) -> Result<Option<ObjectStat>, StorageError> {
        match tokio::fs::metadata(self.path_for(key)?).await {
            Ok(m) => Ok(Some(ObjectStat { byte_size: m.len() })),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(StorageError::Io(e.to_string())),
        }
    }

    async fn presign_get(&self, key: &str, ttl_sec: u32) -> Result<PresignedUrl, StorageError> {
        let path = self.path_for(key)?;
        if !path.exists() {
            return Err(StorageError::NotFound(key.to_string()));
        }
        let abs = path
            .canonicalize()
            .map_err(|e| StorageError::Io(e.to_string()))?;
        // file:// has no real TTL; report a best-effort expiry so callers
        // can treat the field uniformly.
        let expires_at = command_utils::util::datetime::now_millis() + (ttl_sec as i64) * 1000;
        Ok(PresignedUrl {
            url: format!("file://{}", abs.display()),
            expires_at,
        })
    }

    async fn list_temp_older_than(
        &self,
        prefix: &str,
        age_sec: u64,
    ) -> Result<Vec<TempObject>, StorageError> {
        let dir = self.path_for(prefix)?;
        let mut out = Vec::new();
        let mut rd = match tokio::fs::read_dir(&dir).await {
            Ok(rd) => rd,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(out),
            Err(e) => return Err(StorageError::Io(e.to_string())),
        };
        let now = command_utils::util::datetime::now_millis();
        while let Some(entry) = rd
            .next_entry()
            .await
            .map_err(|e| StorageError::Io(e.to_string()))?
        {
            let meta = entry
                .metadata()
                .await
                .map_err(|e| StorageError::Io(e.to_string()))?;
            if !meta.is_file() {
                continue;
            }
            let modified = meta
                .modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0);
            if now - modified >= (age_sec as i64) * 1000 {
                let key = Path::new(prefix)
                    .join(entry.file_name())
                    .to_string_lossy()
                    .into_owned();
                out.push(TempObject {
                    key,
                    last_modified: modified,
                });
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Path-traversal guard (review finding #1): `..`, absolute paths and
    /// empty keys are rejected; ordinary relative keys pass.
    #[test]
    fn validate_storage_key_rejects_traversal() {
        // Valid: server-generated final/temp keys and Register keys.
        assert!(validate_storage_key("memories/ab/cd/deadbeef").is_ok());
        assert!(validate_storage_key("external/dir/picture.png").is_ok());
        assert!(validate_storage_key("memories/_tmp/42").is_ok());
        assert!(validate_storage_key("./a/b.png").is_ok());

        // Invalid: traversal / absolute / empty.
        assert!(validate_storage_key("../etc/passwd").is_err());
        assert!(validate_storage_key("a/../../etc/passwd").is_err());
        assert!(validate_storage_key("/etc/passwd").is_err());
        assert!(validate_storage_key("").is_err());
    }

    /// path_for surfaces the same rejection (defense in depth) and joins
    /// valid keys under the root.
    #[test]
    fn path_for_rejects_traversal_and_joins_valid() {
        let s = FileMediaStorage::new("/tmp/media-root");
        assert!(s.path_for("../escape").is_err());
        assert!(s.path_for("/abs").is_err());
        let ok = s.path_for("memories/ab/cd/x").expect("valid key");
        assert!(ok.starts_with("/tmp/media-root"));
        assert!(ok.ends_with("memories/ab/cd/x"));
    }
}
