//! S3 / minio media backend (production). Design §5.3 / spec §4.7.
//!
//! `endpoint_url` + `force_path_style` make this work against minio
//! (default `force_path_style=true`) and real AWS S3 alike. Presigned
//! GET is computed client-side (SigV4) — no API round-trip — so the
//! Find/search enrich path that issues one URL per memory stays CPU-only.

use super::{ChunkStream, MediaStorage, ObjectStat, PresignedUrl, StorageError, TempObject};
use async_trait::async_trait;
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{BehaviorVersion, Credentials, Region};
use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::primitives::ByteStream;
use bytes::BytesMut;
use futures::StreamExt as _;
use std::time::Duration;

/// Connection config for the s3 backend. Mirrors the `MEDIA_STORAGE_S3_*`
/// env vars (spec §4.7); the app layer reads env and constructs this.
#[derive(Debug, Clone)]
pub struct S3Config {
    pub bucket: String,
    pub region: String,
    /// Empty = real AWS (SDK default endpoint); set for minio.
    pub endpoint: Option<String>,
    pub access_key: String,
    pub secret_key: String,
    /// minio default is true; AWS virtual-hosted style is false.
    pub force_path_style: bool,
}

pub struct S3MediaStorage {
    client: Client,
    bucket: String,
}

impl S3MediaStorage {
    pub fn new(cfg: &S3Config) -> Self {
        let creds = Credentials::new(
            cfg.access_key.clone(),
            cfg.secret_key.clone(),
            None,
            None,
            "memories-media-static",
        );
        let mut builder = aws_sdk_s3::config::Builder::new()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new(cfg.region.clone()))
            .credentials_provider(creds)
            .force_path_style(cfg.force_path_style);
        if let Some(ep) = cfg.endpoint.as_ref().filter(|e| !e.is_empty()) {
            builder = builder.endpoint_url(ep.clone());
        }
        Self {
            client: Client::from_conf(builder.build()),
            bucket: cfg.bucket.clone(),
        }
    }

    fn map_sdk_err(context: &str, e: impl std::fmt::Display) -> StorageError {
        StorageError::Backend(format!("{context}: {e}"))
    }
}

#[async_trait]
impl MediaStorage for S3MediaStorage {
    async fn put_streaming(
        &self,
        temp_key: &str,
        mut chunks: ChunkStream,
        max_bytes: u64,
    ) -> Result<u64, StorageError> {
        // S3 PutObject needs a known length / full body. Multipart upload
        // would stream, but media objects are bounded by
        // MEDIA_UPLOAD_MAX_BYTES (default 20 MiB) so buffering once and
        // doing a single PutObject is simpler and avoids multipart abort
        // bookkeeping on the temp key.
        let mut buf = BytesMut::new();
        let mut total: u64 = 0;
        while let Some(chunk) = chunks.next().await {
            let chunk = chunk?;
            total = total.saturating_add(chunk.len() as u64);
            if total > max_bytes {
                return Err(StorageError::TooLarge(total));
            }
            buf.extend_from_slice(&chunk);
        }
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(temp_key)
            .body(ByteStream::from(buf.freeze()))
            .send()
            .await
            .map_err(|e| Self::map_sdk_err("put_object", e))?;
        Ok(total)
    }

    async fn copy(&self, src_key: &str, dst_key: &str) -> Result<(), StorageError> {
        // CopyObject source is `{bucket}/{key}`, URL-encoded by the SDK.
        let copy_source = format!("{}/{}", self.bucket, src_key);
        self.client
            .copy_object()
            .bucket(&self.bucket)
            .copy_source(copy_source)
            .key(dst_key)
            .send()
            .await
            .map_err(|e| Self::map_sdk_err("copy_object", e))?;
        Ok(())
    }

    async fn delete(&self, key: &str) -> Result<(), StorageError> {
        // S3 DeleteObject is idempotent (deleting a missing key is a 204).
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| Self::map_sdk_err("delete_object", e))?;
        Ok(())
    }

    async fn head(&self, key: &str) -> Result<Option<ObjectStat>, StorageError> {
        match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(out) => Ok(Some(ObjectStat {
                byte_size: out.content_length().unwrap_or(0).max(0) as u64,
            })),
            Err(e) => {
                let svc = e.into_service_error();
                if svc.is_not_found() {
                    Ok(None)
                } else {
                    Err(Self::map_sdk_err("head_object", svc))
                }
            }
        }
    }

    async fn presign_get(&self, key: &str, ttl_sec: u32) -> Result<PresignedUrl, StorageError> {
        let presign_cfg = PresigningConfig::expires_in(Duration::from_secs(ttl_sec as u64))
            .map_err(|e| Self::map_sdk_err("presigning_config", e))?;
        let req = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .presigned(presign_cfg)
            .await
            .map_err(|e| Self::map_sdk_err("presigned get_object", e))?;
        let expires_at = command_utils::util::datetime::now_millis() + (ttl_sec as i64) * 1000;
        Ok(PresignedUrl {
            url: req.uri().to_string(),
            expires_at,
        })
    }

    async fn list_temp_older_than(
        &self,
        prefix: &str,
        age_sec: u64,
    ) -> Result<Vec<TempObject>, StorageError> {
        let mut out = Vec::new();
        let now = command_utils::util::datetime::now_millis();
        let cutoff = now - (age_sec as i64) * 1000;
        let mut paginator = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(prefix)
            .into_paginator()
            .send();
        while let Some(page) = paginator.next().await {
            let page = page.map_err(|e| Self::map_sdk_err("list_objects_v2", e))?;
            for obj in page.contents() {
                let modified = obj
                    .last_modified()
                    .and_then(|t| t.to_millis().ok())
                    .unwrap_or(0);
                if modified <= cutoff
                    && let Some(key) = obj.key()
                {
                    out.push(TempObject {
                        key: key.to_string(),
                        last_modified: modified,
                    });
                }
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::super::{final_key, temp_key};
    use super::*;
    use bytes::Bytes;

    /// Build an `S3MediaStorage` from the `MEDIA_STORAGE_S3_*` env vars,
    /// or `None` when minio is not configured (so the test self-skips
    /// instead of failing on a missing local minio — these tests are
    /// `#[ignore]` and run only against a developer's docker-compose
    /// minio).
    fn s3_from_env() -> Option<S3MediaStorage> {
        let bucket = std::env::var("MEDIA_STORAGE_S3_BUCKET").ok()?;
        let access_key = std::env::var("MEDIA_STORAGE_S3_ACCESS_KEY").ok()?;
        let secret_key = std::env::var("MEDIA_STORAGE_S3_SECRET_KEY").ok()?;
        let cfg = S3Config {
            bucket,
            region: std::env::var("MEDIA_STORAGE_S3_REGION")
                .unwrap_or_else(|_| "us-east-1".to_string()),
            endpoint: std::env::var("MEDIA_STORAGE_S3_ENDPOINT").ok(),
            access_key,
            secret_key,
            force_path_style: std::env::var("MEDIA_STORAGE_S3_FORCE_PATH_STYLE")
                .map(|v| v != "false")
                .unwrap_or(true),
        };
        Some(S3MediaStorage::new(&cfg))
    }

    fn prefix() -> String {
        std::env::var("MEDIA_STORAGE_S3_PREFIX").unwrap_or_else(|_| "memories/".to_string())
    }

    fn stream(parts: Vec<&'static [u8]>) -> ChunkStream {
        Box::pin(futures::stream::iter(
            parts.into_iter().map(|p| Ok(Bytes::from_static(p))),
        ))
    }

    /// Full Upload-confirm roundtrip against minio: temp put → copy to the
    /// deterministic final key → head → presigned GET (HTTP-fetched, body
    /// must match) → delete → head is None (spec §6.1 / §6.2, handover
    /// §11.4). Uses a unique upload id so concurrent test runs / leftover
    /// objects do not collide; cleans both temp and final keys.
    #[tokio::test]
    #[ignore = "requires a running minio (docker compose up -d minio); run with --ignored"]
    async fn s3_put_copy_head_presign_delete_roundtrip() {
        let Some(s) = s3_from_env() else {
            eprintln!(
                "skipping: MEDIA_STORAGE_S3_* env not set \
                 (start minio via docker-compose and export the s3 env)"
            );
            return;
        };
        let p = prefix();
        // A fake but well-formed 64-hex sha drives the final key; the
        // upload id keeps the temp key unique across runs.
        let upload_id = command_utils::util::datetime::now_millis();
        let sha = format!("{:0>64}", format!("{upload_id:x}"));
        let tkey = temp_key(&p, upload_id);
        let fkey = final_key(&p, &sha);
        assert_eq!(
            fkey,
            format!("{p}{}/{}/{}", &sha[0..2], &sha[2..4], sha),
            "final key must be the two-level sha-sharded form (design §5.4)"
        );
        let body: &[u8] = b"image-memory-phase3-roundtrip-body";

        let n = s
            .put_streaming(&tkey, stream(vec![body]), 1024 * 1024)
            .await
            .expect("put_streaming temp");
        assert_eq!(n, body.len() as u64);

        s.copy(&tkey, &fkey).await.expect("copy temp -> final");

        let stat = s
            .head(&fkey)
            .await
            .expect("head final")
            .expect("final object must exist after copy");
        assert_eq!(stat.byte_size, body.len() as u64);

        let presigned = s.presign_get(&fkey, 60).await.expect("presign final");
        let fetched = reqwest::get(&presigned.url)
            .await
            .expect("HTTP GET presigned url")
            .bytes()
            .await
            .expect("read presigned body");
        assert_eq!(
            fetched.as_ref(),
            body,
            "presigned GET body must match the uploaded bytes"
        );

        // Confirm delete cleans the temp object too (idempotent: missing
        // key is Ok).
        s.delete(&tkey).await.expect("delete temp");
        s.delete(&fkey).await.expect("delete final");
        s.delete(&fkey).await.expect("delete is idempotent");
        assert!(
            s.head(&fkey).await.expect("head after delete").is_none(),
            "final object must be gone after delete"
        );
    }

    /// `list_temp_older_than` only returns `_tmp/`-prefixed objects older
    /// than the cutoff; a just-written temp object is excluded by a large
    /// age, and a 0-second age includes it (prefix-scan reservation GC,
    /// design §9.2). Cleans the temp object it creates.
    #[tokio::test]
    #[ignore = "requires a running minio (docker compose up -d minio); run with --ignored"]
    async fn s3_list_temp_prefix_scan_respects_age() {
        let Some(s) = s3_from_env() else {
            eprintln!("skipping: MEDIA_STORAGE_S3_* env not set");
            return;
        };
        let p = prefix();
        let upload_id = command_utils::util::datetime::now_millis();
        let tkey = temp_key(&p, upload_id);
        s.put_streaming(&tkey, stream(vec![b"tmp"]), 1024)
            .await
            .expect("put temp");

        let tmp_prefix = format!("{p}_tmp/");
        // A fresh object is younger than a 1-hour cutoff -> excluded.
        let recent = s
            .list_temp_older_than(&tmp_prefix, 3600)
            .await
            .expect("list temp (1h cutoff)");
        assert!(
            !recent.iter().any(|o| o.key == tkey),
            "freshly written temp object must not be reclaimed (age < cutoff)"
        );
        // age=0 -> everything under the prefix is "older than now".
        let all = s
            .list_temp_older_than(&tmp_prefix, 0)
            .await
            .expect("list temp (age 0)");
        assert!(
            all.iter().any(|o| o.key == tkey),
            "age=0 must include the just-written temp object"
        );

        s.delete(&tkey).await.expect("cleanup temp");
    }
}
