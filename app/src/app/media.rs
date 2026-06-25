//! `MediaApp` — Upload / Register / Find / Resolve / Delete.
//!
//! The whole concurrency story lives in
//! `ai-docs/image-memory-design-storage.md` §7. The non-obvious
//! invariants this module must hold:
//!
//! - Upload is 3 stages with deliberate tx boundaries: temp write →
//!   tx1 reservation INSERT (committed immediately so a concurrent
//!   upload can observe it) → copy → tx2 confirm. It is NOT one tx
//!   (the reservation must be visible for the b-3 `Aborted` branch).
//! - On `ON CONFLICT(sha256)` the existing row's `gc_state` selects one
//!   of b-1..b-5. Only the b-1 claim winner copies (losers never touch
//!   the final key) — §4.1.1.1 invariant.
//! - `inline` is the test-only exception to "DB holds metadata only":
//!   its bytes are base64'd into `media_object.metadata` in the confirm
//!   tx (the `StorageBackend::as_inline` side channel).

use anyhow::Result;
use base64::Engine as _;
use bytes::Bytes;
use futures::Stream;
use infra::error::LlmMemoryError;
use infra::infra::IdGeneratorWrapper;
use infra::infra::media_object::rdb::{
    GC_DELETED_FAILED, GC_DELETING, GC_ORPHAN, GC_PROMOTING, GC_UNRESOLVABLE,
    MediaObjectRepository, MediaObjectRepositoryImpl, MediaObjectReservation, MediaObjectRow,
    UseMediaObjectRepository,
};
use infra::infra::media_storage::{ChunkStream, StorageBackend, StorageError, final_key, temp_key};
use infra_utils::infra::rdb::UseRdbPool;
use protobuf::llm_memory::data::{MediaMetadata, MediaObjectId, MediaPayload};
use sha2::{Digest, Sha256};
use std::sync::Arc;

/// Per-upload metadata gathered from the gRPC `UploadHeader`.
#[derive(Debug, Clone)]
pub struct UploadHeaderMeta {
    pub kind: i32,
    pub media_type: String,
    pub alt: Option<String>,
    pub width: Option<u32>,
    pub height: Option<u32>,
}

/// Result of an Upload: the (possibly pre-existing) media id and whether
/// it was a dedup / promotion hit.
#[derive(Debug, Clone, Copy)]
pub struct UploadOutcome {
    pub media_object_id: i64,
    pub deduplicated: bool,
}

/// Register request, backend already parsed by the gRPC layer.
#[derive(Debug, Clone)]
pub struct RegisterParams {
    pub kind: i32,
    pub media_type: String,
    pub storage_uri: String,
    pub sha256: Option<String>,
    pub byte_size: Option<i64>,
    pub width: Option<u32>,
    pub height: Option<u32>,
    pub alt: Option<String>,
    pub storage_backend: String,
}

/// Resolved URL (or the explicit "no body, not an error" state).
#[derive(Debug, Clone)]
pub struct ResolveOutcome {
    pub url: Option<String>,
    pub expires_at: Option<i64>,
    pub unresolved: bool,
}

pub struct MediaAppImpl {
    media_object_repository: MediaObjectRepositoryImpl,
    storage: Arc<StorageBackend>,
    id_generator: IdGeneratorWrapper,
    s3_prefix: String,
    presign_ttl_sec: u32,
    upload_max_bytes: u64,
}

impl MediaAppImpl {
    pub fn new(
        media_object_repository: MediaObjectRepositoryImpl,
        storage: Arc<StorageBackend>,
        id_generator: IdGeneratorWrapper,
        s3_prefix: String,
        presign_ttl_sec: u32,
        upload_max_bytes: u64,
    ) -> Self {
        Self {
            media_object_repository,
            storage,
            id_generator,
            s3_prefix,
            presign_ttl_sec,
            upload_max_bytes,
        }
    }
}

impl UseMediaObjectRepository for MediaAppImpl {
    fn media_object_repository(&self) -> &MediaObjectRepositoryImpl {
        &self.media_object_repository
    }
}

/// Map a `StorageError` to the app error. `TooLarge` →
/// `ResourceExhausted` so Upload's size guard surfaces as gRPC
/// RESOURCE_EXHAUSTED (spec §4.1.1).
fn map_storage_err(e: StorageError) -> anyhow::Error {
    match e {
        StorageError::TooLarge(n) => LlmMemoryError::ResourceExhausted(format!(
            "object exceeds MEDIA_UPLOAD_MAX_BYTES ({n} bytes)"
        ))
        .into(),
        StorageError::NotFound(k) => {
            LlmMemoryError::NotFound(format!("storage object not found: {k}")).into()
        }
        other => LlmMemoryError::RuntimeError(other.to_string()).into(),
    }
}

/// Build the inline metadata JSON ({"inline_b64": "..."}) consumed by
/// the confirm/promote tx. Only used when backend == inline.
fn inline_metadata_json(bytes: &[u8]) -> String {
    let b64 = base64::engine::general_purpose::STANDARD.encode(bytes);
    serde_json::json!({ "inline_b64": b64 }).to_string()
}

impl MediaAppImpl {
    fn db(&self) -> &infra_utils::infra::rdb::RdbPool {
        self.media_object_repository.db_pool()
    }

    /// Upload (client streaming). 3 stages — design §7.1.
    ///
    /// `chunks` is the boxed chunk stream from the gRPC handler. The
    /// handler also passes the parsed header.
    pub async fn upload<S>(&self, header: UploadHeaderMeta, chunks: S) -> Result<UploadOutcome>
    where
        S: Stream<Item = Result<Bytes, StorageError>> + Send + 'static,
    {
        // Stage 1+2: temp write while computing sha256 incrementally.
        let upload_id = self.id_generator.generate_id()?;
        let tkey = temp_key(&self.s3_prefix, upload_id);
        // The storage backend computes the byte count; we tee the stream
        // to also feed the hasher. Wrapping the stream keeps both the
        // size cap (enforced by put_streaming) and the hash in one pass.
        let hasher = Arc::new(std::sync::Mutex::new(Sha256::new()));
        let hclone = hasher.clone();
        let hashed: ChunkStream = Box::pin(futures::StreamExt::map(chunks, move |item| {
            if let Ok(b) = &item {
                hclone.lock().unwrap().update(b);
            }
            item
        }));
        let byte_count = self
            .storage
            .as_dyn()
            .put_streaming(&tkey, hashed, self.upload_max_bytes)
            .await
            .map_err(|e| {
                // Best-effort temp cleanup is the backend's job on
                // TooLarge; other errors leave the temp for the prefix
                // GC scan.
                map_storage_err(e)
            })?;
        let sha256 = {
            let h = Arc::try_unwrap(hasher)
                .map_err(|_| LlmMemoryError::RuntimeError("sha256 hasher still shared".into()))?
                .into_inner()
                .map_err(|_| LlmMemoryError::RuntimeError("sha256 mutex poisoned".into()))?;
            hex(&h.finalize())
        };

        let backend_name = self.storage.name();
        let now = command_utils::util::datetime::now_millis();

        // Stage 3: [tx1] reservation INSERT (commit immediately).
        let media_id = self.id_generator.generate_id()?;
        let reservation = MediaObjectReservation {
            id: media_id,
            kind: header.kind,
            media_type: header.media_type.clone(),
            byte_size: Some(byte_count as i64),
            sha256: sha256.clone(),
            width: header.width.map(|w| w as i32),
            height: header.height.map(|h| h as i32),
            duration_ms: None,
            alt: header.alt.clone(),
            created_at: now,
            updated_at: now,
        };
        let mut tx = self.db().begin().await.map_err(LlmMemoryError::DBError)?;
        let inserted = self
            .media_object_repository
            .insert_reservation_tx(&mut *tx, &reservation)
            .await?;
        let existing = if inserted {
            None
        } else {
            self.media_object_repository
                .find_by_sha256_tx(&mut *tx, &sha256)
                .await?
        };
        tx.commit().await.map_err(LlmMemoryError::DBError)?;

        if inserted {
            // a) reservation won: copy temp -> final, then [tx2] confirm.
            return self
                .finish_fresh_reservation(media_id, &tkey, &sha256, backend_name, byte_count)
                .await;
        }

        // b) ON CONFLICT — branch on the existing row's gc_state.
        let row = existing.ok_or_else(|| {
            LlmMemoryError::RuntimeError(
                "media_object reservation conflict but row not found".into(),
            )
        })?;
        self.handle_upload_conflict(row, &tkey, &sha256, backend_name, byte_count)
            .await
    }

    /// a) fresh reservation: copy → tx2 confirm → delete temp. Design
    /// §7.1 a). On copy failure roll the reservation back.
    async fn finish_fresh_reservation(
        &self,
        media_id: i64,
        tkey: &str,
        sha256: &str,
        backend_name: &str,
        byte_count: u64,
    ) -> Result<UploadOutcome> {
        let fkey = final_key(&self.s3_prefix, sha256);
        if let Err(e) = self.storage.as_dyn().copy(tkey, &fkey).await {
            // [tx2'] roll the reservation back so a retry can re-INSERT.
            let mut tx = self.db().begin().await.map_err(LlmMemoryError::DBError)?;
            let _ = self
                .media_object_repository
                .delete_if_unreferenced_tx(&mut *tx, media_id)
                .await;
            tx.commit().await.map_err(LlmMemoryError::DBError)?;
            let _ = self.storage.as_dyn().delete(tkey).await;
            return Err(map_storage_err(e));
        }
        let (final_uri, metadata) = self.confirm_args(backend_name, sha256, &fkey)?;
        let mut tx = self.db().begin().await.map_err(LlmMemoryError::DBError)?;
        let ok = self
            .media_object_repository
            .confirm_reservation_tx(
                &mut *tx,
                media_id,
                &final_uri,
                backend_name,
                Some(byte_count as i64),
                metadata.as_deref(),
            )
            .await?;
        tx.commit().await.map_err(LlmMemoryError::DBError)?;
        if !ok {
            // Reservation row changed under us (concurrent GC). The
            // final object may have leaked; reservation GC reclaims it
            // via sha256. Transient — retry resolves it (gRPC ABORTED).
            return Err(LlmMemoryError::Aborted(
                "reservation no longer confirmable; retry upload".into(),
            )
            .into());
        }
        // tx2 committed: temp delete is best-effort (prefix GC mops up).
        let _ = self.storage.as_dyn().delete(tkey).await;
        Ok(UploadOutcome {
            media_object_id: media_id,
            deduplicated: false,
        })
    }

    /// b) conflict dispatch (b-1..b-5). Design §7.1 b).
    async fn handle_upload_conflict(
        &self,
        row: MediaObjectRow,
        tkey: &str,
        sha256: &str,
        backend_name: &str,
        byte_count: u64,
    ) -> Result<UploadOutcome> {
        let confirmed = row.storage_uri.is_some();
        // b-1: {2,3} -> promotion/recovery (claim → copy → confirm).
        if matches!(row.gc_state, GC_DELETED_FAILED | GC_UNRESOLVABLE) {
            return self
                .promote_recover(row.id, tkey, sha256, backend_name, byte_count)
                .await;
        }
        // b-2: gc_state in {0,1} AND storage_uri NOT NULL (confirmed
        // dedup hit) — drop temp, return existing.
        if matches!(row.gc_state, 0 | GC_ORPHAN) && confirmed {
            let _ = self.storage.as_dyn().delete(tkey).await;
            return Ok(UploadOutcome {
                media_object_id: row.id,
                deduplicated: true,
            });
        }
        // b-3/b-4/b-5: transient (reservation / promoting / deleting).
        // The caller never copied; the final key is untouched. Drop temp
        // and ask the client to retry. Design §7.1 specifies these as
        // Aborted (gRPC ABORTED, retryable) — the conflict resolves once
        // the other path finishes or the GC reclaims a crashed one.
        let _ = self.storage.as_dyn().delete(tkey).await;
        let reason = match row.gc_state {
            GC_ORPHAN => "concurrent reservation in progress",
            GC_PROMOTING => "concurrent promotion in progress",
            GC_DELETING => "media is being deleted",
            other => {
                return Err(LlmMemoryError::RuntimeError(format!(
                    "unexpected gc_state {other} on upload conflict"
                ))
                .into());
            }
        };
        Err(LlmMemoryError::Aborted(format!("{reason}; retry upload")).into())
    }

    /// b-1: claim ({2,3}->4) → copy → [tx-promote] confirm. Design §7.1.1.
    async fn promote_recover(
        &self,
        media_id: i64,
        tkey: &str,
        sha256: &str,
        backend_name: &str,
        byte_count: u64,
    ) -> Result<UploadOutcome> {
        let mut tx = self.db().begin().await.map_err(LlmMemoryError::DBError)?;
        let claimed = self
            .media_object_repository
            .claim_promote_tx(&mut *tx, media_id)
            .await?;
        tx.commit().await.map_err(LlmMemoryError::DBError)?;
        if !claimed {
            // Lost the race / state changed. We did NOT copy, so the
            // final key is untouched (§4.1.1.1). Re-read and re-apply
            // the b-* decision.
            let row = self
                .media_object_repository
                .find_by_id(media_id)
                .await?
                .ok_or_else(|| {
                    LlmMemoryError::RuntimeError("media row vanished during promote".into())
                })?;
            // Box the recursive call: a claim loser re-applies the b-*
            // decision, which converges (a loser sees b-2 confirmed /
            // b-4 promoting / b-5 deleting — never an unbounded b-1
            // chain), but the compiler still needs indirection for the
            // mutually recursive async fn.
            return Box::pin(self.handle_upload_conflict(
                row,
                tkey,
                sha256,
                backend_name,
                byte_count,
            ))
            .await;
        }
        // Claimed (gc_state now 4). Copy then confirm. sha256 is the
        // deterministic key, so a residual object is idempotently
        // overwritten.
        let fkey = final_key(&self.s3_prefix, sha256);
        self.storage
            .as_dyn()
            .copy(tkey, &fkey)
            .await
            .map_err(map_storage_err)?;
        // ref_count>0 (elided already linked) -> active, else orphan.
        let row = self
            .media_object_repository
            .find_by_id(media_id)
            .await?
            .ok_or_else(|| LlmMemoryError::RuntimeError("media row vanished post-claim".into()))?;
        let gc_state = if row.ref_count > 0 { 0 } else { GC_ORPHAN };
        let (final_uri, metadata) = self.confirm_args(backend_name, sha256, &fkey)?;
        let mut tx = self.db().begin().await.map_err(LlmMemoryError::DBError)?;
        let ok = self
            .media_object_repository
            .confirm_promote_tx(
                &mut *tx,
                media_id,
                &final_uri,
                backend_name,
                Some(byte_count as i64),
                gc_state,
                metadata.as_deref(),
            )
            .await?;
        tx.commit().await.map_err(LlmMemoryError::DBError)?;
        if !ok {
            // Confirm raced another path after our claim. Transient —
            // retry resolves it (gRPC ABORTED).
            return Err(
                LlmMemoryError::Aborted("promotion confirm lost; retry upload".into()).into(),
            );
        }
        let _ = self.storage.as_dyn().delete(tkey).await;
        Ok(UploadOutcome {
            media_object_id: media_id,
            deduplicated: true,
        })
    }

    /// Compute the (final_uri, metadata) pair for confirm/promote. For
    /// inline the bytes are taken from the in-process buffer and base64'd
    /// into media_object.metadata (the §5.3.1 side channel); s3/file use
    /// the final key and no metadata (COALESCE keeps existing).
    fn confirm_args(
        &self,
        backend_name: &str,
        sha256: &str,
        fkey: &str,
    ) -> Result<(String, Option<String>)> {
        if backend_name == "inline" {
            let inline = self
                .storage
                .as_inline()
                .ok_or_else(|| LlmMemoryError::RuntimeError("inline backend expected".into()))?;
            let bytes = inline.take_bytes(fkey).ok_or_else(|| {
                LlmMemoryError::RuntimeError("inline buffer missing on confirm".into())
            })?;
            Ok((
                format!("inline:{sha256}"),
                Some(inline_metadata_json(&bytes)),
            ))
        } else {
            Ok((fkey.to_string(), None))
        }
    }

    /// Register — design §7.2. s3/file: HEAD + byte_size check; url:
    /// register as-is (no fetch / no SSRF address block, §4.1 note).
    pub async fn register(&self, params: RegisterParams) -> Result<MediaMetadata> {
        let backend = params.storage_backend.as_str();
        match backend {
            "s3" | "file" => {
                // The server runs exactly ONE storage backend (chosen by
                // MEDIA_STORAGE_BACKEND). A Register whose declared
                // backend differs from the running one would HEAD the
                // wrong store and persist a row whose storage_backend
                // does not match what Resolve/Delete actually use.
                // Reject the mismatch (url is exempt: it is an external
                // reference unrelated to the server's managed storage).
                let actual = self.storage.name();
                if params.storage_backend != actual {
                    return Err(LlmMemoryError::InvalidArgument(format!(
                        "storage_backend mismatch: request '{}' but server runs '{}'",
                        params.storage_backend, actual
                    ))
                    .into());
                }
                let sha = params.sha256.clone().ok_or_else(|| {
                    LlmMemoryError::InvalidArgument(
                        "sha256 is required for s3/file Register".into(),
                    )
                })?;
                validate_sha256_hex(&sha)?;
                let declared = params.byte_size.ok_or_else(|| {
                    LlmMemoryError::InvalidArgument(
                        "byte_size is required for s3/file Register".into(),
                    )
                })?;
                // The Register key is client input and flows into head /
                // presign / delete; reject traversal up front (defense in
                // depth — the file backend re-checks). See spec §4.1.2.
                infra::infra::media_storage::validate_storage_key(&params.storage_uri).map_err(
                    |_| {
                        LlmMemoryError::InvalidArgument(format!(
                            "invalid storage_uri (must be a relative backend key, no '..' / \
                             leading '/'): {}",
                            params.storage_uri
                        ))
                    },
                )?;
                let stat = self
                    .storage
                    .as_dyn()
                    .head(&params.storage_uri)
                    .await
                    .map_err(map_storage_err)?
                    .ok_or_else(|| {
                        LlmMemoryError::InvalidArgument(format!(
                            "object not found at storage_uri: {}",
                            params.storage_uri
                        ))
                    })?;
                if stat.byte_size as i64 != declared {
                    return Err(LlmMemoryError::InvalidArgument(format!(
                        "byte_size mismatch: declared {declared}, actual {}",
                        stat.byte_size
                    ))
                    .into());
                }
                self.register_managed(params, sha, declared).await
            }
            "url" => self.register_url(params).await,
            other => Err(LlmMemoryError::InvalidArgument(format!(
                "unsupported storage_backend for Register: {other}"
            ))
            .into()),
        }
    }

    async fn register_managed(
        &self,
        params: RegisterParams,
        sha: String,
        byte_size: i64,
    ) -> Result<MediaMetadata> {
        let now = command_utils::util::datetime::now_millis();
        let media_id = self.id_generator.generate_id()?;
        let reservation = MediaObjectReservation {
            id: media_id,
            kind: params.kind,
            media_type: params.media_type.clone(),
            byte_size: Some(byte_size),
            sha256: sha.clone(),
            width: params.width.map(|w| w as i32),
            height: params.height.map(|h| h as i32),
            duration_ms: None,
            alt: params.alt.clone(),
            created_at: now,
            updated_at: now,
        };
        let mut tx = self.db().begin().await.map_err(LlmMemoryError::DBError)?;
        let inserted = self
            .media_object_repository
            .insert_reservation_tx(&mut *tx, &reservation)
            .await?;
        // Persist the SERVER-VERIFIED backend, not the request value.
        // `register` already rejected a mismatch, so this equals
        // params.storage_backend here, but using the verified source
        // keeps the stored backend impossible to desync from what
        // Resolve/Delete actually use.
        let verified_backend = self.storage.name();
        if inserted {
            // No copy needed (object already externally PUT). Confirm
            // straight to its final URI (stays orphan; memory link
            // promotes it).
            let ok = self
                .media_object_repository
                .confirm_reservation_tx(
                    &mut *tx,
                    media_id,
                    &params.storage_uri,
                    verified_backend,
                    Some(byte_size),
                    None,
                )
                .await?;
            tx.commit().await.map_err(LlmMemoryError::DBError)?;
            if !ok {
                // Reservation row changed under us. Transient — retry
                // resolves it (gRPC ABORTED).
                return Err(LlmMemoryError::Aborted("register confirm lost; retry".into()).into());
            }
            return self.metadata_of(media_id).await;
        }
        // Conflict: existing row. {2,3} -> single conditional UPDATE
        // promotion; confirmed -> dedup; else Aborted-equivalent.
        let row = self
            .media_object_repository
            .find_by_sha256_tx(&mut *tx, &sha)
            .await?
            .ok_or_else(|| {
                LlmMemoryError::RuntimeError("register conflict but row not found".into())
            })?;
        if matches!(row.gc_state, GC_DELETED_FAILED | GC_UNRESOLVABLE) {
            let gc_state = if row.ref_count > 0 { 0 } else { GC_ORPHAN };
            let ok = self
                .media_object_repository
                .register_promote_tx(
                    &mut *tx,
                    row.id,
                    &params.storage_uri,
                    verified_backend,
                    Some(byte_size),
                    gc_state,
                )
                .await?;
            tx.commit().await.map_err(LlmMemoryError::DBError)?;
            if ok {
                return self.metadata_of(row.id).await;
            }
            // UPDATE 0 rows: re-SELECT, mirror Upload b-2/b-3/b-4/b-5.
            let row2 = self
                .media_object_repository
                .find_by_id(row.id)
                .await?
                .ok_or_else(|| LlmMemoryError::RuntimeError("register row vanished".into()))?;
            return self.register_conflict_terminal(row2).await;
        }
        tx.commit().await.map_err(LlmMemoryError::DBError)?;
        self.register_conflict_terminal(row).await
    }

    /// Confirmed -> return existing; promoting/reserving/deleting ->
    /// Aborted (mirrors Upload b-2..b-5; design §7.2 specifies Aborted,
    /// gRPC ABORTED — retryable, the conflict resolves once the other
    /// path finishes or the GC reclaims a crashed one).
    async fn register_conflict_terminal(&self, row: MediaObjectRow) -> Result<MediaMetadata> {
        if matches!(row.gc_state, 0 | GC_ORPHAN) && row.storage_uri.is_some() {
            return self.metadata_of(row.id).await;
        }
        Err(LlmMemoryError::Aborted(
            "media is in a transient state (reserving/promoting/deleting); retry register".into(),
        )
        .into())
    }

    async fn register_url(&self, params: RegisterParams) -> Result<MediaMetadata> {
        // No HEAD / no byte_size check / no SSRF address block (§4.1
        // note): the normal flow points at internal S3/minio.
        let media_id = self.id_generator.generate_id()?;
        let mut tx = self.db().begin().await.map_err(LlmMemoryError::DBError)?;
        self.media_object_repository
            .insert_url_tx(
                &mut *tx,
                media_id,
                params.kind,
                &params.media_type,
                params.byte_size,
                params.width.map(|w| w as i32),
                params.height.map(|h| h as i32),
                &params.storage_uri,
                params.alt.as_deref(),
            )
            .await?;
        tx.commit().await.map_err(LlmMemoryError::DBError)?;
        self.metadata_of(media_id).await
    }

    /// Find — design §7.4. storage_uri is never surfaced.
    pub async fn metadata_of(&self, media_object_id: i64) -> Result<MediaMetadata> {
        let row = self
            .media_object_repository
            .find_by_id(media_object_id)
            .await?
            .ok_or_else(|| {
                LlmMemoryError::NotFound(format!("media_object not found: {media_object_id}"))
            })?;
        Ok(row_to_metadata(&row))
    }

    /// Build the cacheable half of `Memory.media` (design 2/3 §7.5.5):
    /// metadata + the `unresolved` flag, with `presigned_url`/`expires_at`
    /// LEFT EMPTY. The gRPC layer fills the short-lived URL just before
    /// responding (it must not be cached). `unresolved=true` when the
    /// body is absent (storage_backend=unresolvable or gc_state in {3,4})
    /// — that is a normal "recovering" state, NOT an error.
    pub async fn media_payload_metadata(&self, media_object_id: i64) -> Result<MediaPayload> {
        let row = self
            .media_object_repository
            .find_by_id(media_object_id)
            .await?
            .ok_or_else(|| {
                LlmMemoryError::NotFound(format!("media_object not found: {media_object_id}"))
            })?;
        let unresolved = row.storage_backend == "unresolvable"
            || row.storage_uri.is_none()
            || matches!(row.gc_state, GC_UNRESOLVABLE | GC_PROMOTING);
        Ok(MediaPayload {
            metadata: Some(row_to_metadata(&row)),
            presigned_url: None,
            expires_at: None,
            unresolved,
        })
    }

    /// Resolve — design §7.3. unresolvable / NULL-uri (gc_state in
    /// {3,4}) → unresolved=true (NOT an error).
    pub async fn resolve(
        &self,
        media_object_id: i64,
        ttl_sec: Option<u32>,
    ) -> Result<ResolveOutcome> {
        let row = self
            .media_object_repository
            .find_by_id(media_object_id)
            .await?
            .ok_or_else(|| {
                LlmMemoryError::NotFound(format!("media_object not found: {media_object_id}"))
            })?;
        let ttl = ttl_sec.unwrap_or(self.presign_ttl_sec);
        if row.storage_backend == "unresolvable"
            || row.storage_uri.is_none()
            || matches!(row.gc_state, GC_UNRESOLVABLE | GC_PROMOTING)
        {
            return Ok(ResolveOutcome {
                url: None,
                expires_at: None,
                unresolved: true,
            });
        }
        match row.storage_backend.as_str() {
            "url" => Ok(ResolveOutcome {
                // External URL: return as-is, expiry is external (best
                // effort).
                url: row.storage_uri.clone(),
                expires_at: None,
                unresolved: false,
            }),
            "inline" => {
                // data: URI built from media_object.metadata.inline_b64.
                let b64 = row
                    .metadata
                    .as_deref()
                    .and_then(|m| serde_json::from_str::<serde_json::Value>(m).ok())
                    .and_then(|v| {
                        v.get("inline_b64")
                            .and_then(|x| x.as_str())
                            .map(|s| s.to_string())
                    })
                    .ok_or_else(|| {
                        LlmMemoryError::RuntimeError(
                            "inline media missing inline_b64 metadata".into(),
                        )
                    })?;
                let expires_at = command_utils::util::datetime::now_millis() + (ttl as i64) * 1000;
                Ok(ResolveOutcome {
                    url: Some(format!("data:{};base64,{}", row.media_type, b64)),
                    expires_at: Some(expires_at),
                    unresolved: false,
                })
            }
            // s3 / file: presign the stored key. `storage_uri` is the
            // single source of truth for the on-storage key — Upload
            // confirms it to the deterministic final key (§5.4), Register
            // sets the externally-PUT key (§7.2). Deriving the key from
            // sha256 here is wrong: a Register-ed object lives at its
            // registered key, not memories/ab/cd/<sha>. The NULL-uri /
            // unresolvable / gc_state∈{3,4} cases are already returned as
            // `unresolved` above, so on this arm `storage_uri` is Some;
            // a None here is an invariant violation.
            _ => {
                let key = row.storage_uri.clone().ok_or_else(|| {
                    LlmMemoryError::RuntimeError(format!(
                        "confirmed s3/file media {} has NULL storage_uri \
                         (invariant violation)",
                        row.id
                    ))
                })?;
                let p = self
                    .storage
                    .as_dyn()
                    .presign_get(&key, ttl)
                    .await
                    .map_err(map_storage_err)?;
                Ok(ResolveOutcome {
                    url: Some(p.url),
                    expires_at: Some(p.expires_at),
                    unresolved: false,
                })
            }
        }
    }

    /// Explicit Delete — design §7.3. Only when ref_count==0; otherwise
    /// FailedPrecondition. Goes through the same claim→storage→DB path
    /// as memory-driven deletion.
    pub async fn delete(&self, media_object_id: i64) -> Result<()> {
        let row = self
            .media_object_repository
            .find_by_id(media_object_id)
            .await?
            .ok_or_else(|| {
                LlmMemoryError::NotFound(format!("media_object not found: {media_object_id}"))
            })?;
        if row.ref_count != 0 {
            return Err(LlmMemoryError::FailedPrecondition(format!(
                "media_object {media_object_id} still referenced (ref_count={})",
                row.ref_count
            ))
            .into());
        }
        // unresolvable/promoting: keep the row (recovery may still come).
        if matches!(row.gc_state, GC_UNRESOLVABLE | GC_PROMOTING) {
            return Ok(());
        }
        let mut tx = self.db().begin().await.map_err(LlmMemoryError::DBError)?;
        let claimed = self
            .media_object_repository
            .claim_deleting_tx(&mut *tx, media_object_id)
            .await?;
        tx.commit().await.map_err(LlmMemoryError::DBError)?;
        if !claimed {
            // Someone else claimed / re-referenced between read and
            // claim. Nothing to do.
            return Ok(());
        }
        self.finish_delete(media_object_id, &row).await
    }

    /// Post-claim: storage delete (s3/file only) → conditional DB delete.
    /// Storage failure → keep row, 5→2 (GC retries via sha256). Design
    /// §7.5.4 / §6.3.
    pub async fn finish_delete(&self, media_object_id: i64, row: &MediaObjectRow) -> Result<()> {
        let backend = row.storage_backend.as_str();
        if backend == "s3" || backend == "file" {
            // `storage_uri` is the single source of truth for the
            // on-storage key (Upload final key / Register external key);
            // never re-derive from sha256 (a Register-ed object is not at
            // memories/ab/cd/<sha>). A confirmed s3/file row reaching
            // here always has storage_uri Some (callers branch out
            // gc_state∈{3,4}/url/inline first). A None is an invariant
            // violation: log + skip the storage delete (the conditional
            // DB delete below still runs so the row does not leak).
            let key = row.storage_uri.clone();
            if key.is_none() {
                tracing::error!(
                    "invariant violation: confirmed s3/file media_object \
                     {media_object_id} has NULL storage_uri; skipping storage delete"
                );
            }
            if let Some(key) = key
                && let Err(e) = self.storage.as_dyn().delete(&key).await
            {
                let mut tx = self.db().begin().await.map_err(LlmMemoryError::DBError)?;
                let _ = self
                    .media_object_repository
                    .mark_delete_failed_tx(&mut *tx, media_object_id)
                    .await;
                tx.commit().await.map_err(LlmMemoryError::DBError)?;
                tracing::warn!(
                    "storage delete failed (media_object_id={media_object_id}, \
                     sha256={:?}): {e}; marked deleted-failed, GC will retry",
                    row.sha256
                );
                return Ok(());
            }
        }
        // s3/file deleted, or url/inline (storage delete not needed).
        let mut tx = self.db().begin().await.map_err(LlmMemoryError::DBError)?;
        let ok = self
            .media_object_repository
            .delete_if_unreferenced_tx(&mut *tx, media_object_id)
            .await?;
        if !ok {
            // Invariant violation (claim was held; a re-reference can
            // only happen via a bug / manual edit). Mark deleted-failed
            // + alert; GC handles the storage object separately.
            let _ = self
                .media_object_repository
                .mark_delete_failed_tx(&mut *tx, media_object_id)
                .await;
            tx.commit().await.map_err(LlmMemoryError::DBError)?;
            tracing::error!(
                "invariant violation: deleting-claimed media_object {media_object_id} \
                 vanished/re-referenced"
            );
            return Ok(());
        }
        tx.commit().await.map_err(LlmMemoryError::DBError)?;
        Ok(())
    }
}

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// Validate that a client-declared sha256 is exactly 64 hex chars.
/// Register trusts the client's value (no server recompute, spec §4.1.2),
/// but a malformed sha would corrupt dedup (UNIQUE), deleted-failed
/// recovery and unresolvable promotion — all keyed on sha256. Format
/// validation is orthogonal to "no recompute" (design §7.2 reverse-FB).
fn validate_sha256_hex(sha: &str) -> Result<()> {
    if sha.len() == 64 && sha.bytes().all(|b| b.is_ascii_hexdigit()) {
        Ok(())
    } else {
        Err(
            LlmMemoryError::InvalidArgument("sha256 must be exactly 64 hex characters".to_string())
                .into(),
        )
    }
}

fn row_to_metadata(row: &MediaObjectRow) -> MediaMetadata {
    MediaMetadata {
        id: Some(MediaObjectId { value: row.id }),
        kind: row.kind,
        media_type: row.media_type.clone(),
        byte_size: row.byte_size,
        width: row.width.map(|w| w as u32),
        height: row.height.map(|h| h as u32),
        duration_ms: row.duration_ms,
        alt: row.alt.clone(),
        // url backend has sha256 NULL → omitted.
        sha256: row.sha256.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use infra::infra::media_storage::inline::InlineMediaStorage;
    use infra_utils::infra::rdb::RdbPool;
    use infra_utils::infra::test::{TEST_RUNTIME, setup_test_rdb_from};

    async fn pool() -> &'static RdbPool {
        #[cfg(feature = "postgres")]
        {
            setup_test_rdb_from("../infra/sql/postgres").await
        }
        #[cfg(not(feature = "postgres"))]
        {
            setup_test_rdb_from("../infra/sql/sqlite").await
        }
    }

    fn app(pool: &'static RdbPool) -> MediaAppImpl {
        let id_gen = infra::test_helper::shared_id_generator();
        let repo = MediaObjectRepositoryImpl::new(id_gen.clone(), pool);
        let storage = Arc::new(StorageBackend::Inline(InlineMediaStorage::new()));
        MediaAppImpl::new(
            repo,
            storage,
            id_gen,
            "memories/".to_string(),
            900,
            20 * 1024 * 1024,
        )
    }

    fn header() -> UploadHeaderMeta {
        UploadHeaderMeta {
            kind: 2, // IMAGE
            media_type: "image/png".to_string(),
            alt: Some("alt".to_string()),
            width: Some(4),
            height: Some(2),
        }
    }

    fn stream(
        parts: Vec<&'static [u8]>,
    ) -> impl Stream<Item = Result<Bytes, StorageError>> + Send + 'static {
        futures::stream::iter(parts.into_iter().map(|p| Ok(Bytes::from_static(p))))
    }

    /// Fresh Upload: 3-stage flow confirms a new media_object, Find
    /// returns its metadata, Resolve yields a data: URI (inline).
    #[test]
    fn upload_fresh_then_find_resolve() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let a = app(pool().await);
            let out = a
                .upload(header(), stream(vec![b"img-", b"bytes-1"]))
                .await?;
            assert!(!out.deduplicated);
            let meta = a.metadata_of(out.media_object_id).await?;
            assert_eq!(meta.kind, 2);
            assert_eq!(meta.media_type, "image/png");
            assert_eq!(meta.byte_size, Some(11));
            assert!(meta.sha256.is_some());
            let r = a.resolve(out.media_object_id, None).await?;
            assert!(!r.unresolved);
            assert!(r.url.unwrap().starts_with("data:image/png;base64,"));
            Ok(())
        })
    }

    /// sha256 dedup (b-2): the same bytes uploaded twice resolve to the
    /// same media_object_id with deduplicated=true.
    #[test]
    fn upload_dedup_same_sha256() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let a = app(pool().await);
            let first = a.upload(header(), stream(vec![b"dedup-payload-A"])).await?;
            assert!(!first.deduplicated);
            let second = a.upload(header(), stream(vec![b"dedup-payload-A"])).await?;
            assert!(second.deduplicated, "second upload must be a dedup hit");
            assert_eq!(first.media_object_id, second.media_object_id);
            Ok(())
        })
    }

    /// Register url backend: no HEAD, sha256 NULL, resolve returns the
    /// external URL as-is.
    #[test]
    fn register_url_roundtrip() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let a = app(pool().await);
            let meta = a
                .register(RegisterParams {
                    kind: 2,
                    media_type: "image/jpeg".to_string(),
                    storage_uri: "https://example.test/cat.jpg".to_string(),
                    sha256: None,
                    byte_size: None,
                    width: None,
                    height: None,
                    alt: None,
                    storage_backend: "url".to_string(),
                })
                .await?;
            assert_eq!(meta.media_type, "image/jpeg");
            assert!(meta.sha256.is_none(), "url backend has no sha256");
            let id = meta.id.unwrap().value;
            let r = a.resolve(id, None).await?;
            assert!(!r.unresolved);
            assert_eq!(r.url.as_deref(), Some("https://example.test/cat.jpg"));
            Ok(())
        })
    }

    /// Register s3/file requires sha256 + byte_size.
    #[test]
    fn register_managed_requires_sha256() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let a = app(pool().await);
            let err = a
                .register(RegisterParams {
                    kind: 2,
                    media_type: "image/png".to_string(),
                    storage_uri: "s3://b/k".to_string(),
                    sha256: None,
                    byte_size: Some(1),
                    width: None,
                    height: None,
                    alt: None,
                    storage_backend: "s3".to_string(),
                })
                .await
                .unwrap_err();
            assert!(
                matches!(
                    err.downcast_ref::<LlmMemoryError>(),
                    Some(LlmMemoryError::InvalidArgument(_))
                ),
                "missing sha256 must be InvalidArgument: {err}"
            );
            Ok(())
        })
    }

    /// Delete: ref_count==0 deletes the row; a referenced media (we
    /// simulate by incr_ref) returns FailedPrecondition.
    #[test]
    fn delete_requires_zero_ref_count() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let p = pool().await;
            let a = app(p);
            let out = a.upload(header(), stream(vec![b"to-delete-xyz"])).await?;
            // Reference it so ref_count=1, then Delete must refuse.
            let mut tx = p.begin().await?;
            a.media_object_repository
                .incr_ref_tx(&mut *tx, out.media_object_id)
                .await?;
            tx.commit().await?;
            let err = a.delete(out.media_object_id).await.unwrap_err();
            assert!(
                matches!(
                    err.downcast_ref::<LlmMemoryError>(),
                    Some(LlmMemoryError::FailedPrecondition(_))
                ),
                "ref_count>0 Delete must be FailedPrecondition: {err}"
            );
            // Drop the ref, then Delete succeeds and the row is gone.
            let mut tx = p.begin().await?;
            a.media_object_repository
                .decr_ref_tx(&mut *tx, out.media_object_id)
                .await?;
            tx.commit().await?;
            a.delete(out.media_object_id).await?;
            assert!(a.metadata_of(out.media_object_id).await.is_err());
            Ok(())
        })
    }

    /// Upload size guard: exceeding upload_max_bytes surfaces an error
    /// (the gRPC layer maps this to ResourceExhausted).
    #[test]
    fn upload_size_guard() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let p = pool().await;
            let id_gen = infra::test_helper::shared_id_generator();
            let repo = MediaObjectRepositoryImpl::new(id_gen.clone(), p);
            let storage = Arc::new(StorageBackend::Inline(InlineMediaStorage::new()));
            let a = MediaAppImpl::new(
                repo,
                storage,
                id_gen,
                "memories/".to_string(),
                900,
                4, // tiny cap
            );
            let err = a
                .upload(header(), stream(vec![b"way-too-large"]))
                .await
                .unwrap_err();
            assert!(
                matches!(
                    err.downcast_ref::<LlmMemoryError>(),
                    Some(LlmMemoryError::ResourceExhausted(_))
                ),
                "oversize upload must be ResourceExhausted (spec §4.1.1): {err}"
            );
            Ok(())
        })
    }

    // ---- Phase 1-fix regression tests ----

    use infra::infra::media_storage::file::FileMediaStorage;

    /// A MediaApp backed by the file backend rooted at a unique temp dir,
    /// so a Register-ed `storage_uri` (an on-storage key) can be exercised
    /// end to end (the inline backend cannot — its Resolve is a data URI).
    fn app_file(pool: &'static RdbPool) -> (MediaAppImpl, std::path::PathBuf) {
        let id_gen = infra::test_helper::shared_id_generator();
        let repo = MediaObjectRepositoryImpl::new(id_gen.clone(), pool);
        let root = std::env::temp_dir().join(format!(
            "memories-media-fix-{}",
            id_gen.generate_id().unwrap()
        ));
        let storage = Arc::new(StorageBackend::File(FileMediaStorage::new(&root)));
        let a = MediaAppImpl::new(
            repo,
            storage,
            id_gen,
            "memories/".to_string(),
            900,
            20 * 1024 * 1024,
        );
        (a, root)
    }

    /// Fix-1 (review finding 2) + Fix-4 (round 2, finding 3, option A):
    /// a Register-ed s3/file media must Resolve to the *registered*
    /// storage_uri, not a sha256-derived final key. `storage_uri` here is
    /// a backend-internal key ("external/dir/picture.png", NO bucket /
    /// "s3://" prefix) — exactly the A-option contract that the proto /
    /// spec / design comments now document.
    #[test]
    fn register_file_resolves_registered_storage_uri() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let p = pool().await;
            let (a, root) = app_file(p);
            // Pre-place the externally-PUT object at a non-sha key.
            let registered_key = "external/dir/picture.png";
            let body = b"registered-file-body".to_vec();
            let abs = root.join(registered_key);
            tokio::fs::create_dir_all(abs.parent().unwrap()).await?;
            tokio::fs::write(&abs, &body).await?;

            let sha = "a".repeat(64); // arbitrary client-declared sha256
            let meta = a
                .register(RegisterParams {
                    kind: 2,
                    media_type: "image/png".to_string(),
                    storage_uri: registered_key.to_string(),
                    sha256: Some(sha.clone()),
                    byte_size: Some(body.len() as i64),
                    width: None,
                    height: None,
                    alt: None,
                    storage_backend: "file".to_string(),
                })
                .await?;
            let id = meta.id.unwrap().value;

            let r = a.resolve(id, None).await?;
            assert!(!r.unresolved);
            let url = r.url.expect("resolved url");
            // Must presign the REGISTERED key, never the sha256-derived
            // final key (which would be memories/aa/aa/<sha>).
            assert!(
                url.ends_with("picture.png"),
                "Resolve must use the registered storage_uri, got: {url}"
            );
            assert!(
                !url.contains(&sha),
                "Resolve must NOT derive a sha256 final key, got: {url}"
            );

            // Fix-1 also covers Delete: it must delete the registered key.
            // ref_count is 0 (no memory linked), so Delete proceeds.
            a.delete(id).await?;
            assert!(
                !abs.exists(),
                "Delete must remove the registered key's object"
            );
            let _ = tokio::fs::remove_dir_all(&root).await;
            Ok(())
        })
    }

    /// Fix-3 (round 2, finding 2): a Register whose declared
    /// storage_backend differs from the server's running backend must be
    /// rejected with InvalidArgument (a file-backend server cannot honour
    /// an "s3" Register — it would HEAD the wrong store and persist a
    /// desynced row).
    #[test]
    fn register_backend_mismatch_rejected() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let p = pool().await;
            let (a, root) = app_file(p); // server runs the file backend
            let err = a
                .register(RegisterParams {
                    kind: 2,
                    media_type: "image/png".to_string(),
                    storage_uri: "some/key.png".to_string(),
                    sha256: Some("b".repeat(64)),
                    byte_size: Some(1),
                    width: None,
                    height: None,
                    alt: None,
                    storage_backend: "s3".to_string(), // != server "file"
                })
                .await
                .unwrap_err();
            assert!(
                matches!(
                    err.downcast_ref::<LlmMemoryError>(),
                    Some(LlmMemoryError::InvalidArgument(_))
                ),
                "backend mismatch must be InvalidArgument: {err}"
            );
            let _ = tokio::fs::remove_dir_all(&root).await;
            Ok(())
        })
    }

    /// Fix-3: a matching backend Register succeeds and the row's
    /// storage_backend is the server-verified value ("file"), never a
    /// blindly-trusted request value.
    #[test]
    fn register_matching_backend_persists_verified_backend() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let p = pool().await;
            let (a, root) = app_file(p);
            let key = "verified/img.png";
            let body = b"verified-body".to_vec();
            let abs = root.join(key);
            tokio::fs::create_dir_all(abs.parent().unwrap()).await?;
            tokio::fs::write(&abs, &body).await?;
            let meta = a
                .register(RegisterParams {
                    kind: 2,
                    media_type: "image/png".to_string(),
                    storage_uri: key.to_string(),
                    sha256: Some("c".repeat(64)),
                    byte_size: Some(body.len() as i64),
                    width: None,
                    height: None,
                    alt: None,
                    storage_backend: "file".to_string(), // == server
                })
                .await?;
            let id = meta.id.unwrap().value;
            let row = a
                .media_object_repository
                .find_by_id(id)
                .await?
                .expect("row exists");
            assert_eq!(
                row.storage_backend, "file",
                "DB must store the server-verified backend"
            );
            let _ = tokio::fs::remove_dir_all(&root).await;
            Ok(())
        })
    }

    /// Fix-3: `url` Register is exempt from the backend match (it is an
    /// external reference, unrelated to the server's managed storage) —
    /// regression guard that the existing url path still works.
    #[test]
    fn register_url_skips_backend_match() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let p = pool().await;
            let (a, root) = app_file(p); // server backend = file
            // url Register must NOT require storage_backend == "file".
            let meta = a
                .register(RegisterParams {
                    kind: 2,
                    media_type: "image/jpeg".to_string(),
                    storage_uri: "https://example.test/x.jpg".to_string(),
                    sha256: None,
                    byte_size: None,
                    width: None,
                    height: None,
                    alt: None,
                    storage_backend: "url".to_string(),
                })
                .await?;
            assert!(meta.sha256.is_none());
            let _ = tokio::fs::remove_dir_all(&root).await;
            Ok(())
        })
    }

    /// Fix-1 regression guard: an Upload-created media (storage_uri =
    /// confirmed final key) still Resolves/Deletes correctly under the
    /// file backend (no regression from removing the sha256 derivation).
    #[test]
    fn upload_file_resolve_delete_roundtrip() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let p = pool().await;
            let (a, root) = app_file(p);
            let out = a
                .upload(header(), stream(vec![b"upload-", b"file-body"]))
                .await?;
            let r = a.resolve(out.media_object_id, None).await?;
            assert!(!r.unresolved);
            assert!(r.url.unwrap().starts_with("file://"));
            a.delete(out.media_object_id).await?;
            assert!(a.metadata_of(out.media_object_id).await.is_err());
            let _ = tokio::fs::remove_dir_all(&root).await;
            Ok(())
        })
    }

    /// Fix-2 (review finding 3): a b-3 transient conflict (existing row
    /// gc_state=1, storage_uri NULL = concurrent reservation) must be
    /// Aborted (gRPC ABORTED, retryable), not FailedPrecondition.
    #[test]
    fn upload_b3_conflict_is_aborted() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let p = pool().await;
            let a = app(p);
            // Seed a NULL-uri reservation row directly (a concurrent upload
            // that has committed tx1 but not yet confirmed).
            let id = a.id_generator.generate_id()?;
            let sha = {
                // Compute the sha256 the upload below will produce so the
                // INSERT collides.
                use sha2::{Digest, Sha256};
                let mut h = Sha256::new();
                h.update(b"b3-conflict-body");
                super::hex(&h.finalize())
            };
            let mut tx = p.begin().await?;
            a.media_object_repository
                .insert_reservation_tx(
                    &mut *tx,
                    &infra::infra::media_object::rdb::MediaObjectReservation {
                        id,
                        kind: 2,
                        media_type: "image/png".to_string(),
                        byte_size: Some(1),
                        sha256: sha.clone(),
                        width: None,
                        height: None,
                        duration_ms: None,
                        alt: None,
                        created_at: command_utils::util::datetime::now_millis(),
                        updated_at: command_utils::util::datetime::now_millis(),
                    },
                )
                .await?;
            tx.commit().await?;
            // gc_state=1, storage_uri NULL → b-3.
            let err = a
                .upload(header(), stream(vec![b"b3-conflict-body"]))
                .await
                .unwrap_err();
            assert!(
                matches!(
                    err.downcast_ref::<LlmMemoryError>(),
                    Some(LlmMemoryError::Aborted(_))
                ),
                "b-3 transient conflict must be Aborted (design §7.1): {err}"
            );
            Ok(())
        })
    }

    /// Fix-2: a b-5 transient conflict (existing row gc_state=5 =
    /// deleting in progress) must be Aborted.
    #[test]
    fn upload_b5_conflict_is_aborted() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let p = pool().await;
            let a = app(p);
            // Create + confirm a media, then drive it to gc_state=5.
            let first = a
                .upload(header(), stream(vec![b"b5-conflict-body"]))
                .await?;
            let mut tx = p.begin().await?;
            // ref_count must be 0 to claim deleting.
            let claimed = a
                .media_object_repository
                .claim_deleting_tx(&mut *tx, first.media_object_id)
                .await?;
            tx.commit().await?;
            assert!(claimed, "should claim deleting on a fresh orphan");
            // Same bytes → same sha → conflict on a gc_state=5 row → b-5.
            let err = a
                .upload(header(), stream(vec![b"b5-conflict-body"]))
                .await
                .unwrap_err();
            assert!(
                matches!(
                    err.downcast_ref::<LlmMemoryError>(),
                    Some(LlmMemoryError::Aborted(_))
                ),
                "b-5 transient conflict must be Aborted (design §7.1): {err}"
            );
            Ok(())
        })
    }
}
