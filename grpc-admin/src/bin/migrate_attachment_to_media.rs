//! Image memory Phase 4 required deliverable (spec §5.4 / design 3/3
//! §15): convert every memory carrying a legacy `metadata.attachment`
//! into a `media_object` reference.
//!
//! The `metadata.attachment` JSON (built by
//! `agent-chat-import::common::canonical::build_attachment`) has a
//! `storage` discriminant; conversion is per-`storage`:
//!
//! | storage        | action                                           |
//! |----------------|--------------------------------------------------|
//! | inline_base64  | base64-decode → MediaStorage put → s3/file       |
//! |                | media_object; set media_object_id, +1 ref,       |
//! |                | drop metadata.attachment (one tx)                |
//! | url / ref      | register an external-URL media_object            |
//! |                | (sha256/byte_size NULL); same wiring + drop      |
//! | elided         | body was discarded over the inline size guard —  |
//! |                | unresolvable media_object (gc_state=3); set      |
//! |                | media_object_id, +1 ref, KEEP metadata.attachment|
//! |                | (sha256 there is the only manual-recovery key)   |
//! | invalid        | skip entirely (no media_object, attachment kept) |
//!
//! Idempotent: a memory whose `media_object_id` is already set is
//! skipped. sha256 de-dup means the same image collapses to one
//! media_object across memories. `--dry-run` reports the breakdown
//! without writing; `--batch-size` / `--after-id` drive keyset paging so
//! production can run it in staged chunks (per user / time window upstream).

use anyhow::{Context, Result};
use clap::Parser;
use infra::infra::UseIdGenerator;
use infra::infra::media_object::rdb::{MediaObjectRepository, MediaObjectRepositoryImpl};
use infra::infra::media_storage::{MediaConfig, StorageBackend};
use infra::infra::memory::rdb::{MemoryRepository, MemoryRepositoryImpl};
use infra::infra::module::RepositoryModule;
use infra_utils::infra::rdb::UseRdbPool;
use serde_json::Value;
use std::sync::Arc;

/// Per-`storage` conversion plan, decided purely from the attachment
/// JSON (no I/O). The orchestration layer maps each variant to the
/// matching media_object write; keeping the decision pure makes the
/// spec §5.4 table directly unit-testable.
#[derive(Debug, Clone, PartialEq, Eq)]
enum AttachmentAction {
    /// `inline_base64`: decode `data`, put the bytes, make an s3/file
    /// media_object, then drop `metadata.attachment`.
    PutInline {
        media_type: String,
        data_b64: String,
        width: Option<u32>,
        height: Option<u32>,
        alt: Option<String>,
    },
    /// `url` / `ref`: register the external URL (no fetch). sha256 /
    /// byte_size stay NULL. Drop `metadata.attachment`.
    RegisterUrl {
        media_type: String,
        url: String,
        width: Option<u32>,
        height: Option<u32>,
        alt: Option<String>,
    },
    /// `elided`: body gone; only sha256/size survive. Make an
    /// unresolvable media_object and KEEP `metadata.attachment` (its
    /// sha256 is the manual-recovery key).
    Unresolvable {
        media_type: String,
        sha256: String,
        byte_size: Option<i64>,
        width: Option<u32>,
        height: Option<u32>,
        alt: Option<String>,
    },
    /// `invalid` (or an unrecognised `storage`): do nothing — no
    /// media_object, attachment left in place. Carries the reason for
    /// the dry-run / log breakdown.
    Skip { reason: String },
}

/// `metadata.attachment.storage` discriminants, as produced by
/// `agent-chat-import::common::canonical::build_attachment`. Named here
/// so the cross-crate string contract is explicit on the consuming side
/// (the producer is in a different crate; a silent typo would
/// misclassify a row instead of failing to compile).
mod storage_kind {
    pub const INLINE_BASE64: &str = "inline_base64";
    pub const URL: &str = "url";
    pub const REF: &str = "ref";
    pub const ELIDED: &str = "elided";
    pub const INVALID: &str = "invalid";
}

fn opt_str(v: &Value, key: &str) -> Option<String> {
    v.get(key).and_then(|x| x.as_str()).map(|s| s.to_string())
}

fn opt_u32(v: &Value, key: &str) -> Option<u32> {
    v.get(key).and_then(|x| x.as_u64()).map(|n| n as u32)
}

/// Classify one `metadata.attachment` object (spec §5.4 / design 3/3
/// §15). Pure — the orchestration layer performs the I/O the variant
/// describes. A missing/blank required field downgrades to `Skip` so a
/// malformed row never aborts the batch.
fn classify_attachment(att: &Value) -> AttachmentAction {
    let storage = att.get("storage").and_then(|s| s.as_str()).unwrap_or("");
    let media_type =
        opt_str(att, "media_type").unwrap_or_else(|| "application/octet-stream".into());
    let width = opt_u32(att, "width");
    let height = opt_u32(att, "height");
    let alt = opt_str(att, "alt");
    match storage {
        storage_kind::INLINE_BASE64 => match opt_str(att, "data") {
            Some(data_b64) if !data_b64.is_empty() => AttachmentAction::PutInline {
                media_type,
                data_b64,
                width,
                height,
                alt,
            },
            // inline_base64 with no `data` is contradictory (the elided
            // path is what drops `data`); treat as invalid, don't guess.
            _ => AttachmentAction::Skip {
                reason: "inline_base64_without_data".into(),
            },
        },
        storage_kind::URL | storage_kind::REF => match opt_str(att, "url") {
            Some(url) if !url.is_empty() => AttachmentAction::RegisterUrl {
                media_type,
                url,
                width,
                height,
                alt,
            },
            _ => AttachmentAction::Skip {
                reason: "url_storage_without_url".into(),
            },
        },
        storage_kind::ELIDED => match opt_str(att, "data_sha256") {
            Some(sha256) if !sha256.is_empty() => AttachmentAction::Unresolvable {
                media_type,
                sha256,
                byte_size: att.get("size_bytes").and_then(|x| x.as_i64()),
                width,
                height,
                alt,
            },
            // elided guarantees data_sha256 (build_attachment sets it);
            // missing it means we cannot dedup/recover — skip.
            _ => AttachmentAction::Skip {
                reason: "elided_without_sha256".into(),
            },
        },
        storage_kind::INVALID => AttachmentAction::Skip {
            reason: opt_str(att, "invalid_reason").unwrap_or_else(|| "invalid".into()),
        },
        other => AttachmentAction::Skip {
            reason: format!("unknown_storage:{other}"),
        },
    }
}

/// Outcome tally for the run summary / `--dry-run` report.
#[derive(Debug, Default)]
struct Stats {
    scanned: u64,
    already_linked: u64,
    no_attachment: u64,
    put_inline: u64,
    register_url: u64,
    unresolvable: u64,
    skipped: u64,
    skip_reasons: std::collections::BTreeMap<String, u64>,
}

#[derive(Parser, Debug)]
#[command(
    name = "migrate-attachment-to-media",
    about = "Image memory Phase 4: convert metadata.attachment rows into media_object (spec §5.4)"
)]
struct Cli {
    /// Report the conversion breakdown without writing anything.
    #[arg(long)]
    dry_run: bool,
    /// Keyset page size (memories scanned per round).
    #[arg(long, default_value_t = 500)]
    batch_size: i32,
    /// Resume from `memory.id > after_id` (staged batching).
    #[arg(long, default_value_t = 0)]
    after_id: i64,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    // Fall back to the default logging config when no LOG_* env is set
    // (k8s ConfigMaps carry none — only the local .env does). Mirrors
    // the `front` server's init so the batch boots in-cluster instead of
    // erroring out before the first log line.
    use command_utils::util::tracing::{load_tracing_config_from_env, tracing_init};
    tracing_init(load_tracing_config_from_env().unwrap_or_default()).await?;

    let cli = Cli::parse();
    let repositories = RepositoryModule::new_by_env().await;
    let memory_repo = repositories.create_memory_repository();
    // ref_count is bumped through this repo in the SAME tx as the memory
    // update (see `link_and_drop_attachment`), so it must share the pool
    // with `memory_repo` — both come from the one RepositoryModule.
    let media_repo = repositories.create_media_object_repository();

    let mcfg = MediaConfig::from_env();
    let storage = Arc::new(
        StorageBackend::from_env(&mcfg).context("MEDIA_STORAGE_BACKEND configuration error")?,
    );
    // Reuse the production MediaApp for inline (Upload 3-stage,
    // reservation/copy/confirm, sha256 dedup, conflict b-1..b-5) and url
    // (Register) so the storage invariants are not re-implemented here —
    // only the unresolvable path (Register has no such backend) is bin-local.
    let media_app = app::app::media::MediaAppImpl::new(
        repositories.create_media_object_repository(),
        storage.clone(),
        repositories.id_generator(),
        mcfg.s3_prefix.clone(),
        mcfg.presign_ttl_sec,
        mcfg.upload_max_bytes,
    );

    let mut stats = Stats::default();
    let mut after_id = cli.after_id;

    loop {
        // ROLE filter `&[]` = all roles; user_id/thread_id None = global.
        // Keyset paging (id ASC) so a row converted this pass is never
        // revisited and the scan is restartable via --after-id.
        let page = memory_repo
            .find_list_by_condition_after_id(cli.batch_size, after_id, &[], None, None)
            .await
            .context("scanning memories")?;
        if page.is_empty() {
            break;
        }
        for memory in &page {
            let Some(id) = memory.id.as_ref() else {
                continue;
            };
            after_id = after_id.max(id.value);
            stats.scanned += 1;

            let Some(data) = memory.data.as_ref() else {
                continue;
            };
            // Idempotent: an already-linked memory was converted by an
            // earlier (possibly partial) run.
            if data.media_object_id.is_some() {
                stats.already_linked += 1;
                continue;
            }
            let Some(meta_str) = data.metadata.as_ref() else {
                stats.no_attachment += 1;
                continue;
            };
            let meta: Value = match serde_json::from_str(meta_str) {
                Ok(v) => v,
                Err(_) => {
                    stats.no_attachment += 1;
                    continue;
                }
            };
            let Some(att) = meta.get("attachment").filter(|a| !a.is_null()) else {
                stats.no_attachment += 1;
                continue;
            };

            let action = classify_attachment(att);
            apply_action(
                &cli,
                &memory_repo,
                &media_repo,
                &media_app,
                id.value,
                data,
                &meta,
                action,
                &mut stats,
            )
            .await?;
        }
        if page.len() < cli.batch_size as usize {
            break;
        }
    }

    print_summary(&cli, &stats);
    Ok(())
}

/// The attachment is image-only by spec §1.4 (AUDIO/VIDEO are out of
/// Phase 4 scope), so every converted media_object is `IMAGE`.
fn image_kind() -> i32 {
    protobuf::llm_memory::data::ContentType::Image as i32
}

#[allow(clippy::too_many_arguments)]
async fn apply_action(
    cli: &Cli,
    memory_repo: &MemoryRepositoryImpl,
    media_repo: &MediaObjectRepositoryImpl,
    media_app: &app::app::media::MediaAppImpl,
    memory_id: i64,
    data: &protobuf::llm_memory::data::MemoryData,
    meta: &Value,
    action: AttachmentAction,
    stats: &mut Stats,
) -> Result<()> {
    // Each non-skip arm resolves a media_object_id (creating/dedup'ing
    // the media_object) and whether to drop the attachment; the link tx
    // is then shared. `dry_run` counts the action but performs no write.
    let (media_object_id, drop_attachment) = match action {
        AttachmentAction::Skip { reason } => {
            stats.skipped += 1;
            *stats.skip_reasons.entry(reason).or_insert(0) += 1;
            return Ok(());
        }
        AttachmentAction::PutInline {
            media_type,
            data_b64,
            width,
            height,
            alt,
        } => {
            stats.put_inline += 1;
            if cli.dry_run {
                return Ok(());
            }
            use app::app::media::UploadHeaderMeta;
            use base64::Engine;
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(data_b64.as_bytes())
                .context("inline_base64 decode")?;
            // Reuse MediaService.Upload: reservation→copy→confirm,
            // sha256 dedup, and the b-1..b-5 conflict handling all come
            // for free. The bytes are already in memory (decoded from
            // the metadata string) so a single-chunk stream is fine.
            let stream = futures::stream::once({
                let b = bytes::Bytes::from(bytes);
                async move { Ok(b) }
            });
            let outcome = media_app
                .upload(
                    UploadHeaderMeta {
                        kind: image_kind(),
                        media_type,
                        alt,
                        width,
                        height,
                    },
                    stream,
                )
                .await?;
            (outcome.media_object_id, true)
        }
        AttachmentAction::RegisterUrl {
            media_type,
            url,
            width,
            height,
            alt,
        } => {
            stats.register_url += 1;
            if cli.dry_run {
                return Ok(());
            }
            use app::app::media::RegisterParams;
            // Reuse MediaService.Register (url backend): no fetch,
            // sha256/byte_size NULL, gc_state=1.
            let meta = media_app
                .register(RegisterParams {
                    kind: image_kind(),
                    media_type,
                    storage_uri: url,
                    sha256: None,
                    byte_size: None,
                    width,
                    height,
                    alt,
                    storage_backend: "url".to_string(),
                })
                .await?;
            let id = meta
                .id
                .context("Register(url) returned no media_object id")?
                .value;
            (id, true)
        }
        AttachmentAction::Unresolvable {
            media_type,
            sha256,
            byte_size,
            width,
            height,
            alt,
        } => {
            stats.unresolvable += 1;
            if cli.dry_run {
                return Ok(());
            }
            // Register has no `unresolvable` backend (it is a
            // migration-only state), so this stays a bin-local insert.
            let id = unresolvable_media_object(
                media_repo,
                image_kind(),
                &media_type,
                &sha256,
                byte_size,
                width,
                height,
                alt.as_deref(),
            )
            .await?;
            // elided KEEPS metadata.attachment (manual-recovery key).
            (id, false)
        }
    };

    link_and_drop_attachment(
        memory_repo,
        media_repo,
        memory_id,
        data,
        meta,
        media_object_id,
        drop_attachment,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn unresolvable_media_object(
    media_repo: &MediaObjectRepositoryImpl,
    kind: i32,
    media_type: &str,
    sha256: &str,
    byte_size: Option<i64>,
    width: Option<u32>,
    height: Option<u32>,
    alt: Option<&str>,
) -> Result<i64> {
    let media_id = media_repo.id_generator().generate_id()?;
    let mut tx = media_repo.db_pool().begin().await?;
    let inserted = media_repo
        .insert_unresolvable_tx(
            &mut *tx,
            media_id,
            kind,
            media_type,
            byte_size,
            sha256,
            width.map(|w| w as i32),
            height.map(|h| h as i32),
            alt,
        )
        .await?;
    if !inserted {
        // sha256 de-dup / idempotent re-run: reuse the existing row.
        let existing = media_repo
            .find_by_sha256_tx(&mut *tx, sha256)
            .await?
            .context("unresolvable conflict but row vanished")?;
        tx.commit().await?;
        return Ok(existing.id);
    }
    tx.commit().await?;
    Ok(media_id)
}

/// Wire `memory.media_object_id`, bump the media's ref_count, and
/// (unless `keep_attachment`) strip `metadata.attachment` — all in one
/// tx so a crash never leaves an orphaned ref_count or a half-linked
/// memory. `keep_attachment=true` for inline/url/ref (the attachment is
/// fully migrated); `false`→keep for elided (its sha256 is the only
/// manual-recovery handle, design 3/3 §15).
async fn link_and_drop_attachment(
    memory_repo: &MemoryRepositoryImpl,
    media_repo: &MediaObjectRepositoryImpl,
    memory_id: i64,
    data: &protobuf::llm_memory::data::MemoryData,
    meta: &Value,
    media_object_id: i64,
    drop_attachment: bool,
) -> Result<()> {
    // ref_count lives on the media side; the migration bumps it through
    // the SAME memory tx (one shared pool) so a crash rolls back both —
    // the same discipline MemoryApp create/update uses. memory_repo and
    // media_repo are built from the same RepositoryModule pool.
    let mut new_data = data.clone();
    new_data.media_object_id = Some(protobuf::llm_memory::data::MediaObjectId {
        value: media_object_id,
    });
    if drop_attachment {
        let mut m = meta.clone();
        if let Some(obj) = m.as_object_mut() {
            obj.remove("attachment");
        }
        new_data.metadata = Some(m.to_string());
    }

    let mut tx = memory_repo.db_pool().begin().await?;
    // Row-lock the media_object then incr — mirrors MemoryApp's
    // create_memory primary defence so a concurrent delete cannot race.
    let locked = media_repo
        .find_by_id_for_update_tx(&mut tx, media_object_id)
        .await?;
    if locked.is_none() {
        anyhow::bail!("media_object {media_object_id} vanished before ref bump");
    }
    let ok = media_repo.incr_ref_tx(&mut *tx, media_object_id).await?;
    if !ok {
        anyhow::bail!(
            "incr_ref rejected for media_object {media_object_id} \
             (gc_state in {{2,5}}?) — aborting this memory"
        );
    }
    let updated = memory_repo
        .update(
            &mut *tx,
            &protobuf::llm_memory::data::MemoryId { value: memory_id },
            &new_data,
        )
        .await?;
    if !updated {
        anyhow::bail!("memory {memory_id} update affected 0 rows");
    }
    tx.commit().await?;
    Ok(())
}

fn print_summary(cli: &Cli, s: &Stats) {
    let mode = if cli.dry_run { "[DRY-RUN] " } else { "" };
    tracing::info!(
        "{mode}migrate-attachment-to-media done: scanned={} already_linked={} \
         no_attachment={} put_inline={} register_url={} unresolvable={} skipped={}",
        s.scanned,
        s.already_linked,
        s.no_attachment,
        s.put_inline,
        s.register_url,
        s.unresolvable,
        s.skipped,
    );
    for (reason, n) in &s.skip_reasons {
        tracing::info!("{mode}  skip[{reason}] = {n}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn inline_base64_with_data_is_put() {
        let att = json!({
            "storage": "inline_base64",
            "media_type": "image/png",
            "data": "aGVsbG8=",
            "width": 10, "height": 20, "alt": "hi"
        });
        assert_eq!(
            classify_attachment(&att),
            AttachmentAction::PutInline {
                media_type: "image/png".into(),
                data_b64: "aGVsbG8=".into(),
                width: Some(10),
                height: Some(20),
                alt: Some("hi".into()),
            }
        );
    }

    #[test]
    fn inline_base64_without_data_is_skipped() {
        let att = json!({ "storage": "inline_base64", "media_type": "image/png" });
        assert!(matches!(
            classify_attachment(&att),
            AttachmentAction::Skip { reason } if reason == "inline_base64_without_data"
        ));
    }

    #[test]
    fn url_and_ref_register_external() {
        for storage in ["url", "ref"] {
            let att = json!({
                "storage": storage,
                "media_type": "image/jpeg",
                "url": "https://example.com/a.jpg"
            });
            assert_eq!(
                classify_attachment(&att),
                AttachmentAction::RegisterUrl {
                    media_type: "image/jpeg".into(),
                    url: "https://example.com/a.jpg".into(),
                    width: None,
                    height: None,
                    alt: None,
                }
            );
        }
    }

    #[test]
    fn url_without_url_field_is_skipped() {
        let att = json!({ "storage": "url", "media_type": "image/jpeg" });
        assert!(matches!(
            classify_attachment(&att),
            AttachmentAction::Skip { reason } if reason == "url_storage_without_url"
        ));
    }

    #[test]
    fn elided_becomes_unresolvable_with_sha256() {
        let att = json!({
            "storage": "elided",
            "media_type": "image/png",
            "data_sha256": "abc123",
            "size_bytes": 4096
        });
        assert_eq!(
            classify_attachment(&att),
            AttachmentAction::Unresolvable {
                media_type: "image/png".into(),
                sha256: "abc123".into(),
                byte_size: Some(4096),
                width: None,
                height: None,
                alt: None,
            }
        );
    }

    #[test]
    fn elided_without_sha256_is_skipped() {
        let att = json!({ "storage": "elided", "media_type": "image/png" });
        assert!(matches!(
            classify_attachment(&att),
            AttachmentAction::Skip { reason } if reason == "elided_without_sha256"
        ));
    }

    #[test]
    fn invalid_carries_reason() {
        let att = json!({
            "storage": "invalid",
            "invalid_reason": "base64_decode_failed"
        });
        assert!(matches!(
            classify_attachment(&att),
            AttachmentAction::Skip { reason } if reason == "base64_decode_failed"
        ));
    }

    #[test]
    fn unknown_storage_is_skipped_not_converted() {
        let att = json!({ "storage": "future_thing" });
        assert!(matches!(
            classify_attachment(&att),
            AttachmentAction::Skip { reason } if reason == "unknown_storage:future_thing"
        ));
    }

    #[test]
    fn missing_media_type_defaults_octet_stream() {
        let att = json!({ "storage": "inline_base64", "data": "eA==" });
        match classify_attachment(&att) {
            AttachmentAction::PutInline { media_type, .. } => {
                assert_eq!(media_type, "application/octet-stream");
            }
            other => panic!("expected PutInline, got {other:?}"),
        }
    }
}
