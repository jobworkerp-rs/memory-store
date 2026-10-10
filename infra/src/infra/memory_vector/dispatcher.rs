use crate::infra::embedding_dispatch::{
    DispatchSpec, EmbeddingConfig, EmbeddingDispatcherCore, ImageSearchMode,
};
use crate::infra::embedding_space::registration::WorkerRegistry;
use anyhow::Result;
use async_trait::async_trait;
use jobworkerp_client::jobworkerp::data::JobId;
use std::sync::Arc;

pub use crate::infra::embedding_dispatch::{
    DispatchError, DispatchKind, DispatchTarget, EmbeddingDispatch, EmbeddingDispatchStatus,
    EmbeddingJobId, dispatch_kinds,
};

const SPEC: DispatchSpec = DispatchSpec {
    target_worker_name: "memories-auto-embedding",
    id_field_name: "memory_id",
    text_source: crate::infra::embedding_index::source_version::TextSource::MemoryText,
};

const WORKERS_YAML_ENV: &str = "MEMORY_WORKERS_YAML";
const DEFAULT_WORKERS_YAML_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../workflows/auto-embedding-workers.yaml"
);

const IMAGE_WORKERS_YAML_ENV: &str = "MEMORY_IMAGE_WORKERS_YAML";
const DEFAULT_IMAGE_WORKERS_YAML_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../workflows/auto-image-embedding-workers.yaml"
);

/// Configuration for the memory embedding dispatcher. Re-exported as a
/// type alias of [`EmbeddingConfig`] so the thread dispatcher can promote
/// an existing memory config to its own kind without an extra struct.
pub type AutoEmbeddingConfig = EmbeddingConfig;

/// Read configuration from `MEMORY_EMBEDDING_*` env vars and the
/// `MEMORY_WORKERS_YAML` path. Same shape as [`EmbeddingConfig::from_env`]
/// with the memory-side defaults baked in.
/// The workers YAML whose mm-embedding worker defines the embedding model.
pub fn workers_yaml_path_from_env() -> std::path::PathBuf {
    std::env::var(WORKERS_YAML_ENV)
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|_| std::path::PathBuf::from(DEFAULT_WORKERS_YAML_PATH))
}

pub fn auto_embedding_config_from_env() -> Result<AutoEmbeddingConfig> {
    EmbeddingConfig::from_env()
}

/// Workers YAML files the memory dispatcher needs registered: the text
/// pipeline (which also defines the mm-embedding worker) and, in any
/// image mode, the image pipeline.
pub fn workers_yaml_paths(mode: ImageSearchMode) -> Vec<std::path::PathBuf> {
    let mut paths = vec![workers_yaml_path_from_env()];
    if mode.is_image_enabled() {
        paths.push(
            std::env::var(IMAGE_WORKERS_YAML_ENV)
                .map(std::path::PathBuf::from)
                .unwrap_or_else(|_| std::path::PathBuf::from(DEFAULT_IMAGE_WORKERS_YAML_PATH)),
        );
    }
    paths
}

pub use crate::infra::embedding_dispatch::{
    CAPTION_WORKFLOW_WORKER, IMAGE_WORKFLOW_WORKER, TEXT_WORKFLOW_WORKER,
};

/// Dispatches embedding generation jobs to jobworkerp (fire-and-forget).
pub struct EmbeddingJobDispatcher {
    core: EmbeddingDispatcherCore,
    media: Option<crate::infra::media_object::rdb::MediaObjectRepositoryImpl>,
}

impl EmbeddingJobDispatcher {
    /// Spawn dispatch for a resolved target (fire-and-forget). Each
    /// `DispatchKind` is enqueued to its own workflow worker so the text
    /// and image pipelines never clobber each other's `vector_kind` rows.
    pub fn spawn_dispatch(dispatcher: &Arc<Self>, target: DispatchTarget) {
        if target.kinds.is_empty() {
            return;
        }
        let d = dispatcher.clone();
        let memory_id = target.memory_id;
        tokio::spawn(async move {
            // catch_unwind to detect unexpected panics in fire-and-forget
            // task. Per-kind enqueue errors are logged here; the overall
            // Result is dropped intentionally (best-effort dispatch).
            use futures::FutureExt as _;
            use std::panic::AssertUnwindSafe;
            match AssertUnwindSafe(async {
                for (kind, r) in target
                    .kinds
                    .iter()
                    .copied()
                    .zip(d.dispatch_target(&target).await)
                {
                    if let Err(e) = r {
                        tracing::error!(memory_id, ?kind, "embedding dispatch failed: {e}");
                    }
                }
            })
            .catch_unwind()
            .await
            {
                Ok(()) => {}
                Err(e) => {
                    tracing::error!("embedding dispatch panicked for memory_id={memory_id}: {e:?}");
                }
            }
        });
    }

    pub fn from_env(registry: Arc<WorkerRegistry>) -> Result<Self> {
        Ok(Self {
            core: EmbeddingDispatcherCore::new(auto_embedding_config_from_env()?, SPEC, registry),
            media: None,
        })
    }

    /// Media rows are read at dispatch time to version image requests
    /// (content digest or URL). Without it, image requests carry no token.
    pub fn with_media_repository(
        mut self,
        media: crate::infra::media_object::rdb::MediaObjectRepositoryImpl,
    ) -> Self {
        self.media = Some(media);
        self
    }

    /// Dispatch the caption embedding of an image memory whose body is
    /// `caption` (the caption kind is the body embedded as a caption).
    pub async fn dispatch_caption(
        &self,
        memory_id: i64,
        caption: &str,
    ) -> std::result::Result<Option<JobId>, DispatchError> {
        let args = self.core.build_kind_job_args(
            memory_id,
            caption,
            crate::infra::memory_vector::record::vector_kind::CAPTION,
            crate::infra::embedding_index::source_version::TextSource::Caption,
        );
        self.core
            .dispatch_to_worker(TEXT_WORKFLOW_WORKER, &args)
            .await
    }

    /// Dispatch the image embedding of a memory's media.
    pub async fn dispatch_image(
        &self,
        memory_id: i64,
        media_object_id: i64,
    ) -> std::result::Result<Option<JobId>, DispatchError> {
        let version = self.media_version(media_object_id).await;
        let args = build_image_job_args(memory_id, media_object_id, version.as_ref());
        self.core
            .dispatch_to_worker(IMAGE_WORKFLOW_WORKER, &args)
            .await
    }

    /// Request a caption for an image memory whose body is empty.
    pub async fn dispatch_caption_generation(
        &self,
        memory_id: i64,
        media_object_id: i64,
    ) -> std::result::Result<Option<JobId>, DispatchError> {
        self.core
            .dispatch_to_fixed_worker(
                CAPTION_WORKFLOW_WORKER,
                &build_caption_job_args(memory_id, media_object_id),
            )
            .await
    }

    async fn media_version(
        &self,
        media_object_id: i64,
    ) -> Option<crate::infra::embedding_index::SourceVersion> {
        use crate::infra::media_object::rdb::MediaObjectRepository as _;
        let repo = self.media.as_ref()?;
        match repo.find_by_ids(&[media_object_id]).await {
            Ok(rows) => rows.into_iter().find(|r| r.id == media_object_id).map(|r| {
                crate::infra::embedding_index::source_version::media(
                    r.id,
                    r.sha256.as_deref(),
                    r.storage_uri.as_deref(),
                )
            }),
            Err(e) => {
                tracing::warn!(
                    media_object_id,
                    "media lookup for the dispatch token failed: {e:#}"
                );
                None
            }
        }
    }

    /// Synchronously embed a short text query in the stored-vector model
    /// space (SearchSemantic). See `EmbeddingDispatcherCore::query_embed`.
    /// The worker name comes from the crate-level single source of truth
    /// (`mm_embedding_worker_name`) so it always matches the storage YAML.
    pub async fn query_embed_text(
        &self,
        text: &str,
    ) -> Result<crate::infra::embedding_dispatch::QueryEmbedding> {
        let worker = crate::infra::embedding_dispatch::mm_embedding_worker_name();
        self.core.query_embed_text(&worker, text).await
    }

    /// Synchronously embed an image query (by URL) in the stored-vector
    /// model space (SearchByMedia).
    pub async fn query_embed_image_url(
        &self,
        url: &str,
    ) -> Result<crate::infra::embedding_dispatch::QueryEmbedding> {
        let worker = crate::infra::embedding_dispatch::mm_embedding_worker_name();
        self.core.query_embed_image_url(&worker, url).await
    }

    /// Enqueue a single kind. Shared by the trait `dispatch_target`
    /// (counted, used by redispatch) and `spawn_dispatch` (fire-and-
    /// forget). Returns the enqueue result so callers can count it.
    async fn dispatch_one(
        &self,
        target: &DispatchTarget,
        kind: DispatchKind,
    ) -> std::result::Result<Option<JobId>, DispatchError> {
        match kind {
            DispatchKind::Text => {
                let args = self
                    .core
                    .build_text_job_args(target.memory_id, &target.content);
                self.core
                    .dispatch_to_worker(TEXT_WORKFLOW_WORKER, &args)
                    .await
            }
            DispatchKind::Media => {
                let Some(mid) = target.media_object_id else {
                    // A Media kind without a media_object_id is a caller
                    // bug (dispatch_kinds only yields Media when media is
                    // present); surface it instead of silently skipping.
                    return Err(DispatchError::Enqueue(tonic::Status::invalid_argument(
                        "Media dispatch requested without media_object_id",
                    )));
                };
                let mode = target.image_search_mode;
                let mut first = None;
                if matches!(mode, ImageSearchMode::Multimodal | ImageSearchMode::Both) {
                    let version = self.media_version(mid).await;
                    let args = build_image_job_args(target.memory_id, mid, version.as_ref());
                    first = self
                        .core
                        .dispatch_to_worker(IMAGE_WORKFLOW_WORKER, &args)
                        .await?;
                }
                if matches!(mode, ImageSearchMode::VlmCaption | ImageSearchMode::Both) {
                    // The caption is the memory body: embed it when there
                    // is one, otherwise have one generated (its write-back
                    // dispatches the embedding).
                    let job = if target.content.trim().is_empty() {
                        self.core
                            .dispatch_to_fixed_worker(
                                CAPTION_WORKFLOW_WORKER,
                                &build_caption_job_args(target.memory_id, mid),
                            )
                            .await?
                    } else {
                        let args = self.core.build_kind_job_args(
                            target.memory_id,
                            &target.content,
                            crate::infra::memory_vector::record::vector_kind::CAPTION,
                            crate::infra::embedding_index::source_version::TextSource::Caption,
                        );
                        self.core
                            .dispatch_to_worker(TEXT_WORKFLOW_WORKER, &args)
                            .await?
                    };
                    first = first.or(job);
                }
                Ok(first)
            }
        }
    }
}

/// Build the image workflow `input` JSON. The WORKFLOW runner takes a
/// single string-typed parameter, so `input` is itself a JSON string. The
/// `embedding_model` is read from the runner output inside the workflow.
fn build_image_job_args(
    memory_id: i64,
    media_object_id: i64,
    version: Option<&crate::infra::embedding_index::SourceVersion>,
) -> serde_json::Value {
    let mut inner = serde_json::json!({
        "memory_id": memory_id.to_string(),
        "media_object_id": media_object_id.to_string(),
    });
    if let Some(v) = version {
        crate::infra::embedding_dispatch::with_token(&mut inner, v);
    }
    serde_json::json!({ "input": inner.to_string() })
}

/// Build the caption generation workflow `input` JSON. The media object
/// ID is carried so the write-back only lands while the memory still
/// links the same media.
fn build_caption_job_args(memory_id: i64, media_object_id: i64) -> serde_json::Value {
    let inner = serde_json::json!({
        "memory_id": memory_id.to_string(),
        "media_object_id": media_object_id.to_string(),
    });
    serde_json::json!({ "input": inner.to_string() })
}

#[async_trait]
impl EmbeddingDispatch for EmbeddingJobDispatcher {
    /// Legacy text-only entry point. Kept for the `redispatch_embeddings`
    /// recovery API and its stub tests, which dispatch text content by
    /// `(memory_id, content)`. The kind-routed path is `dispatch_target`.
    async fn dispatch(
        &self,
        memory_id: i64,
        content: &str,
    ) -> std::result::Result<Option<JobId>, DispatchError> {
        if content.is_empty() {
            return Ok(None);
        }
        let args = self.core.build_text_job_args(memory_id, content);
        self.core
            .dispatch_to_worker(TEXT_WORKFLOW_WORKER, &args)
            .await
    }

    /// Route each kind to its workflow worker (text → text workflow,
    /// media → image workflow). Overrides the text-only default so the
    /// memory dispatcher (and `redispatch_embeddings` through it) can
    /// drive the image pipeline too.
    async fn dispatch_target(
        &self,
        target: &DispatchTarget,
    ) -> Vec<std::result::Result<Option<JobId>, DispatchError>> {
        let mut out = Vec::with_capacity(target.kinds.len());
        for kind in &target.kinds {
            out.push(self.dispatch_one(target, *kind).await);
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    #[test]
    fn dispatch_kinds_text_only() {
        use protobuf::llm_memory::data::{ContentType, MessageRole};
        let text = ContentType::Text as i32;
        // Allowed roles + non-empty content + no media → [Text].
        for role in [
            MessageRole::RoleUser,
            MessageRole::RoleAssistant,
            MessageRole::RoleSystem,
            MessageRole::RoleReflection,
        ] {
            let k = dispatch_kinds(
                "hello",
                role as i32,
                text,
                None,
                None,
                ImageSearchMode::None,
            );
            assert_eq!(&k[..], &[DispatchKind::Text], "role={role:?}");
        }
    }

    #[test]
    fn dispatch_kinds_role_not_allowed_is_empty() {
        use protobuf::llm_memory::data::{ContentType, MessageRole};
        let text = ContentType::Text as i32;
        // UNSPECIFIED rejected (must be an explicit speaker); TOOL/META
        // rejected.
        for role in [
            MessageRole::RoleUnspecified,
            MessageRole::RoleTool,
            MessageRole::RoleMeta,
        ] {
            let k = dispatch_kinds(
                "hello",
                role as i32,
                text,
                Some(ContentType::Image as i32),
                Some("s3"),
                ImageSearchMode::Multimodal,
            );
            assert!(k.is_empty(), "role={role:?} must yield no kinds");
        }
        // Out-of-range role also yields nothing.
        assert!(dispatch_kinds("hi", 99, text, None, None, ImageSearchMode::None).is_empty());
    }

    #[test]
    fn dispatch_kinds_tool_content_excluded_from_text() {
        use protobuf::llm_memory::data::{ContentType, MessageRole};
        let user = MessageRole::RoleUser as i32;
        // content_type=TOOL → no Text (tool-call preview noise), even
        // though role is allowed and content is non-empty.
        let k = dispatch_kinds(
            "Read({...})",
            user,
            ContentType::Tool as i32,
            None,
            None,
            ImageSearchMode::None,
        );
        assert!(k.is_empty());
        // But a TOOL memory's screenshot is still Media (content_type is
        // independent of the Media axis).
        let k = dispatch_kinds(
            "Read({...})",
            user,
            ContentType::Tool as i32,
            Some(ContentType::Image as i32),
            Some("s3"),
            ImageSearchMode::Multimodal,
        );
        assert_eq!(&k[..], &[DispatchKind::Media]);
    }

    #[test]
    fn dispatch_kinds_empty_content_image_only() {
        use protobuf::llm_memory::data::{ContentType, MessageRole};
        let user = MessageRole::RoleUser as i32;
        let img = ContentType::Image as i32;
        // Empty / whitespace content + image + mode≠none → [Media] only
        // (image-only memory is still embeddable; the old "empty content
        // → skip" behaviour is replaced by "kinds non-empty").
        for content in ["", "   ", "\n\t "] {
            let k = dispatch_kinds(
                content,
                user,
                ContentType::Text as i32,
                Some(img),
                Some("file"),
                ImageSearchMode::Multimodal,
            );
            assert_eq!(&k[..], &[DispatchKind::Media], "content={content:?}");
        }
    }

    #[test]
    fn dispatch_kinds_text_and_media_coexist() {
        use protobuf::llm_memory::data::{ContentType, MessageRole};
        let k = dispatch_kinds(
            "caption text",
            MessageRole::RoleUser as i32,
            ContentType::Text as i32,
            Some(ContentType::Image as i32),
            Some("s3"),
            ImageSearchMode::Both,
        );
        assert_eq!(&k[..], &[DispatchKind::Text, DispatchKind::Media]);
    }

    #[test]
    fn dispatch_kinds_media_excluded_cases() {
        use protobuf::llm_memory::data::{ContentType, MessageRole};
        let user = MessageRole::RoleUser as i32;
        let img = ContentType::Image as i32;
        // mode=none → no Media (text still emitted).
        let k = dispatch_kinds(
            "hi",
            user,
            ContentType::Text as i32,
            Some(img),
            Some("s3"),
            ImageSearchMode::None,
        );
        assert_eq!(&k[..], &[DispatchKind::Text]);
        // unresolvable / inline backends → no Media (no bytes / not
        // supported by the embedding workflow). text still emitted.
        for backend in ["unresolvable", "inline"] {
            let k = dispatch_kinds(
                "hi",
                user,
                ContentType::Text as i32,
                Some(img),
                Some(backend),
                ImageSearchMode::Both,
            );
            assert_eq!(&k[..], &[DispatchKind::Text], "backend={backend}");
        }
        // AUDIO/VIDEO media → no Media.
        for mk in [ContentType::Audio as i32, ContentType::Video as i32] {
            let k = dispatch_kinds(
                "",
                user,
                ContentType::Text as i32,
                Some(mk),
                Some("s3"),
                ImageSearchMode::Multimodal,
            );
            assert!(k.is_empty(), "media_kind={mk} must not yield Media");
        }
    }

    #[test]
    fn dispatch_kind_wire_roundtrip() {
        assert_eq!(DispatchKind::Text.as_wire(), 1);
        assert_eq!(DispatchKind::Media.as_wire(), 2);
        assert_eq!(DispatchKind::try_from(1), Ok(DispatchKind::Text));
        assert_eq!(DispatchKind::try_from(2), Ok(DispatchKind::Media));
        // UNSPECIFIED(0) and out-of-range are rejected, not defaulted.
        assert_eq!(DispatchKind::try_from(0), Err(()));
        assert_eq!(DispatchKind::try_from(3), Err(()));
        assert_eq!(DispatchKind::try_from(-1), Err(()));
    }

    #[test]
    fn dispatch_target_from_memory_skips_when_no_kinds() {
        use protobuf::llm_memory::data::{ContentType, MessageRole};
        // role not allowed → None (caller skips without spawning).
        assert!(
            DispatchTarget::from_memory(
                1,
                "hi",
                MessageRole::RoleTool as i32,
                ContentType::Text as i32,
                None,
                None,
                None,
                ImageSearchMode::None,
            )
            .is_none()
        );
        // text → Some with [Text].
        let t = DispatchTarget::from_memory(
            7,
            "hi",
            MessageRole::RoleUser as i32,
            ContentType::Text as i32,
            None,
            None,
            None,
            ImageSearchMode::None,
        )
        .expect("should dispatch text");
        assert_eq!(t.memory_id, 7);
        assert_eq!(&t.kinds[..], &[DispatchKind::Text]);
    }

    #[test]
    #[serial]
    fn image_job_args_carry_a_token_for_the_media_version() {
        use crate::infra::embedding_space::{SpaceId, workers};
        let version = crate::infra::embedding_index::source_version::media(9, Some("d"), None);
        workers::set_current_space(Some(SpaceId("ab".repeat(32))));
        let v = build_image_job_args(
            7_465_246_090_942_480_532,
            7_465_246_090_942_480_757,
            Some(&version),
        );
        workers::set_current_space(None);
        let inner: serde_json::Value = serde_json::from_str(v["input"].as_str().unwrap()).unwrap();
        assert_eq!(inner["memory_id"], "7465246090942480532");
        assert_eq!(inner["media_object_id"], "7465246090942480757");
        assert_eq!(inner["token"]["source_version"], version.as_str());
        assert_eq!(inner["token"]["space_id"], "ab".repeat(32));
        assert_eq!(inner["token"]["attempt_id"], "");
        assert!(!inner["token"]["generation_id"].as_str().unwrap().is_empty());
        // embedding_model is read from runner output in the workflow.
        assert!(inner.get("embedding_model").is_none());
    }

    #[test]
    #[serial]
    fn requests_carry_no_token_without_a_space() {
        crate::infra::embedding_space::workers::set_current_space(None);
        let version = crate::infra::embedding_index::source_version::media(9, Some("d"), None);
        let v = build_image_job_args(1, 2, Some(&version));
        let inner: serde_json::Value = serde_json::from_str(v["input"].as_str().unwrap()).unwrap();
        assert!(inner.get("token").is_none());
    }

    #[test]
    fn caption_job_args_carry_the_media_object() {
        let v = build_caption_job_args(1, 2);
        let inner: serde_json::Value = serde_json::from_str(v["input"].as_str().unwrap()).unwrap();
        assert_eq!(inner["memory_id"], "1");
        assert_eq!(inner["media_object_id"], "2");
    }

    #[test]
    #[serial]
    fn test_config_from_env_minimal() {
        // SAFETY: #[serial] guards against concurrent env access.
        unsafe {
            std::env::remove_var("MEMORY_EMBEDDING_TIMEOUT_SEC");
            std::env::remove_var("MEMORY_EMBEDDING_MAX_CONTENT_LEN");
            std::env::remove_var("MEMORY_WORKERS_YAML");
        }
        let config = auto_embedding_config_from_env().unwrap();
        assert_eq!(config.timeout_sec, 120);
        assert_eq!(config.max_content_len, 8192);
        assert!(
            workers_yaml_path_from_env().ends_with("auto-embedding-workers.yaml"),
            "expected default workers YAML path"
        );
    }

    /// The persisted `embedding_model` metadata is sourced from the
    /// runner's `model_info.model_name` inside the workflow YAML, with a
    /// fallback when that optional field is absent. This guards against
    /// the workflow regressing to e.g. `$workflow.input.embedding_model`,
    /// which would re-introduce the metadata-vs-runner drift this PR
    /// removes. Asserts file content because the workflow is consumed by
    /// jobworkerp at runtime, not by code in this crate.
    #[test]
    fn workflow_yaml_sources_embedding_model_from_runner_output() {
        let workflow_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../workflows/auto-embedding.yaml");
        let yaml = std::fs::read_to_string(&workflow_path).expect("auto-embedding.yaml must exist");
        assert!(
            yaml.contains(".model_info.model_name"),
            "auto-embedding.yaml must read embedding_model from \
             .model_info.model_name; got:\n{yaml}"
        );
        assert!(
            !yaml.contains("$workflow.input.embedding_model"),
            "auto-embedding.yaml must not pull embedding_model from \
             workflow input (that would re-introduce env/runner drift)"
        );
    }

    /// The `memories-auto-embedding` workflow worker MUST be pinned to the
    /// dedicated `embedding_workflow` channel. Leaving it on the default
    /// channel allows multiple workflows to fan out concurrently into the
    /// single-slot `embedding` channel, inflating each workflow's wall
    /// clock by the queue depth and tripping the 600s job timeout under
    /// thread-summary-style bursts. This guard prevents that channel from
    /// being silently dropped during YAML edits. Plain text scan keeps the
    /// infra crate free of a YAML parser dependency.
    #[test]
    fn workers_yaml_pins_workflow_to_embedding_workflow_channel() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../workflows/auto-embedding-workers.yaml");
        let yaml = std::fs::read_to_string(&path).expect("auto-embedding-workers.yaml must exist");

        let block = yaml
            .split_once("- name: \"memories-auto-embedding%{MEMORY_EMBEDDING_SPACE_SUFFIX}\"")
            .map(|(_, rest)| rest)
            .expect("memories-auto-embedding worker must be defined");
        // Stop at the next top-level worker entry (or end of file).
        let block = block.split("\n  - ").next().unwrap_or(block);

        assert!(
            block.contains("channel: embedding_workflow"),
            "memories-auto-embedding must run on `embedding_workflow` \
             (single-slot) channel; see auto-embedding-workers.yaml header \
             for the rationale. Got worker block:\n{block}"
        );
    }

    /// The text workflow MUST use the MultimodalEmbeddingRunner's
    /// `embed_text` and the N-row BatchUpsertEmbeddings `rows` path
    /// (replace_kinds=["text"]). A regression to the old single-vector
    /// UpsertEmbedding / EmbeddingLlmRunner would silently break
    /// cross-modal search (text and image vectors must share one model
    /// space) and stop writing the N-row schema. Plain text scan to keep
    /// the infra crate free of a YAML parser dependency.
    #[test]
    fn auto_embedding_yaml_uses_mm_embedding_rows_path() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../workflows/auto-embedding.yaml");
        let yaml = std::fs::read_to_string(&path).expect("auto-embedding.yaml must exist");
        let placeholder = crate::infra::embedding_dispatch::MM_EMBEDDING_WORKER_PLACEHOLDER;
        assert!(
            yaml.contains(placeholder) && yaml.contains("using: embed_text"),
            "auto-embedding.yaml must call the {placeholder} env placeholder via embed_text"
        );
        assert!(
            yaml.contains("replace_kinds: [\"text\"]"),
            "auto-embedding.yaml must use the N-row rows path with \
             replace_kinds=[\"text\"] (kind isolation)"
        );
        assert!(
            !yaml.contains("memories-embedding-llm"),
            "auto-embedding.yaml must not reference the retired \
             EmbeddingLlmRunner worker"
        );
    }

    /// The image workflow embeds only the image and replaces only image
    /// rows; captions are generated by a separate workflow that writes
    /// back through the conditional WriteBackCaption RPC (never an
    /// unconditional content update).
    #[test]
    fn image_and_caption_workflows_are_separate() {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../workflows");
        let image = std::fs::read_to_string(root.join("auto-image-embedding.yaml")).unwrap();
        assert!(image.contains("name: memories-media-resolve"));
        assert!(image.contains("using: embed_image"));
        assert!(image.contains("replace_kinds: [\"image\"]"));
        assert!(!image.contains("memories-vlm-caption"));
        assert!(!image.contains("UpdateContentNoDispatch") && !image.contains("update-content"));
        assert!(image.contains("$workflow.input.token"));

        let caption = std::fs::read_to_string(root.join("auto-image-caption.yaml")).unwrap();
        assert!(caption.contains("name: memories-vlm-caption"));
        assert!(caption.contains("name: memories-write-back-caption"));
        assert!(caption.contains("media_object_id: { value: $workflow.input.media_object_id }"));
        assert!(
            !caption.contains("embed_text"),
            "memories dispatches the caption embedding"
        );
    }

    /// The image workflow worker shares the GPU-bound embedding
    /// bottleneck with the text workflow, so it must run on the same
    /// single-slot `embedding_workflow` channel (same rationale as the
    /// text worker — avoids wall-clock inflation / deadlock).
    #[test]
    fn image_workers_yaml_pins_workflow_to_embedding_workflow_channel() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../workflows/auto-image-embedding-workers.yaml");
        let yaml =
            std::fs::read_to_string(&path).expect("auto-image-embedding-workers.yaml must exist");
        let block = yaml
            .split_once("- name: \"memories-auto-image-embedding%{MEMORY_EMBEDDING_SPACE_SUFFIX}\"")
            .map(|(_, rest)| rest)
            .expect("memories-auto-image-embedding worker must be defined");
        let block = block.split("\n  - ").next().unwrap_or(block);
        assert!(
            block.contains("channel: embedding_workflow"),
            "memories-auto-image-embedding must run on the single-slot \
             `embedding_workflow` channel. Got worker block:\n{block}"
        );
    }
}
