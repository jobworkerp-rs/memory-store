use crate::infra::embedding_dispatch::{
    DispatchSpec, EmbeddingConfig, EmbeddingDispatcherCore, ImageSearchMode,
};
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
pub fn auto_embedding_config_from_env() -> Result<AutoEmbeddingConfig> {
    EmbeddingConfig::from_env(WORKERS_YAML_ENV, DEFAULT_WORKERS_YAML_PATH)
}

pub use crate::infra::embedding_dispatch::{IMAGE_WORKFLOW_WORKER, TEXT_WORKFLOW_WORKER};

/// Dispatches embedding generation jobs to jobworkerp (fire-and-forget).
pub struct EmbeddingJobDispatcher {
    core: EmbeddingDispatcherCore,
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

    pub fn from_env() -> Result<Self> {
        let mut config = auto_embedding_config_from_env()?;
        // Resolve the image workflow worker too so `Media` jobs can be
        // routed to it. Best-effort at init: absent in text-only
        // deployments (see EmbeddingConfig.extra_worker_names).
        config.extra_worker_names = vec![IMAGE_WORKFLOW_WORKER];
        // In any image mode, register the image-pipeline workers
        // (resolve / image+caption workflow / VLM / update-content) as a
        // prerequisite YAML so the image workflow worker exists before
        // the text YAML's dispatcher references it. text-only (`none`)
        // skips this so a deployment with no image workers still starts.
        if ImageSearchMode::from_env().is_image_enabled() {
            let image_yaml = std::env::var(IMAGE_WORKERS_YAML_ENV)
                .map(std::path::PathBuf::from)
                .unwrap_or_else(|_| std::path::PathBuf::from(DEFAULT_IMAGE_WORKERS_YAML_PATH));
            config.prerequisite_yaml_paths.push(image_yaml);
        }
        Ok(Self {
            core: EmbeddingDispatcherCore::new(config, SPEC),
        })
    }

    /// Eagerly run the lazy init so configuration errors (missing YAML,
    /// unparseable settings, jobworkerp unreachable, ...) surface here
    /// instead of inside the first fire-and-forget dispatch call.
    pub async fn ensure_initialized(&self) -> Result<()> {
        self.core.ensure_initialized().await
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
                let args = build_image_job_args(target.memory_id, mid, target.image_search_mode);
                self.core
                    .dispatch_to_worker(IMAGE_WORKFLOW_WORKER, &args)
                    .await
            }
        }
    }
}

/// Build the image workflow `input` JSON. The WORKFLOW runner takes a
/// single string-typed parameter, so `input` is itself a JSON string.
/// `image_search_mode` lets the workflow branch image vs caption; the
/// `embedding_model` is read from the runner output inside the workflow.
fn build_image_job_args(
    memory_id: i64,
    media_object_id: i64,
    mode: ImageSearchMode,
) -> serde_json::Value {
    let inner = serde_json::json!({
        "memory_id": memory_id,
        "media_object_id": media_object_id,
        "image_search_mode": mode.as_str(),
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
        // rejected; REFLECTION rejected so reflection memories don't
        // double-dispatch through this generic path.
        for role in [
            MessageRole::RoleUnspecified,
            MessageRole::RoleTool,
            MessageRole::RoleMeta,
            MessageRole::RoleReflection,
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
    fn build_image_job_args_shape() {
        let v = build_image_job_args(11, 22, ImageSearchMode::VlmCaption);
        let inner: serde_json::Value = serde_json::from_str(v["input"].as_str().unwrap()).unwrap();
        assert_eq!(inner["memory_id"], 11);
        assert_eq!(inner["media_object_id"], 22);
        assert_eq!(inner["image_search_mode"], "vlm_caption");
        // embedding_model is read from runner output in the workflow.
        assert!(inner.get("embedding_model").is_none());
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
            config
                .workers_yaml_path
                .ends_with("auto-embedding-workers.yaml"),
            "expected default workers YAML path, got {:?}",
            config.workers_yaml_path
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
            .split_once("- name: memories-auto-embedding")
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

    /// The image workflow MUST resolve a URL, embed the image, and route
    /// the caption write-back through UpdateContentNoDispatch — using the
    /// public Update RPC there would re-trigger dispatch_kinds and loop
    /// the caption workflow forever. It must also keep image/caption rows
    /// isolated from the text dispatch's rows.
    #[test]
    fn auto_image_embedding_yaml_shape_and_loop_break() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../workflows/auto-image-embedding.yaml");
        let yaml = std::fs::read_to_string(&path).expect("auto-image-embedding.yaml must exist");
        assert!(
            yaml.contains("name: memories-media-resolve"),
            "must resolve a media URL first"
        );
        assert!(
            yaml.contains("using: embed_image"),
            "must call embed_image for the image vector"
        );
        assert!(
            yaml.contains("name: memories-update-content-no-dispatch"),
            "caption write-back MUST go through UpdateContentNoDispatch \
             (loop break); the public Update RPC would re-dispatch"
        );
        assert!(
            yaml.contains("replace_kinds: [\"image\", \"caption\"]"),
            "image workflow must replace only image/caption rows, never \
             the text dispatch's text rows (kind isolation)"
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
            .split_once("- name: memories-auto-image-embedding")
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
