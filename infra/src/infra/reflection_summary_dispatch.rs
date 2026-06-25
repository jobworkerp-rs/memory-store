//! Reflection summary embedding dispatcher.
//!
//! Reuses the existing `EmbeddingDispatcherCore` plumbing but routes
//! to a dedicated `memories-auto-reflection-summary-embedding` workflow
//! that *upserts into the existing `memory_vector` table* (no new
//! vector store) and reports status back via
//! `MarkReflectionEmbeddingStatus`.
//!
//! This dispatcher exists as a sibling of `EmbeddingJobDispatcher`
//! because reflection memories must NOT flow through the generic
//! memory auto-embedding path: `is_embeddable` rejects ROLE_REFLECTION
//! to prevent double dispatch.

use crate::infra::embedding_dispatch::{DispatchSpec, EmbeddingConfig, EmbeddingDispatcherCore};
use anyhow::Result;
use async_trait::async_trait;
use jobworkerp_client::jobworkerp::data::JobId;

pub use crate::infra::embedding_dispatch::{
    DispatchError, EmbeddingDispatch, EmbeddingDispatchStatus, EmbeddingJobId,
};

const SPEC: DispatchSpec = DispatchSpec {
    target_worker_name: "memories-auto-reflection-summary-embedding",
    // memory_id matches the existing UpsertEmbedding RPC parameter so
    // the workflow can pass it through transparently to MemoryVectorService.
    id_field_name: "memory_id",
};

const WORKERS_YAML_ENV: &str = "REFLECTION_WORKERS_YAML";
const DEFAULT_WORKERS_YAML_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../workflows/thread-reflection/auto-reflection-summary-embedding-workers.yaml"
);

pub fn summary_dispatch_config_from_env() -> Result<EmbeddingConfig> {
    let cfg = EmbeddingConfig::from_env(WORKERS_YAML_ENV, DEFAULT_WORKERS_YAML_PATH)?;
    // The matching workflow YAML lands in Phase F; until then the
    // dispatcher cannot register its worker. Fail loudly here
    // instead of letting `ensure_initialized` blow up inside
    // `register_workers_from_yaml`.
    if !cfg.workers_yaml_path.exists() {
        anyhow::bail!(
            "reflection summary embedding workers YAML not found at {} \
             (set {WORKERS_YAML_ENV} to override). The workflow ships in \
             a follow-up PR; until then the reflection summary dispatcher \
             cannot be initialised.",
            cfg.workers_yaml_path.display()
        );
    }
    Ok(cfg)
}

/// Public dispatcher type. Single-method surface: app-layer
/// `ReflectionApp::finalize_generated_reflection` calls
/// `dispatch(memory_id, summary)` after Phase 3 commits.
pub struct ReflectionSummaryDispatcher {
    core: EmbeddingDispatcherCore,
}

impl ReflectionSummaryDispatcher {
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            core: EmbeddingDispatcherCore::new(summary_dispatch_config_from_env()?, SPEC),
        })
    }

    pub async fn ensure_initialized(&self) -> Result<()> {
        self.core.ensure_initialized().await
    }

    pub async fn dispatch(
        &self,
        reflection_memory_id: i64,
        summary: &str,
    ) -> std::result::Result<Option<JobId>, DispatchError> {
        self.core.dispatch(reflection_memory_id, summary).await
    }
}

#[async_trait]
impl EmbeddingDispatch for ReflectionSummaryDispatcher {
    async fn dispatch(
        &self,
        reflection_memory_id: i64,
        summary: &str,
    ) -> std::result::Result<Option<JobId>, DispatchError> {
        ReflectionSummaryDispatcher::dispatch(self, reflection_memory_id, summary).await
    }
}

#[cfg(test)]
mod tests {
    /// Reflection summaries can be long post-mortem prose, and they land
    /// in the shared `memory_vector` table which already supports the
    /// N-row `BatchUpsertEmbeddings` schema. So the summary workflow MUST
    /// store EVERY chunk (not just chunk 0) via the rows path with
    /// `replace_kinds: ["text"]` and `chunk_index`, exactly like
    /// `auto-embedding.yaml`. A regression to the old single-vector
    /// `UpsertEmbedding{skipped}` contract would drop chunks past the
    /// first and silently shrink summary recall. Plain text scan to keep
    /// the infra crate free of a YAML parser dependency.
    #[test]
    fn auto_reflection_summary_embedding_yaml_uses_mm_embedding_rows_path() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../workflows/thread-reflection/auto-reflection-summary-embedding.yaml");
        let yaml = std::fs::read_to_string(&path)
            .expect("auto-reflection-summary-embedding.yaml must exist");
        let placeholder = crate::infra::embedding_dispatch::MM_EMBEDDING_WORKER_PLACEHOLDER;
        assert!(
            yaml.contains(placeholder) && yaml.contains("using: embed_text"),
            "summary workflow must call the {placeholder} env placeholder via embed_text"
        );
        assert!(
            !yaml.contains("memories-embedding-llm"),
            "summary workflow must not reference the retired EmbeddingLlmRunner worker"
        );
        assert!(
            !yaml.contains("normalize_embeddings"),
            "summary workflow must drop the retired normalize_embeddings argument"
        );
        // N-row rows path: all chunks, kind-isolated to "text".
        assert!(
            yaml.contains("replace_kinds: [\"text\"]") && yaml.contains("chunk_index"),
            "summary workflow must use the N-row rows path with \
             replace_kinds=[\"text\"] and per-chunk chunk_index"
        );
        // The batch response has no `skipped` field; the status branch
        // must instead read `success_count` off the upsert response so
        // the sidecar reflects what the server actually wrote (not an
        // inference from the input).
        assert!(
            !yaml.contains(".skipped"),
            "summary workflow must not read `.skipped` (BatchUpsertEmbeddings \
             response has no such field)"
        );
        // The jq lookup MUST use the protobuf-JSON lowerCamelCase field
        // name (`successCount`); `success_count` would always read null
        // and mark every successful batch FAILED.
        assert!(
            yaml.contains(".successCount"),
            "summary workflow must read the `successCount` (lowerCamelCase) \
             response field from the GRPC jsonBody, not snake_case"
        );
        // Empty content is gated before the embed/upsert round trip so we
        // never spend an embedding call on an empty summary. The guard is
        // the `markEmptyAsFailed` task that records
        // FAILED("empty_embedding") and short-circuits via `then: exit`
        // (mirrors the intent path). Assert on the recorded failure reason
        // rather than the task name alone so a rename of the task does not
        // silently drop the guard from coverage.
        assert!(
            yaml.contains("markEmptyAsFailed") && yaml.contains("empty_embedding"),
            "summary workflow must guard empty content up front, recording \
             FAILED(\"empty_embedding\") before the embed/upsert round trip"
        );
        // Status bookkeeping must survive on every terminal branch.
        assert!(
            yaml.contains("EMBEDDING_KIND_SUMMARY")
                && yaml.contains("EMBEDDING_STATUS_OK")
                && yaml.contains("EMBEDDING_STATUS_FAILED"),
            "summary workflow must keep MarkReflectionEmbeddingStatus(SUMMARY) \
             on OK and FAILED branches"
        );
    }
}
