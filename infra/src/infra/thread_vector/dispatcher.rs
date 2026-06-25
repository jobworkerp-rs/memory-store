use crate::infra::embedding_dispatch::{
    DispatchError, DispatchSpec, EmbeddingConfig, EmbeddingDispatcherCore,
};
use crate::infra::memory_vector::dispatcher::AutoEmbeddingConfig;
use anyhow::Result;
use jobworkerp_client::jobworkerp::data::JobId;
use std::path::PathBuf;
use std::sync::Arc;

const SPEC: DispatchSpec = DispatchSpec {
    target_worker_name: "memories-auto-thread-embedding",
    id_field_name: "thread_id",
};

const WORKERS_YAML_ENV: &str = "MEMORY_THREAD_WORKERS_YAML";
const DEFAULT_WORKERS_YAML_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../workflows/auto-thread-embedding-workers.yaml"
);

/// Resolve the thread-side workers YAML path from env (or compile-time default).
fn workers_yaml_path() -> PathBuf {
    std::env::var(WORKERS_YAML_ENV)
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(DEFAULT_WORKERS_YAML_PATH))
}

/// Dispatches thread description embedding jobs to jobworkerp (fire-and-forget).
///
/// Reuses the shared embedding model/runner config but targets the
/// thread-side workflow worker.
pub struct ThreadEmbeddingJobDispatcher {
    core: EmbeddingDispatcherCore,
}

impl ThreadEmbeddingJobDispatcher {
    /// Build from an existing memory-side `AutoEmbeddingConfig`. The model,
    /// timeout, and content-length limits are inherited; the workers YAML
    /// path is rebound to the thread-side YAML, and the original memory
    /// YAML is recorded as a prerequisite so the shared
    /// `memories-mm-embedding` worker is registered from its single
    /// source of truth before the thread YAML's workflow references it.
    pub fn from_config(config: AutoEmbeddingConfig) -> Self {
        let memory_yaml = config.workers_yaml_path.clone();
        let thread_config = EmbeddingConfig {
            workers_yaml_path: workers_yaml_path(),
            prerequisite_yaml_paths: vec![memory_yaml],
            ..config
        };
        Self {
            core: EmbeddingDispatcherCore::new(thread_config, SPEC),
        }
    }

    pub fn from_env() -> Result<Self> {
        let memory_config =
            crate::infra::memory_vector::dispatcher::auto_embedding_config_from_env()?;
        Ok(Self::from_config(memory_config))
    }

    /// Eagerly run the lazy init so configuration errors surface at
    /// startup. See `EmbeddingDispatcherCore::ensure_initialized`.
    pub async fn ensure_initialized(&self) -> Result<()> {
        self.core.ensure_initialized().await
    }

    pub async fn dispatch(
        &self,
        thread_id: i64,
        content: &str,
    ) -> std::result::Result<Option<JobId>, DispatchError> {
        self.core.dispatch(thread_id, content).await
    }

    /// Spawn thread embedding dispatch (fire-and-forget).
    pub fn spawn_dispatch(dispatcher: &Arc<Self>, thread_id: i64, content: &str) {
        if content.is_empty() {
            return;
        }
        let d = dispatcher.clone();
        let content = content.to_string();
        tokio::spawn(async move {
            use futures::FutureExt as _;
            use std::panic::AssertUnwindSafe;
            match AssertUnwindSafe(async {
                d.dispatch(thread_id, &content).await.ok();
            })
            .catch_unwind()
            .await
            {
                Ok(()) => {}
                Err(e) => {
                    tracing::error!(
                        "thread embedding dispatch panicked for thread_id={thread_id}: {e:?}"
                    );
                }
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infra::embedding_dispatch::EmbeddingConfig;
    use serial_test::serial;

    fn memory_config() -> EmbeddingConfig {
        EmbeddingConfig {
            timeout_sec: 90,
            max_content_len: 4096,
            workers_yaml_path: PathBuf::from("/tmp/memory-workers.yaml"),
            prerequisite_yaml_paths: Vec::new(),
            extra_worker_names: Vec::new(),
        }
    }

    /// `from_config` must promote the memory YAML to a prerequisite so the
    /// shared `memories-mm-embedding` worker is registered from its
    /// single source of truth before the thread YAML's workflow runs.
    #[test]
    #[serial]
    fn from_config_records_memory_yaml_as_prerequisite() {
        // SAFETY: serialized via `#[serial]`.
        unsafe { std::env::remove_var(WORKERS_YAML_ENV) };
        let memory = memory_config();
        let memory_yaml = memory.workers_yaml_path.clone();
        let dispatcher = ThreadEmbeddingJobDispatcher::from_config(memory);
        let cfg = &dispatcher.core.config_for_test();
        assert_eq!(cfg.prerequisite_yaml_paths, vec![memory_yaml]);
        assert!(
            cfg.workers_yaml_path
                .ends_with("auto-thread-embedding-workers.yaml"),
            "expected default thread YAML, got {:?}",
            cfg.workers_yaml_path
        );
    }

    #[test]
    #[serial]
    fn from_config_inherits_shared_knobs() {
        // SAFETY: serialized via `#[serial]`.
        unsafe { std::env::remove_var(WORKERS_YAML_ENV) };
        let dispatcher = ThreadEmbeddingJobDispatcher::from_config(memory_config());
        let cfg = &dispatcher.core.config_for_test();
        assert_eq!(cfg.timeout_sec, 90);
        assert_eq!(cfg.max_content_len, 4096);
    }

    /// Mirror of the memory-side guard in `memory_vector::dispatcher`. The
    /// thread workflow worker shares the GPU-bound embedding bottleneck
    /// with the memory workflow, so it must run on the same single-slot
    /// `embedding_workflow` channel to avoid the same wall-clock inflation
    /// that originally caused 600s timeouts under bursty loads. Plain text
    /// scan to avoid pulling a YAML parser into the infra crate just for
    /// this assertion.
    #[test]
    fn workers_yaml_pins_workflow_to_embedding_workflow_channel() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../workflows/auto-thread-embedding-workers.yaml");
        let yaml =
            std::fs::read_to_string(&path).expect("auto-thread-embedding-workers.yaml must exist");

        let block = yaml
            .split_once("- name: memories-auto-thread-embedding")
            .map(|(_, rest)| rest)
            .expect("memories-auto-thread-embedding worker must be defined");
        let block = block.split("\n  - ").next().unwrap_or(block);

        assert!(
            block.contains("channel: embedding_workflow"),
            "memories-auto-thread-embedding must run on `embedding_workflow` \
             (single-slot) channel; see auto-thread-embedding-workers.yaml \
             header for the rationale. Got worker block:\n{block}"
        );
    }

    /// Thread description embedding MUST call the shared
    /// `memories-mm-embedding` (MultimodalEmbeddingRunner) worker via
    /// `embed_text`, not the retired `memories-embedding-llm`
    /// (EmbeddingLlmRunner). The old worker no longer exists — the
    /// memory-side YAML that registered it was switched to
    /// `memories-mm-embedding`, so a stale reference here would fail at
    /// workflow-execution time with worker-not-found. Plain text scan to
    /// keep the infra crate free of a YAML parser dependency.
    #[test]
    fn auto_thread_embedding_yaml_uses_mm_embedding() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../workflows/auto-thread-embedding.yaml");
        let yaml = std::fs::read_to_string(&path).expect("auto-thread-embedding.yaml must exist");
        let placeholder = crate::infra::embedding_dispatch::MM_EMBEDDING_WORKER_PLACEHOLDER;
        assert!(
            yaml.contains(placeholder) && yaml.contains("using: embed_text"),
            "auto-thread-embedding.yaml must call the {placeholder} env placeholder via embed_text"
        );
        assert!(
            !yaml.contains("memories-embedding-llm"),
            "auto-thread-embedding.yaml must not reference the retired \
             EmbeddingLlmRunner worker"
        );
        // The MultimodalEmbeddingRunner does not accept the old
        // `normalize_embeddings` argument; leaving it in would be a
        // silent schema-validation failure at dispatch time.
        assert!(
            !yaml.contains("normalize_embeddings"),
            "auto-thread-embedding.yaml must drop the retired \
             normalize_embeddings argument"
        );
    }

    /// N-row migration: thread embedding now stores every chunk via the
    /// BatchUpsertEmbeddingsRows path, not the retired chunk-0-only
    /// single-vector UpsertEmbedding. Assert the YAML builds `text_rows`
    /// (all chunks) + `replace_kinds` + `rows`, and no longer picks
    /// `embeddings[0]`.
    #[test]
    fn auto_thread_embedding_yaml_uses_nrow_rows_path() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../workflows/auto-thread-embedding.yaml");
        let yaml = std::fs::read_to_string(&path).expect("auto-thread-embedding.yaml must exist");
        assert!(
            yaml.contains("text_rows")
                && yaml.contains("replace_kinds")
                && yaml.contains("rows: .text_rows"),
            "auto-thread-embedding.yaml must build N-row chunks (text_rows / replace_kinds / rows)"
        );
        assert!(
            !yaml.contains("embeddings[0]"),
            "auto-thread-embedding.yaml must store all chunks, not just embeddings[0]"
        );
    }

    /// The thread upsert worker must target the N-row
    /// BatchUpsertEmbeddingsRows RPC, not the removed single-vector
    /// UpsertEmbedding RPC.
    #[test]
    fn auto_thread_embedding_workers_yaml_targets_rows_rpc() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../workflows/auto-thread-embedding-workers.yaml");
        let yaml =
            std::fs::read_to_string(&path).expect("auto-thread-embedding-workers.yaml must exist");
        assert!(
            yaml.contains("ThreadVectorService/BatchUpsertEmbeddingsRows"),
            "thread upsert worker must target BatchUpsertEmbeddingsRows"
        );
        assert!(
            !yaml.contains("ThreadVectorService/UpsertEmbedding"),
            "thread upsert worker must not target the removed UpsertEmbedding RPC"
        );
    }
}
