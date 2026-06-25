//! Reflection intent embedding dispatcher.
//!
//! Routes to `memories-auto-reflection-intent-embedding` which
//! generates a vector for `task_intent` and upserts into the dedicated
//! `reflection_intent_vector` LanceDB table (separate namespace from
//! `memory_vector`). The workflow also calls
//! `MarkReflectionEmbeddingStatus(kind=INTENT)` on success/failure.
//!
//! Built on the same `EmbeddingDispatcherCore` as the memory and
//! summary dispatchers; the only differences are the worker name,
//! the JSON id field (`reflection_id`), and the YAML path.

use crate::infra::embedding_dispatch::{DispatchSpec, EmbeddingConfig, EmbeddingDispatcherCore};
use anyhow::Result;
use async_trait::async_trait;
use jobworkerp_client::jobworkerp::data::JobId;

pub use crate::infra::embedding_dispatch::{
    DispatchError, EmbeddingDispatch, EmbeddingDispatchStatus, EmbeddingJobId,
};

const SPEC: DispatchSpec = DispatchSpec {
    target_worker_name: "memories-auto-reflection-intent-embedding",
    // reflection_id is the dedicated key for the intent vector table;
    // distinct from memory_id used by the summary dispatcher to keep
    // the workflow input schemas explicit per spec §4.1.3.
    id_field_name: "reflection_id",
};

const WORKERS_YAML_ENV: &str = "REFLECTION_INTENT_WORKERS_YAML";
const DEFAULT_WORKERS_YAML_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../workflows/thread-reflection/auto-reflection-intent-embedding-workers.yaml"
);

pub fn intent_dispatch_config_from_env() -> Result<EmbeddingConfig> {
    let cfg = EmbeddingConfig::from_env(WORKERS_YAML_ENV, DEFAULT_WORKERS_YAML_PATH)?;
    // Phase C lands the dispatcher ahead of the workflow YAML
    // (Phase F). `EmbeddingDispatcherCore::ensure_initialized`
    // would otherwise fail deep inside `register_workers_from_yaml`
    // with a generic "no such file" error; surface it here so the
    // operator can see exactly which env var to point at the
    // workflow PR's YAML.
    if !cfg.workers_yaml_path.exists() {
        anyhow::bail!(
            "reflection intent embedding workers YAML not found at {} \
             (set {WORKERS_YAML_ENV} to override). The workflow ships in \
             a follow-up PR; until then the reflection intent dispatcher \
             cannot be initialised.",
            cfg.workers_yaml_path.display()
        );
    }
    Ok(cfg)
}

pub struct ReflectionIntentDispatcher {
    core: EmbeddingDispatcherCore,
}

impl ReflectionIntentDispatcher {
    pub fn from_env() -> Result<Self> {
        Ok(Self {
            core: EmbeddingDispatcherCore::new(intent_dispatch_config_from_env()?, SPEC),
        })
    }

    pub async fn ensure_initialized(&self) -> Result<()> {
        self.core.ensure_initialized().await
    }

    pub async fn dispatch(
        &self,
        reflection_id: i64,
        task_intent: &str,
    ) -> std::result::Result<Option<JobId>, DispatchError> {
        self.core.dispatch(reflection_id, task_intent).await
    }
}

#[async_trait]
impl EmbeddingDispatch for ReflectionIntentDispatcher {
    async fn dispatch(
        &self,
        reflection_id: i64,
        task_intent: &str,
    ) -> std::result::Result<Option<JobId>, DispatchError> {
        ReflectionIntentDispatcher::dispatch(self, reflection_id, task_intent).await
    }
}

#[cfg(test)]
mod tests {
    use crate::infra::embedding_dispatch::MM_EMBEDDING_WORKER_PLACEHOLDER;

    /// The intent embedding workflow MUST call the shared mm-embedding
    /// worker via `embed_text`, not the retired `memories-embedding-llm`.
    /// N-row migration: `embed_text` chunks are all stored via the
    /// BatchUpsertIntentEmbeddings rows path (text_rows / replace_kinds /
    /// rows) into the `(memory_id, vector_kind, chunk_index)`-keyed
    /// `reflection_intent_vector` table. The worker name is the
    /// `MEMORY_MM_EMBEDDING_WORKER` env placeholder so the F-S8 query path
    /// (search.rs, same env helper) and this storage workflow can never
    /// drift to different workers. Plain text scan to avoid a YAML parser
    /// dependency in infra.
    #[test]
    fn auto_reflection_intent_embedding_yaml_uses_mm_embedding() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../workflows/thread-reflection/auto-reflection-intent-embedding.yaml");
        let yaml = std::fs::read_to_string(&path)
            .expect("auto-reflection-intent-embedding.yaml must exist");
        assert!(
            yaml.contains(MM_EMBEDDING_WORKER_PLACEHOLDER) && yaml.contains("using: embed_text"),
            "auto-reflection-intent-embedding.yaml must call the \
             {MM_EMBEDDING_WORKER_PLACEHOLDER} env placeholder via embed_text"
        );
        assert!(
            !yaml.contains("memories-embedding-llm"),
            "auto-reflection-intent-embedding.yaml must not reference the \
             retired EmbeddingLlmRunner worker"
        );
        assert!(
            !yaml.contains("normalize_embeddings"),
            "auto-reflection-intent-embedding.yaml must drop the retired \
             normalize_embeddings argument"
        );
        // N-row: every chunk is stored via the rows path. The
        // empty-embedding status branch now keys on success_count==0.
        assert!(
            yaml.contains("text_rows")
                && yaml.contains("replace_kinds")
                && yaml.contains("rows: .text_rows"),
            "intent path must build N-row chunks (text_rows / replace_kinds / rows)"
        );
        assert!(
            !yaml.contains("embeddings[0]"),
            "intent path must store all chunks, not just embeddings[0]"
        );
    }

    /// The intent upsert worker must target the N-row
    /// BatchUpsertIntentEmbeddings RPC, not the removed single-vector
    /// UpsertIntentEmbedding RPC.
    #[test]
    fn auto_reflection_intent_embedding_workers_yaml_targets_rows_rpc() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../workflows/thread-reflection/auto-reflection-intent-embedding-workers.yaml");
        let yaml = std::fs::read_to_string(&path)
            .expect("auto-reflection-intent-embedding-workers.yaml must exist");
        assert!(
            yaml.contains("ReflectionVectorService/BatchUpsertIntentEmbeddings"),
            "intent upsert worker must target BatchUpsertIntentEmbeddings"
        );
        assert!(
            !yaml.contains("ReflectionVectorService/UpsertIntentEmbedding"),
            "intent upsert worker must not target the removed UpsertIntentEmbedding RPC"
        );
    }
}
