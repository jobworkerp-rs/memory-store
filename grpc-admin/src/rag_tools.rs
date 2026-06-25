//! Startup-time registration of memories RAG tools into jobworkerp.
//!
//! When `MEMORY_RAG_TOOLS_ENABLED=true`, the manifest at
//! `MEMORY_RAG_MANIFEST_YAML` (default: `workflows/rag-tools-manifest.yaml`)
//! is upserted to the jobworkerp server pointed to by `JOBWORKERP_ADDR`.
//! The manifest defines three workers (`recall_memories`,
//! `find_conversations`, `expand_memory_context`) and the `memory-recall`
//! function set that bundles them.
//!
//! Failure policy: log a warning and continue. RAG tool exposure is an
//! adjunct feature; the memories gRPC server should still start so that
//! user-facing reads/writes work even if jobworkerp is unreachable.

use jobworkerp_client::client::manifest_yaml;
use jobworkerp_client::client::wrapper::JobworkerpClientWrapper;
use jobworkerp_client::client::yaml_common;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

const MANIFEST_ENV: &str = "MEMORY_RAG_MANIFEST_YAML";
const DEFAULT_MANIFEST_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../workflows/rag-tools-manifest.yaml"
);

fn manifest_path_from_env() -> PathBuf {
    std::env::var(MANIFEST_ENV)
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(DEFAULT_MANIFEST_PATH))
}

/// jobworkerp-client expands `%{VAR}` only on the manifest YAML itself,
/// not on the bodies pulled in via `$file:` (documented as "No nested
/// expansion" in worker-yaml.md). Our workflow YAMLs reference
/// `%{MEMORY_GRPC_HOST}` / `%{MEMORY_GRPC_PORT}` because the workflows
/// run on the jobworkerp host and need to call back into the memories
/// gRPC endpoint by routable address — those values must resolve at
/// manifest-registration time so they end up baked into the
/// `runner_settings.workflow_data` payload that jobworkerp persists.
///
/// To bridge the gap, we resolve `$file:` includes ourselves and run
/// `expand_env` over the included content before handing the resulting
/// raw YAML to `register_manifest_from_yaml_str` (which still does its
/// own outer expansion at the manifest level — idempotent, since the
/// manifest body no longer contains unresolved placeholders).
async fn read_manifest_with_inlined_includes(
    yaml_path: &Path,
) -> anyhow::Result<(String, PathBuf)> {
    let raw = tokio::fs::read_to_string(yaml_path).await.map_err(|e| {
        anyhow::anyhow!(
            "failed to read manifest YAML at {}: {e}",
            yaml_path.display()
        )
    })?;
    let base_dir = yaml_path
        .parent()
        .map_or_else(|| PathBuf::from("."), Path::to_path_buf);

    // Outer expansion must run *before* parse: `%{VAR:-default}` placeholders
    // sit at YAML positions where the raw `%` would otherwise be rejected by
    // the YAML scanner (it reserves `%` for directives at the start of a
    // line / scalar).
    let expanded_outer = yaml_common::expand_env(&raw)
        .map_err(|e| anyhow::anyhow!("env expansion failed on manifest YAML: {e}"))?;

    let mut doc: serde_yaml::Value = serde_yaml::from_str(&expanded_outer)
        .map_err(|e| anyhow::anyhow!("manifest YAML parse error: {e}"))?;

    yaml_common::resolve_includes(&mut doc, &base_dir)
        .await
        .map_err(|e| anyhow::anyhow!("failed to resolve $file: includes: {e}"))?;

    expand_env_in_string_scalars(&mut doc)?;

    let serialized = serde_yaml::to_string(&doc)
        .map_err(|e| anyhow::anyhow!("failed to re-serialize manifest YAML: {e}"))?;
    Ok((serialized, base_dir))
}

/// Walk every string scalar in `value` and apply `%{VAR}` expansion.
/// Targets workflow_data bodies that `resolve_includes` just inlined as
/// opaque string scalars — those are the only place a literal
/// `%{MEMORY_GRPC_HOST}` would survive without this pass.
fn expand_env_in_string_scalars(value: &mut serde_yaml::Value) -> anyhow::Result<()> {
    match value {
        serde_yaml::Value::String(s) => {
            let expanded = yaml_common::expand_env(s)
                .map_err(|e| anyhow::anyhow!("env expansion failed in inlined content: {e}"))?;
            *s = expanded;
        }
        serde_yaml::Value::Mapping(map) => {
            for (_, v) in map.iter_mut() {
                expand_env_in_string_scalars(v)?;
            }
        }
        serde_yaml::Value::Sequence(seq) => {
            for v in seq.iter_mut() {
                expand_env_in_string_scalars(v)?;
            }
        }
        _ => {}
    }
    Ok(())
}

async fn register(yaml_path: &Path) -> anyhow::Result<manifest_yaml::ManifestResult> {
    let (raw_yaml, base_dir) = read_manifest_with_inlined_includes(yaml_path).await?;
    // 10s: this is a fire-and-forget background task; stalling longer
    // just delays surfacing a jobworkerp connectivity issue in the log.
    let client = JobworkerpClientWrapper::new_by_env(Some(10)).await?;
    let metadata: Arc<HashMap<String, String>> = Arc::new(HashMap::new());
    manifest_yaml::register_manifest_from_yaml_str(&client, None, metadata, &raw_yaml, &base_dir)
        .await
}

/// Never returns an error: the front server must stay up even when
/// jobworkerp is unreachable, so failures are downgraded to WARN logs.
pub(crate) async fn register_on_startup() {
    let path = manifest_path_from_env();
    match register(&path).await {
        Ok(result) => {
            let workers: Vec<&String> = result.workers.keys().collect();
            let function_sets: Vec<&String> = result.function_sets.keys().collect();
            tracing::info!(
                manifest = %path.display(),
                "RAG tools registered: workers={workers:?}, function_sets={function_sets:?}"
            );
        }
        Err(e) => {
            tracing::warn!(
                manifest = %path.display(),
                "RAG tools registration failed (server will start without them): {e:?}"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    #[test]
    #[serial]
    fn manifest_path_uses_default_when_env_unset() {
        // SAFETY: #[serial] guards against concurrent env access.
        unsafe { std::env::remove_var(MANIFEST_ENV) };
        let path = manifest_path_from_env();
        assert!(
            path.ends_with("workflows/rag-tools-manifest.yaml"),
            "expected default manifest path, got {path:?}"
        );
    }

    #[test]
    #[serial]
    fn manifest_path_honors_env_override() {
        // SAFETY: #[serial] guards against concurrent env access.
        unsafe { std::env::set_var(MANIFEST_ENV, "/tmp/custom-rag.yaml") };
        let path = manifest_path_from_env();
        assert_eq!(path, PathBuf::from("/tmp/custom-rag.yaml"));
        // SAFETY: #[serial] guards against concurrent env access.
        unsafe { std::env::remove_var(MANIFEST_ENV) };
    }

    /// The bundled manifest must exist in-tree so the default path
    /// resolves at runtime. Catches accidental rename / move during
    /// refactors without needing a live jobworkerp connection.
    #[test]
    fn bundled_manifest_file_exists() {
        let path = std::path::Path::new(DEFAULT_MANIFEST_PATH);
        assert!(
            path.is_file(),
            "bundled RAG manifest missing at {DEFAULT_MANIFEST_PATH}"
        );
    }

    /// Validates the full nested-env-expansion pipeline against the
    /// real bundled manifest: every `$file:` include must be replaced
    /// with the env-expanded body of the referenced workflow YAML.
    #[tokio::test]
    #[serial]
    async fn inlines_workflow_files_with_env_expansion() {
        // SAFETY: #[serial] guards against concurrent env access.
        unsafe {
            std::env::set_var("MEMORY_GRPC_HOST", "test-host");
            std::env::set_var("MEMORY_GRPC_PORT", "12345");
        }
        let path = std::path::Path::new(DEFAULT_MANIFEST_PATH);
        let (raw, _base) = read_manifest_with_inlined_includes(path).await.unwrap();

        // SAFETY: cleanup; #[serial] keeps the env access exclusive.
        unsafe {
            std::env::remove_var("MEMORY_GRPC_HOST");
            std::env::remove_var("MEMORY_GRPC_PORT");
        }

        assert!(
            !raw.contains("$file"),
            "all $file: includes must be inlined, but raw manifest still contains $file"
        );
        assert!(
            raw.contains("test-host"),
            "MEMORY_GRPC_HOST must be substituted into the inlined workflow body"
        );
        assert!(
            raw.contains("12345"),
            "MEMORY_GRPC_PORT must be substituted into the inlined workflow body"
        );
    }

    /// Pin the contract that LLM-facing input schemas expose int64 IDs as
    /// JSON strings. JSON-Schema-driven function-calling clients commonly
    /// coerce `type: integer` into JS `number`, which silently rounds
    /// snowflake-sized values past 2^53-1 and routes the call to the wrong
    /// memory. If a future edit reverts `memory_id` / `thread_id` /
    /// `user_id` to `type: integer`, this test fails and points at the
    /// regression before it ships to the LLM tool catalog.
    #[tokio::test]
    #[serial]
    async fn rag_input_schemas_use_string_for_int64_ids() {
        // SAFETY: #[serial] guards against concurrent env access.
        unsafe {
            std::env::set_var("MEMORY_GRPC_HOST", "test-host");
            std::env::set_var("MEMORY_GRPC_PORT", "12345");
        }
        let path = std::path::Path::new(DEFAULT_MANIFEST_PATH);
        let (raw, _base) = read_manifest_with_inlined_includes(path).await.unwrap();
        // SAFETY: cleanup; #[serial] keeps the env access exclusive.
        unsafe {
            std::env::remove_var("MEMORY_GRPC_HOST");
            std::env::remove_var("MEMORY_GRPC_PORT");
        }

        // The string-typing claim only holds if these property names still
        // appear in the manifest at all; guard against a workflow rename
        // silently masking a regression.
        for prop in ["memory_id:", "thread_id:", "user_id:"] {
            assert!(
                raw.contains(prop),
                "expected RAG manifest to still declare `{prop}` somewhere — \
                 schema may have been refactored, update this test"
            );
        }
        assert!(
            !raw.contains("type: integer\n          description: \"Anchor memory id"),
            "expand_memory_context.memory_id must be `type: string`, not integer"
        );
        assert!(
            !raw.contains("type: integer\n          description: \"Tenant user id"),
            "user_id (search-memories / search-threads) must be `type: string`, not integer"
        );
        // Positive check: every `type: integer` must NOT immediately precede
        // a description that names an int64 id. We approximate by asserting
        // the new string-typed phrasing is present.
        assert!(
            raw.contains("Anchor memory id (int64 as decimal string)"),
            "memory_id description must declare the decimal-string contract"
        );
        assert!(
            raw.contains("Tenant user id (int64 as decimal string)"),
            "user_id description must declare the decimal-string contract"
        );
    }

    /// Pin the implementation default of `LabelMatchMode` (LABEL_ANY = 0
    /// in proto3 zero-value semantics, see common.proto). The previous
    /// description claimed LABEL_ALL was the default, which would teach
    /// the LLM to expect AND-semantics on multi-label filters and silently
    /// over-broaden recall.
    #[tokio::test]
    #[serial]
    async fn rag_label_match_mode_description_matches_implementation() {
        // SAFETY: #[serial] guards against concurrent env access.
        unsafe {
            std::env::set_var("MEMORY_GRPC_HOST", "test-host");
            std::env::set_var("MEMORY_GRPC_PORT", "12345");
        }
        let path = std::path::Path::new(DEFAULT_MANIFEST_PATH);
        let (raw, _base) = read_manifest_with_inlined_includes(path).await.unwrap();
        // SAFETY: cleanup; #[serial] keeps the env access exclusive.
        unsafe {
            std::env::remove_var("MEMORY_GRPC_HOST");
            std::env::remove_var("MEMORY_GRPC_PORT");
        }

        assert!(
            raw.contains("LABEL_ANY (default)"),
            "label_match_mode description must declare LABEL_ANY as the default"
        );
        assert!(
            !raw.contains("LABEL_ALL (default)"),
            "stale `LABEL_ALL (default)` text must be gone — it contradicts \
             the proto3 zero-value default and misleads the LLM"
        );
    }
}
