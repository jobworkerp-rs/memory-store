//! Rendering checks for the bundled RAG tools manifest.
//!
//! The manifest is registered by the worker registry
//! (`infra::infra::embedding_space::registration`) together with the
//! embedding workers; these tests pin what the rendered manifest exposes
//! to the LLM.

use infra::infra::embedding_space::plan::{
    DEFAULT_RAG_MANIFEST_PATH as DEFAULT_MANIFEST_PATH, RAG_MANIFEST_ENV as MANIFEST_ENV,
    rag_manifest_path_from_env as manifest_path_from_env, registration_overrides,
};
use jobworkerp_client::client::yaml_common;
use std::path::{Path, PathBuf};

async fn read_manifest_with_inlined_includes(
    yaml_path: &Path,
) -> anyhow::Result<(String, PathBuf)> {
    infra::infra::embedding_space::registration::render_yaml(
        yaml_path,
        &registration_overrides(None),
    )
    .await
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

    #[tokio::test]
    #[serial]
    async fn inlined_workflows_receive_the_memories_query_prefix_literal() {
        // SAFETY: #[serial] guards against concurrent env access.
        unsafe {
            std::env::set_var("MEMORY_GRPC_HOST", "test-host");
            std::env::set_var("MEMORY_GRPC_PORT", "12345");
            std::env::set_var(
                "MEMORY_EMBEDDING_QUERY_PREFIX",
                "literal %{RAG_PREFIX_MUST_NOT_EXPAND}",
            );
        }
        let path = std::path::Path::new(DEFAULT_MANIFEST_PATH);
        let (raw, _base) = read_manifest_with_inlined_includes(path).await.unwrap();

        let final_manifest = yaml_common::expand_env(&raw);
        // SAFETY: cleanup; #[serial] keeps the env access exclusive.
        unsafe {
            std::env::remove_var("MEMORY_GRPC_HOST");
            std::env::remove_var("MEMORY_GRPC_PORT");
            std::env::remove_var("MEMORY_EMBEDDING_QUERY_PREFIX");
        }

        assert!(
            raw.contains("RAG_PREFIX_MUST_NOT_EXPAND"),
            "the registration process must bake the query-prefix marker into RAG workflows: {raw}"
        );
        assert!(
            final_manifest.is_ok(),
            "the registration process must preserve a literal %{{...}} query prefix through final manifest expansion: {final_manifest:?}"
        );
        assert!(
            !raw.contains("%{MEMORY_EMBEDDING_QUERY_PREFIX_JAQ")
                && std::env::var_os("MEMORY_EMBEDDING_QUERY_PREFIX_JAQ").is_none(),
            "the registration process must expand the workflow placeholder without mutating the process environment"
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
