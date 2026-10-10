//! Which workers a memories process registers, derived from its
//! configuration.

use super::SpaceId;
use super::registration::RegistrationPlan;
use super::workers::{SPACE_SCOPED_WORKFLOW_BASES, template_overrides};
use crate::infra::embedding_dispatch::{
    EMBEDDING_QUERY_PREFIX_JAQ_ENV, ImageSearchMode, embedding_query_prefix,
    mm_embedding_worker_base, query_prefix_jaq_literal,
};
use std::collections::HashMap;
use std::path::PathBuf;

pub const RAG_MANIFEST_ENV: &str = "MEMORY_RAG_MANIFEST_YAML";
pub const DEFAULT_RAG_MANIFEST_PATH: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../workflows/rag-tools-manifest.yaml"
);

pub fn rag_manifest_path_from_env() -> PathBuf {
    std::env::var(RAG_MANIFEST_ENV)
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from(DEFAULT_RAG_MANIFEST_PATH))
}

fn env_flag(name: &str) -> bool {
    std::env::var(name)
        .unwrap_or_default()
        .eq_ignore_ascii_case("true")
}

/// Features that decide which workers are required.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct EnabledWorkers {
    pub auto_embedding: bool,
    pub image_search_mode: ImageSearchMode,
    pub reflection_dispatch: bool,
    pub rag_tools: bool,
}

impl EnabledWorkers {
    pub fn from_env() -> Self {
        Self {
            auto_embedding: env_flag("MEMORY_AUTO_EMBEDDING_ENABLED"),
            image_search_mode: ImageSearchMode::from_env(),
            reflection_dispatch: env_flag("MEMORY_REFLECTION_DISPATCH_ENABLED"),
            rag_tools: env_flag("MEMORY_RAG_TOOLS_ENABLED"),
        }
    }

    pub fn any(&self) -> bool {
        self.auto_embedding || self.reflection_dispatch || self.rag_tools
    }

    /// The registration plan for these features. The memory workers YAML
    /// comes first because it defines the mm-embedding worker every
    /// other pipeline calls.
    pub fn plan(&self) -> RegistrationPlan {
        let mut plan = RegistrationPlan {
            cleanup_bases: std::iter::once(mm_embedding_worker_base())
                .chain(SPACE_SCOPED_WORKFLOW_BASES.iter().map(|b| b.to_string()))
                .collect(),
            ..Default::default()
        };
        if self.auto_embedding || self.reflection_dispatch || self.rag_tools {
            plan.add_worker_yaml(
                crate::infra::memory_vector::dispatcher::workers_yaml_path_from_env(),
            );
        }
        if self.auto_embedding {
            for path in
                crate::infra::memory_vector::dispatcher::workers_yaml_paths(self.image_search_mode)
            {
                plan.add_worker_yaml(path);
            }
            plan.add_worker_yaml(crate::infra::thread_vector::dispatcher::workers_yaml_path());
        }
        if self.reflection_dispatch {
            plan.add_worker_yaml(crate::infra::reflection_summary_dispatch::workers_yaml_path());
            plan.add_worker_yaml(crate::infra::reflection_intent_dispatch::workers_yaml_path());
        }
        if self.rag_tools {
            plan.manifests.push(rag_manifest_path_from_env());
        }
        plan
    }
}

/// Placeholder values memories supplies when rendering any worker YAML
/// or manifest: the space suffix and the query-prefix literal.
pub fn registration_overrides(space: Option<&SpaceId>) -> HashMap<String, String> {
    let mut overrides = template_overrides(space);
    overrides.insert(
        EMBEDDING_QUERY_PREFIX_JAQ_ENV.to_string(),
        query_prefix_jaq_literal(embedding_query_prefix().as_deref()),
    );
    overrides
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    #[test]
    #[serial]
    fn plan_registers_only_enabled_pipelines_with_memory_yaml_first() {
        // SAFETY: serialized via `#[serial]`.
        unsafe {
            std::env::remove_var("MEMORY_WORKERS_YAML");
            std::env::remove_var("MEMORY_IMAGE_WORKERS_YAML");
            std::env::remove_var("MEMORY_THREAD_WORKERS_YAML");
        }
        let text_only = EnabledWorkers {
            auto_embedding: true,
            ..Default::default()
        }
        .plan();
        let names: Vec<String> = text_only
            .worker_yamls
            .iter()
            .map(|p| p.file_name().unwrap().to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            names,
            vec![
                "auto-embedding-workers.yaml",
                "auto-thread-embedding-workers.yaml"
            ]
        );
        assert!(text_only.manifests.is_empty());

        let all = EnabledWorkers {
            auto_embedding: true,
            image_search_mode: ImageSearchMode::Multimodal,
            reflection_dispatch: true,
            rag_tools: true,
        }
        .plan();
        assert_eq!(all.worker_yamls.len(), 5);
        assert!(all.worker_yamls[0].ends_with("auto-embedding-workers.yaml"));
        assert_eq!(all.manifests.len(), 1);

        assert!(EnabledWorkers::default().plan().worker_yamls.is_empty());
    }

    /// Every shipped worker YAML and the RAG manifest must render with no
    /// placeholder left, and every space-scoped worker and mm-embedding
    /// reference must carry the space suffix.
    #[tokio::test]
    #[serial]
    async fn shipped_yaml_renders_without_leftover_placeholders() {
        // SAFETY: serialized via `#[serial]`.
        unsafe {
            for k in [
                "MEMORY_WORKERS_YAML",
                "MEMORY_IMAGE_WORKERS_YAML",
                "MEMORY_THREAD_WORKERS_YAML",
                "REFLECTION_WORKERS_YAML",
                "REFLECTION_INTENT_WORKERS_YAML",
                "MEMORY_RAG_MANIFEST_YAML",
                "MEMORY_MM_EMBEDDING_WORKER",
            ] {
                std::env::remove_var(k);
            }
            std::env::set_var("MEMORY_GRPC_HOST", "test-host");
            std::env::set_var("MEMORY_GRPC_PORT", "12345");
        }
        let space = SpaceId("cd".repeat(32));
        let suffix = format!("-{}", space.short());
        let overrides = registration_overrides(Some(&space));
        let plan = EnabledWorkers {
            auto_embedding: true,
            image_search_mode: ImageSearchMode::Multimodal,
            reflection_dispatch: true,
            rag_tools: true,
        }
        .plan();
        let mut all = String::new();
        for path in plan.worker_yamls.iter().chain(plan.manifests.iter()) {
            let (raw, _) = super::super::registration::render_yaml(path, &overrides)
                .await
                .unwrap();
            assert!(!raw.contains("%{"), "{} left a placeholder", path.display());
            all.push_str(&raw);
        }
        for base in SPACE_SCOPED_WORKFLOW_BASES {
            assert!(
                all.contains(&format!("{base}{suffix}")),
                "{base} lacks the suffix"
            );
        }
        let mm = format!("memories-mm-embedding{suffix}");
        assert!(all.matches(&mm).count() >= 2, "definition and references");
        assert!(
            !all.contains("memories-mm-embedding\n") && !all.contains("memories-mm-embedding\""),
            "an unsuffixed mm-embedding reference remains"
        );
        // SAFETY: serialized via `#[serial]`.
        unsafe {
            std::env::remove_var("MEMORY_GRPC_HOST");
            std::env::remove_var("MEMORY_GRPC_PORT");
        }
    }

    #[test]
    fn cleanup_covers_every_space_scoped_base() {
        let plan = EnabledWorkers::default().plan();
        for base in SPACE_SCOPED_WORKFLOW_BASES {
            assert!(plan.cleanup_bases.iter().any(|b| b == base));
        }
    }
}
