//! Shared building blocks for source-specific importers.
//!
//! - `labels`: truncate helpers for `Thread.label` (VARCHAR(512))
//! - `ids`: hash helpers for identifier columns (`memory.external_id`)
//!
//! `parser` / `converter` / `importer` for claude-code remain at the
//! crate root for now; the next refactor wave will move them under
//! `source/claude_code/`. Spec §4.1.
//!
//! `#[allow(dead_code)]` here covers helpers that are exercised by
//! unit tests in this PR but only consumed by codex/plain in
//! follow-up PRs. Removing them prematurely would require
//! reintroducing the same logic when those sources land.
#![allow(dead_code)]

pub mod canonical;
pub mod ids;
pub mod importer;
pub mod labels;
pub mod path;

pub mod workflow_input {
    use anyhow::{Result, anyhow};
    use serde_json::Map;

    /// Reject removed owner-routing fields before dispatching a workflow.
    pub(crate) fn reject_removed_fields(
        input: &Map<String, serde_json::Value>,
        removed_fields: &[&str],
    ) -> Result<()> {
        for field in removed_fields {
            if input.contains_key(*field) {
                return Err(anyhow!(
                    "{field} is no longer supported; use user_id and memory_kind"
                ));
            }
        }
        Ok(())
    }
}

/// Output-language whitelist shared by the import CLI, the post-import
/// dispatchers, and the lang-worker registrar. Single source of truth so
/// adding a language touches one place.
pub mod language {
    use anyhow::{Result, anyhow};

    pub(crate) const SUPPORTED_LANGUAGES: [&str; 2] = ["ja", "en"];

    const DEFAULT_LANGUAGE_ENV: &str = "MEMORY_DEFAULT_LANGUAGE";
    const FALLBACK_LANGUAGE: &str = "ja";

    pub(crate) fn resolve_output_language(req_language: Option<&str>) -> Result<String> {
        let candidate = req_language
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .or_else(|| {
                std::env::var(DEFAULT_LANGUAGE_ENV)
                    .ok()
                    .map(|v| v.trim().to_string())
                    .filter(|s| !s.is_empty())
            })
            .unwrap_or_else(|| FALLBACK_LANGUAGE.to_string());
        if !SUPPORTED_LANGUAGES.contains(&candidate.as_str()) {
            return Err(anyhow!(
                "unsupported output_language `{candidate}`; supported: {}",
                SUPPORTED_LANGUAGES.join(", ")
            ));
        }
        Ok(candidate)
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn resolve_output_language_accepts_explicit_supported_values() {
            assert_eq!(resolve_output_language(Some("ja")).unwrap(), "ja");
            assert_eq!(resolve_output_language(Some(" en ")).unwrap(), "en");
        }

        #[test]
        fn resolve_output_language_rejects_path_like_unsupported_value() {
            let err = resolve_output_language(Some("../en")).unwrap_err();
            assert!(err.to_string().contains("unsupported output_language"));
        }
    }
}
