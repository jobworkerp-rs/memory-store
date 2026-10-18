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
pub mod git;
pub mod ids;
pub mod importer;
pub mod labels;
pub mod path;

pub mod workflow_input {
    use anyhow::{Result, anyhow};
    use serde_json::{Map, Value, json};

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

    /// Absolute lower bound on a thread's last message time accepted by the
    /// thread-level batch workflows.
    const LAST_MESSAGE_AFTER_MS: &str = "last_message_after_ms";

    /// Message-window inputs renamed in the thread-level batch workflows.
    /// The batches ignore unknown keys, so a stale template would silently
    /// widen the window to the whole history instead of failing.
    const RENAMED_MESSAGE_WINDOW_FIELDS: [(&str, &str); 3] = [
        ("updated_after_ms", LAST_MESSAGE_AFTER_MS),
        (
            "updated_before_ms",
            "last_message_before_ms (thread-summary batch only)",
        ),
        ("updated_within_hours", "last_message_within_hours"),
    ];

    fn reject_renamed_message_window_fields(input: &Map<String, Value>) -> Result<()> {
        for (old, new) in RENAMED_MESSAGE_WINDOW_FIELDS {
            if input.contains_key(old) {
                return Err(anyhow!("{old} is no longer supported; use {new}"));
            }
        }
        Ok(())
    }

    /// Overlay import-derived values on a user template for a thread-level
    /// batch dispatched after import. `--since` becomes an absolute
    /// `last_message_after_ms` rather than a relative window, because a
    /// relative window is re-anchored at workflow start and drifts past the
    /// import boundary whenever the job waits in the queue.
    pub(crate) fn merge_thread_batch_template(
        mut template: Value,
        flag: &str,
        removed_fields: &[&str],
        user_id: i64,
        last_message_after_ms: Option<i64>,
        output_language: &str,
    ) -> Result<Value> {
        let obj = template
            .as_object_mut()
            .ok_or_else(|| anyhow!("{flag} must be a JSON object"))?;
        reject_removed_fields(obj, removed_fields)?;
        reject_renamed_message_window_fields(obj)?;
        obj.insert("user_id".into(), json!(user_id));
        obj.insert("output_language".into(), json!(output_language));
        if let Some(ms) = last_message_after_ms {
            obj.insert(LAST_MESSAGE_AFTER_MS.into(), json!(ms));
        }
        Ok(template)
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        fn merge(template: Value, since: Option<i64>) -> Result<Value> {
            merge_thread_batch_template(template, "--flag", &["legacy_owner"], 1, since, "ja")
        }

        #[test]
        fn overrides_user_and_language_and_keeps_other_fields() {
            let out = merge(json!({"user_id": 999, "custom": "x"}), None).unwrap();
            assert_eq!(out["user_id"], json!(1));
            assert_eq!(out["output_language"], json!("ja"));
            assert_eq!(out["custom"], json!("x"));
        }

        #[test]
        fn since_sets_or_overrides_last_message_after_ms() {
            let out = merge(json!({"last_message_after_ms": 100}), Some(2_000)).unwrap();
            assert_eq!(out["last_message_after_ms"], json!(2_000));
            let out = merge(json!({}), Some(2_000)).unwrap();
            assert_eq!(out["last_message_after_ms"], json!(2_000));
            assert!(out.get("last_message_within_hours").is_none());
        }

        #[test]
        fn without_since_keeps_template_window() {
            let out = merge(json!({"last_message_after_ms": 100}), None).unwrap();
            assert_eq!(out["last_message_after_ms"], json!(100));
        }

        #[test]
        fn rejects_every_renamed_message_window_field() {
            for (old, new) in RENAMED_MESSAGE_WINDOW_FIELDS {
                let mut template = json!({});
                template[old] = json!(1);
                let message = merge(template, Some(2_000)).unwrap_err().to_string();
                assert!(
                    message.contains(old) && message.contains(new),
                    "renamed field {old} must point to {new}: {message}"
                );
            }
        }

        #[test]
        fn rejects_removed_fields_and_non_objects() {
            let error = merge(json!({"legacy_owner": 1}), None).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("legacy_owner is no longer supported")
            );
            for template in [json!([1]), json!("x"), json!(42), Value::Null] {
                let error = merge(template, None).unwrap_err();
                assert!(error.to_string().contains("--flag must be a JSON object"));
            }
        }
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
