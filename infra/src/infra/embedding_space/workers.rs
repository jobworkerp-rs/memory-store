//! Per-space worker names.
//!
//! Workers whose behavior depends on the embedding model are registered
//! under `<base>-<first 16 hex of the space ID>`, so a worker definition
//! of one space is never overwritten by another space's settings.
//! Callback and RAG tool workers keep fixed names.

use super::SpaceId;
use crate::infra::startup_error::StartupError;
use std::collections::HashMap;
use std::sync::RwLock;

/// Placeholder the worker YAML templates and workflows append to the
/// base names of space-scoped workers. memories supplies its value when
/// it renders the YAML; it is not read from the environment.
pub const SPACE_SUFFIX_PLACEHOLDER: &str = "MEMORY_EMBEDDING_SPACE_SUFFIX";

/// jobworkerp's worker name column is 128 characters (MySQL); the
/// suffix takes 17 (`-` + 16 hex digits).
pub const MAX_BASE_NAME_LEN: usize = 111;

/// Base names of the space-scoped workflow workers memories registers.
/// The mm-embedding base comes from `MEMORY_MM_EMBEDDING_WORKER`.
pub const SPACE_SCOPED_WORKFLOW_BASES: &[&str] = &[
    crate::infra::embedding_dispatch::TEXT_WORKFLOW_WORKER,
    crate::infra::embedding_dispatch::IMAGE_WORKFLOW_WORKER,
    "memories-auto-thread-embedding",
    "memories-auto-reflection-summary-embedding",
    "memories-auto-reflection-intent-embedding",
];

static CURRENT_SPACE: RwLock<Option<SpaceId>> = RwLock::new(None);

/// Set the space this process serves. Called once startup verified it.
pub fn set_current_space(space: Option<SpaceId>) {
    *CURRENT_SPACE.write().unwrap_or_else(|e| e.into_inner()) = space;
}

pub fn current_space() -> Option<SpaceId> {
    CURRENT_SPACE
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .clone()
}

/// `-<16 hex>` for `space`.
pub fn space_suffix(space: &SpaceId) -> String {
    format!("-{}", space.short())
}

/// Suffix of the current space, or empty when no space is set (no vector
/// store enabled; names then stay at their bases).
pub fn current_space_suffix() -> String {
    current_space()
        .as_ref()
        .map(space_suffix)
        .unwrap_or_default()
}

pub fn space_worker_name(base: &str, space: &SpaceId) -> String {
    format!("{base}{}", space_suffix(space))
}

/// Name of a space-scoped worker in the current space.
pub fn current_space_worker_name(base: &str) -> String {
    format!("{base}{}", current_space_suffix())
}

/// Reject a base name that would not fit jobworkerp's name column once
/// suffixed.
pub fn validate_base_name(env_name: &str, base: &str) -> Result<(), StartupError> {
    let len = base.chars().count();
    if len > MAX_BASE_NAME_LEN {
        return Err(StartupError::EnvVarInvalid {
            name: env_name.to_string(),
            message: format!(
                "worker base name is {len} characters; at most {MAX_BASE_NAME_LEN} are allowed \
                 so the per-space suffix fits jobworkerp's 128-character limit"
            ),
        });
    }
    Ok(())
}

/// Placeholder values memories supplies when rendering worker YAML.
pub fn template_overrides(space: Option<&SpaceId>) -> HashMap<String, String> {
    HashMap::from([(
        SPACE_SUFFIX_PLACEHOLDER.to_string(),
        space.map(space_suffix).unwrap_or_default(),
    )])
}

/// Workers named after `base` that belong to another space: the bare
/// base (names used before spaces were introduced) and `<base>-<16 hex>`
/// with a different suffix. Other names sharing the prefix are left
/// alone.
pub fn stale_space_workers(base: &str, current: &SpaceId, names: &[String]) -> Vec<String> {
    let keep = space_worker_name(base, current);
    names
        .iter()
        .filter(|name| *name != &keep)
        .filter(|name| match name.strip_prefix(base) {
            Some("") => true,
            Some(rest) => rest
                .strip_prefix('-')
                .is_some_and(|hex| hex.len() == 16 && hex.bytes().all(|b| b.is_ascii_hexdigit())),
            None => false,
        })
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn space() -> SpaceId {
        SpaceId("0123456789abcdef".repeat(4))
    }

    #[test]
    fn worker_name_appends_short_space_id() {
        assert_eq!(
            space_worker_name("memories-mm-embedding", &space()),
            "memories-mm-embedding-0123456789abcdef"
        );
    }

    #[test]
    fn base_name_length_boundary() {
        assert!(validate_base_name("X", &"a".repeat(MAX_BASE_NAME_LEN)).is_ok());
        let err = validate_base_name("X", &"a".repeat(MAX_BASE_NAME_LEN + 1)).unwrap_err();
        assert!(matches!(err, StartupError::EnvVarInvalid { ref name, .. } if name == "X"));
        assert_eq!(
            space_worker_name(&"a".repeat(MAX_BASE_NAME_LEN), &space()).len(),
            128
        );
    }

    #[test]
    fn stale_workers_are_other_spaces_of_the_same_base() {
        let names: Vec<String> = [
            "memories-mm-embedding",
            "memories-mm-embedding-0123456789abcdef",
            "memories-mm-embedding-fedcba9876543210",
            "memories-mm-embedding-extra",
            "memories-mm-embedding-0123",
            "memories-mm-embeddingX",
        ]
        .iter()
        .map(|s| s.to_string())
        .collect();
        assert_eq!(
            stale_space_workers("memories-mm-embedding", &space(), &names),
            vec![
                "memories-mm-embedding".to_string(),
                "memories-mm-embedding-fedcba9876543210".to_string(),
            ]
        );
    }

    #[test]
    fn overrides_carry_the_suffix() {
        assert_eq!(
            template_overrides(Some(&space()))[SPACE_SUFFIX_PLACEHOLDER],
            "-0123456789abcdef"
        );
        assert_eq!(template_overrides(None)[SPACE_SUFFIX_PLACEHOLDER], "");
    }
}
