use sha2::{Digest, Sha256};

/// Default storage limit for externally supplied identifiers.
pub const EXTERNAL_ID_MAX_BYTES: usize = 512;

/// Returns the source namespace encoded in an external ID.
///
/// Some legacy producers stored a hyphenated metadata source under an
/// underscore namespace. Preserve that namespace so migrations and future
/// imports derive the same owner-scoped identifier.
pub fn namespace_for_external_id(source: &str, external_id: &str) -> Option<String> {
    if external_id.starts_with(&format!("{source}:")) {
        return Some(source.to_string());
    }
    let normalized = source.replace('-', "_");
    external_id
        .starts_with(&format!("{normalized}:"))
        .then_some(normalized)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalIdError {
    pub max_bytes: usize,
    pub minimum_hashed_bytes: usize,
}

impl std::fmt::Display for ExternalIdError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            formatter,
            "external ID maximum {} is too small for the deterministic hashed form (requires {})",
            self.max_bytes, self.minimum_hashed_bytes
        )
    }
}

impl std::error::Error for ExternalIdError {}

/// Adds an owner scope to an identifier in a namespace.
///
/// The returned value preserves the namespace-local suffix when `external_id`
/// already begins with `namespace:`. Values exceeding `max_bytes` are replaced
/// with a deterministic SHA-256 form in the same namespace.
pub fn owner_scoped(
    namespace: &str,
    owner_id: i64,
    external_id: &str,
    max_bytes: usize,
) -> Result<String, ExternalIdError> {
    let namespace_prefix = format!("{namespace}:");
    let suffix = external_id
        .strip_prefix(&namespace_prefix)
        .unwrap_or(external_id);
    let scoped = format!("{namespace}:{owner_id}:{suffix}");
    if scoped.len() <= max_bytes {
        return Ok(scoped);
    }

    let minimum_hashed_bytes = format!("{namespace}:{owner_id}:~").len() + 64;
    if max_bytes < minimum_hashed_bytes {
        return Err(ExternalIdError {
            max_bytes,
            minimum_hashed_bytes,
        });
    }

    let digest = Sha256::digest(scoped.as_bytes());
    let digest_hex = digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    Ok(format!("{namespace}:{owner_id}:~{digest_hex}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owner_scoped_preserves_the_namespace_local_suffix() {
        let actual = owner_scoped("codex", 7, "codex:session:entry", 512).unwrap();

        assert_eq!(actual, "codex:7:session:entry");
    }

    #[test]
    fn owner_scoped_keeps_an_unprefixed_value_as_the_suffix() {
        let actual = owner_scoped("entity", 7, "opaque-key", 512).unwrap();

        assert_eq!(actual, "entity:7:opaque-key");
    }

    #[test]
    fn owner_scoped_hashes_overflow_deterministically_within_the_limit() {
        let value = format!("codex:{}", "あ".repeat(200));

        let first = owner_scoped("codex", 7, &value, 512).unwrap();
        let second = owner_scoped("codex", 7, &value, 512).unwrap();

        assert_eq!(first, second);
        assert!(first.len() <= 512);
        assert!(first.starts_with("codex:7:~"));
    }

    #[test]
    fn owner_scoped_rejects_a_limit_smaller_than_the_hashed_form() {
        let error = owner_scoped("x", 1, &"x:y".repeat(100), 8).unwrap_err();

        assert_eq!(error.max_bytes, 8);
        assert!(error.minimum_hashed_bytes > error.max_bytes);
    }

    #[test]
    fn namespace_preserves_the_legacy_underscore_form() {
        assert_eq!(
            namespace_for_external_id(
                "news-aggregator",
                "news_aggregator:https://example.test/article",
            ),
            Some("news_aggregator".to_string())
        );
    }

    #[test]
    fn namespace_rejects_an_unrelated_external_id() {
        assert_eq!(
            namespace_for_external_id("news-aggregator", "codex:session:entry"),
            None
        );
    }
}
