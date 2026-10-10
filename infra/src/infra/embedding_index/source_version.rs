//! Source version: a fingerprint of exactly what an embedding is
//! generated from. Compared only for equality, so clock precision,
//! simultaneous updates, and RDB rollbacks do not matter. The rule
//! version is part of the value: changing the rule makes every old value
//! differ, which errs on the side of regenerating.

use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

const RULE: &str = "sv1";

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SourceVersion(pub String);

impl SourceVersion {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for SourceVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

fn digest(parts: &[&str]) -> SourceVersion {
    let canonical = serde_json::to_string(parts).expect("string slices serialize");
    let hash = Sha256::digest(canonical.as_bytes());
    let hex: String = hash.iter().map(|b| format!("{b:02x}")).collect();
    SourceVersion(format!("{RULE}:{hex}"))
}

/// What a text input is embedded as; part of the version so equal texts
/// of different kinds never share a version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TextSource {
    MemoryText,
    Caption,
    ThreadDescription,
    ReflectionIntent,
}

impl TextSource {
    fn as_str(self) -> &'static str {
        match self {
            Self::MemoryText => "memory_text",
            Self::Caption => "caption",
            Self::ThreadDescription => "thread_description",
            Self::ReflectionIntent => "reflection_intent",
        }
    }
}

/// Version of a text input. `text` must be exactly what is sent to the
/// embedding worker, i.e. after truncation.
pub fn text(source: TextSource, text: &str) -> SourceVersion {
    digest(&[source.as_str(), text])
}

/// Version of a linked media object. Stored media is identified by its
/// immutable content digest; URL media by its URL (a later change of the
/// content behind the URL is not detected).
pub fn media(media_object_id: i64, sha256: Option<&str>, url: Option<&str>) -> SourceVersion {
    let id = media_object_id.to_string();
    match sha256.filter(|s| !s.is_empty()) {
        Some(sha) => digest(&["media_stored", &id, sha]),
        None => digest(&["media_url", &id, url.unwrap_or_default()]),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_input_same_version_and_any_change_differs() {
        let a = text(TextSource::MemoryText, "hello");
        assert_eq!(a, text(TextSource::MemoryText, "hello"));
        assert_ne!(a, text(TextSource::MemoryText, "hello!"));
        assert_ne!(a, text(TextSource::Caption, "hello"));
        assert!(a.as_str().starts_with("sv1:"));
    }

    #[test]
    fn stored_media_uses_the_digest_and_url_media_the_url() {
        let stored = media(1, Some("abc"), Some("file:///x"));
        assert_eq!(stored, media(1, Some("abc"), Some("file:///moved")));
        assert_ne!(stored, media(1, Some("abd"), None));
        assert_ne!(stored, media(2, Some("abc"), None));
        let url = media(1, None, Some("https://a/x.png"));
        assert_ne!(url, media(1, None, Some("https://a/y.png")));
        assert_eq!(url, media(1, Some(""), Some("https://a/x.png")));
    }

    #[test]
    fn field_boundaries_do_not_collide() {
        assert_ne!(media(12, Some("3"), None), media(1, Some("23"), None));
    }
}
