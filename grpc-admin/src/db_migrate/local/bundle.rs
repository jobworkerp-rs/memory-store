//! Identity of a migration bundle: a digest of everything it ships.
//!
//! Applications compare the digest as an opaque value to detect a stale
//! bundle; the source revision is diagnostic only because a bundle built from
//! a modified working tree shares the revision of a different content.

use super::files::{hash_file, hex_digest, regular_files, write_atomically};
use super::output::{ErrorCode, LocalFailure, Resolution};
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::Path;

pub const MANIFEST_FILE: &str = "bundle-manifest.json";
const MANIFEST_FORMAT: &str = "memories-db-migrate-bundle-v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BundleManifest {
    pub format: String,
    pub content_digest: String,
    pub source_revision: String,
    pub source_dirty: bool,
}

impl BundleManifest {
    /// Record the identity of the bundle rooted at `root`.
    pub fn write(root: &Path, source_revision: &str, source_dirty: bool) -> Result<Self> {
        let manifest = Self {
            format: MANIFEST_FORMAT.to_string(),
            content_digest: content_digest(root)?,
            source_revision: source_revision.to_string(),
            source_dirty,
        };
        let bytes = serde_json::to_vec_pretty(&manifest).context("serializing bundle manifest")?;
        write_atomically(&root.join(MANIFEST_FILE), &bytes)?;
        Ok(manifest)
    }

    /// The recorded identity, when the bundle at `root` still matches it.
    pub fn verify(root: &Path) -> Result<Self> {
        let manifest = Self::load(root)?;
        let actual = content_digest(root)?;
        if manifest.format != MANIFEST_FORMAT || manifest.content_digest != actual {
            return Err(invalid(format!(
                "bundle content at {} does not match {MANIFEST_FILE}",
                root.display()
            )));
        }
        Ok(manifest)
    }

    pub fn load(root: &Path) -> Result<Self> {
        let path = root.join(MANIFEST_FILE);
        let bytes = std::fs::read(&path)
            .map_err(|error| invalid(format!("{} is unreadable: {error}", path.display())))?;
        serde_json::from_slice(&bytes)
            .map_err(|error| invalid(format!("{} is invalid: {error}", path.display())))
    }
}

fn invalid(message: String) -> anyhow::Error {
    LocalFailure::new(
        ErrorCode::BundleInvalid,
        Resolution::ToolUpdateRequired,
        message,
    )
    .into()
}

/// SHA-256 over every shipped file's relative path and content, excluding the
/// manifest that stores the result.
pub fn content_digest(root: &Path) -> Result<String> {
    let mut hasher = Sha256::new();
    for relative in regular_files(root)?
        .into_iter()
        .filter(|relative| relative != Path::new(MANIFEST_FILE))
    {
        hasher.update(relative.to_string_lossy().as_bytes());
        hasher.update([0]);
        hasher.update(hash_file(&root.join(&relative))?.as_bytes());
        hasher.update(b"\n");
    }
    Ok(hex_digest(hasher))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db_migrate::local::output::{ErrorCode, LocalFailure};

    fn bundle() -> tempfile::TempDir {
        let root = tempfile::tempdir().unwrap();
        std::fs::write(root.path().join("memories-db-migrate"), b"binary").unwrap();
        std::fs::create_dir_all(root.path().join("atlas/sqlite/migrations")).unwrap();
        std::fs::write(root.path().join("atlas/sqlite/migrations/1.sql"), b"create").unwrap();
        root
    }

    fn code(error: anyhow::Error) -> Option<ErrorCode> {
        error.downcast_ref::<LocalFailure>().map(|f| f.error_code)
    }

    #[test]
    fn manifest_identifies_the_bundle_content() {
        let root = bundle();
        let written = BundleManifest::write(root.path(), "abc123", true).unwrap();
        assert_eq!(written.source_revision, "abc123");
        assert!(written.source_dirty);
        assert_eq!(BundleManifest::verify(root.path()).unwrap(), written);
        // The manifest does not change the digest it records.
        assert_eq!(content_digest(root.path()).unwrap(), written.content_digest);
    }

    #[test]
    fn digest_changes_with_any_shipped_file() {
        let root = bundle();
        let before = content_digest(root.path()).unwrap();
        std::fs::write(
            root.path().join("atlas/sqlite/migrations/1.sql"),
            b"changed",
        )
        .unwrap();
        assert_ne!(content_digest(root.path()).unwrap(), before);
        let after_edit = content_digest(root.path()).unwrap();
        std::fs::rename(
            root.path().join("atlas/sqlite/migrations/1.sql"),
            root.path().join("atlas/sqlite/migrations/2.sql"),
        )
        .unwrap();
        assert_ne!(content_digest(root.path()).unwrap(), after_edit);
    }

    #[test]
    fn modified_or_unidentified_bundle_fails_verification() {
        let root = bundle();
        assert_eq!(
            code(BundleManifest::verify(root.path()).unwrap_err()),
            Some(ErrorCode::BundleInvalid)
        );
        BundleManifest::write(root.path(), "abc123", false).unwrap();
        std::fs::write(root.path().join("memories-db-migrate"), b"tampered").unwrap();
        assert_eq!(
            code(BundleManifest::verify(root.path()).unwrap_err()),
            Some(ErrorCode::BundleInvalid)
        );
    }
}
