//! Build script that captures the `lance-index` version string at compile
//! time and exposes it via the `MEMORIES_LANCE_INDEX_VERSION` env var to
//! the crate source.
//!
//! Why: `FtsConfig::fingerprint` incorporates the `lance-index` version so
//! that a breaking change to the tokenizer or inverted index format
//! automatically triggers index rebuilds on the next boot. `lance-index`
//! does not publish a public `VERSION` constant, so we extract it from
//! the workspace build metadata.
//!
//! Resolution order:
//! 1. `<workspace>/Cargo.lock` — the exact resolved version (preferred).
//!    `cargo` always regenerates Cargo.lock before running build.rs, so
//!    this file is available even in a clean checkout.
//! 2. `<workspace>/Cargo.toml` `[workspace.dependencies] lance-index = "..."`
//!    — the declared version, used only as a fallback for unusual build
//!    environments (cargo vendor, sdist packaging) where Cargo.lock may
//!    be absent. Less precise than the lock version but still captures
//!    major/minor bumps that typically drive format changes.
//! 3. Build failure — we refuse to embed a placeholder like `"unknown"`
//!    because that would silently make every future `lance-index` upgrade
//!    a no-op fingerprint change, defeating the purpose of the check.

use std::fs;
use std::path::{Path, PathBuf};

const TARGET_CRATE: &str = "lance-index";

fn main() {
    // build.rs runs with CWD = CARGO_MANIFEST_DIR (= memories/infra), so
    // the workspace root is exactly one directory up.
    let workspace_root = PathBuf::from("..");
    let lock_path = workspace_root.join("Cargo.lock");
    let toml_path = workspace_root.join("Cargo.toml");

    let (version, source) = match resolve_version(&lock_path, &toml_path) {
        Ok(pair) => pair,
        Err(err) => {
            // Build `panic!` message via `format!` first to avoid format-string
            // edition quirks in build.rs.
            let msg = format!(
                "build.rs: failed to resolve `{TARGET_CRATE}` version for FTS fingerprint input.\n\
                 Checked {lock_path:?} and {toml_path:?}.\n\
                 Error: {err}\n\
                 \n\
                 This build script refuses to fall back to a placeholder version because doing so \
                 would silently break FTS index rebuild detection when the `{TARGET_CRATE}` crate is \
                 upgraded. Ensure Cargo.lock exists or that Cargo.toml declares `{TARGET_CRATE}` in \
                 `[workspace.dependencies]`."
            );
            panic!("{msg}");
        }
    };

    println!("cargo:rustc-env=MEMORIES_LANCE_INDEX_VERSION={version}");
    // Rerun whenever either source changes so new resolved versions
    // propagate without a manual `cargo clean`.
    println!("cargo:rerun-if-changed=../Cargo.lock");
    println!("cargo:rerun-if-changed=../Cargo.toml");
    // Only emit a `cargo:warning` when we had to fall back to the less
    // precise Cargo.toml source — the Cargo.lock case is the happy path
    // and should stay quiet so CI logs remain readable.
    if source.starts_with("Cargo.toml") {
        println!(
            "cargo:warning=FTS fingerprint resolved from Cargo.toml declaration (fallback); \
             Cargo.lock was unavailable. This is expected for sdist/vendor builds but means \
             patch-level `{TARGET_CRATE}` bumps will not trigger index rebuilds."
        );
    }
}

/// Attempt Cargo.lock → Cargo.toml fallback. Returns `(version, source_label)`
/// on success. Returns an explanatory message on failure.
fn resolve_version(lock_path: &Path, toml_path: &Path) -> Result<(String, String), String> {
    // Primary: exact resolved version from Cargo.lock.
    if let Ok(content) = fs::read_to_string(lock_path)
        && let Some(v) = extract_lock_version(&content, TARGET_CRATE)
    {
        return Ok((v, format!("Cargo.lock ({lock_path:?})")));
    }

    // Fallback: declared version from workspace Cargo.toml. Less precise
    // (the string literally written by the developer, e.g. "4.0") but
    // robust against Cargo.lock-less environments.
    match fs::read_to_string(toml_path) {
        Ok(content) => match extract_workspace_dep_version(&content, TARGET_CRATE) {
            Some(v) => Ok((v, format!("Cargo.toml ({toml_path:?})"))),
            None => Err(format!(
                "Cargo.toml did not contain a `[workspace.dependencies]` \
                 entry for `{TARGET_CRATE}`"
            )),
        },
        Err(e) => Err(format!("Cargo.toml read failed: {e}")),
    }
}

/// Scan Cargo.lock for `[[package]]` blocks and return the `version` field
/// of the first block whose `name` matches `target`.
///
/// Hand-rolled to avoid taking a TOML parser as a build-dependency; the
/// format of Cargo.lock is stable enough that a tiny scanner is more
/// robust than introducing extra crates into the build graph.
///
/// If `target` appears multiple times in the lockfile (duplicate major
/// versions resolved side-by-side), only the FIRST block encountered is
/// returned. For `lance-index` today that is a non-issue because the
/// workspace pins a single version; if that ever changes, this function
/// will silently pick whichever version Cargo wrote first, which may not
/// match what the runtime actually links against — the caller should
/// audit the lockfile in that case.
fn extract_lock_version(lock: &str, target: &str) -> Option<String> {
    let target_name_line = format!("name = \"{target}\"");
    let mut lines = lock.lines();
    while let Some(line) = lines.next() {
        if line.trim() != "[[package]]" {
            continue;
        }
        let mut name_matches = false;
        let mut version: Option<String> = None;
        // Read subsequent lines until the next blank line or section marker.
        for inner in lines.by_ref() {
            let trimmed = inner.trim();
            if trimmed.is_empty() || trimmed.starts_with("[[") {
                break;
            }
            if trimmed == target_name_line {
                name_matches = true;
            } else if let Some(rest) = trimmed.strip_prefix("version = \"")
                && let Some(v) = rest.strip_suffix('"')
            {
                version = Some(v.to_string());
            }
        }
        // Both the name-match and the version field can appear in either
        // order within a [[package]] block (Cargo's output puts name first
        // in practice, but the test suite covers both orderings).
        if name_matches && let Some(v) = version {
            return Some(v);
        }
    }
    None
}

/// Scan workspace Cargo.toml for a `[workspace.dependencies]` entry of
/// `target` and return the value of its `version = "..."` field.
///
/// Supports both shorthand (`target = "X.Y"`) and table
/// (`target = { version = "X.Y", features = ... }`) forms.
fn extract_workspace_dep_version(toml: &str, target: &str) -> Option<String> {
    let mut in_workspace_deps = false;
    for raw in toml.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        if line.starts_with('[') {
            in_workspace_deps = line == "[workspace.dependencies]";
            continue;
        }
        if !in_workspace_deps {
            continue;
        }
        // Match lines like:
        //   lance-index = "4.0"
        //   lance-index = { version = "4.0", features = [...] }
        let Some(rest) = line.strip_prefix(target) else {
            continue;
        };
        // Defensive boundary check: require the next byte to be whitespace
        // or `=` so that e.g. `lance-index-core = ...` or `lance-indexer = ...`
        // cannot accidentally match `target = "lance-index"`. Today the
        // downstream `strip_prefix('=')` already rejects those cases in
        // practice, but making the boundary explicit here prevents a
        // future refactor from reintroducing the prefix-match hazard.
        let next = rest.as_bytes().first().copied();
        if !matches!(next, Some(b' ') | Some(b'\t') | Some(b'=') | None) {
            continue;
        }
        let rest = rest.trim_start();
        let Some(rest) = rest.strip_prefix('=') else {
            continue;
        };
        let rest = rest.trim();
        if let Some(stripped) = rest.strip_prefix('"') {
            if let Some(end) = stripped.find('"') {
                return Some(stripped[..end].to_string());
            }
        } else if rest.starts_with('{') {
            // Table form: locate version = "..."
            if let Some(vpos) = rest.find("version") {
                let after = &rest[vpos..];
                if let Some(q1) = after.find('"') {
                    let after_q1 = &after[q1 + 1..];
                    if let Some(q2) = after_q1.find('"') {
                        return Some(after_q1[..q2].to_string());
                    }
                }
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_version_from_minimal_lockfile() {
        let lock = r#"
[[package]]
name = "foo"
version = "1.2.3"

[[package]]
name = "lance-index"
version = "4.0.0"
source = "registry+https://github.com/rust-lang/crates.io-index"

[[package]]
name = "bar"
version = "0.1.0"
"#;
        assert_eq!(
            extract_lock_version(lock, "lance-index"),
            Some("4.0.0".to_string())
        );
        assert_eq!(extract_lock_version(lock, "foo"), Some("1.2.3".to_string()));
        assert_eq!(extract_lock_version(lock, "missing"), None);
    }

    #[test]
    fn extracts_version_when_version_precedes_name() {
        // Defensive: future Cargo changes might shuffle field order.
        let lock = r#"
[[package]]
version = "4.1.0"
name = "lance-index"
source = "..."
"#;
        assert_eq!(
            extract_lock_version(lock, "lance-index"),
            Some("4.1.0".to_string())
        );
    }

    #[test]
    fn extracts_shorthand_from_workspace_toml() {
        let toml = r#"
[workspace.dependencies]
anyhow = "1"
lance-index = "4.0"
lancedb = { version = "0.27" }
"#;
        assert_eq!(
            extract_workspace_dep_version(toml, "lance-index"),
            Some("4.0".to_string())
        );
    }

    #[test]
    fn extracts_table_form_from_workspace_toml() {
        let toml = r#"
[workspace.dependencies]
lance-index = { version = "4.0.1", features = ["tokenizer-lindera"] }
"#;
        assert_eq!(
            extract_workspace_dep_version(toml, "lance-index"),
            Some("4.0.1".to_string())
        );
    }

    #[test]
    fn workspace_toml_section_scope_is_respected() {
        // A `lance-index = ...` line in [dependencies] (not workspace) should
        // be ignored — we only consult the workspace declaration.
        let toml = r#"
[dependencies]
lance-index = "99.0"

[workspace.dependencies]
lance-index = "4.0"
"#;
        assert_eq!(
            extract_workspace_dep_version(toml, "lance-index"),
            Some("4.0".to_string())
        );
    }

    #[test]
    fn workspace_toml_absent_returns_none() {
        let toml = r#"
[workspace.dependencies]
anyhow = "1"
"#;
        assert_eq!(extract_workspace_dep_version(toml, "lance-index"), None);
    }

    #[test]
    fn workspace_toml_ignores_confusable_prefix_names() {
        // Regression guard: `target = "lance-index"` must not be fooled by
        // a `lance-index-core = ...` entry sitting immediately above in
        // the same section. Both hyphenated and non-hyphenated confusables
        // are covered.
        let toml = r#"
[workspace.dependencies]
lance-index-core = "99.0"
lance-indexer = "98.0"
lance-index = "4.0"
"#;
        assert_eq!(
            extract_workspace_dep_version(toml, "lance-index"),
            Some("4.0".to_string())
        );
    }

    #[test]
    fn workspace_toml_returns_none_when_only_confusable_present() {
        // If only a confusable (e.g. lance-index-core) exists and the
        // real target is absent, we must return None rather than
        // accidentally returning the confusable's version.
        let toml = r#"
[workspace.dependencies]
lance-index-core = "99.0"
lance-indexer = "98.0"
"#;
        assert_eq!(extract_workspace_dep_version(toml, "lance-index"), None);
    }
}
