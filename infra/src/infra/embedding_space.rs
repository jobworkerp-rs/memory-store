//! Embedding space identity.
//!
//! An embedding space is the range within which vectors are comparable.
//! It is identified only by configuration (model, tokenizer, declared
//! revision, dimension, distance), never by runner-reported values, so
//! the server, the migration CLI, and the client app all derive the same
//! space ID without talking to jobworkerp.

pub mod bootstrap;
pub mod plan;
pub mod rdb_targets;
pub mod record;
pub mod registration;
pub mod replace;
pub mod startup;
pub mod storage;
pub mod token;
pub mod workers;
pub mod writer_lock;

use anyhow::{Context as _, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::collections::HashMap;
use std::path::Path;

pub use record::{MarkerState, MigrationMarker, SpaceRecord};

/// Env var declaring the model distribution revision (weights and input
/// tokenizer). The runner does not report it, so changing this value is
/// the only way to tell memories that the distribution changed.
pub const MODEL_REVISION_ENV: &str = "MEMORY_EMBEDDING_MODEL_REVISION";
pub const MODEL_REVISION_DEFAULT: &str = "unversioned";

/// Version tag mixed into the space ID so a future change of the
/// derivation rule cannot collide with IDs computed by this one.
const SPACE_ID_DERIVATION: &str = "memories-embedding-space/v1";

/// Runner type of the worker whose settings define the model.
const MM_EMBEDDING_RUNNER: &str = "MultimodalEmbeddingRunner";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpaceComponents {
    pub model_id: String,
    pub tokenizer_model_id: String,
    pub revision: String,
    pub dimension: u32,
    pub distance: String,
}

impl SpaceComponents {
    pub fn space_id(&self) -> SpaceId {
        let canonical = serde_json::json!([
            SPACE_ID_DERIVATION,
            self.model_id,
            self.tokenizer_model_id,
            self.revision,
            self.dimension.to_string(),
            self.distance,
        ]);
        let digest = Sha256::digest(canonical.to_string().as_bytes());
        SpaceId(hex_lower(&digest))
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(self).expect("SpaceComponents always serializes")
    }

    /// Resolve the components from the workers YAML that memories
    /// registers, the revision env var, and the vector table config.
    pub fn resolve(workers_yaml: &Path, dimension: u32, distance: &str) -> Result<Self> {
        let raw = std::fs::read_to_string(workers_yaml).with_context(|| {
            format!(
                "failed to read embedding workers YAML at {}",
                workers_yaml.display()
            )
        })?;
        let (model_id, tokenizer_model_id) = model_from_workers_yaml(&raw)
            .with_context(|| format!("in {}", workers_yaml.display()))?;
        let revision = std::env::var(MODEL_REVISION_ENV)
            .ok()
            .filter(|v| !v.is_empty())
            .unwrap_or_else(|| MODEL_REVISION_DEFAULT.to_string());
        Ok(Self {
            model_id,
            tokenizer_model_id,
            revision,
            dimension,
            distance: distance.to_string(),
        })
    }
}

/// Whether a vector store enable flag (`MEMORY_VECTOR_ENABLED` etc.) is
/// set, read exactly as the server reads it (`true` only), so the server
/// and the migration tool agree on which stores exist.
pub fn vector_store_enabled(name: &str) -> bool {
    std::env::var(name).is_ok_and(|v| v == "true")
}

/// Full space ID: 64 lowercase hex digits of a SHA-256.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SpaceId(pub String);

impl SpaceId {
    /// The 16-hex-digit prefix used in per-space worker names.
    pub fn short(&self) -> &str {
        &self.0[..self.0.len().min(16)]
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for SpaceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

fn hex_lower(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(out, "{b:02x}");
    }
    out
}

/// Names of `%{NAME}` placeholders that have no `:-default`.
fn required_placeholders(raw: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut rest = raw;
    while let Some(start) = rest.find("%{") {
        let body = &rest[start + 2..];
        let Some(end) = body.find('}') else { break };
        let inner = &body[..end];
        if !inner.contains(":-") {
            out.push(inner.to_string());
        }
        rest = &body[end + 1..];
    }
    out
}

/// The `settings` of the single MultimodalEmbeddingRunner worker in a
/// workers YAML. Env placeholders are expanded with the same rules the
/// registration uses.
fn mm_worker_settings(raw: &str) -> Result<serde_yaml::Value> {
    // Placeholders unrelated to the model (e.g. the callback host) may be
    // unset in processes that never register workers; blank them instead
    // of failing, since only the model settings matter here.
    let blank_unset: HashMap<String, String> = required_placeholders(raw)
        .into_iter()
        .filter(|name| std::env::var(name).is_err())
        .map(|name| (name, String::new()))
        .collect();
    let expanded =
        jobworkerp_client::client::yaml_common::expand_env_with_overrides(raw, &blank_unset)?;
    let doc: serde_yaml::Value =
        serde_yaml::from_str(&expanded).context("failed to parse workers YAML")?;
    let workers = doc
        .get("workers")
        .and_then(|w| w.as_sequence())
        .ok_or_else(|| anyhow::anyhow!("workers YAML has no `workers` list"))?;
    let mut found = workers
        .iter()
        .filter(|w| w.get("runner").and_then(|r| r.as_str()) == Some(MM_EMBEDDING_RUNNER));
    let worker = found
        .next()
        .ok_or_else(|| anyhow::anyhow!("no {MM_EMBEDDING_RUNNER} worker is defined"))?;
    if found.next().is_some() {
        anyhow::bail!("more than one {MM_EMBEDDING_RUNNER} worker is defined");
    }
    Ok(worker.get("settings").cloned().unwrap_or_default())
}

/// Extract `(model_id, tokenizer_model_id)` from the mm-embedding worker.
fn model_from_workers_yaml(raw: &str) -> Result<(String, String)> {
    let settings = mm_worker_settings(raw)?;
    let setting = |key: &str| {
        settings
            .get(key)
            .and_then(|v| v.as_str())
            .map(str::to_string)
    };
    let model_id = setting("model_id")
        .filter(|m| !m.is_empty())
        .ok_or_else(|| anyhow::anyhow!("{MM_EMBEDDING_RUNNER} worker has no settings.model_id"))?;
    Ok((model_id, setting("tokenizer_model_id").unwrap_or_default()))
}

/// Settings of the mm-embedding worker that decide how text is split into
/// chunks. They do not change the space, but a rebuild must not mix two
/// splittings (spec §3.6), so the rebuild pins them.
const CHUNKING_SETTINGS: [&str; 2] = ["chunking_config", "max_sequence_length"];

/// Canonical text of the chunking settings in a workers YAML file.
pub fn chunking_fingerprint(workers_yaml: &Path) -> Result<String> {
    let raw = std::fs::read_to_string(workers_yaml).with_context(|| {
        format!(
            "failed to read embedding workers YAML at {}",
            workers_yaml.display()
        )
    })?;
    chunking_from_workers_yaml(&raw).with_context(|| format!("in {}", workers_yaml.display()))
}

fn chunking_from_workers_yaml(raw: &str) -> Result<String> {
    let settings = mm_worker_settings(raw)?;
    let mut picked = serde_json::Map::new();
    for key in CHUNKING_SETTINGS {
        let value = match settings.get(key) {
            Some(v) => {
                serde_json::to_value(v).context("chunking settings are not JSON-compatible")?
            }
            None => serde_json::Value::Null,
        };
        picked.insert(key.to_string(), canonical(value));
    }
    Ok(serde_json::Value::Object(picked).to_string())
}

/// Objects rebuilt with sorted keys, so the text does not depend on the
/// YAML key order.
fn canonical(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => {
            let mut entries: Vec<_> = map.into_iter().collect();
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            serde_json::Value::Object(
                entries
                    .into_iter()
                    .map(|(k, v)| (k, canonical(v)))
                    .collect(),
            )
        }
        serde_json::Value::Array(items) => {
            serde_json::Value::Array(items.into_iter().map(canonical).collect())
        }
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn components() -> SpaceComponents {
        SpaceComponents {
            model_id: "Qwen/Qwen3-VL-Embedding-2B".into(),
            tokenizer_model_id: String::new(),
            revision: "unversioned".into(),
            dimension: 2048,
            distance: "cosine".into(),
        }
    }

    #[test]
    fn space_id_is_deterministic_hex() {
        let id = components().space_id();
        assert_eq!(id, components().space_id());
        assert_eq!(id.as_str().len(), 64);
        assert!(
            id.as_str()
                .chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
        );
        assert_eq!(id.short(), &id.as_str()[..16]);
    }

    #[test]
    fn every_component_changes_the_space_id() {
        let base = components().space_id();
        let variants = [
            SpaceComponents {
                model_id: "other".into(),
                ..components()
            },
            SpaceComponents {
                tokenizer_model_id: "tok".into(),
                ..components()
            },
            SpaceComponents {
                revision: "r2".into(),
                ..components()
            },
            SpaceComponents {
                dimension: 1024,
                ..components()
            },
            SpaceComponents {
                distance: "l2".into(),
                ..components()
            },
        ];
        for v in variants {
            assert_ne!(v.space_id(), base, "{v:?}");
        }
    }

    #[test]
    fn component_boundaries_do_not_collide() {
        let a = SpaceComponents {
            model_id: "ab".into(),
            tokenizer_model_id: "c".into(),
            ..components()
        };
        let b = SpaceComponents {
            model_id: "a".into(),
            tokenizer_model_id: "bc".into(),
            ..components()
        };
        assert_ne!(a.space_id(), b.space_id());
    }

    #[test]
    fn model_is_read_from_the_mm_embedding_worker() {
        let yaml = r#"
workers:
  - name: callback
    runner: GRPC
    settings: { host: h }
  - name: "%{MEMORY_TEST_UNSET_WORKER_NAME:-memories-mm-embedding}"
    runner: MultimodalEmbeddingRunner
    settings:
      model_id: m/one
      tokenizer_model_id: t/one
      device: CUDA
"#;
        let (model, tok) = model_from_workers_yaml(yaml).unwrap();
        assert_eq!((model.as_str(), tok.as_str()), ("m/one", "t/one"));
    }

    #[test]
    fn chunking_fingerprint_ignores_key_order_and_other_settings() {
        let a = "workers:\n  - name: x\n    runner: MultimodalEmbeddingRunner\n    settings:\n      model_id: m\n      device: CUDA\n      max_sequence_length: 8192\n      chunking_config: {max_chunk_tokens: 512, min_chunk_tokens: 0}\n";
        let b = "workers:\n  - name: x\n    runner: MultimodalEmbeddingRunner\n    settings:\n      chunking_config: {min_chunk_tokens: 0, max_chunk_tokens: 512}\n      max_sequence_length: 8192\n      model_id: m\n      device: CPU\n";
        assert_eq!(
            chunking_from_workers_yaml(a).unwrap(),
            chunking_from_workers_yaml(b).unwrap()
        );
        let changed = a.replace("512", "256");
        assert_ne!(
            chunking_from_workers_yaml(a).unwrap(),
            chunking_from_workers_yaml(&changed).unwrap()
        );
        let seq = a.replace("8192", "4096");
        assert_ne!(
            chunking_from_workers_yaml(a).unwrap(),
            chunking_from_workers_yaml(&seq).unwrap()
        );
        let absent = "workers:\n  - name: x\n    runner: MultimodalEmbeddingRunner\n    settings: {model_id: m}\n";
        assert_eq!(
            chunking_from_workers_yaml(absent).unwrap(),
            r#"{"chunking_config":null,"max_sequence_length":null}"#
        );
    }

    #[test]
    fn required_placeholders_skip_defaulted_ones() {
        assert_eq!(
            required_placeholders("a: %{X}\nb: %{Y:-d}\nc: \"%{Z}\""),
            vec!["X".to_string(), "Z".to_string()]
        );
    }

    #[test]
    fn missing_tokenizer_is_empty() {
        let yaml = "workers:\n  - name: x\n    runner: MultimodalEmbeddingRunner\n    settings:\n      model_id: m\n";
        assert_eq!(model_from_workers_yaml(yaml).unwrap().1, "");
    }

    #[test]
    fn model_resolution_rejects_ambiguous_or_missing_worker() {
        let none = "workers:\n  - name: x\n    runner: GRPC\n";
        assert!(model_from_workers_yaml(none).is_err());
        let two = "workers:\n  - name: a\n    runner: MultimodalEmbeddingRunner\n    settings: {model_id: m}\n  - name: b\n    runner: MultimodalEmbeddingRunner\n    settings: {model_id: m}\n";
        assert!(model_from_workers_yaml(two).is_err());
        let no_model =
            "workers:\n  - name: a\n    runner: MultimodalEmbeddingRunner\n    settings: {}\n";
        assert!(model_from_workers_yaml(no_model).is_err());
    }

    #[test]
    #[serial_test::serial]
    fn shipped_workers_yaml_defines_a_model() {
        let path =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("../workflows/auto-embedding-workers.yaml");
        let raw = std::fs::read_to_string(path).unwrap();
        // SAFETY: serialized via `#[serial]`. The callback placeholders
        // must not be required just to read the model.
        unsafe {
            std::env::remove_var("MEMORY_GRPC_HOST");
            std::env::remove_var("MEMORY_GRPC_PORT");
        }
        let (model, _) = model_from_workers_yaml(&raw).unwrap();
        assert!(!model.is_empty());
    }
}
