//! Configuration for the reflection intent vector store.
//!
//! Both knobs intentionally fall back to the existing memory-vector
//! configuration so a deployment that already runs `memory_vector`
//! gets reflection-intent search "for free" without further env edits.
//! Spec §C: `MEMORY_VECTOR_SIZE` is required to be shared across the
//! memory / summary / intent dispatchers because the F-S8
//! `FindSimilarByIntentText` path embeds the query with the same
//! model used at insert time.

use crate::infra::memory_vector::config::{
    DistanceType, OptimizeConfig, warn_if_deprecated_auto_optimize_interval,
};
use anyhow::Result;

/// Effective intent-vector configuration.
#[derive(Debug, Clone)]
pub struct ReflectionIntentVectorConfig {
    pub uri: String,
    pub table_name: String,
    pub vector_size: usize,
    pub distance_type: DistanceType,
    /// LanceDB table maintenance policy. Without it this store would never
    /// compact or prune, accumulating versions until startup `open_table`
    /// becomes pathologically slow — the same problem the memory/thread
    /// stores solve. Falls back to the shared `MEMORY_OPTIMIZE_*` knobs.
    pub optimize: OptimizeConfig,
}

/// Default LanceDB directory used when neither `REFLECTION_LANCEDB_URI`
/// nor `MEMORY_LANCEDB_URI` is set. Mirrors the path
/// `VectorDBConfig::from_env` / `ThreadVectorDBConfig::from_env` use, so
/// a deployment that only sets `MEMORY_VECTOR_SIZE` still gets a
/// working reflection intent store under the same directory.
const DEFAULT_LANCEDB_DIR: &str = "data/lancedb/memories.lancedb";

impl ReflectionIntentVectorConfig {
    /// Read from env. Falls back to the memory-vector knobs when
    /// `REFLECTION_*` is unset; bails when neither side defines
    /// `*_VECTOR_SIZE` because we cannot create an embedding column
    /// without a fixed dimension.
    pub fn from_env() -> Result<Self> {
        // URI resolution: prefer an explicit reflection URI, fall back
        // to the shared memory URI (with a `/reflection_intent`
        // subdirectory so we don't collide with the memory_vector
        // table files), and finally the same hard-coded default the
        // memory and thread vector configs use.
        let memory_uri =
            std::env::var("MEMORY_LANCEDB_URI").unwrap_or_else(|_| DEFAULT_LANCEDB_DIR.to_string());
        let uri = std::env::var("REFLECTION_LANCEDB_URI")
            .unwrap_or_else(|_| format!("{}/reflection_intent", memory_uri.trim_end_matches('/')));

        let table_name = std::env::var("REFLECTION_LANCEDB_TABLE")
            .unwrap_or_else(|_| "reflection_intent".to_string());

        // Vector size is shared with memory_vector by spec; this lets
        // F-S8 embed queries with the same model at search time.
        let vector_size: usize = std::env::var("REFLECTION_VECTOR_SIZE")
            .or_else(|_| std::env::var("MEMORY_VECTOR_SIZE"))
            .map_err(|_| {
                anyhow::anyhow!(
                    "REFLECTION_VECTOR_SIZE (or MEMORY_VECTOR_SIZE as fallback) is required \
                     when the reflection intent vector store is enabled"
                )
            })?
            .parse()?;

        let distance_type = match std::env::var("REFLECTION_DISTANCE_TYPE")
            .or_else(|_| std::env::var("MEMORY_DISTANCE_TYPE"))
            .unwrap_or_else(|_| "cosine".to_string())
            .as_str()
        {
            "l2" => DistanceType::L2,
            "dot" => DistanceType::Dot,
            _ => DistanceType::Cosine,
        };

        warn_if_deprecated_auto_optimize_interval(&["REFLECTION_", "MEMORY_"]);

        Ok(Self {
            uri,
            table_name,
            vector_size,
            distance_type,
            optimize: OptimizeConfig::from_env_with_prefixes(&["REFLECTION_", "MEMORY_"]),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    fn clear_uri_env() {
        // SAFETY: tests in this module are #[serial], no other thread
        // observes env at this moment.
        unsafe {
            std::env::remove_var("REFLECTION_LANCEDB_URI");
            std::env::remove_var("MEMORY_LANCEDB_URI");
            std::env::remove_var("REFLECTION_LANCEDB_TABLE");
            std::env::remove_var("REFLECTION_VECTOR_SIZE");
            std::env::remove_var("MEMORY_VECTOR_SIZE");
            std::env::remove_var("REFLECTION_DISTANCE_TYPE");
            std::env::remove_var("MEMORY_DISTANCE_TYPE");
        }
    }

    #[test]
    #[serial]
    fn falls_back_to_default_lancedb_dir_when_no_uri_env_is_set() {
        clear_uri_env();
        // SAFETY: serial-guarded.
        unsafe { std::env::set_var("MEMORY_VECTOR_SIZE", "1024") };
        let cfg = ReflectionIntentVectorConfig::from_env()
            .expect("must succeed with MEMORY_VECTOR_SIZE alone");
        assert_eq!(
            cfg.uri,
            format!("{DEFAULT_LANCEDB_DIR}/reflection_intent"),
            "with no URI env, fall back to the shared memory default \
             plus the reflection subdirectory"
        );
        clear_uri_env();
    }

    #[test]
    #[serial]
    fn memory_uri_env_routes_into_reflection_subdirectory() {
        clear_uri_env();
        // SAFETY: serial-guarded.
        unsafe {
            std::env::set_var("MEMORY_LANCEDB_URI", "/var/lib/lance/memories");
            std::env::set_var("MEMORY_VECTOR_SIZE", "1024");
        }
        let cfg = ReflectionIntentVectorConfig::from_env().unwrap();
        assert_eq!(cfg.uri, "/var/lib/lance/memories/reflection_intent");
        clear_uri_env();
    }

    #[test]
    #[serial]
    fn reflection_uri_env_takes_precedence_over_memory_uri() {
        clear_uri_env();
        // SAFETY: serial-guarded.
        unsafe {
            std::env::set_var("MEMORY_LANCEDB_URI", "/should/be/ignored");
            std::env::set_var("REFLECTION_LANCEDB_URI", "/explicit/reflection.lance");
            std::env::set_var("MEMORY_VECTOR_SIZE", "1024");
        }
        let cfg = ReflectionIntentVectorConfig::from_env().unwrap();
        assert_eq!(cfg.uri, "/explicit/reflection.lance");
        clear_uri_env();
    }
}
