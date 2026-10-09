use super::super::memory_vector::config::{DistanceType, FtsConfig, VectorIndexConfig};

/// LanceDB vector storage configuration for thread descriptions.
/// Uses THREAD_VECTOR_* environment variables; falls back to same
/// LanceDB database as memory but with a separate table.
#[derive(Debug, Clone)]
pub struct ThreadVectorDBConfig {
    pub uri: String,
    pub table_name: String,
    pub vector_size: usize,
    pub distance_type: DistanceType,
    pub fts: FtsConfig,
    pub vector_index: VectorIndexConfig,
}

impl ThreadVectorDBConfig {
    /// LanceDB location of thread vectors. Exposed separately because a
    /// pre-migration backup must copy exactly the directory the server uses.
    pub fn lancedb_uri_from(env: impl Fn(&str) -> Option<String>) -> String {
        env("THREAD_LANCEDB_URI")
            .or_else(|| env("MEMORY_LANCEDB_URI"))
            .unwrap_or_else(|| "data/lancedb/memories.lancedb".to_string())
    }

    /// Build from environment variables. THREAD_VECTOR_SIZE is required.
    pub fn from_env() -> anyhow::Result<Self> {
        let vector_size: usize = std::env::var("THREAD_VECTOR_SIZE")
            .or_else(|_| std::env::var("MEMORY_VECTOR_SIZE"))
            .map_err(|_| {
                anyhow::anyhow!(
                    "THREAD_VECTOR_SIZE (or MEMORY_VECTOR_SIZE) is required \
                     when THREAD_VECTOR_ENABLED=true"
                )
            })?
            .parse()?;

        let cfg = Self {
            uri: Self::lancedb_uri_from(|key| std::env::var(key).ok()),
            table_name: std::env::var("THREAD_LANCEDB_TABLE")
                .unwrap_or_else(|_| "threads".to_string()),
            vector_size,
            distance_type: match std::env::var("THREAD_DISTANCE_TYPE")
                .or_else(|_| std::env::var("MEMORY_DISTANCE_TYPE"))
                .unwrap_or_else(|_| "cosine".to_string())
                .as_str()
            {
                "l2" => DistanceType::L2,
                "dot" => DistanceType::Dot,
                _ => DistanceType::Cosine,
            },
            fts: FtsConfig::from_env()?,
            vector_index: VectorIndexConfig::from_env_with_prefixes(&["THREAD_", "MEMORY_"]),
        };
        crate::infra::search_index_maintenance::reject_legacy_environment()?;
        Ok(cfg)
    }
}
