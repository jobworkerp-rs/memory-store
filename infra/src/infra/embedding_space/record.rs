//! Space and migration-marker records kept in each vector table's
//! manifest config, so they are backed up and restored together with
//! the table they describe.

use super::{SpaceComponents, SpaceId};
use crate::infra::vector_table;
use lancedb::Table;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

const KEY_SPACE_ID: &str = "memories.embedding.space_id";
const KEY_SPACE_COMPONENTS: &str = "memories.embedding.space";
const KEY_LEGACY_ACCEPT: &str = "memories.embedding.legacy_accept";
const KEY_MARKER: &str = "memories.embedding.migration_marker";
const KEY_MARKER_ATTEMPT: &str = "memories.embedding.migration_attempt";
const KEY_REBUILD_CHUNKING: &str = "memories.embedding.rebuild_chunking";

/// The space a table's rows belong to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpaceRecord {
    pub space_id: SpaceId,
    /// `None` only when the stored component JSON cannot be parsed; the
    /// ID alone is authoritative for comparisons.
    pub components: Option<SpaceComponents>,
    /// Whether writes without a dispatch token may be accepted.
    pub legacy_accept: bool,
}

impl SpaceRecord {
    pub fn new(components: &SpaceComponents, legacy_accept: bool) -> Self {
        Self {
            space_id: components.space_id(),
            components: Some(components.clone()),
            legacy_accept,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MarkerState {
    Switching,
    Pending,
    Committing,
    Restoring,
    Discarding,
}

impl MarkerState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Switching => "switching",
            Self::Pending => "pending",
            Self::Committing => "committing",
            Self::Restoring => "restoring",
            Self::Discarding => "discarding",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        Some(match value {
            "switching" => Self::Switching,
            "pending" => Self::Pending,
            "committing" => Self::Committing,
            "restoring" => Self::Restoring,
            "discarding" => Self::Discarding,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MigrationMarker {
    pub state: MarkerState,
    pub attempt_id: String,
}

/// Everything memories keeps in one table's manifest config.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TableRecord {
    pub space: Option<SpaceRecord>,
    pub marker: Option<MigrationMarker>,
    /// Chunking settings pinned by the first start of a pending rebuild.
    pub rebuild_chunking: Option<String>,
}

impl TableRecord {
    pub fn from_config(config: &HashMap<String, String>) -> anyhow::Result<Self> {
        let space = config
            .get(KEY_SPACE_ID)
            .filter(|id| !id.is_empty())
            .map(|id| SpaceRecord {
                space_id: SpaceId(id.clone()),
                components: config
                    .get(KEY_SPACE_COMPONENTS)
                    .and_then(|c| serde_json::from_str(c).ok()),
                legacy_accept: config.get(KEY_LEGACY_ACCEPT).map(String::as_str) == Some("true"),
            });
        let marker = match config.get(KEY_MARKER).filter(|m| !m.is_empty()) {
            None => None,
            Some(raw) => Some(MigrationMarker {
                state: MarkerState::parse(raw)
                    .ok_or_else(|| anyhow::anyhow!("unknown embedding migration marker {raw:?}"))?,
                attempt_id: config.get(KEY_MARKER_ATTEMPT).cloned().unwrap_or_default(),
            }),
        };
        let rebuild_chunking = config
            .get(KEY_REBUILD_CHUNKING)
            .filter(|c| !c.is_empty())
            .cloned();
        Ok(Self {
            space,
            marker,
            rebuild_chunking,
        })
    }
}

fn space_entries(space: &SpaceRecord) -> Vec<(String, String)> {
    vec![
        (KEY_SPACE_ID.to_string(), space.space_id.0.clone()),
        (
            KEY_SPACE_COMPONENTS.to_string(),
            space
                .components
                .as_ref()
                .map(SpaceComponents::to_json)
                .unwrap_or_default(),
        ),
        (
            KEY_LEGACY_ACCEPT.to_string(),
            space.legacy_accept.to_string(),
        ),
    ]
}

fn marker_entries(marker: Option<&MigrationMarker>) -> Vec<(String, String)> {
    vec![
        (
            KEY_MARKER.to_string(),
            marker
                .map(|m| m.state.as_str().to_string())
                .unwrap_or_default(),
        ),
        (
            KEY_MARKER_ATTEMPT.to_string(),
            marker.map(|m| m.attempt_id.clone()).unwrap_or_default(),
        ),
    ]
}

/// Read a table's record. Fails when the backend cannot expose its
/// manifest: deciding startup without the record would be guessing.
pub async fn read_table_record(table: &Table) -> anyhow::Result<TableRecord> {
    let config = vector_table::read_manifest_config(table)
        .await
        .ok_or_else(|| anyhow::anyhow!("LanceDB table manifest is not readable"))?;
    TableRecord::from_config(&config)
}

pub async fn write_space_record(table: &Table, space: &SpaceRecord) -> anyhow::Result<()> {
    write_entries(table, space_entries(space)).await
}

/// Set or clear (`None`) a table's migration marker.
pub async fn write_marker(table: &Table, marker: Option<&MigrationMarker>) -> anyhow::Result<()> {
    write_entries(table, marker_entries(marker)).await
}

/// Pin (or clear with `None`) the chunking settings of a pending rebuild.
pub async fn write_rebuild_chunking(table: &Table, chunking: Option<&str>) -> anyhow::Result<()> {
    write_entries(
        table,
        vec![(
            KEY_REBUILD_CHUNKING.to_string(),
            chunking.unwrap_or_default().to_string(),
        )],
    )
    .await
}

async fn write_entries(table: &Table, entries: Vec<(String, String)>) -> anyhow::Result<()> {
    if vector_table::write_manifest_config(table, entries).await? {
        Ok(())
    } else {
        anyhow::bail!("LanceDB table manifest is not writable")
    }
}

impl From<&TableRecord> for protobuf::llm_memory::data::EmbeddingSpaceRecord {
    fn from(r: &TableRecord) -> Self {
        Self {
            space: r.space.as_ref().map(|s| {
                let c = s.components.clone().unwrap_or(SpaceComponents {
                    model_id: String::new(),
                    tokenizer_model_id: String::new(),
                    revision: String::new(),
                    dimension: 0,
                    distance: String::new(),
                });
                protobuf::llm_memory::data::EmbeddingSpace {
                    space_id: s.space_id.to_string(),
                    model_id: c.model_id,
                    tokenizer_model_id: c.tokenizer_model_id,
                    revision: c.revision,
                    dimension: c.dimension,
                    distance: c.distance,
                }
            }),
            legacy_accept: r.space.as_ref().is_some_and(|s| s.legacy_accept),
            migration_marker: r.marker.as_ref().map(|m| {
                protobuf::llm_memory::data::EmbeddingMigrationMarker {
                    state: m.state.as_str().to_string(),
                    attempt_id: m.attempt_id.clone(),
                }
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn components() -> SpaceComponents {
        SpaceComponents {
            model_id: "m".into(),
            tokenizer_model_id: String::new(),
            revision: "unversioned".into(),
            dimension: 4,
            distance: "cosine".into(),
        }
    }

    #[test]
    fn empty_config_has_no_record() {
        assert_eq!(
            TableRecord::from_config(&HashMap::new()).unwrap(),
            TableRecord::default()
        );
    }

    #[test]
    fn unknown_marker_is_an_error() {
        let config = HashMap::from([(KEY_MARKER.to_string(), "bogus".to_string())]);
        assert!(TableRecord::from_config(&config).is_err());
    }

    #[tokio::test]
    async fn record_roundtrip_through_table_manifest() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let uri = dir.path().to_string_lossy().to_string();
        let schema =
            std::sync::Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
                "id",
                arrow_schema::DataType::Int64,
                false,
            )]));
        let opened = vector_table::open_or_create(&uri, "t", &schema).await?;
        let table = opened.table;
        assert_eq!(read_table_record(&table).await?, TableRecord::default());

        let space = SpaceRecord::new(&components(), true);
        write_space_record(&table, &space).await?;
        let marker = MigrationMarker {
            state: MarkerState::Pending,
            attempt_id: "att-1".into(),
        };
        write_marker(&table, Some(&marker)).await?;
        let record = read_table_record(&table).await?;
        assert_eq!(record.space, Some(space));
        assert_eq!(record.marker, Some(marker));

        write_rebuild_chunking(&table, Some("{\"c\":1}")).await?;
        assert_eq!(
            read_table_record(&table).await?.rebuild_chunking.as_deref(),
            Some("{\"c\":1}")
        );
        write_rebuild_chunking(&table, None).await?;
        write_marker(&table, None).await?;
        let cleared = read_table_record(&table).await?;
        assert_eq!((cleared.marker, cleared.rebuild_chunking), (None, None));
        Ok(())
    }

    #[test]
    fn marker_state_strings_roundtrip() {
        for s in [
            MarkerState::Switching,
            MarkerState::Pending,
            MarkerState::Committing,
            MarkerState::Restoring,
            MarkerState::Discarding,
        ] {
            assert_eq!(MarkerState::parse(s.as_str()), Some(s));
        }
    }
}
