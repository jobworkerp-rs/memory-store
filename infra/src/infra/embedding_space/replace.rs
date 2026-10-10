//! Replacement of vector tables and their embedding index during an
//! embedding migration: an empty table of the target dimension carrying
//! the given space record and migration marker.

use super::record::{MigrationMarker, SpaceRecord, write_marker, write_space_record};
use crate::infra::embedding_index::TableLabel;
use crate::infra::vector_table;
use anyhow::Result;

/// A configured vector table.
#[derive(Debug, Clone)]
pub struct StoreSpec {
    pub label: TableLabel,
    pub uri: String,
    pub table_name: String,
}

fn schema(label: TableLabel, dimension: usize) -> std::sync::Arc<arrow_schema::Schema> {
    match label {
        TableLabel::Memory => crate::infra::memory_vector::schema::memory_arrow_schema(dimension),
        TableLabel::Thread => crate::infra::thread_vector::schema::thread_arrow_schema(dimension),
        TableLabel::ReflectionIntent => {
            crate::infra::reflection_intent_vector::schema::intent_arrow_schema(dimension)
        }
    }
}

/// The table if it exists, otherwise a new empty one of `dimension`.
pub async fn open_or_create(spec: &StoreSpec, dimension: usize) -> Result<vector_table::Table> {
    Ok(
        vector_table::open_or_create(&spec.uri, &spec.table_name, &schema(spec.label, dimension))
            .await?
            .table,
    )
}

/// Drop the table and create it empty with `dimension`, its scalar
/// indexes, `space` (when given), and `marker`. Idempotent.
pub async fn replace_with_empty(
    spec: &StoreSpec,
    dimension: usize,
    space: Option<&SpaceRecord>,
    marker: Option<&MigrationMarker>,
) -> Result<vector_table::Table> {
    vector_table::drop_if_exists(&spec.uri, &spec.table_name).await?;
    let table =
        vector_table::open_or_create(&spec.uri, &spec.table_name, &schema(spec.label, dimension))
            .await?
            .table;
    match spec.label {
        TableLabel::Memory => {
            crate::infra::memory_vector::repository::MemoryVectorRepositoryImpl::create_btree_indexes(&table).await?
        }
        TableLabel::Thread => {
            crate::infra::thread_vector::repository::ThreadVectorRepositoryImpl::create_indexes(&table).await?
        }
        TableLabel::ReflectionIntent => {
            crate::infra::reflection_intent_vector::repository::create_scalar_indexes(&table).await?
        }
    }
    if let Some(space) = space {
        write_space_record(&table, space).await?;
    }
    write_marker(&table, marker).await?;
    Ok(table)
}

/// Drop the table if it exists. Idempotent.
pub async fn drop_table(spec: &StoreSpec) -> Result<()> {
    vector_table::drop_if_exists(&spec.uri, &spec.table_name).await
}

/// Empty the embedding index of `uri`. Idempotent.
pub async fn reset_index(uri: &str) -> Result<()> {
    vector_table::drop_if_exists(uri, crate::infra::embedding_index::table::INDEX_TABLE_NAME)
        .await?;
    crate::infra::embedding_index::EmbeddingIndex::open(uri).await?;
    Ok(())
}

/// Whether `table` has the schema of `spec`'s table at `dimension` (the
/// dimension is part of the vector column's type).
pub async fn schema_matches(
    spec: &StoreSpec,
    table: &vector_table::Table,
    dimension: usize,
) -> Result<bool> {
    let actual = table.schema().await?;
    Ok(vector_table::schema_fingerprint(&actual)
        == vector_table::schema_fingerprint(&schema(spec.label, dimension)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn schema_matches_only_the_dimension_it_was_created_with() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let spec = StoreSpec {
            label: TableLabel::Thread,
            uri: dir.path().to_string_lossy().to_string(),
            table_name: "threads".into(),
        };
        let table = replace_with_empty(&spec, 8, None, None).await?;
        assert!(schema_matches(&spec, &table, 8).await?);
        assert!(!schema_matches(&spec, &table, 4).await?);
        drop_table(&spec).await?;
        drop_table(&spec).await?;
        assert!(
            vector_table::open_existing(&spec.uri, &spec.table_name)
                .await?
                .is_none()
        );
        Ok(())
    }
}
