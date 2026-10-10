//! Table-level plumbing shared by every LanceDB vector store
//! (`memory_vector`, `thread_vector`, `reflection_intent_vector`).
//!
//! Each store owns its own schema, indexes, and query surface; what they
//! share is how a table is opened (or created empty), how its on-disk
//! schema is checked against the expected one, and how small key/value
//! records are persisted in the table manifest config.

use crate::infra::startup_error::StartupError;
use arrow_array::{RecordBatch, RecordBatchIterator};
use arrow_schema::{Field, Schema};
pub use lancedb::Table;
use lancedb::connection::Connection;
use std::collections::HashMap;
use std::sync::Arc;

/// A table opened by [`open_or_create`]. `is_new` is true when the table
/// did not exist and was created empty with the expected schema.
pub(crate) struct OpenedTable {
    pub connection: Connection,
    pub table: Table,
    pub is_new: bool,
}

/// Connect to `uri` and open `table_name`, creating it empty with
/// `schema` when it does not exist yet.
pub(crate) async fn open_or_create(
    uri: &str,
    table_name: &str,
    schema: &Arc<Schema>,
) -> anyhow::Result<OpenedTable> {
    let connection = lancedb::connect(uri)
        .execute()
        .await
        .map_err(|e| anyhow::anyhow!("LanceDB connect failed: {e}"))?;
    let (table, is_new) = match connection.open_table(table_name).execute().await {
        Ok(t) => (t, false),
        Err(_) => {
            let empty = RecordBatch::new_empty(schema.clone());
            let reader: Box<dyn arrow_array::RecordBatchReader + Send> =
                Box::new(RecordBatchIterator::new(vec![Ok(empty)], schema.clone()));
            let t = connection
                .create_table(table_name, reader)
                .execute()
                .await
                .map_err(|e| anyhow::anyhow!("LanceDB create_table failed: {e}"))?;
            (t, true)
        }
    };
    Ok(OpenedTable {
        connection,
        table,
        is_new,
    })
}

/// Open `table_name` in `uri` without creating anything: `Ok(None)` when
/// the directory or the table does not exist. For read-only callers that
/// must not change storage (e.g. `embedding inspect`).
pub async fn open_existing(uri: &str, table_name: &str) -> anyhow::Result<Option<Table>> {
    if let Some(dir) = crate::infra::embedding_space::storage::local_path(uri)
        && !dir.exists()
    {
        return Ok(None);
    }
    let connection = lancedb::connect(uri)
        .execute()
        .await
        .map_err(|e| anyhow::anyhow!("LanceDB connect failed: {e}"))?;
    let names = connection
        .table_names()
        .execute()
        .await
        .map_err(|e| anyhow::anyhow!("LanceDB table listing failed: {e}"))?;
    if !names.iter().any(|n| n == table_name) {
        return Ok(None);
    }
    let table = connection
        .open_table(table_name)
        .execute()
        .await
        .map_err(|e| anyhow::anyhow!("LanceDB open_table failed: {e}"))?;
    Ok(Some(table))
}

pub fn schema_fingerprint(schema: &Schema) -> String {
    schema
        .fields()
        .iter()
        .map(|field| field_fingerprint(field.as_ref()))
        .collect::<Vec<_>>()
        .join("|")
}

pub fn field_fingerprint(field: &Field) -> String {
    format!(
        "{}:{:?}:nullable={}",
        field.name(),
        field.data_type(),
        field.is_nullable()
    )
}

/// Compare an on-disk schema with the expected one. A mismatch is
/// returned as `StartupError::LancedbSchemaMismatch` wrapped in
/// `anyhow::Error`, because the startup path downcasts it into the
/// structured `fatal()` output that agent-app matches on by `code`.
pub(crate) fn check_schema_fingerprint(
    table_name: &str,
    uri: &str,
    expected: &Schema,
    actual: &Schema,
) -> anyhow::Result<()> {
    let expected_fp = schema_fingerprint(expected);
    let actual_fp = schema_fingerprint(actual);
    if actual_fp == expected_fp {
        return Ok(());
    }
    let expected_dim =
        crate::infra::memory_vector::schema::extract_embedding_dim_from_schema(expected)
            .unwrap_or(0);
    let actual_dim =
        crate::infra::memory_vector::schema::extract_embedding_dim_from_schema(actual).unwrap_or(0);
    Err(anyhow::Error::new(StartupError::LancedbSchemaMismatch {
        table: table_name.to_string(),
        uri: uri.to_string(),
        expected_dim,
        actual_dim,
        expected_fingerprint: expected_fp,
        actual_fingerprint: actual_fp,
    }))
}

/// Read the table's current schema and check it with
/// [`check_schema_fingerprint`].
pub(crate) async fn verify_schema_fingerprint(
    table: &Table,
    table_name: &str,
    uri: &str,
    expected: &Schema,
) -> anyhow::Result<()> {
    let actual = table
        .schema()
        .await
        .map_err(|e| anyhow::anyhow!("LanceDB schema read failed: {e}"))?;
    check_schema_fingerprint(table_name, uri, expected, actual.as_ref())
}

/// Manifest config of a table, or `None` when the backend does not expose
/// a native manifest or the read fails. Callers treat `None` as "cannot
/// introspect" rather than "empty".
pub(crate) async fn read_manifest_config(table: &Table) -> Option<HashMap<String, String>> {
    let native = table.as_native()?;
    match native.manifest().await {
        Ok(m) => Some(m.config.clone()),
        Err(e) => {
            tracing::warn!("failed to read lancedb manifest config: {e}");
            None
        }
    }
}

/// Upsert `entries` into the table manifest config. Returns `Ok(false)`
/// without writing when the backend has no native manifest.
pub(crate) async fn write_manifest_config(
    table: &Table,
    entries: Vec<(String, String)>,
) -> anyhow::Result<bool> {
    let Some(native) = table.as_native() else {
        return Ok(false);
    };
    native
        .update_config(entries)
        .await
        .map_err(|e| anyhow::anyhow!("LanceDB manifest update_config failed: {e}"))?;
    Ok(true)
}

/// Create BTree indexes on `columns`, skipping those that already exist.
/// Other failures are logged: queries still work, only slower.
pub async fn ensure_btree_indexes(table: &Table, columns: &[&str]) {
    for column in columns {
        if let Err(e) = table
            .create_index(&[*column], lancedb::index::Index::BTree(Default::default()))
            .execute()
            .await
        {
            let msg = e.to_string();
            if !(msg.contains("already exists") || msg.contains("duplicate")) {
                tracing::warn!("failed to create BTree index on {}: {e}", column);
            }
        }
    }
}

/// Drop the table `name` of `uri` if it exists. Idempotent.
pub async fn drop_if_exists(uri: &str, name: &str) -> anyhow::Result<()> {
    use anyhow::Context as _;
    let connection = lancedb::connect(uri)
        .execute()
        .await
        .context("LanceDB connect failed")?;
    let names = connection.table_names().execute().await?;
    if names.iter().any(|n| n == name) {
        connection
            .drop_table(name, &[])
            .await
            .with_context(|| format!("dropping {name}"))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::DataType;

    fn schema(dim: i32) -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "embedding",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), dim),
                false,
            ),
        ]))
    }

    #[tokio::test]
    async fn open_or_create_creates_then_reopens() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let uri = dir.path().to_string_lossy().to_string();
        let first = open_or_create(&uri, "t", &schema(4)).await?;
        assert!(first.is_new);
        let second = open_or_create(&uri, "t", &schema(4)).await?;
        assert!(!second.is_new);
        Ok(())
    }

    #[tokio::test]
    async fn verify_schema_reports_structured_mismatch() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let uri = dir.path().to_string_lossy().to_string();
        let opened = open_or_create(&uri, "t", &schema(4)).await?;
        verify_schema_fingerprint(&opened.table, "t", &uri, &schema(4)).await?;
        let err = verify_schema_fingerprint(&opened.table, "t", &uri, &schema(8))
            .await
            .expect_err("dimension change must be rejected");
        match err.downcast::<StartupError>() {
            Ok(StartupError::LancedbSchemaMismatch {
                expected_dim,
                actual_dim,
                table,
                ..
            }) => {
                assert_eq!((expected_dim, actual_dim), (8, 4));
                assert_eq!(table, "t");
            }
            other => panic!("unexpected error: {other:?}"),
        }
        Ok(())
    }

    #[tokio::test]
    async fn open_existing_never_creates() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let missing_dir = dir.path().join("absent").to_string_lossy().to_string();
        assert!(open_existing(&missing_dir, "t").await?.is_none());
        assert!(!dir.path().join("absent").exists());
        let uri = dir.path().to_string_lossy().to_string();
        assert!(open_existing(&uri, "t").await?.is_none());
        open_or_create(&uri, "t", &schema(4)).await?;
        assert!(open_existing(&uri, "t").await?.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn manifest_config_roundtrip_preserves_other_keys() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let uri = dir.path().to_string_lossy().to_string();
        let opened = open_or_create(&uri, "t", &schema(4)).await?;
        assert!(write_manifest_config(&opened.table, vec![("a".into(), "1".into())]).await?);
        assert!(write_manifest_config(&opened.table, vec![("b".into(), "2".into())]).await?);
        let config = read_manifest_config(&opened.table)
            .await
            .expect("native table exposes its manifest");
        assert_eq!(config.get("a").map(String::as_str), Some("1"));
        assert_eq!(config.get("b").map(String::as_str), Some("2"));
        Ok(())
    }
}
