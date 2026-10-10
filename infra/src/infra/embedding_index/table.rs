//! The embedding index table. One per LanceDB directory, holding the
//! entries of every vector table in that directory, so it is backed up,
//! restored, replaced, and discarded together with those tables.

use super::TableLabel;
use super::source_version::SourceVersion;
use crate::infra::embedding_space::SpaceId;
use crate::infra::vector_table;
use anyhow::{Context as _, Result};
use arc_swap::ArcSwap;
use arrow_array::{Array, Int32Array, Int64Array, RecordBatch, RecordBatchIterator, StringArray};
use arrow_schema::{DataType, Field, Schema};
use futures::StreamExt as _;
use lancedb::Table;
use lancedb::query::{ExecutableQuery as _, QueryBase as _};
use std::sync::Arc;

pub const INDEX_TABLE_NAME: &str = "memories_embedding_index";
const KEY_COLUMNS: [&str; 3] = ["table_label", "entity_id", "vector_kind"];

fn schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("table_label", DataType::Utf8, false),
        Field::new("entity_id", DataType::Int64, false),
        Field::new("vector_kind", DataType::Utf8, false),
        Field::new("outcome", DataType::Utf8, false),
        Field::new("space_id", DataType::Utf8, false),
        Field::new("source_version", DataType::Utf8, false),
        Field::new("generation_id", DataType::Utf8, false),
        Field::new("chunk_count", DataType::Int32, true),
        Field::new("failure_reason", DataType::Utf8, true),
        Field::new("failure_class", DataType::Utf8, true),
        Field::new("media_digest", DataType::Utf8, true),
        Field::new("recorded_at", DataType::Int64, false),
    ]))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EntryOutcome {
    Success {
        chunk_count: u32,
    },
    /// `class` is `transient` or `permanent`.
    Failure {
        reason: String,
        class: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexEntry {
    pub table: TableLabel,
    pub entity_id: i64,
    pub vector_kind: String,
    pub space_id: SpaceId,
    pub source_version: SourceVersion,
    pub generation_id: String,
    pub outcome: EntryOutcome,
    /// Digest of URL media content as fetched by the workflow
    /// (diagnostics only).
    pub media_digest: Option<String>,
    pub recorded_at: i64,
}

/// Vector kinds are internal constants; reject anything that could break
/// out of a SQL string literal rather than escaping it.
fn checked_kind(kind: &str) -> Result<&str> {
    if !kind.is_empty() && kind.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
        Ok(kind)
    } else {
        anyhow::bail!("invalid vector kind {kind:?}")
    }
}

#[derive(Clone)]
pub struct EmbeddingIndex {
    table: Arc<ArcSwap<Table>>,
}

impl EmbeddingIndex {
    /// Open the index in the LanceDB directory `uri`, creating it empty
    /// when absent.
    pub async fn open(uri: &str) -> Result<Self> {
        let opened = vector_table::open_or_create(uri, INDEX_TABLE_NAME, &schema()).await?;
        vector_table::verify_schema_fingerprint(&opened.table, INDEX_TABLE_NAME, uri, &schema())
            .await?;
        // Every write and delete filters on the key columns.
        vector_table::ensure_btree_indexes(&opened.table, &KEY_COLUMNS).await;
        Ok(Self {
            table: Arc::new(ArcSwap::from_pointee(opened.table)),
        })
    }

    /// Open the index of `uri` if it exists, without creating it.
    pub async fn open_existing(uri: &str) -> Result<Option<Self>> {
        Ok(vector_table::open_existing(uri, INDEX_TABLE_NAME)
            .await?
            .map(|table| Self {
                table: Arc::new(ArcSwap::from_pointee(table)),
            }))
    }

    pub fn table_handle(&self) -> Table {
        (*self.table.load_full()).clone()
    }

    async fn reload(&self) -> Result<()> {
        self.table
            .load_full()
            .checkout_latest()
            .await
            .context("embedding index checkout_latest failed")?;
        Ok(())
    }

    /// Insert or replace the entry for its target (at most one entry per
    /// target).
    pub async fn put(&self, entries: &[IndexEntry]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let batch = to_batch(entries)?;
        let table = self.table.load_full();
        let mut merge = table.merge_insert(&KEY_COLUMNS);
        merge
            .when_matched_update_all(None)
            .when_not_matched_insert_all();
        merge
            .execute(Box::new(RecordBatchIterator::new(
                vec![Ok(batch)],
                schema(),
            )))
            .await
            .context("embedding index upsert failed")?;
        self.reload().await
    }

    /// Delete the entries of `entity_id` for `kinds` (all kinds when empty).
    pub async fn delete(&self, label: TableLabel, entity_id: i64, kinds: &[&str]) -> Result<()> {
        let mut filter = format!(
            "table_label = '{}' AND entity_id = {entity_id}",
            label.as_str()
        );
        if !kinds.is_empty() {
            let list = kinds
                .iter()
                .map(|k| checked_kind(k).map(|k| format!("'{k}'")))
                .collect::<Result<Vec<_>>>()?
                .join(", ");
            filter.push_str(&format!(" AND vector_kind IN ({list})"));
        }
        self.delete_where(&filter).await
    }

    /// Delete every entry of the given entities.
    pub async fn delete_entities(&self, label: TableLabel, entity_ids: &[i64]) -> Result<()> {
        for chunk in entity_ids.chunks(500) {
            let ids = chunk
                .iter()
                .map(i64::to_string)
                .collect::<Vec<_>>()
                .join(", ");
            self.delete_where(&format!(
                "table_label = '{}' AND entity_id IN ({ids})",
                label.as_str()
            ))
            .await?;
        }
        Ok(())
    }

    async fn delete_where(&self, filter: &str) -> Result<()> {
        self.table
            .load_full()
            .delete(filter)
            .await
            .context("embedding index delete failed")?;
        self.reload().await
    }

    /// Entries of `label` with `lo <= entity_id <= hi`.
    pub async fn entries_in_range(
        &self,
        label: TableLabel,
        lo: i64,
        hi: i64,
    ) -> Result<Vec<IndexEntry>> {
        let mut stream = self
            .table
            .load_full()
            .query()
            .only_if(format!(
                "table_label = '{}' AND entity_id >= {lo} AND entity_id <= {hi}",
                label.as_str()
            ))
            .execute()
            .await
            .context("embedding index query failed")?;
        let mut out = Vec::new();
        while let Some(batch) = stream.next().await {
            out.extend(from_batch(&batch?)?);
        }
        Ok(out)
    }

    /// Entries of `label` whose entity id is above `after` (for orphans
    /// beyond the last RDB entity).
    pub async fn entries_after(&self, label: TableLabel, after: i64) -> Result<Vec<IndexEntry>> {
        self.entries_in_range(label, after.saturating_add(1), i64::MAX)
            .await
    }

    pub async fn count(&self) -> Result<usize> {
        Ok(self.table.load_full().count_rows(None).await?)
    }

    /// Compact data files; run with the maintenance of the vector tables
    /// in the same directory, since every write rewrites entries.
    pub async fn compact(&self) -> Result<()> {
        self.optimize(lancedb::table::OptimizeAction::Compact {
            options: lancedb::table::CompactionOptions::default(),
            remap_options: None,
        })
        .await?;
        // Bring rows written since the last run into the key indexes.
        self.optimize(lancedb::table::OptimizeAction::Index(Default::default()))
            .await
    }

    /// Prune old versions, never deleting unverified files.
    pub async fn prune(&self, older_than_secs: u64) -> Result<()> {
        let older_than = i64::try_from(older_than_secs)
            .ok()
            .and_then(lancedb::table::Duration::try_seconds)
            .context("invalid prune retention")?;
        self.optimize(lancedb::table::OptimizeAction::Prune {
            older_than: Some(older_than),
            delete_unverified: Some(false),
            error_if_tagged_old_versions: None,
        })
        .await
    }

    async fn optimize(&self, action: lancedb::table::OptimizeAction) -> Result<()> {
        self.table
            .load_full()
            .optimize(action)
            .await
            .context("embedding index optimize failed")?;
        self.reload().await
    }

    /// Number of table versions, for checking that maintenance ran.
    pub async fn version(&self) -> Result<u64> {
        Ok(self.table.load_full().version().await?)
    }
}

fn to_batch(entries: &[IndexEntry]) -> Result<RecordBatch> {
    let col_str = |f: &dyn Fn(&IndexEntry) -> String| {
        Arc::new(StringArray::from(entries.iter().map(f).collect::<Vec<_>>())) as Arc<dyn Array>
    };
    let col_opt_str = |f: &dyn Fn(&IndexEntry) -> Option<String>| {
        Arc::new(StringArray::from(entries.iter().map(f).collect::<Vec<_>>())) as Arc<dyn Array>
    };
    for e in entries {
        checked_kind(&e.vector_kind)?;
    }
    Ok(RecordBatch::try_new(
        schema(),
        vec![
            col_str(&|e| e.table.as_str().to_string()),
            Arc::new(Int64Array::from(
                entries.iter().map(|e| e.entity_id).collect::<Vec<_>>(),
            )),
            col_str(&|e| e.vector_kind.clone()),
            col_str(&|e| match e.outcome {
                EntryOutcome::Success { .. } => "success".to_string(),
                EntryOutcome::Failure { .. } => "failure".to_string(),
            }),
            col_str(&|e| e.space_id.to_string()),
            col_str(&|e| e.source_version.to_string()),
            col_str(&|e| e.generation_id.clone()),
            Arc::new(Int32Array::from(
                entries
                    .iter()
                    .map(|e| match e.outcome {
                        EntryOutcome::Success { chunk_count } => {
                            Some(i32::try_from(chunk_count).unwrap_or(i32::MAX))
                        }
                        EntryOutcome::Failure { .. } => None,
                    })
                    .collect::<Vec<_>>(),
            )),
            col_opt_str(&|e| match &e.outcome {
                EntryOutcome::Failure { reason, .. } => Some(reason.clone()),
                EntryOutcome::Success { .. } => None,
            }),
            col_opt_str(&|e| match &e.outcome {
                EntryOutcome::Failure { class, .. } => Some(class.clone()),
                EntryOutcome::Success { .. } => None,
            }),
            col_opt_str(&|e| e.media_digest.clone()),
            Arc::new(Int64Array::from(
                entries.iter().map(|e| e.recorded_at).collect::<Vec<_>>(),
            )),
        ],
    )?)
}

fn from_batch(batch: &RecordBatch) -> Result<Vec<IndexEntry>> {
    fn strings<'a>(b: &'a RecordBatch, name: &str) -> Result<&'a StringArray> {
        b.column_by_name(name)
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .with_context(|| format!("embedding index column {name} missing"))
    }
    let label = strings(batch, "table_label")?;
    let kind = strings(batch, "vector_kind")?;
    let outcome = strings(batch, "outcome")?;
    let space = strings(batch, "space_id")?;
    let version = strings(batch, "source_version")?;
    let generation = strings(batch, "generation_id")?;
    let reason = strings(batch, "failure_reason")?;
    let class = strings(batch, "failure_class")?;
    let digest = strings(batch, "media_digest")?;
    let entity = batch
        .column_by_name("entity_id")
        .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
        .context("embedding index column entity_id missing")?;
    let chunks = batch
        .column_by_name("chunk_count")
        .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
        .context("embedding index column chunk_count missing")?;
    let recorded = batch
        .column_by_name("recorded_at")
        .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
        .context("embedding index column recorded_at missing")?;
    let opt = |a: &StringArray, i: usize| (!a.is_null(i)).then(|| a.value(i).to_string());
    (0..batch.num_rows())
        .map(|i| {
            Ok(IndexEntry {
                table: TableLabel::parse(label.value(i))
                    .with_context(|| format!("unknown table label {:?}", label.value(i)))?,
                entity_id: entity.value(i),
                vector_kind: kind.value(i).to_string(),
                space_id: SpaceId(space.value(i).to_string()),
                source_version: SourceVersion(version.value(i).to_string()),
                generation_id: generation.value(i).to_string(),
                outcome: match outcome.value(i) {
                    "success" => EntryOutcome::Success {
                        chunk_count: u32::try_from(chunks.value(i)).unwrap_or(0),
                    },
                    _ => EntryOutcome::Failure {
                        reason: opt(reason, i).unwrap_or_default(),
                        class: opt(class, i).unwrap_or_default(),
                    },
                },
                media_digest: opt(digest, i),
                recorded_at: recorded.value(i),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(label: TableLabel, id: i64, kind: &str, outcome: EntryOutcome) -> IndexEntry {
        IndexEntry {
            table: label,
            entity_id: id,
            vector_kind: kind.into(),
            space_id: SpaceId("s".into()),
            source_version: SourceVersion("v".into()),
            generation_id: format!("g{id}"),
            outcome,
            media_digest: None,
            recorded_at: 7,
        }
    }

    #[tokio::test]
    async fn put_replaces_the_single_entry_of_a_target() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let index = EmbeddingIndex::open(&dir.path().to_string_lossy()).await?;
        let ok = entry(
            TableLabel::Memory,
            1,
            "text",
            EntryOutcome::Success { chunk_count: 2 },
        );
        index.put(std::slice::from_ref(&ok)).await?;
        let failure = entry(
            TableLabel::Memory,
            1,
            "text",
            EntryOutcome::Failure {
                reason: "rejected".into(),
                class: "permanent".into(),
            },
        );
        index.put(std::slice::from_ref(&failure)).await?;
        assert_eq!(
            index.entries_in_range(TableLabel::Memory, 0, 10).await?,
            vec![failure]
        );
        Ok(())
    }

    #[tokio::test]
    async fn entries_are_separated_by_table_and_range() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let index = EmbeddingIndex::open(&dir.path().to_string_lossy()).await?;
        let s = |n| EntryOutcome::Success { chunk_count: n };
        index
            .put(&[
                entry(TableLabel::Memory, 1, "text", s(1)),
                entry(TableLabel::Memory, 1, "image", s(1)),
                entry(TableLabel::Memory, 5, "text", s(1)),
                entry(TableLabel::Thread, 1, "text", s(1)),
            ])
            .await?;
        assert_eq!(
            index
                .entries_in_range(TableLabel::Memory, 1, 4)
                .await?
                .len(),
            2
        );
        assert_eq!(index.entries_after(TableLabel::Memory, 1).await?.len(), 1);
        assert_eq!(
            index
                .entries_in_range(TableLabel::Thread, 0, 9)
                .await?
                .len(),
            1
        );

        index.delete(TableLabel::Memory, 1, &["image"]).await?;
        let left = index.entries_in_range(TableLabel::Memory, 1, 1).await?;
        assert_eq!(left.len(), 1);
        assert_eq!(left[0].vector_kind, "text");
        index.delete_entities(TableLabel::Memory, &[1, 5]).await?;
        assert!(
            index
                .entries_in_range(TableLabel::Memory, 0, 9)
                .await?
                .is_empty()
        );
        assert_eq!(index.count().await?, 1, "other tables' entries stay");
        Ok(())
    }

    #[tokio::test]
    async fn opening_next_to_existing_vector_tables_and_reopening() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let uri = dir.path().to_string_lossy().to_string();
        let other = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        vector_table::open_or_create(&uri, "memories", &other).await?;
        EmbeddingIndex::open(&uri)
            .await?
            .put(&[entry(
                TableLabel::Memory,
                1,
                "text",
                EntryOutcome::Success { chunk_count: 1 },
            )])
            .await?;
        let reopened = EmbeddingIndex::open(&uri).await?;
        assert_eq!(reopened.count().await?, 1);
        Ok(())
    }

    #[tokio::test]
    async fn compaction_rewrites_the_index_and_keeps_entries() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let index = EmbeddingIndex::open(&dir.path().to_string_lossy()).await?;
        for id in 0..3 {
            index
                .put(&[entry(
                    TableLabel::Memory,
                    id,
                    "text",
                    EntryOutcome::Success { chunk_count: 1 },
                )])
                .await?;
        }
        let before = index.version().await?;
        index.compact().await?;
        index.prune(0).await?;
        assert!(index.version().await? > before);
        assert_eq!(index.count().await?, 3);
        Ok(())
    }

    #[tokio::test]
    async fn rejects_kinds_that_are_not_identifiers() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let index = EmbeddingIndex::open(&dir.path().to_string_lossy()).await?;
        assert!(
            index
                .delete(TableLabel::Memory, 1, &["x' OR '1'='1"])
                .await
                .is_err()
        );
        Ok(())
    }
}
