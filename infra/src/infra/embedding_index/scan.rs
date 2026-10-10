//! Key-ordered scan that classifies every embedding target and finds
//! orphans, one page of entities at a time: an RDB page, the vector rows
//! of the same id range, and the index entries of that range. Nothing is
//! held for the whole corpus, so it scales to large stores.

use super::TableLabel;
use super::classify::{TargetState, classify};
use super::source_version::SourceVersion;
use super::table::{EmbeddingIndex, IndexEntry};
use crate::infra::embedding_dispatch::ImageSearchMode;
use crate::infra::embedding_space::SpaceId;
use crate::infra::embedding_target::{DerivedTarget, LinkedMedia, memory_targets, thread_target};
use crate::infra::media_object::rdb::{MediaObjectRepository, MediaObjectRepositoryImpl};
use anyhow::{Context as _, Result};
use arrow_array::{Array, Int32Array, Int64Array, StringArray};
use futures::StreamExt as _;
use infra_utils::infra::rdb::RdbPool;
use lancedb::Table;
use lancedb::query::{ExecutableQuery as _, QueryBase as _};
use std::collections::{BTreeMap, BTreeSet, HashMap};

/// A vector table taking part in a scan, with the index of its directory.
/// A table or index that does not exist yet scans as empty.
#[derive(Clone)]
pub struct ScanTable {
    pub label: TableLabel,
    pub table: Option<Table>,
    pub index: Option<EmbeddingIndex>,
}

#[derive(Debug, Clone)]
pub struct ScanConfig {
    pub space: SpaceId,
    pub image_search_mode: ImageSearchMode,
    pub max_content_len: usize,
    pub page_size: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScanItem {
    Target {
        table: TableLabel,
        entity_id: i64,
        vector_kind: &'static str,
        state: TargetState,
        version: SourceVersion,
        /// The index entry, for failure details.
        entry: Option<Box<IndexEntry>>,
    },
    /// An image memory with an empty body in a caption mode: not an
    /// embedding target yet, but its caption has to be generated.
    CaptionMissing {
        entity_id: i64,
        media_object_id: i64,
    },
    /// Vector rows or an index entry of something that is not an
    /// embedding target (deleted entity, kind no longer applicable).
    Orphan {
        table: TableLabel,
        entity_id: i64,
        vector_kind: String,
        has_rows: bool,
        has_entry: bool,
    },
}

/// Which RDB entities feed a scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Source {
    /// Memory rows feed the memory and reflection intent tables.
    Memories,
    Threads,
}

/// Pull-based scan: call [`Scanner::next_page`] until it returns `None`.
pub struct Scanner<'a> {
    pool: &'static RdbPool,
    media: &'a MediaObjectRepositoryImpl,
    config: ScanConfig,
    memory_tables: Vec<ScanTable>,
    thread_table: Option<ScanTable>,
    source: Option<Source>,
    cursor: i64,
}

impl<'a> Scanner<'a> {
    pub fn new(
        pool: &'static RdbPool,
        media: &'a MediaObjectRepositoryImpl,
        config: ScanConfig,
        tables: Vec<ScanTable>,
    ) -> Self {
        let (thread, memory): (Vec<_>, Vec<_>) = tables
            .into_iter()
            .partition(|t| t.label == TableLabel::Thread);
        let source = if !memory.is_empty() {
            Some(Source::Memories)
        } else if !thread.is_empty() {
            Some(Source::Threads)
        } else {
            None
        };
        Self {
            pool,
            media,
            config,
            memory_tables: memory,
            thread_table: thread.into_iter().next(),
            source,
            cursor: 0,
        }
    }

    /// Classified targets and orphans of the next id range.
    pub async fn next_page(&mut self) -> Result<Option<Vec<ScanItem>>> {
        let Some(source) = self.source else {
            return Ok(None);
        };
        let (ids, targets, captions) = match source {
            Source::Memories => self.memory_page().await?,
            Source::Threads => {
                let (ids, targets) = self.thread_page().await?;
                (ids, targets, Vec::new())
            }
        };
        let lo = self.cursor.saturating_add(1);
        // A short page is the last one: extend its range to the end so
        // rows and entries of entities beyond the last RDB row surface
        // as orphans.
        let last = (ids.len() as i64) < self.config.page_size;
        let hi = if last {
            i64::MAX
        } else {
            *ids.last().expect("a full page is not empty")
        };
        let tables: Vec<ScanTable> = match source {
            Source::Memories => self.memory_tables.clone(),
            Source::Threads => self.thread_table.clone().into_iter().collect(),
        };
        let mut items: Vec<ScanItem> = captions
            .into_iter()
            .map(|(entity_id, media_object_id)| ScanItem::CaptionMissing {
                entity_id,
                media_object_id,
            })
            .collect();
        for t in &tables {
            let wanted: Vec<&(i64, DerivedTarget)> =
                targets.iter().filter(|(_, d)| d.table == t.label).collect();
            items.extend(classify_range(t, lo, hi, &self.config.space, &wanted).await?);
        }
        self.cursor = hi;
        if last {
            self.source = match source {
                Source::Memories if self.thread_table.is_some() => {
                    self.cursor = 0;
                    Some(Source::Threads)
                }
                _ => None,
            };
        }
        Ok(Some(items))
    }

    #[allow(clippy::type_complexity)]
    async fn memory_page(&self) -> Result<(Vec<i64>, Vec<(i64, DerivedTarget)>, Vec<(i64, i64)>)> {
        let rows = fetch_memory_page(self.pool, self.cursor, self.config.page_size).await?;
        let media_ids: Vec<i64> = rows.iter().filter_map(|r| r.media_object_id).collect();
        let media: HashMap<i64, _> = if media_ids.is_empty() {
            HashMap::new()
        } else {
            self.media
                .find_by_ids(&media_ids)
                .await?
                .into_iter()
                .map(|m| (m.id, m))
                .collect()
        };
        let mut targets = Vec::new();
        let mut captions = Vec::new();
        let caption_mode = matches!(
            self.config.image_search_mode,
            ImageSearchMode::VlmCaption | ImageSearchMode::Both
        );
        for r in &rows {
            let linked = r
                .media_object_id
                .and_then(|id| media.get(&id))
                .map(|m| LinkedMedia {
                    id: m.id,
                    kind: m.kind,
                    storage_backend: &m.storage_backend,
                    sha256: m.sha256.as_deref(),
                    url: m.storage_uri.as_deref(),
                });
            for t in memory_targets(
                &r.content,
                r.role,
                r.content_type,
                r.metadata.as_deref(),
                linked,
                self.config.image_search_mode,
                self.config.max_content_len,
            ) {
                targets.push((r.id, t));
            }
            // Same gate as dispatch (role and media), so only memories
            // whose caption would be embedded get one generated.
            if caption_mode
                && r.content.trim().is_empty()
                && let Some(m) = linked
                && crate::infra::embedding_target::dispatch_kinds(
                    &r.content,
                    r.role,
                    r.content_type,
                    Some(m.kind),
                    Some(m.storage_backend),
                    self.config.image_search_mode,
                )
                .contains(&crate::infra::embedding_dispatch::DispatchKind::Media)
            {
                captions.push((r.id, m.id));
            }
        }
        Ok((rows.iter().map(|r| r.id).collect(), targets, captions))
    }

    async fn thread_page(&self) -> Result<(Vec<i64>, Vec<(i64, DerivedTarget)>)> {
        let rows = fetch_thread_page(self.pool, self.cursor, self.config.page_size).await?;
        let targets = rows
            .iter()
            .filter_map(|(id, desc)| {
                thread_target(desc.as_deref().unwrap_or(""), self.config.max_content_len)
                    .map(|t| (*id, t))
            })
            .collect();
        Ok((rows.iter().map(|(id, _)| *id).collect(), targets))
    }
}

async fn classify_range(
    t: &ScanTable,
    lo: i64,
    hi: i64,
    space: &SpaceId,
    wanted: &[&(i64, DerivedTarget)],
) -> Result<Vec<ScanItem>> {
    let mut rows = match &t.table {
        Some(table) => vector_rows(table, t.label, lo, hi).await?,
        None => BTreeMap::new(),
    };
    let found = match &t.index {
        Some(index) => index.entries_in_range(t.label, lo, hi).await?,
        None => Vec::new(),
    };
    let mut entries: BTreeMap<(i64, String), IndexEntry> = found
        .into_iter()
        .map(|e| ((e.entity_id, e.vector_kind.clone()), e))
        .collect();
    let mut out = Vec::with_capacity(wanted.len());
    for (id, target) in wanted {
        let key = (*id, target.vector_kind.to_string());
        let chunks = rows.remove(&key).unwrap_or_default();
        let entry = entries.remove(&key);
        out.push(ScanItem::Target {
            table: t.label,
            entity_id: *id,
            vector_kind: target.vector_kind,
            state: classify(space, &target.version, entry.as_ref(), &chunks),
            version: target.version.clone(),
            entry: entry.map(Box::new),
        });
    }
    let orphan_keys: BTreeSet<(i64, String)> = rows.keys().chain(entries.keys()).cloned().collect();
    for (entity_id, vector_kind) in orphan_keys {
        let has_rows = rows.contains_key(&(entity_id, vector_kind.clone()));
        let has_entry = entries.contains_key(&(entity_id, vector_kind.clone()));
        out.push(ScanItem::Orphan {
            table: t.label,
            entity_id,
            vector_kind,
            has_rows,
            has_entry,
        });
    }
    Ok(out)
}

/// Chunk indexes of one target's vector rows.
pub async fn target_chunk_indexes(
    table: &Table,
    label: TableLabel,
    entity_id: i64,
    vector_kind: &str,
) -> Result<Vec<i32>> {
    Ok(vector_rows(table, label, entity_id, entity_id)
        .await?
        .remove(&(entity_id, vector_kind.to_string()))
        .unwrap_or_default())
}

/// Chunk indexes per `(entity, kind)` of the vector rows in `lo..=hi`.
async fn vector_rows(
    table: &Table,
    label: TableLabel,
    lo: i64,
    hi: i64,
) -> Result<BTreeMap<(i64, String), Vec<i32>>> {
    let id = label.id_column();
    let mut stream = table
        .query()
        .only_if(format!("{id} >= {lo} AND {id} <= {hi}"))
        .select(lancedb::query::Select::columns(&[
            id,
            "vector_kind",
            "chunk_index",
        ]))
        .execute()
        .await
        .context("vector row scan failed")?;
    let mut out: BTreeMap<(i64, String), Vec<i32>> = BTreeMap::new();
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        let ids = batch
            .column_by_name(id)
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
            .context("vector id column missing")?;
        let kinds = batch
            .column_by_name("vector_kind")
            .and_then(|c| c.as_any().downcast_ref::<StringArray>())
            .context("vector_kind column missing")?;
        let chunks = batch
            .column_by_name("chunk_index")
            .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
            .context("chunk_index column missing")?;
        for i in 0..batch.num_rows() {
            out.entry((ids.value(i), kinds.value(i).to_string()))
                .or_default()
                .push(chunks.value(i));
        }
    }
    Ok(out)
}

struct MemoryScanRow {
    id: i64,
    content: String,
    content_type: i32,
    role: i32,
    metadata: Option<String>,
    media_object_id: Option<i64>,
}

#[cfg(feature = "postgres")]
const MEMORY_PAGE_SQL: &str = "SELECT id, content, content_type, role, metadata::text AS metadata, \
     media_object_id FROM memory WHERE id > $1 ORDER BY id LIMIT $2";
#[cfg(not(feature = "postgres"))]
const MEMORY_PAGE_SQL: &str = "SELECT id, content, content_type, role, metadata, \
     media_object_id FROM memory WHERE id > ? ORDER BY id LIMIT ?";

#[cfg(feature = "postgres")]
const THREAD_PAGE_SQL: &str =
    "SELECT id, description FROM thread WHERE id > $1 ORDER BY id LIMIT $2";
#[cfg(not(feature = "postgres"))]
const THREAD_PAGE_SQL: &str = "SELECT id, description FROM thread WHERE id > ? ORDER BY id LIMIT ?";

async fn fetch_memory_page(pool: &RdbPool, after: i64, limit: i64) -> Result<Vec<MemoryScanRow>> {
    #[allow(clippy::type_complexity)]
    let rows: Vec<(i64, String, i32, i32, Option<String>, Option<i64>)> =
        sqlx::query_as(MEMORY_PAGE_SQL)
            .bind(after)
            .bind(limit)
            .fetch_all(pool)
            .await
            .context("memory scan page failed")?;
    Ok(rows
        .into_iter()
        .map(
            |(id, content, content_type, role, metadata, media_object_id)| MemoryScanRow {
                id,
                content,
                content_type,
                role,
                metadata,
                media_object_id,
            },
        )
        .collect())
}

async fn fetch_thread_page(
    pool: &RdbPool,
    after: i64,
    limit: i64,
) -> Result<Vec<(i64, Option<String>)>> {
    sqlx::query_as(THREAD_PAGE_SQL)
        .bind(after)
        .bind(limit)
        .fetch_all(pool)
        .await
        .context("thread scan page failed")
}

/// Which of `ids` still exist in the RDB (memory or thread rows).
async fn existing_ids(
    pool: &'static RdbPool,
    table: TableLabel,
    ids: &[i64],
) -> Result<BTreeSet<i64>> {
    let rdb_table = match table {
        TableLabel::Memory | TableLabel::ReflectionIntent => "memory",
        TableLabel::Thread => "thread",
    };
    let mut found = BTreeSet::new();
    for chunk in ids.chunks(crate::sql::IN_LIST_CHUNK_SIZE) {
        let list = chunk
            .iter()
            .map(i64::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        // Both values are produced here (a fixed name and integers).
        let sql = format!("SELECT id FROM {rdb_table} WHERE id IN ({list})");
        let rows: Vec<i64> = sqlx::query_scalar(sqlx::AssertSqlSafe(sql))
            .fetch_all(pool)
            .await?;
        found.extend(rows);
    }
    Ok(found)
}

/// `(entity_id, has_rows, has_entry)` of one orphan.
type OrphanItem = (i64, bool, bool);

/// Delete orphans (vector rows and index entries of things that are not
/// embedding targets) found by a full scan; with `deleted_only`, only
/// those of entities no longer in the RDB. Returns how many
/// `(table, entity, kind)` keys were removed. Used where no repository is
/// at hand, e.g. after restoring a backup taken before RDB deletions.
pub async fn delete_orphans(
    pool: &'static RdbPool,
    deleted_only: bool,
    media: &MediaObjectRepositoryImpl,
    config: ScanConfig,
    tables: Vec<ScanTable>,
) -> Result<u64> {
    let handles: Vec<(TableLabel, Option<Table>, Option<EmbeddingIndex>)> = tables
        .iter()
        .map(|t| (t.label, t.table.clone(), t.index.clone()))
        .collect();
    let mut scanner = Scanner::new(pool, media, config, tables);
    let mut removed = 0;
    while let Some(page) = scanner.next_page().await? {
        // Batched per page: one existence query per table and one row
        // delete per (table, kind), instead of one of each per orphan.
        let mut orphans: BTreeMap<(TableLabel, String), Vec<OrphanItem>> = BTreeMap::new();
        for item in page {
            if let ScanItem::Orphan {
                table,
                entity_id,
                vector_kind,
                has_rows,
                has_entry,
            } = item
            {
                orphans
                    .entry((table, vector_kind))
                    .or_default()
                    .push((entity_id, has_rows, has_entry));
            }
        }
        for ((table, kind), mut items) in orphans {
            let Some((_, rows, index)) = handles.iter().find(|(l, _, _)| *l == table) else {
                continue;
            };
            if deleted_only {
                let ids: Vec<i64> = items.iter().map(|(id, _, _)| *id).collect();
                let existing = existing_ids(pool, table, &ids).await?;
                items.retain(|(id, _, _)| !existing.contains(id));
            }
            let with_rows: Vec<String> = items
                .iter()
                .filter(|(_, has_rows, _)| *has_rows)
                .map(|(id, _, _)| id.to_string())
                .collect();
            if let Some(rows) = rows
                && !with_rows.is_empty()
            {
                rows.delete(&format!(
                    "{} IN ({}) AND vector_kind = '{}'",
                    table.id_column(),
                    with_rows.join(", "),
                    kind.replace('\'', "''")
                ))
                .await
                .with_context(|| format!("deleting orphan rows of {}", table.as_str()))?;
            }
            if let Some(index) = index {
                for (id, _, _) in items.iter().filter(|(_, _, has_entry)| *has_entry) {
                    index.delete(table, *id, &[kind.as_str()]).await?;
                }
            }
            removed += items.len() as u64;
        }
    }
    Ok(removed)
}

#[cfg(test)]
mod tests;
