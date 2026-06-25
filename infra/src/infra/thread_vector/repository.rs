use super::config::ThreadVectorDBConfig;
use super::record::ThreadVectorRecord;
use super::safe_filter::ThreadSafeFilter;
use super::schema::thread_arrow_schema;
use crate::infra::memory_vector::config::{
    FTS_FINGERPRINT_SCHEMA_VERSION, FTS_MANIFEST_KEY_SCHEMA_VERSION, LANCE_INDEX_VERSION,
};
use crate::infra::memory_vector::record::vector_kind;
use crate::infra::memory_vector::repository::HybridOptions;
pub use crate::infra::memory_vector::repository::HybridStrategy;
pub use crate::infra::memory_vector::repository::IndexStats;
use crate::infra::memory_vector::repository::apply_vector_query_options;
use crate::infra::memory_vector::repository::ensure_vector_index_inner;
use crate::infra::memory_vector::repository::{
    InFlightGuard, compact_with, create_index_with_retry, prune_with, try_claim_gate,
};
use arc_swap::ArcSwap;
use arrow_array::{
    Array, FixedSizeListArray, Float32Array, Int32Array, Int64Array, RecordBatch,
    RecordBatchIterator, StringArray,
    builder::{ListBuilder, StringBuilder},
};
use arrow_schema::{DataType, Field, Schema};
use futures::StreamExt;
use lancedb::Table;
use lancedb::index::{Index, IndexType};
use lancedb::query::{ExecutableQuery, QueryBase};
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use tokio::sync::Mutex;

/// Maximum candidate IDs for two-phase hybrid search IN filter.
const MAX_HYBRID_CANDIDATES: usize = 200;

/// FTS index column. N-row schema: BM25 indexes the per-chunk `content`
/// substring (not the whole `description`) so a multi-chunk thread has
/// every chunk searchable. Search folds chunk hits back to one per thread.
const FTS_INDEX_COLUMN: &str = "content";

/// Over-fetch factor for the distinct-thread COUNT path only. A thread
/// owns N chunk rows, so counting distinct threads up to `hard_cap`
/// requires reading more raw rows than `hard_cap`. The search path no
/// longer multiplies by this — it honors the caller's entity-level
/// `limit` and relies on the app's staged over-fetch loop (mirroring
/// memory_vector) so one long thread cannot starve top-k results.
const CHUNK_OVERFETCH: usize = 4;

/// Search hit with thread_id and scores
#[derive(Debug, Clone)]
pub struct ThreadVectorSearchHit {
    pub thread_id: i64,
    pub score: f32,
    pub distance: f32,
}

pub struct ThreadVectorRepositoryImpl {
    /// Lock-free table handle (see `MemoryVectorRepositoryImpl::table` for
    /// the `ArcSwap` rationale — readers never block on a `reload_table`).
    table: Arc<ArcSwap<Table>>,
    config: ThreadVectorDBConfig,
    /// Monotonic write counter shared by the two maintenance gates. See
    /// `MemoryVectorRepositoryImpl` for the two-gate rationale.
    operation_count: Arc<AtomicUsize>,
    /// `operation_count` value at which the prune path last fired.
    last_prune_count: Arc<AtomicUsize>,
    /// `operation_count` value at which the compact+index path last fired.
    last_compact_count: Arc<AtomicUsize>,
    last_optimized_at: Arc<AtomicI64>,
    /// Single-slot guard: at most one background compaction at a time.
    /// Mirrors `MemoryVectorRepositoryImpl::compact_in_flight`.
    compact_in_flight: Arc<AtomicBool>,
    fts_init_state: Arc<Mutex<Option<FtsInitState>>>,
    fts_init_ready: Arc<AtomicBool>,
    /// Vector (ANN) index gate. Same policy as the memory repository: a
    /// lock-free fast path plus a mutex-serialized one-time build. Shares
    /// the implementation via `ensure_vector_index_inner`. See
    /// `MemoryVectorRepositoryImpl` for the full rationale.
    vector_index_ready: Arc<AtomicBool>,
    /// Whether a real ANN index currently backs the `embedding` column;
    /// read lock-free on the hot path to gate `nprobes`.
    vector_index_active: Arc<AtomicBool>,
    /// Serializes vector-index ensure passes across concurrent searches.
    vector_index_lock: Arc<Mutex<()>>,
    /// Serializes index-creation DDL across the vector AND FTS paths so a
    /// `parallel_hybrid_search` `try_join!` cannot drive two overlapping
    /// `create_index` commits on the same table. Mirrors the memory repo's
    /// `index_ddl_lock`.
    index_ddl_lock: Arc<Mutex<()>>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
struct FtsInitState {
    fingerprint: String,
    ready_at_unix_ms: i64,
}

impl ThreadVectorRepositoryImpl {
    /// Mirrors `MemoryVectorRepositoryImpl::fts_config` so the app
    /// layer can re-tokenize a thread description against the same
    /// settings the BM25 inverted index runs on.
    pub fn fts_config(&self) -> &crate::infra::memory_vector::config::FtsConfig {
        &self.config.fts
    }

    pub async fn new(config: ThreadVectorDBConfig) -> anyhow::Result<Self> {
        // Connection is only needed to open/create the table; the repo keeps
        // the `Table` handle and refreshes it via `checkout_latest`, so the
        // `Connection` is not retained (see memory repo for the rationale).
        let database = lancedb::connect(&config.uri)
            .execute()
            .await
            .map_err(|e| anyhow::anyhow!("LanceDB connect failed: {e}"))?;

        let schema = thread_arrow_schema(config.vector_size);
        let (table, is_new) = match database.open_table(&config.table_name).execute().await {
            Ok(t) => (t, false),
            Err(_) => {
                let empty_batch = RecordBatch::new_empty(schema.clone());
                let reader: Box<dyn arrow_array::RecordBatchReader + Send> = Box::new(
                    RecordBatchIterator::new(vec![Ok(empty_batch)], schema.clone()),
                );
                let t = database
                    .create_table(&config.table_name, reader)
                    .execute()
                    .await
                    .map_err(|e| anyhow::anyhow!("LanceDB create_table failed: {e}"))?;
                (t, true)
            }
        };

        if !is_new {
            // Validate existing table schema matches expected schema
            let actual = table
                .schema()
                .await
                .map_err(|e| anyhow::anyhow!("LanceDB schema read failed: {e}"))?;
            let actual_arrow = actual.as_ref().clone();
            let expected_fp =
                crate::infra::memory_vector::repository::schema_fingerprint(schema.as_ref());
            let actual_fp =
                crate::infra::memory_vector::repository::schema_fingerprint(&actual_arrow);
            if actual_fp != expected_fp {
                // Surface as structured StartupError so the parent
                // process (agent-app) can route into the LanceDB-dim
                // recovery flow without parsing the message text.
                let expected_dim =
                    crate::infra::memory_vector::schema::extract_embedding_dim_from_schema(
                        schema.as_ref(),
                    )
                    .unwrap_or(0);
                let actual_dim =
                    crate::infra::memory_vector::schema::extract_embedding_dim_from_schema(
                        &actual_arrow,
                    )
                    .unwrap_or(0);
                return Err(anyhow::Error::new(
                    crate::infra::startup_error::StartupError::LancedbSchemaMismatch {
                        table: config.table_name.clone(),
                        uri: config.uri.clone(),
                        expected_dim,
                        actual_dim,
                        expected_fingerprint: expected_fp,
                        actual_fingerprint: actual_fp,
                    },
                ));
            }
        }

        // Ensure indexes exist for both new and existing tables.
        // Idempotent — skips "already exists" errors.
        Self::create_indexes(&table).await?;

        let repo = Self {
            table: Arc::new(ArcSwap::from_pointee(table)),
            config,
            operation_count: Arc::new(AtomicUsize::new(0)),
            last_prune_count: Arc::new(AtomicUsize::new(0)),
            last_compact_count: Arc::new(AtomicUsize::new(0)),
            last_optimized_at: Arc::new(AtomicI64::new(0)),
            compact_in_flight: Arc::new(AtomicBool::new(false)),
            fts_init_state: Arc::new(Mutex::new(None)),
            index_ddl_lock: Arc::new(Mutex::new(())),
            fts_init_ready: Arc::new(AtomicBool::new(false)),
            vector_index_ready: Arc::new(AtomicBool::new(false)),
            vector_index_active: Arc::new(AtomicBool::new(false)),
            vector_index_lock: Arc::new(Mutex::new(())),
        };

        tracing::info!(
            "Thread LanceDB initialized: uri={}, table={}, vector_size={}, new={}",
            repo.config.uri,
            repo.config.table_name,
            repo.config.vector_size,
            is_new
        );

        // Startup prune: clear the old-manifest backlog so the next boot is
        // fast. Best-effort; live data is protected by LanceDB's 7-day floor.
        // See `MemoryVectorRepositoryImpl::new` for the full rationale.
        if repo.config.optimize.prune_on_startup {
            tracing::info!("Thread LanceDB startup prune starting (clearing manifest backlog)...");
            if let Err(e) = repo.prune().await {
                tracing::warn!("Thread LanceDB startup prune failed (continuing): {e}");
            }
        }

        Ok(repo)
    }

    async fn create_indexes(table: &Table) -> anyhow::Result<()> {
        // BTree indexes on scalar filter columns. `vector_kind` /
        // `chunk_index` back the N-row replace_kinds delete and chunk-0
        // narrowing (count / scalar sync).
        let btree_columns = [
            "thread_id",
            "vector_kind",
            "chunk_index",
            "user_id",
            "created_at",
            "updated_at",
        ];
        for col_name in btree_columns {
            if let Err(e) = table
                .create_index(&[col_name], Index::BTree(Default::default()))
                .execute()
                .await
            {
                let msg = e.to_string();
                if msg.contains("already exists") || msg.contains("duplicate") {
                    continue;
                }
                tracing::warn!("Failed to create BTree index on {}: {}", col_name, e);
            }
        }

        // LABEL_LIST index on labels column for array_contains filtering
        if let Err(e) = table
            .create_index(&["labels"], Index::LabelList(Default::default()))
            .execute()
            .await
        {
            let msg = e.to_string();
            if !msg.contains("already exists") && !msg.contains("duplicate") {
                tracing::warn!("Failed to create LABEL_LIST index on labels: {}", e);
            }
        }

        tracing::info!("Ensured thread vector indexes");
        Ok(())
    }

    // ===== Write operations =====

    pub async fn upsert(&self, record: &ThreadVectorRecord) -> anyhow::Result<()> {
        if record.embedding.len() != self.config.vector_size {
            anyhow::bail!(
                "Embedding dimension mismatch: expected {}, got {}",
                self.config.vector_size,
                record.embedding.len()
            );
        }
        let schema = thread_arrow_schema(self.config.vector_size);
        let batch = Self::build_record_batch(
            std::slice::from_ref(record),
            &schema,
            self.config.vector_size,
        )?;

        let table = self.table.load_full();
        let mut merge = table.merge_insert(&["thread_id", "vector_kind", "chunk_index"]);
        merge
            .when_matched_update_all(None)
            .when_not_matched_insert_all();

        let records = Box::new(RecordBatchIterator::new(vec![Ok(batch)], schema));
        merge
            .execute(records)
            .await
            .map_err(|e| anyhow::anyhow!("LanceDB thread upsert failed: {e}"))?;

        drop(table);
        self.reload_table().await?;
        self.track_operation(1).await;
        Ok(())
    }

    pub async fn batch_upsert(&self, records: Vec<ThreadVectorRecord>) -> anyhow::Result<usize> {
        if records.is_empty() {
            return Ok(0);
        }
        for (i, rec) in records.iter().enumerate() {
            if rec.embedding.len() != self.config.vector_size {
                anyhow::bail!(
                    "Embedding dimension mismatch at index {}: expected {}, got {} (thread_id={})",
                    i,
                    self.config.vector_size,
                    rec.embedding.len(),
                    rec.thread_id
                );
            }
        }

        let schema = thread_arrow_schema(self.config.vector_size);
        let mut total = 0usize;

        for chunk in records.chunks(1000) {
            let batch = Self::build_record_batch(chunk, &schema, self.config.vector_size)?;
            let table = self.table.load_full();
            let mut merge = table.merge_insert(&["thread_id", "vector_kind", "chunk_index"]);
            merge
                .when_matched_update_all(None)
                .when_not_matched_insert_all();
            let iter = Box::new(RecordBatchIterator::new(vec![Ok(batch)], schema.clone()));
            merge
                .execute(iter)
                .await
                .map_err(|e| anyhow::anyhow!("LanceDB batch upsert failed: {e}"))?;
            total += chunk.len();
            drop(table);
        }

        self.reload_table().await?;
        self.track_operation(total).await;
        Ok(total)
    }

    pub async fn delete(&self, thread_id: i64) -> anyhow::Result<bool> {
        let filter = ThreadSafeFilter::thread_id(thread_id).to_sql()?;
        let table = self.table.load_full();
        table
            .delete(&filter)
            .await
            .map_err(|e| anyhow::anyhow!("LanceDB delete failed: {e}"))?;
        drop(table);
        self.reload_table().await?;
        // A delete creates a new version too, so it must drive maintenance —
        // otherwise a delete-heavy workload grows `_versions/` without ever
        // tripping the prune gate.
        self.track_operation(1).await;
        Ok(true)
    }

    /// N-row: replace a thread's rows for a set of `vector_kind`s, then
    /// insert the new chunk rows. Mirrors
    /// `MemoryVectorRepositoryImpl::replace_kinds_upsert`. The delete is
    /// scoped to `thread_id == t AND vector_kind IN (replace_kinds)` so a
    /// stale re-embedding (fewer chunks than before) does not leave
    /// orphan chunk rows. `replace_kinds` empty is rejected; an empty
    /// `records` is a valid stale-delete (re-embedding produced no rows).
    pub async fn replace_kinds_upsert(
        &self,
        thread_id: i64,
        replace_kinds: &[&str],
        records: Vec<ThreadVectorRecord>,
    ) -> anyhow::Result<usize> {
        if replace_kinds.is_empty() {
            anyhow::bail!("replace_kinds_upsert: replace_kinds must not be empty");
        }
        if let Some(bad) = records.iter().find(|r| r.thread_id != thread_id) {
            anyhow::bail!(
                "replace_kinds_upsert: all records must share thread_id={thread_id}, \
                 got {}",
                bad.thread_id
            );
        }

        let filter = ThreadSafeFilter::thread_id(thread_id)
            .and(ThreadSafeFilter::in_str_list("vector_kind", replace_kinds)?)
            .to_sql()?;
        {
            let table = self.table.load_full();
            table
                .delete(&filter)
                .await
                .map_err(|e| anyhow::anyhow!("LanceDB replace_kinds delete failed: {e}"))?;
            drop(table);
        }

        if records.is_empty() {
            self.reload_table().await?;
            // Stale-delete path: the delete above created a new version but
            // no batch_upsert follows to count it, so track it here. The
            // non-empty path is covered by batch_upsert's own tracking.
            self.track_operation(1).await;
            return Ok(0);
        }
        self.batch_upsert(records).await
    }

    /// Retrieve all chunk records for a thread (including embeddings).
    /// N-row: a thread owns N rows keyed by chunk_index; the scalar-sync
    /// path re-upserts every chunk so labels/channel/timestamps stay
    /// consistent across the whole thread, not just chunk 0.
    pub async fn find_records_by_thread_id(
        &self,
        thread_id: i64,
    ) -> anyhow::Result<Vec<ThreadVectorRecord>> {
        let filter = ThreadSafeFilter::thread_id(thread_id).to_sql()?;
        let table = self.table.load_full();
        let mut stream = table.query().only_if(filter).execute().await?;

        let mut records = Vec::new();
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result?;
            for row in 0..batch.num_rows() {
                records.push(Self::record_from_row(&batch, row)?);
            }
        }
        Ok(records)
    }

    /// Build a `ThreadVectorRecord` from one row of a full-column batch.
    fn record_from_row(batch: &RecordBatch, row: usize) -> anyhow::Result<ThreadVectorRecord> {
        let nullable_str = |col: &str| -> Option<String> {
            batch
                .column_by_name(col)
                .and_then(|c| c.as_any().downcast_ref::<StringArray>())
                .and_then(|a| {
                    if a.is_null(row) {
                        None
                    } else {
                        Some(a.value(row).to_string())
                    }
                })
        };
        let i32_col = |col: &str| -> i32 {
            batch
                .column_by_name(col)
                .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
                .map(|a| a.value(row))
                .unwrap_or(0)
        };

        let embedding = batch
            .column_by_name("embedding")
            .and_then(|c| c.as_any().downcast_ref::<FixedSizeListArray>())
            .map(|fsl| {
                fsl.value(row)
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .map(|a| a.values().to_vec())
                    .unwrap_or_default()
            })
            .unwrap_or_default();

        let labels = batch
            .column_by_name("labels")
            .and_then(|c| c.as_any().downcast_ref::<arrow_array::ListArray>())
            .map(|list_arr| {
                let values = list_arr.value(row);
                match values.as_any().downcast_ref::<StringArray>() {
                    Some(sa) => (0..sa.len())
                        .filter(|i| !sa.is_null(*i))
                        .map(|i| sa.value(i).to_string())
                        .collect(),
                    None => Vec::new(),
                }
            })
            .unwrap_or_default();

        Ok(ThreadVectorRecord {
            thread_id: Self::extract_i64(batch, "thread_id", row)?,
            vector_kind: nullable_str("vector_kind")
                .unwrap_or_else(|| vector_kind::TEXT.to_string()),
            chunk_index: i32_col("chunk_index"),
            begin_position: i32_col("begin_position"),
            end_position: i32_col("end_position"),
            user_id: Self::extract_i64(batch, "user_id", row)?,
            content: nullable_str("content").unwrap_or_default(),
            description: nullable_str("description"),
            labels,
            embedding,
            embedding_model: nullable_str("embedding_model"),
            channel: nullable_str("channel"),
            created_at: Self::extract_i64(batch, "created_at", row)?,
            updated_at: Self::extract_i64(batch, "updated_at", row)?,
            indexed_at: Self::extract_i64(batch, "indexed_at", row)?,
        })
    }

    /// Re-sync thread-level scalar columns (user_id / description /
    /// labels / channel / timestamps) onto every chunk row, preserving
    /// each chunk's embedding / offsets / content. N-row: a label/channel
    /// edit must touch all chunks because LanceDB search filters
    /// (`only_if`) evaluate these columns per row, and a chunk 1+ left
    /// with stale labels would leak from or miss those filters. Best-
    /// effort no-op when the thread has no rows yet.
    pub async fn sync_scalars(
        &self,
        thread_id: i64,
        data: &protobuf::llm_memory::data::ThreadData,
        labels: Vec<String>,
    ) -> anyhow::Result<()> {
        let existing = self.find_records_by_thread_id(thread_id).await?;
        if existing.is_empty() {
            return Ok(());
        }
        let updated: Vec<ThreadVectorRecord> = existing
            .into_iter()
            .map(|chunk| {
                ThreadVectorRecord::from_chunk_with_content(
                    thread_id,
                    data,
                    labels.clone(),
                    &chunk.embedding,
                    chunk.embedding_model.as_deref(),
                    &chunk.vector_kind,
                    chunk.chunk_index,
                    chunk.begin_position,
                    chunk.end_position,
                    chunk.content,
                )
            })
            .collect();
        self.batch_upsert(updated).await?;
        Ok(())
    }

    // ===== Search operations =====

    /// Ensure the vector (ANN) index on `embedding` is present, building it
    /// once the corpus passes `vector_index.min_rows`. Without it, every
    /// thread vector query is a brute-force full-table scan. Delegates to
    /// the shared `ensure_vector_index_inner` so memory- and thread-side
    /// index behavior stay identical.
    async fn ensure_vector_index(&self) -> anyhow::Result<()> {
        ensure_vector_index_inner(
            &self.table,
            self.config.vector_index,
            self.config.distance_type,
            &self.vector_index_ready,
            &self.vector_index_active,
            &self.vector_index_lock,
            &self.index_ddl_lock,
        )
        .await
    }

    pub async fn search_by_vector(
        &self,
        query_vector: &[f32],
        filter: Option<&ThreadSafeFilter>,
        limit: usize,
    ) -> anyhow::Result<Vec<ThreadVectorSearchHit>> {
        if query_vector.len() != self.config.vector_size {
            anyhow::bail!(
                "Vector size mismatch: expected {}, got {}",
                self.config.vector_size,
                query_vector.len()
            );
        }

        // Build (once) the ANN index so this query avoids a brute-force
        // full-table scan. Cheap lock-free fast path after the first call.
        self.ensure_vector_index().await?;

        let table = self.table.load_full();
        let mut query = apply_vector_query_options(
            table.query().nearest_to(query_vector)?,
            self.config.vector_index,
            self.config.distance_type,
            self.vector_index_active.load(Ordering::Acquire),
        );
        // N-row: `limit` is the caller's entity-level distinct target. The
        // app's staged over-fetch loop sizes it large enough that one long
        // thread (many chunk rows) cannot starve the result; fetching a
        // fixed multiple of raw chunk rows here would let such a thread
        // fill the page and collapse to a single hit after de-dup.
        query = query.limit(limit);

        if let Some(f) = filter {
            let sql = f.to_sql()?;
            query = query.only_if(sql);
        }

        let mut stream = query.execute().await?;
        let mut results = Vec::new();

        while let Some(batch_result) = stream.next().await {
            let batch = batch_result?;
            self.extract_search_hits(&batch, &mut results)?;
        }
        let mut results = dedup_thread_hits(results);
        results.truncate(limit);
        Ok(results)
    }

    pub async fn search_by_text(
        &self,
        query_text: &str,
        filter: Option<&ThreadSafeFilter>,
        limit: usize,
    ) -> anyhow::Result<Vec<ThreadVectorSearchHit>> {
        let mut attempted_recovery = false;
        // N-row: `limit` is the caller's entity-level distinct target (the
        // app's staged loop widens it). Fetch that many BM25 chunk rows,
        // then fold to one raw score per thread (max) before normalization
        // so the [0.1, 1.0] range is computed over thread-level scores.
        let fetch_limit = limit;
        let raw_results = loop {
            match self.run_bm25_query(query_text, filter, fetch_limit).await {
                Ok(hits) => break hits,
                Err(e) if !attempted_recovery && is_missing_fts_index_error(&e) => {
                    tracing::warn!(
                        "FTS index appears missing during search ({e}); \
                         resetting init gate and rebuilding once before retry"
                    );
                    {
                        let mut guard = self.fts_init_state.lock().await;
                        self.fts_init_ready.store(false, Ordering::Release);
                        *guard = None;
                        self.rebuild_fts_index_locked(&mut guard).await?;
                    }
                    attempted_recovery = true;
                    continue;
                }
                Err(e) => return Err(e),
            }
        };

        if raw_results.is_empty() {
            return Ok(Vec::new());
        }

        // Fold chunk rows to one max raw score per thread, preserving
        // first-appearance (rank) order.
        let mut order: Vec<i64> = Vec::new();
        let mut best: std::collections::HashMap<i64, f32> = std::collections::HashMap::new();
        for (id, score) in raw_results {
            match best.get_mut(&id) {
                None => {
                    order.push(id);
                    best.insert(id, score);
                }
                Some(existing) => {
                    if score > *existing {
                        *existing = score;
                    }
                }
            }
        }
        let folded: Vec<(i64, f32)> = order.iter().map(|id| (*id, best[id])).collect();

        // Min-Max normalization to [0.1, 1.0]
        let (min, max) = folded
            .iter()
            .map(|(_, s)| *s)
            .fold((f32::INFINITY, f32::NEG_INFINITY), |(mn, mx), s| {
                (mn.min(s), mx.max(s))
            });
        let range = max - min;

        let mut hits: Vec<ThreadVectorSearchHit> = folded
            .into_iter()
            .map(|(id, score)| {
                let normalized = if range < f32::EPSILON {
                    1.0 / (1.0 + (-score).exp())
                } else {
                    (score - min) / range * 0.9 + 0.1
                };
                ThreadVectorSearchHit {
                    thread_id: id,
                    score: normalized,
                    distance: score,
                }
            })
            .collect();
        hits.truncate(limit);
        Ok(hits)
    }

    pub async fn hybrid_search(
        &self,
        query_vector: &[f32],
        query_text: &str,
        filter: Option<&ThreadSafeFilter>,
        limit: usize,
        options: &HybridOptions,
    ) -> anyhow::Result<Vec<ThreadVectorSearchHit>> {
        match options.strategy {
            HybridStrategy::Rrf | HybridStrategy::Weighted => {
                self.parallel_hybrid_search(query_vector, query_text, filter, limit, options)
                    .await
            }
            HybridStrategy::VectorThenFts => {
                self.two_phase_hybrid_search(query_vector, query_text, filter, limit, options, true)
                    .await
            }
            HybridStrategy::FtsThenVector => {
                self.two_phase_hybrid_search(
                    query_vector,
                    query_text,
                    filter,
                    limit,
                    options,
                    false,
                )
                .await
            }
        }
    }

    async fn parallel_hybrid_search(
        &self,
        query_vector: &[f32],
        query_text: &str,
        filter: Option<&ThreadSafeFilter>,
        limit: usize,
        options: &HybridOptions,
    ) -> anyhow::Result<Vec<ThreadVectorSearchHit>> {
        let fetch_limit = limit * 2;

        let (vec_results, fts_results) = tokio::try_join!(
            self.search_by_vector(query_vector, filter, fetch_limit),
            self.search_by_text(query_text, filter, fetch_limit),
        )?;

        options.validate_vector_weight()?;
        match options.strategy {
            HybridStrategy::Rrf => {
                let rrf_k = options.rrf_k.unwrap_or(60.0);
                Self::merge_rrf(vec_results, fts_results, rrf_k, limit)
            }
            HybridStrategy::Weighted => {
                let vector_weight = options.vector_weight.unwrap_or(0.7);
                Self::merge_weighted(vec_results, fts_results, vector_weight, limit)
            }
            _ => unreachable!(),
        }
    }

    async fn two_phase_hybrid_search(
        &self,
        query_vector: &[f32],
        query_text: &str,
        filter: Option<&ThreadSafeFilter>,
        limit: usize,
        options: &HybridOptions,
        vector_first: bool,
    ) -> anyhow::Result<Vec<ThreadVectorSearchHit>> {
        let fetch_limit = limit * 2;
        options.validate_vector_weight()?;
        let vector_weight = options.vector_weight.unwrap_or(0.7);

        let primary_hits = if vector_first {
            self.search_by_vector(query_vector, filter, fetch_limit)
                .await?
        } else {
            self.search_by_text(query_text, filter, fetch_limit).await?
        };
        if primary_hits.is_empty() {
            return Ok(Vec::new());
        }

        let mut primary_hits = primary_hits;
        let mut candidate_ids: Vec<i64> = primary_hits.iter().map(|h| h.thread_id).collect();
        if candidate_ids.len() > MAX_HYBRID_CANDIDATES {
            candidate_ids.truncate(MAX_HYBRID_CANDIDATES);
            primary_hits.truncate(MAX_HYBRID_CANDIDATES);
        }

        let id_filter = ThreadSafeFilter::in_i64_list("thread_id", &candidate_ids)?;
        let combined_filter = match filter {
            Some(f) => f.clone().and(id_filter),
            None => id_filter,
        };
        let secondary_hits = if vector_first {
            self.search_by_text(query_text, Some(&combined_filter), fetch_limit)
                .await?
        } else {
            self.search_by_vector(query_vector, Some(&combined_filter), fetch_limit)
                .await?
        };

        let secondary_map: std::collections::HashMap<i64, f32> = secondary_hits
            .iter()
            .map(|h| (h.thread_id, h.score))
            .collect();

        let (primary_weight, secondary_weight) = if vector_first {
            (vector_weight, 1.0 - vector_weight)
        } else {
            (1.0 - vector_weight, vector_weight)
        };

        let mut merged: Vec<ThreadVectorSearchHit> = primary_hits
            .into_iter()
            .map(|h| {
                let sec_score = secondary_map.get(&h.thread_id).copied().unwrap_or(0.0);
                ThreadVectorSearchHit {
                    thread_id: h.thread_id,
                    score: h.score * primary_weight + sec_score * secondary_weight,
                    distance: 0.0,
                }
            })
            .collect();

        merged.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        merged.truncate(limit);
        Ok(merged)
    }

    fn merge_rrf(
        vec_hits: Vec<ThreadVectorSearchHit>,
        fts_hits: Vec<ThreadVectorSearchHit>,
        rrf_k: f32,
        limit: usize,
    ) -> anyhow::Result<Vec<ThreadVectorSearchHit>> {
        let mut scores: std::collections::HashMap<i64, f32> = std::collections::HashMap::new();
        for (rank, hit) in vec_hits.iter().enumerate() {
            *scores.entry(hit.thread_id).or_default() += 1.0 / (rrf_k + rank as f32 + 1.0);
        }
        for (rank, hit) in fts_hits.iter().enumerate() {
            *scores.entry(hit.thread_id).or_default() += 1.0 / (rrf_k + rank as f32 + 1.0);
        }

        let mut merged: Vec<ThreadVectorSearchHit> = scores
            .into_iter()
            .map(|(id, score)| ThreadVectorSearchHit {
                thread_id: id,
                score,
                distance: 0.0,
            })
            .collect();

        merged.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        merged.truncate(limit);
        Ok(merged)
    }

    fn merge_weighted(
        vec_hits: Vec<ThreadVectorSearchHit>,
        fts_hits: Vec<ThreadVectorSearchHit>,
        vector_weight: f32,
        limit: usize,
    ) -> anyhow::Result<Vec<ThreadVectorSearchHit>> {
        let fts_map: std::collections::HashMap<i64, f32> =
            fts_hits.iter().map(|h| (h.thread_id, h.score)).collect();
        let vec_map: std::collections::HashMap<i64, f32> =
            vec_hits.iter().map(|h| (h.thread_id, h.score)).collect();

        let mut all_ids: std::collections::HashSet<i64> = std::collections::HashSet::new();
        all_ids.extend(vec_map.keys());
        all_ids.extend(fts_map.keys());

        let mut merged: Vec<ThreadVectorSearchHit> = all_ids
            .into_iter()
            .map(|id| {
                let vs = vec_map.get(&id).copied().unwrap_or(0.0);
                let fs = fts_map.get(&id).copied().unwrap_or(0.0);
                ThreadVectorSearchHit {
                    thread_id: id,
                    score: vs * vector_weight + fs * (1.0 - vector_weight),
                    distance: 0.0,
                }
            })
            .collect();

        merged.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        merged.truncate(limit);
        Ok(merged)
    }

    // ===== Management =====

    /// Compact data files and fold unindexed rows into the ANN/FTS indices.
    /// The heavy maintenance path; does NOT prune (see [`prune`](Self::prune)).
    /// Updates `last_optimized_at` for the GetIndexStats RPC. Mirrors
    /// `MemoryVectorRepositoryImpl::compact_and_optimize_index`.
    pub async fn compact_and_optimize_index(&self) -> anyhow::Result<()> {
        // Shared with the spawned auto-compaction in `track_operation` via
        // the same free function the memory repository uses. Pass
        // `index_ddl_lock` so compaction serializes against `create_index`.
        compact_with(
            &self.table,
            &self.last_optimized_at,
            Some(&self.index_ddl_lock),
        )
        .await
    }

    /// Prune old manifests. Cheap; honors the configured retention (which
    /// bounds time-travel history only). Passes `delete_unverified: Some(false)`
    /// so LanceDB's 7-day floor always protects live data. Does NOT update
    /// `last_optimized_at`. Mirrors `MemoryVectorRepositoryImpl::prune`.
    pub async fn prune(&self) -> anyhow::Result<()> {
        prune_with(&self.table, &self.config.optimize).await
    }

    /// Backward-compatible "do everything" entry point: heavy path + prune.
    pub async fn optimize(&self) -> anyhow::Result<()> {
        self.compact_and_optimize_index().await?;
        self.prune().await?;
        Ok(())
    }

    pub async fn get_all_thread_ids(&self) -> anyhow::Result<Vec<i64>> {
        let table = self.table.load_full();
        let mut stream = table
            .query()
            .select(lancedb::query::Select::columns(&["thread_id"]))
            .execute()
            .await?;

        let mut ids = Vec::new();
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result?;
            if let Some(col) = batch
                .column_by_name("thread_id")
                .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
            {
                for i in 0..col.len() {
                    ids.push(col.value(i));
                }
            }
        }
        Ok(ids)
    }

    pub async fn get_stats(&self) -> anyhow::Result<IndexStats> {
        // N-row: a thread owns N chunk rows, so `count_rows(None)` would
        // report chunk rows, not embedded threads — inflating
        // records_with_embedding past the RDB thread total and zeroing
        // records_without_embedding. Count distinct threads via the
        // chunk-0 row (one per embedded thread).
        let table = self.table.load_full();
        let total = table
            .count_rows(Some(ThreadSafeFilter::chunk_index(0).to_sql()?))
            .await? as u64;
        let fts_tokenizer = self.config.fts.tokenizer;
        let (fts_ngram_min, fts_ngram_max) = if matches!(
            fts_tokenizer,
            crate::infra::memory_vector::config::FtsTokenizerKind::Ngram
        ) {
            (
                Some(self.config.fts.ngram_min),
                Some(self.config.fts.ngram_max),
            )
        } else {
            (None, None)
        };
        Ok(IndexStats {
            total_records: total,
            rdb_total_records: None,
            vector_dimension: self.config.vector_size as u32,
            distance_type: self.config.distance_type,
            last_optimized_at: self.last_optimized_at.load(Ordering::Relaxed),
            fts_tokenizer,
            fts_ngram_min,
            fts_ngram_max,
        })
    }

    // ===== Count operations (P2, Phase 5-1) =====

    /// FILTER_ONLY count via LanceDB `Table::count_rows(filter_sql)`.
    /// Mirrors `MemoryVectorRepositoryImpl::count_by_filter`; see that
    /// method for the contract details.
    pub async fn count_by_filter(
        &self,
        filter: Option<&ThreadSafeFilter>,
    ) -> anyhow::Result<(u64, bool)> {
        // N-row: `count_rows` counts chunk rows, so for a multi-chunk
        // thread this over-counts. Restrict to chunk_index 0 so the result
        // is exactly one row per thread, matching the FILTER_ONLY contract
        // (distinct entities). The chunk-0 row always exists for any
        // embedded thread.
        let table = self.table.load_full();
        let chunk0 = ThreadSafeFilter::chunk_index(0);
        let combined = match filter {
            Some(f) => f.clone().and(chunk0),
            None => chunk0,
        };
        let total = table.count_rows(Some(combined.to_sql()?)).await? as u64;
        Ok((total, false))
    }

    /// TEXT count via stream-and-count.
    pub async fn count_by_text(
        &self,
        query_text: &str,
        filter: Option<&ThreadSafeFilter>,
        hard_cap: u64,
    ) -> anyhow::Result<(u64, bool)> {
        let filter_sql = match filter {
            Some(f) => Some(f.to_sql()?),
            None => None,
        };
        self.run_bm25_count_query(query_text, filter_sql, hard_cap)
            .await
    }

    /// Count-only BM25 query. See the memory_vector counterpart for the
    /// projection / scoring contract — the only thread-side difference
    /// is `Select::columns(&["thread_id"])`.
    async fn run_bm25_count_query(
        &self,
        query_text: &str,
        filter_sql: Option<String>,
        hard_cap: u64,
    ) -> anyhow::Result<(u64, bool)> {
        self.ensure_fts_index().await?;

        let table = self.table.load_full();
        let fts_query = lance_index::scalar::FullTextSearchQuery::new(query_text.to_owned());
        let mut query = table
            .query()
            .full_text_search(fts_query)
            .select(lancedb::query::Select::columns(&["thread_id"]));

        if let Some(sql) = filter_sql {
            query = query.only_if(sql);
        }

        // N-row: a thread owns N chunk rows, so count distinct thread_id
        // (not raw rows). Over-fetch to keep the distinct-id cap meaningful.
        let fetch_limit = hard_cap
            .saturating_mul(CHUNK_OVERFETCH as u64)
            .saturating_add(1) as usize;
        let stream = query.limit(fetch_limit).execute().await?;
        count_distinct_thread_ids_capped(stream, hard_cap, fetch_limit).await
    }

    // ===== Count operations (P2, Phase 5-2: VECTOR / HYBRID) =====

    /// VECTOR count via stream-and-count of `nearest_to(...).execute()`.
    /// Same distinct-id + truncation contract as
    /// `MemoryVectorRepositoryImpl::count_by_vector`, but the thread side
    /// reads `hard_cap * CHUNK_OVERFETCH + 1` raw rows (not `hard_cap + 1`)
    /// so a thread owning many chunk rows cannot make the distinct-thread
    /// count fall short of the cap.
    pub async fn count_by_vector(
        &self,
        query_vector: &[f32],
        filter: Option<&ThreadSafeFilter>,
        hard_cap: u64,
    ) -> anyhow::Result<(u64, bool)> {
        if query_vector.len() != self.config.vector_size {
            anyhow::bail!(
                "Vector size mismatch: expected {}, got {}",
                self.config.vector_size,
                query_vector.len()
            );
        }

        // Build (once) the ANN index so this count avoids a brute-force
        // full-table scan (count paths can be the first vector traffic
        // after startup).
        self.ensure_vector_index().await?;

        let filter_sql = match filter {
            Some(f) => Some(f.to_sql()?),
            None => None,
        };
        self.run_vector_count_query(query_vector, filter_sql, hard_cap)
            .await
    }

    async fn run_vector_count_query(
        &self,
        query_vector: &[f32],
        filter_sql: Option<String>,
        hard_cap: u64,
    ) -> anyhow::Result<(u64, bool)> {
        let table = self.table.load_full();
        let mut query = apply_vector_query_options(
            table.query().nearest_to(query_vector)?,
            self.config.vector_index,
            self.config.distance_type,
            self.vector_index_active.load(Ordering::Acquire),
        )
        .select(lancedb::query::Select::columns(&["thread_id"]));

        if let Some(sql) = filter_sql {
            query = query.only_if(sql);
        }

        // N-row: count distinct thread_id, not raw chunk rows.
        let fetch_limit = hard_cap
            .saturating_mul(CHUNK_OVERFETCH as u64)
            .saturating_add(1) as usize;
        let stream = query.limit(fetch_limit).execute().await?;
        count_distinct_thread_ids_capped(stream, hard_cap, fetch_limit).await
    }

    pub async fn collect_vector_ids_capped(
        &self,
        query_vector: &[f32],
        filter: Option<&ThreadSafeFilter>,
        cap: u64,
    ) -> anyhow::Result<(HashSet<i64>, bool)> {
        if query_vector.len() != self.config.vector_size {
            anyhow::bail!(
                "Vector size mismatch: expected {}, got {}",
                self.config.vector_size,
                query_vector.len()
            );
        }

        // Build (once) the ANN index so this count avoids a brute-force
        // full-table scan (count paths can be the first vector traffic
        // after startup).
        self.ensure_vector_index().await?;

        let filter_sql = match filter {
            Some(f) => Some(f.to_sql()?),
            None => None,
        };

        let table = self.table.load_full();
        let mut query = apply_vector_query_options(
            table.query().nearest_to(query_vector)?,
            self.config.vector_index,
            self.config.distance_type,
            self.vector_index_active.load(Ordering::Acquire),
        )
        .select(lancedb::query::Select::columns(&["thread_id"]));

        if let Some(sql) = filter_sql {
            query = query.only_if(sql);
        }

        // N-row: read more raw rows than `cap` so distinct threads up to
        // `cap` can be collected despite one thread owning many chunks.
        let fetch_limit = cap.saturating_mul(CHUNK_OVERFETCH as u64).saturating_add(1) as usize;
        let stream = query.limit(fetch_limit).execute().await?;
        collect_distinct_thread_ids_capped(stream, cap, fetch_limit).await
    }

    pub async fn collect_fts_ids_capped(
        &self,
        query_text: &str,
        filter: Option<&ThreadSafeFilter>,
        cap: u64,
    ) -> anyhow::Result<(HashSet<i64>, bool)> {
        self.ensure_fts_index().await?;

        let filter_sql = match filter {
            Some(f) => Some(f.to_sql()?),
            None => None,
        };

        let table = self.table.load_full();
        let fts_query = lance_index::scalar::FullTextSearchQuery::new(query_text.to_owned());
        let mut query = table
            .query()
            .full_text_search(fts_query)
            .select(lancedb::query::Select::columns(&["thread_id"]));
        if let Some(sql) = filter_sql {
            query = query.only_if(sql);
        }

        // N-row: read more raw rows than `cap` so distinct threads up to
        // `cap` can be collected despite one thread owning many chunks.
        let fetch_limit = cap.saturating_mul(CHUNK_OVERFETCH as u64).saturating_add(1) as usize;
        let stream = query.limit(fetch_limit).execute().await?;
        collect_distinct_thread_ids_capped(stream, cap, fetch_limit).await
    }

    /// HYBRID count. See `MemoryVectorRepositoryImpl::count_by_hybrid`
    /// for the full contract: both parallel branches are capped at
    /// `vector_hard_cap` so the `MEMORY_COUNT_VECTOR_HARD_CAP` bound
    /// holds regardless of how `MEMORY_FTS_COUNT_HARD_CAP` is set.
    /// `fts_hard_cap` only matters for the `FtsThenVector` two-phase
    /// strategy. N-row: each branch's `collect_*_ids_capped` reads up to
    /// `vector_hard_cap * CHUNK_OVERFETCH + 1` raw rows (one `thread_id`
    /// column only) so chunk fan-out cannot make the distinct count fall
    /// short; with the default cap (1000) that is ~4k rows per branch.
    pub async fn count_by_hybrid(
        &self,
        query_vector: &[f32],
        query_text: &str,
        filter: Option<&ThreadSafeFilter>,
        options: &HybridOptions,
        vector_hard_cap: u64,
        fts_hard_cap: u64,
    ) -> anyhow::Result<(u64, bool)> {
        // Match the validation `parallel_hybrid_search` /
        // `two_phase_hybrid_search` perform on the search side so a
        // bad `vector_weight` cannot pass Count and then fail Search.
        options.validate_vector_weight()?;
        match options.strategy {
            HybridStrategy::Rrf | HybridStrategy::Weighted => {
                let (vec_res, fts_res) = tokio::try_join!(
                    self.collect_vector_ids_capped(query_vector, filter, vector_hard_cap),
                    self.collect_fts_ids_capped(query_text, filter, vector_hard_cap),
                )?;
                let (mut vec_ids, vec_trunc) = vec_res;
                let (fts_ids, fts_trunc) = fts_res;
                vec_ids.extend(fts_ids);
                let merged = vec_ids.len() as u64;
                let total = merged.min(vector_hard_cap);
                let is_truncated = vec_trunc || fts_trunc || merged > vector_hard_cap;
                Ok((total, is_truncated))
            }
            HybridStrategy::VectorThenFts => {
                self.count_by_vector(query_vector, filter, vector_hard_cap)
                    .await
            }
            HybridStrategy::FtsThenVector => {
                self.count_by_text(query_text, filter, fts_hard_cap).await
            }
        }
    }

    // ===== Internal helpers =====

    async fn run_bm25_query(
        &self,
        query_text: &str,
        filter: Option<&ThreadSafeFilter>,
        limit: usize,
    ) -> anyhow::Result<Vec<(i64, f32)>> {
        self.ensure_fts_index().await?;

        let table = self.table.load_full();
        let fts_query = lance_index::scalar::FullTextSearchQuery::new(query_text.to_owned());
        let mut query = table.query().full_text_search(fts_query);

        if let Some(f) = filter {
            query = query.only_if(f.to_sql()?);
        }

        let mut stream = query.limit(limit).execute().await?;
        let mut raw_results: Vec<(i64, f32)> = Vec::new();

        while let Some(batch_result) = stream.next().await {
            let batch = batch_result?;
            for row_idx in 0..batch.num_rows() {
                let thread_id = Self::extract_i64(&batch, "thread_id", row_idx)?;
                let bm25_score = batch
                    .column_by_name("_score")
                    .and_then(|c| c.as_any().downcast_ref::<Float32Array>())
                    .map(|a| a.value(row_idx))
                    .unwrap_or(0.0);
                raw_results.push((thread_id, bm25_score));
            }
        }
        Ok(raw_results)
    }

    async fn ensure_fts_index(&self) -> anyhow::Result<()> {
        if self.fts_init_ready.load(Ordering::Acquire) {
            return Ok(());
        }
        let mut guard = self.fts_init_state.lock().await;
        if guard.is_some() {
            self.fts_init_ready.store(true, Ordering::Release);
            return Ok(());
        }
        self.rebuild_fts_index_locked(&mut guard).await
    }

    async fn rebuild_fts_index_locked(
        &self,
        guard: &mut tokio::sync::MutexGuard<'_, Option<FtsInitState>>,
    ) -> anyhow::Result<()> {
        let current_fp = self
            .config
            .fts
            .fingerprint(LANCE_INDEX_VERSION, FTS_INDEX_COLUMN);

        use crate::infra::memory_vector::repository::{
            ManifestFingerprint, read_fts_manifest_fingerprint, write_fts_manifest_fingerprint,
        };

        let mut need_rebuild = false;
        let mut reason = "none";

        if self.config.fts.force_rebuild {
            tracing::warn!("FTS_FORCE_REBUILD=true: forcing thread FTS index rebuild");
            need_rebuild = true;
            reason = "force_rebuild";
        } else {
            // Check if FTS index exists
            let mut index_exists = false;
            {
                let table = self.table.load_full();
                match table.list_indices().await {
                    Ok(indices) => {
                        index_exists = indices.iter().any(|idx| {
                            idx.columns.iter().any(|c| c == FTS_INDEX_COLUMN)
                                && matches!(idx.index_type, IndexType::FTS)
                        });
                    }
                    Err(e) => {
                        tracing::warn!("list_indices failed: {e}");
                        need_rebuild = true;
                        reason = "list_indices_failed";
                    }
                }
            }
            if !index_exists && !need_rebuild {
                need_rebuild = true;
                reason = "index_missing";
            }

            // Manifest-config fingerprint comparison (drift detection)
            if !need_rebuild {
                let table = self.table.load_full();
                match read_fts_manifest_fingerprint(&table).await {
                    ManifestFingerprint::Match(saved_fp) if saved_fp == current_fp => {}
                    ManifestFingerprint::Match(_) => {
                        need_rebuild = true;
                        reason = "fingerprint_mismatch";
                    }
                    ManifestFingerprint::MissingFingerprint => {
                        need_rebuild = true;
                        reason = "fingerprint_missing";
                    }
                    ManifestFingerprint::SchemaVersionMismatch(found) => {
                        tracing::warn!(
                            "FTS manifest {FTS_MANIFEST_KEY_SCHEMA_VERSION}={found} \
                             differs from expected {FTS_FINGERPRINT_SCHEMA_VERSION}; \
                             rebuilding thread FTS index"
                        );
                        need_rebuild = true;
                        reason = "schema_version_mismatch";
                    }
                    ManifestFingerprint::ManifestUnavailable => {
                        tracing::warn!(
                            "Thread FTS config drift detection is DISABLED on this table backend: \
                             the manifest config API is unavailable, so changes to \
                             THREAD_FTS_* env vars will NOT trigger an automatic rebuild. \
                             Set THREAD_FTS_FORCE_REBUILD=true once after any such change."
                        );
                        reason = "manifest_api_unavailable";
                    }
                }
            }
        }

        if need_rebuild {
            tracing::info!("Building thread FTS index (reason={reason})");
            let builder = self.config.fts.to_builder();
            {
                // Serialize this DDL against a concurrent vector
                // `create_index` on the same table (the two can overlap
                // under `parallel_hybrid_search`). See `index_ddl_lock`.
                let _ddl = self.index_ddl_lock.lock().await;
                let table = self.table.load_full();
                // Refresh-then-retry: a concurrent write's reload_table waits
                // on index_ddl_lock, so this handle may be a pre-write snapshot
                // when the build starts. Mirror the memory FTS path so a
                // version race surfaces as a bounded retry, not a hard failure.
                create_index_with_retry("thread FTS", &table, || {
                    table
                        .create_index(&[FTS_INDEX_COLUMN], Index::FTS(builder.clone()))
                        .replace(true)
                        .execute()
                })
                .await
                .map_err(|e| anyhow::anyhow!("FTS index creation failed: {e}"))?;
            }

            // Write fingerprint to manifest (best-effort)
            {
                let table = self.table.load_full();
                if let Err(e) = write_fts_manifest_fingerprint(&table, &current_fp).await {
                    tracing::warn!("Failed to write thread FTS fingerprint to manifest: {e}");
                }
            }

            tracing::info!(
                "Thread FTS index built (reason={reason}, fingerprint={}..)",
                &current_fp[..current_fp.len().min(16)]
            );
        }

        **guard = Some(FtsInitState {
            fingerprint: current_fp,
            ready_at_unix_ms: command_utils::util::datetime::now_millis(),
        });
        self.fts_init_ready.store(true, Ordering::Release);
        Ok(())
    }

    fn extract_search_hits(
        &self,
        batch: &RecordBatch,
        results: &mut Vec<ThreadVectorSearchHit>,
    ) -> anyhow::Result<()> {
        for row_idx in 0..batch.num_rows() {
            let thread_id = Self::extract_i64(batch, "thread_id", row_idx)?;
            let distance = batch
                .column_by_name("_distance")
                .and_then(|c| c.as_any().downcast_ref::<Float32Array>())
                .map(|a| a.value(row_idx))
                .unwrap_or(0.0);

            // Convert distance to normalized score in [0, 1] (higher is better)
            let score = match self.config.distance_type {
                crate::infra::memory_vector::config::DistanceType::Cosine => {
                    (1.0 - distance).clamp(0.0, 1.0)
                }
                crate::infra::memory_vector::config::DistanceType::Dot => {
                    ((1.0 + distance) / 2.0).clamp(0.0, 1.0)
                }
                crate::infra::memory_vector::config::DistanceType::L2 => {
                    (1.0 / (1.0 + distance)).clamp(0.0, 1.0)
                }
            };

            results.push(ThreadVectorSearchHit {
                thread_id,
                score,
                distance,
            });
        }
        Ok(())
    }

    fn extract_i64(batch: &RecordBatch, column: &str, row: usize) -> anyhow::Result<i64> {
        batch
            .column_by_name(column)
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
            .map(|a| a.value(row))
            .ok_or_else(|| anyhow::anyhow!("Missing column: {column}"))
    }

    /// Build Arrow RecordBatch from ThreadVectorRecords.
    /// Unlike memory records, includes List<Utf8> labels column.
    fn build_record_batch(
        records: &[ThreadVectorRecord],
        schema: &Arc<Schema>,
        vector_size: usize,
    ) -> anyhow::Result<RecordBatch> {
        let thread_ids: Vec<i64> = records.iter().map(|r| r.thread_id).collect();
        let vector_kinds: Vec<&str> = records.iter().map(|r| r.vector_kind.as_str()).collect();
        let chunk_indexes: Vec<i32> = records.iter().map(|r| r.chunk_index).collect();
        let begin_positions: Vec<i32> = records.iter().map(|r| r.begin_position).collect();
        let end_positions: Vec<i32> = records.iter().map(|r| r.end_position).collect();
        let user_ids: Vec<i64> = records.iter().map(|r| r.user_id).collect();
        let contents: Vec<&str> = records.iter().map(|r| r.content.as_str()).collect();
        let descriptions: Vec<Option<&str>> =
            records.iter().map(|r| r.description.as_deref()).collect();
        let embedding_models: Vec<Option<&str>> = records
            .iter()
            .map(|r| r.embedding_model.as_deref())
            .collect();
        let channels: Vec<Option<&str>> = records.iter().map(|r| r.channel.as_deref()).collect();
        let created_ats: Vec<i64> = records.iter().map(|r| r.created_at).collect();
        let updated_ats: Vec<i64> = records.iter().map(|r| r.updated_at).collect();
        let indexed_ats: Vec<i64> = records.iter().map(|r| r.indexed_at).collect();

        // Build FixedSizeList for embeddings
        let flat_values: Vec<f32> = records
            .iter()
            .flat_map(|r| r.embedding.iter().copied())
            .collect();
        let values_array = Float32Array::from(flat_values);
        let list_field = Arc::new(Field::new("item", DataType::Float32, true));
        let embedding_array =
            FixedSizeListArray::new(list_field, vector_size as i32, Arc::new(values_array), None);

        // Build List<Utf8> for labels
        let mut labels_builder = ListBuilder::new(StringBuilder::new());
        for record in records {
            for label in &record.labels {
                labels_builder.values().append_value(label);
            }
            labels_builder.append(true);
        }
        let labels_array = labels_builder.finish();

        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(thread_ids)),
                Arc::new(StringArray::from(vector_kinds)),
                Arc::new(Int32Array::from(chunk_indexes)),
                Arc::new(Int32Array::from(begin_positions)),
                Arc::new(Int32Array::from(end_positions)),
                Arc::new(Int64Array::from(user_ids)),
                Arc::new(StringArray::from(contents)),
                Arc::new(StringArray::from(descriptions)),
                Arc::new(labels_array),
                Arc::new(embedding_array),
                Arc::new(StringArray::from(embedding_models)),
                Arc::new(StringArray::from(channels)),
                Arc::new(Int64Array::from(created_ats)),
                Arc::new(Int64Array::from(updated_ats)),
                Arc::new(Int64Array::from(indexed_ats)),
            ],
        )
        .map_err(|e| anyhow::anyhow!("Failed to build RecordBatch: {e}"))
    }

    /// Advance the single shared table handle to the latest committed version
    /// via `checkout_latest()`. Takes no `index_ddl_lock`, so it never waits
    /// behind a spawned compaction; the writer's own commit already published
    /// its version into this shared handle, and version advances are monotonic.
    /// Mirrors `MemoryVectorRepositoryImpl::reload_table` (see its docs for the
    /// single-handle / no-stale invariant).
    async fn reload_table(&self) -> anyhow::Result<()> {
        self.table
            .load_full()
            .checkout_latest()
            .await
            .map_err(|e| anyhow::anyhow!("LanceDB reload_table (checkout_latest) failed: {e}"))?;
        Ok(())
    }

    /// Track writes and trigger maintenance on two independent cadences.
    /// Mirrors `MemoryVectorRepositoryImpl::track_operation`: the heavy
    /// compact+index gate is **spawned** under an [`InFlightGuard`] (at most
    /// one background compaction), and prune is kept **ordered after** compact
    /// (run inside the same task when compaction fires, inline otherwise, and
    /// skipped while a compaction is in flight) so the two `optimize` commits
    /// never race on the same table.
    async fn track_operation(&self, count: usize) {
        let opt = &self.config.optimize;
        let now = self
            .operation_count
            .fetch_add(count, Ordering::Relaxed)
            .saturating_add(count);

        let prune_due = opt.prune_interval != 0
            && try_claim_gate(now, opt.prune_interval, &self.last_prune_count);

        if opt.compact_interval != 0
            && try_claim_gate(now, opt.compact_interval, &self.last_compact_count)
            && let Some(guard) = InFlightGuard::try_claim(&self.compact_in_flight)
        {
            let table = Arc::clone(&self.table);
            let last_optimized_at = Arc::clone(&self.last_optimized_at);
            let ddl_lock = Arc::clone(&self.index_ddl_lock);
            let optimize = self.config.optimize; // Copy
            tokio::spawn(async move {
                let _guard = guard;
                if let Err(e) = compact_with(&table, &last_optimized_at, Some(&ddl_lock)).await {
                    tracing::warn!("Auto-compact failed: {e}");
                }
                if prune_due && let Err(e) = prune_with(&table, &optimize).await {
                    tracing::warn!("Auto-prune failed: {e}");
                }
            });
            return;
        }

        if prune_due
            && !self.compact_in_flight.load(Ordering::Acquire)
            && let Err(e) = self.prune().await
        {
            tracing::warn!("Auto-prune failed: {e}");
        }
    }
}

fn is_missing_fts_index_error(e: &anyhow::Error) -> bool {
    let msg = e.to_string().to_lowercase();
    msg.contains("not found") || msg.contains("no fts") || msg.contains("does not exist")
}

/// N-row: drive a count-only stream projecting `thread_id` and return
/// `(distinct_thread_count, is_truncated)`.
///
/// Unlike `memory_vector::repository::count_distinct_capped`, the row
/// clip (LanceDB `limit` hit) is evaluated AFTER de-duping the batch, not
/// before. With the N-row schema a single thread owns many chunk rows, so
/// the ANN/FTS `limit` can be saturated by one long thread; checking the
/// raw `rows_seen` first would report `(hard_cap, true)` for a corpus of
/// one thread. Instead:
///   - distinct ids exceed `hard_cap` -> `(hard_cap, true)` (true count
///     is unknown and larger).
///   - row stream was clipped but distinct <= hard_cap -> `(distinct,
///     true)`: the distinct count seen is exact, but unread rows may add
///     more threads, so flag truncation while reporting the lower bound.
///   - stream exhausted under the clip -> `(distinct, false)`: exact.
async fn count_distinct_thread_ids_capped<S>(
    mut stream: S,
    hard_cap: u64,
    fetch_limit: usize,
) -> anyhow::Result<(u64, bool)>
where
    S: futures::Stream<Item = lancedb::error::Result<RecordBatch>> + Unpin,
{
    let mut ids: HashSet<i64> = HashSet::new();
    let mut rows_seen: usize = 0;
    while let Some(batch_result) = stream.next().await {
        let batch = batch_result?;
        rows_seen += batch.num_rows();
        let Some(col) = batch
            .column_by_name("thread_id")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
        else {
            anyhow::bail!("count query batch missing `thread_id` column");
        };
        for i in 0..col.len() {
            ids.insert(col.value(i));
            if ids.len() as u64 > hard_cap {
                return Ok((hard_cap, true));
            }
        }
        // The LanceDB `limit` clipped the row stream: distinct so far is
        // exact, but unread rows may add threads — report the lower bound
        // with the truncation flag rather than collapsing to `hard_cap`.
        if rows_seen >= fetch_limit {
            return Ok((ids.len() as u64, true));
        }
    }
    Ok((ids.len() as u64, false))
}

/// N-row: drive a count-only stream projecting `thread_id` and collect
/// the distinct `thread_id` set (for the HYBRID / multi-vector count
/// union). Returns `(ids, is_truncated)`. `is_truncated` is true when the
/// distinct count exceeded `cap` OR the LanceDB row `limit` clipped the
/// stream (unread rows may add more threads). The clip is evaluated after
/// de-duping each batch so a page saturated by one long thread's chunks
/// does not falsely report `false` truncation. Caller MUST size
/// `fetch_limit` (= `cap * CHUNK_OVERFETCH + 1`) so distinct ids up to
/// `cap` can be reached despite the chunk fan-out.
async fn collect_distinct_thread_ids_capped<S>(
    mut stream: S,
    cap: u64,
    fetch_limit: usize,
) -> anyhow::Result<(HashSet<i64>, bool)>
where
    S: futures::Stream<Item = lancedb::error::Result<RecordBatch>> + Unpin,
{
    let mut ids: HashSet<i64> = HashSet::new();
    let mut rows_seen: usize = 0;
    while let Some(batch_result) = stream.next().await {
        let batch = batch_result?;
        rows_seen += batch.num_rows();
        if let Some(col) = batch
            .column_by_name("thread_id")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>())
        {
            for i in 0..col.len() {
                ids.insert(col.value(i));
                if ids.len() as u64 > cap {
                    return Ok((ids, true));
                }
            }
        }
        if rows_seen >= fetch_limit {
            return Ok((ids, true));
        }
    }
    Ok((ids, false))
}

/// N-row: collapse chunk-level hits to one hit per thread, keeping the
/// max-score row and preserving first-appearance (rank) order. Mirrors
/// `app::app::memory_vector::dedup_vector_hits`.
fn dedup_thread_hits(hits: Vec<ThreadVectorSearchHit>) -> Vec<ThreadVectorSearchHit> {
    let mut order: Vec<i64> = Vec::new();
    let mut best: std::collections::HashMap<i64, ThreadVectorSearchHit> =
        std::collections::HashMap::new();
    for hit in hits {
        match best.get_mut(&hit.thread_id) {
            None => {
                order.push(hit.thread_id);
                best.insert(hit.thread_id, hit);
            }
            Some(existing) => {
                if hit.score > existing.score {
                    *existing = hit;
                }
            }
        }
    }
    order
        .into_iter()
        .filter_map(|id| best.remove(&id))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infra::memory_vector::config::{DistanceType, FtsConfig, VectorIndexConfig};

    fn search_hit(thread_id: i64, score: f32) -> ThreadVectorSearchHit {
        ThreadVectorSearchHit {
            thread_id,
            score,
            distance: 1.0 - score,
        }
    }

    /// N-row: chunk-level hits collapse to one per thread, max score kept,
    /// first-appearance order preserved.
    #[test]
    fn dedup_thread_hits_keeps_max_score_per_thread() {
        let deduped = dedup_thread_hits(vec![
            search_hit(1, 0.4),
            search_hit(2, 0.9),
            search_hit(1, 0.7),
        ]);
        assert_eq!(deduped.len(), 2);
        assert_eq!(deduped[0].thread_id, 1);
        assert_eq!(deduped[0].score, 0.7);
        assert_eq!(deduped[1].thread_id, 2);
    }

    #[test]
    fn dedup_thread_hits_empty_is_empty() {
        assert!(dedup_thread_hits(Vec::new()).is_empty());
    }

    fn thread_id_batch(ids: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "thread_id",
            DataType::Int64,
            false,
        )]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(ids))]).unwrap()
    }

    fn stream_of(
        batches: Vec<RecordBatch>,
    ) -> impl futures::Stream<Item = lancedb::error::Result<RecordBatch>> + Unpin {
        futures::stream::iter(batches.into_iter().map(Ok))
    }

    /// N-row regression: a corpus of one long thread whose chunk rows
    /// exactly fill the row `limit` must report `(1, true)` — the distinct
    /// thread lower bound — not `(hard_cap, true)`. The clip check runs
    /// after de-dup so the saturated page is not collapsed to the cap.
    #[tokio::test]
    async fn count_distinct_one_thread_filling_limit_is_not_capped() {
        let hard_cap = 10u64;
        let fetch_limit = (hard_cap as usize) * CHUNK_OVERFETCH + 1; // 41
        // One thread, `fetch_limit` chunk rows — the page is saturated.
        let batch = thread_id_batch(vec![1i64; fetch_limit]);
        let (total, truncated) =
            count_distinct_thread_ids_capped(stream_of(vec![batch]), hard_cap, fetch_limit)
                .await
                .unwrap();
        assert_eq!(total, 1, "distinct thread count is 1, not hard_cap");
        assert!(
            truncated,
            "row stream was clipped, so truncation is flagged"
        );
    }

    /// Distinct ids over the cap report `(hard_cap, true)`.
    #[tokio::test]
    async fn count_distinct_over_cap_reports_cap() {
        let hard_cap = 3u64;
        let fetch_limit = (hard_cap as usize) * CHUNK_OVERFETCH + 1;
        let batch = thread_id_batch(vec![1, 2, 3, 4, 5]);
        let (total, truncated) =
            count_distinct_thread_ids_capped(stream_of(vec![batch]), hard_cap, fetch_limit)
                .await
                .unwrap();
        assert_eq!(total, hard_cap);
        assert!(truncated);
    }

    /// Stream exhausted under the clip returns the exact distinct count.
    #[tokio::test]
    async fn count_distinct_exact_when_not_clipped() {
        let hard_cap = 10u64;
        let fetch_limit = (hard_cap as usize) * CHUNK_OVERFETCH + 1;
        let batch = thread_id_batch(vec![1, 1, 2, 2, 3]);
        let (total, truncated) =
            count_distinct_thread_ids_capped(stream_of(vec![batch]), hard_cap, fetch_limit)
                .await
                .unwrap();
        assert_eq!(total, 3);
        assert!(!truncated);
    }

    struct TestDb {
        path: String,
    }

    impl TestDb {
        fn config(dim: usize) -> (ThreadVectorDBConfig, Self) {
            let ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let path = format!("/tmp/test_thread_lancedb_{ts}");
            let config = ThreadVectorDBConfig {
                uri: path.clone(),
                table_name: "test_threads".to_string(),
                vector_size: dim,
                distance_type: DistanceType::Cosine,
                optimize: crate::infra::memory_vector::repository::test_optimize_config(),
                fts: FtsConfig::default(),
                vector_index: VectorIndexConfig::default(),
            };
            (config, Self { path })
        }

        fn config_with_vector_index(
            dim: usize,
            vector_index: VectorIndexConfig,
        ) -> (ThreadVectorDBConfig, Self) {
            // Timestamp + counter so concurrent tests can't collide on the
            // same LanceDB directory.
            static COUNTER: AtomicUsize = AtomicUsize::new(0);
            let ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let n = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = format!("/tmp/test_thread_lancedb_vidx_{ts}_{n}");
            let config = ThreadVectorDBConfig {
                uri: path.clone(),
                table_name: "test_threads".to_string(),
                vector_size: dim,
                distance_type: DistanceType::Cosine,
                optimize: crate::infra::memory_vector::repository::test_optimize_config(),
                fts: FtsConfig::default(),
                vector_index,
            };
            (config, Self { path })
        }
    }

    impl Drop for TestDb {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    async fn embedding_vector_index_count(repo: &ThreadVectorRepositoryImpl) -> usize {
        use crate::infra::memory_vector::repository::is_embedding_vector_index;
        let table = repo.table.load_full();
        table
            .list_indices()
            .await
            .unwrap()
            .iter()
            .filter(|idx| is_embedding_vector_index(idx))
            .count()
    }

    fn rand_chunk(thread_id: i64, dim: usize) -> ThreadVectorRecord {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        let embedding: Vec<f32> = (0..dim).map(|_| rng.gen_range(-1.0..1.0)).collect();
        chunk_record(thread_id, 0, embedding)
    }

    #[tokio::test]
    async fn thread_vector_index_built_above_threshold() {
        let dim = 32;
        let (config, _db) = TestDb::config_with_vector_index(
            dim,
            VectorIndexConfig {
                enabled: true,
                min_rows: 64,
                nprobes: 8,
            },
        );
        let repo = ThreadVectorRepositoryImpl::new(config).await.unwrap();
        let records: Vec<_> = (1..=256).map(|i| rand_chunk(i, dim)).collect();
        repo.batch_upsert(records).await.unwrap();

        assert_eq!(embedding_vector_index_count(&repo).await, 0);
        let probe: Vec<f32> = (0..dim).map(|_| 0.1).collect();
        let _ = repo.search_by_vector(&probe, None, 5).await.unwrap();

        assert_eq!(
            embedding_vector_index_count(&repo).await,
            1,
            "thread ANN index must build once row count >= min_rows"
        );
        assert!(repo.vector_index_active.load(Ordering::Acquire));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn thread_hybrid_build_survives_concurrent_writer() -> anyhow::Result<()> {
        // The first thread HybridSearch drives the vector AND FTS `create_index`
        // under try_join! while a writer streams merge_insert commits (each
        // advances the table version without taking index_ddl_lock). Under the
        // ArcSwap handle the FTS build can load a pre-write snapshot and commit
        // its CreateIndex against a version the writer moved underneath it; the
        // memory path already wraps both builds in create_index_with_retry
        // (checkout_latest + bounded retry) and the thread FTS path now does
        // too. This guards against a regression where the concurrent FTS build
        // deadlocks or hard-fails on a commit conflict instead of completing.
        // (The exact stale-snapshot interleaving is timing-dependent and not
        // forced deterministically here; the retry is what makes any such hit
        // recover rather than surface as an error.)
        let dim = 32;
        let (config, _db) = TestDb::config_with_vector_index(
            dim,
            VectorIndexConfig {
                enabled: true,
                min_rows: 256,
                nprobes: 8,
            },
        );
        let repo = Arc::new(ThreadVectorRepositoryImpl::new(config).await?);

        let base: Vec<f32> = (0..dim).map(|_| 0.1).collect();
        let records: Vec<_> = (1..=256).map(|i| rand_chunk(i, dim)).collect();
        repo.batch_upsert(records).await?;

        // Writer: a steady stream of merge_insert commits (each advances the
        // table version WITHOUT taking index_ddl_lock) racing the FTS build,
        // so the build — which loaded its handle, then holds index_ddl_lock —
        // is liable to commit its CreateIndex against a version the writer
        // moved underneath it.
        let writer = {
            let r = Arc::clone(&repo);
            tokio::spawn(async move {
                for i in 1000..1060 {
                    r.batch_upsert(vec![rand_chunk(i, dim)]).await?;
                }
                anyhow::Ok(())
            })
        };
        let searcher = {
            let r = Arc::clone(&repo);
            let q = base.clone();
            tokio::spawn(async move {
                let options = HybridOptions {
                    strategy: HybridStrategy::Rrf,
                    vector_weight: None,
                    rrf_k: Some(60.0),
                };
                // "chunk" matches every record's content -> FTS index is built.
                r.hybrid_search(&q, "chunk", None, 10, &options).await
            })
        };

        let joined = tokio::time::timeout(std::time::Duration::from_secs(30), async {
            let (w, s) = tokio::join!(writer, searcher);
            w.unwrap()?;
            s.unwrap()?;
            anyhow::Ok(())
        })
        .await;

        assert!(
            joined.is_ok(),
            "concurrent thread hybrid build + writer must not deadlock"
        );
        joined.unwrap()?;
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn thread_reload_does_not_block_writes_behind_index_ddl_lock() -> anyhow::Result<()> {
        // Regression (P1), mirror of the memory test: reload_table must not
        // take index_ddl_lock, so a write's trailing reload never blocks behind
        // a long compaction / create_index holding that lock. Hold the lock for
        // 3s and assert an upsert completes within 2s.
        let dim = 16;
        let (config, _db) = TestDb::config(dim);
        let repo = Arc::new(ThreadVectorRepositoryImpl::new(config).await?);
        repo.upsert(&rand_chunk(1, dim)).await?;

        let held = {
            let r = Arc::clone(&repo);
            tokio::spawn(async move {
                let _g = r.index_ddl_lock.lock().await;
                tokio::time::sleep(std::time::Duration::from_secs(3)).await;
            })
        };
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        tokio::time::timeout(
            std::time::Duration::from_secs(2),
            repo.upsert(&rand_chunk(2, dim)),
        )
        .await
        .expect("thread upsert/reload must not block behind a held index_ddl_lock")?;

        held.await.unwrap();
        Ok(())
    }

    #[tokio::test]
    async fn thread_vector_index_deferred_below_threshold() {
        let dim = 32;
        let (config, _db) = TestDb::config_with_vector_index(
            dim,
            VectorIndexConfig {
                enabled: true,
                min_rows: 1000,
                nprobes: 8,
            },
        );
        let repo = ThreadVectorRepositoryImpl::new(config).await.unwrap();
        let base: Vec<f32> = {
            let mut v = vec![0.0; dim];
            v[0] = 1.0;
            v
        };
        repo.batch_upsert(vec![
            chunk_record(1, 0, base.clone()),
            rand_chunk(2, dim),
            rand_chunk(3, dim),
        ])
        .await
        .unwrap();

        // Brute-force search still returns the exact match.
        let hits = repo.search_by_vector(&base, None, 3).await.unwrap();
        assert_eq!(hits.first().map(|h| h.thread_id), Some(1));
        assert_eq!(embedding_vector_index_count(&repo).await, 0);
        assert!(!repo.vector_index_active.load(Ordering::Acquire));
        // Below-threshold must not latch ready (would freeze brute-force).
        assert!(!repo.vector_index_ready.load(Ordering::Acquire));
    }

    fn chunk_record(thread_id: i64, chunk_index: i32, embedding: Vec<f32>) -> ThreadVectorRecord {
        ThreadVectorRecord {
            thread_id,
            vector_kind: vector_kind::TEXT.to_string(),
            chunk_index,
            begin_position: chunk_index * 10,
            end_position: chunk_index * 10 + 10,
            user_id: 1,
            content: format!("chunk {chunk_index}"),
            description: Some("desc".to_string()),
            labels: vec![],
            embedding,
            embedding_model: Some("test-model".to_string()),
            channel: None,
            created_at: 1,
            updated_at: 1,
            indexed_at: 1,
        }
    }

    /// N-row batch_upsert stores every chunk under the 3-column merge key.
    #[tokio::test]
    async fn batch_upsert_stores_all_chunks() {
        let (config, _db) = TestDb::config(4);
        let repo = ThreadVectorRepositoryImpl::new(config).await.unwrap();
        let n = repo
            .batch_upsert(vec![
                chunk_record(1, 0, vec![1.0, 0.0, 0.0, 0.0]),
                chunk_record(1, 1, vec![0.0, 1.0, 0.0, 0.0]),
                chunk_record(1, 2, vec![0.0, 0.0, 1.0, 0.0]),
            ])
            .await
            .unwrap();
        assert_eq!(n, 3);
    }

    /// replace_kinds_upsert drops stale chunks on a shorter re-embed.
    #[tokio::test]
    async fn replace_kinds_upsert_drops_stale_chunks() {
        let (config, _db) = TestDb::config(4);
        let repo = ThreadVectorRepositoryImpl::new(config).await.unwrap();
        repo.batch_upsert(vec![
            chunk_record(1, 0, vec![1.0, 0.0, 0.0, 0.0]),
            chunk_record(1, 1, vec![0.0, 1.0, 0.0, 0.0]),
            chunk_record(1, 2, vec![0.0, 0.0, 1.0, 0.0]),
        ])
        .await
        .unwrap();
        // count_by_filter is chunk-0-restricted, so it reports 1 thread.
        assert_eq!(repo.count_by_filter(None).await.unwrap().0, 1);

        repo.replace_kinds_upsert(
            1,
            &["text"],
            vec![chunk_record(1, 0, vec![0.5, 0.5, 0.0, 0.0])],
        )
        .await
        .unwrap();
        // Still one thread, and the table now holds a single chunk row.
        assert_eq!(repo.count_by_filter(None).await.unwrap().0, 1);
        let table = repo.table.load_full();
        assert_eq!(table.count_rows(None).await.unwrap(), 1);
    }

    /// search_by_vector returns one hit per thread even when several
    /// chunks rank near the query.
    #[tokio::test]
    async fn search_by_vector_dedups_chunks_per_thread() {
        let (config, _db) = TestDb::config(4);
        let repo = ThreadVectorRepositoryImpl::new(config).await.unwrap();
        repo.batch_upsert(vec![
            chunk_record(1, 0, vec![1.0, 0.0, 0.0, 0.0]),
            chunk_record(1, 1, vec![0.9, 0.1, 0.0, 0.0]),
            chunk_record(2, 0, vec![0.0, 0.0, 0.0, 1.0]),
        ])
        .await
        .unwrap();

        let hits = repo
            .search_by_vector(&[1.0, 0.0, 0.0, 0.0], None, 10)
            .await
            .unwrap();
        let t1 = hits.iter().filter(|h| h.thread_id == 1).count();
        assert_eq!(t1, 1, "thread 1 must appear exactly once after de-dup");
    }

    /// N-row regression (over-fetch shortfall): one thread with many
    /// near chunks must not starve other threads. Because the repo no
    /// longer multiplies `limit` by a fixed factor, a small `limit` can
    /// be filled by the dominating thread's chunks (the app's staged loop
    /// is what widens it); a larger `limit` (what the app would pass on a
    /// later attempt) lets the other threads surface.
    #[tokio::test]
    async fn search_by_vector_widens_past_dominating_thread() {
        let (config, _db) = TestDb::config(4);
        let repo = ThreadVectorRepositoryImpl::new(config).await.unwrap();
        // Thread 1 owns 6 near chunks; threads 2 and 3 own one near chunk.
        let mut records = Vec::new();
        for i in 0..6 {
            records.push(chunk_record(1, i, vec![1.0, 0.0, 0.0, 0.0]));
        }
        records.push(chunk_record(2, 0, vec![0.95, 0.05, 0.0, 0.0]));
        records.push(chunk_record(3, 0, vec![0.9, 0.1, 0.0, 0.0]));
        repo.batch_upsert(records).await.unwrap();

        // A small limit can collapse to just the dominating thread...
        let narrow = repo
            .search_by_vector(&[1.0, 0.0, 0.0, 0.0], None, 2)
            .await
            .unwrap();
        assert!(
            narrow.iter().any(|h| h.thread_id == 1),
            "dominating thread is present"
        );

        // ...but a wider limit (what the app's staged loop supplies)
        // surfaces the other distinct threads instead of more chunk rows.
        let wide = repo
            .search_by_vector(&[1.0, 0.0, 0.0, 0.0], None, 8)
            .await
            .unwrap();
        let distinct: std::collections::HashSet<i64> = wide.iter().map(|h| h.thread_id).collect();
        assert!(
            distinct.contains(&2) && distinct.contains(&3),
            "wider fetch must surface threads 2 and 3, not just thread 1's chunks; got {distinct:?}"
        );
    }

    /// N-row: sync_scalars must update labels/channel on EVERY chunk row,
    /// not just chunk 0, so per-row search filters stay consistent.
    #[tokio::test]
    async fn sync_scalars_updates_all_chunks() {
        let (config, _db) = TestDb::config(4);
        let repo = ThreadVectorRepositoryImpl::new(config).await.unwrap();
        repo.batch_upsert(vec![
            chunk_record(1, 0, vec![1.0, 0.0, 0.0, 0.0]),
            chunk_record(1, 1, vec![0.0, 1.0, 0.0, 0.0]),
            chunk_record(1, 2, vec![0.0, 0.0, 1.0, 0.0]),
        ])
        .await
        .unwrap();

        let data = protobuf::llm_memory::data::ThreadData {
            user_id: Some(protobuf::llm_memory::data::UserId { value: 1 }),
            description: Some("desc".to_string()),
            channel: Some("slack".to_string()),
            created_at: 1,
            updated_at: 2,
            ..Default::default()
        };
        repo.sync_scalars(1, &data, vec!["rust".to_string(), "async".to_string()])
            .await
            .unwrap();

        // Every chunk row must now carry the new labels + channel.
        let records = repo.find_records_by_thread_id(1).await.unwrap();
        assert_eq!(records.len(), 3, "all chunks preserved (embeddings intact)");
        for r in &records {
            assert_eq!(
                r.labels,
                vec!["rust".to_string(), "async".to_string()],
                "chunk {} must have synced labels",
                r.chunk_index
            );
            assert_eq!(r.channel.as_deref(), Some("slack"));
        }
        // The filter path (chunk 1+) now matches the new label.
        let filter = ThreadSafeFilter::labels_any(&["async".to_string()]);
        let hits = repo
            .search_by_vector(&[0.0, 0.0, 1.0, 0.0], Some(&filter), 10)
            .await
            .unwrap();
        assert!(
            hits.iter().any(|h| h.thread_id == 1),
            "a chunk-2 vector hit must survive the new-label filter"
        );
    }

    /// N-row: get_stats counts distinct threads (chunk-0 rows), not raw
    /// chunk rows, so records_with_embedding stays <= RDB thread total.
    #[tokio::test]
    async fn get_stats_counts_distinct_threads() {
        let (config, _db) = TestDb::config(4);
        let repo = ThreadVectorRepositoryImpl::new(config).await.unwrap();
        // 2 threads, 3 + 1 chunk rows = 4 raw rows.
        repo.batch_upsert(vec![
            chunk_record(1, 0, vec![1.0, 0.0, 0.0, 0.0]),
            chunk_record(1, 1, vec![0.0, 1.0, 0.0, 0.0]),
            chunk_record(1, 2, vec![0.0, 0.0, 1.0, 0.0]),
            chunk_record(2, 0, vec![0.0, 0.0, 0.0, 1.0]),
        ])
        .await
        .unwrap();

        let stats = repo.get_stats().await.unwrap();
        assert_eq!(
            stats.total_records, 2,
            "must count 2 distinct threads, not 4 chunk rows"
        );
    }

    /// N-row regression: the HYBRID / multi-vector count id-collector must
    /// over-fetch chunk rows so one long thread does not crowd out other
    /// threads' ids. With a small `cap` but `cap * CHUNK_OVERFETCH` raw
    /// rows available, all distinct threads within `cap` must be found.
    #[tokio::test]
    async fn collect_vector_ids_capped_finds_distinct_threads_past_chunk_fanout() {
        let (config, _db) = TestDb::config(4);
        let repo = ThreadVectorRepositoryImpl::new(config).await.unwrap();
        // Thread 1 owns several near chunks; threads 2 and 3 own one each.
        let mut records = Vec::new();
        for i in 0..5 {
            records.push(chunk_record(1, i, vec![1.0, 0.0, 0.0, 0.0]));
        }
        records.push(chunk_record(2, 0, vec![0.95, 0.05, 0.0, 0.0]));
        records.push(chunk_record(3, 0, vec![0.9, 0.1, 0.0, 0.0]));
        repo.batch_upsert(records).await.unwrap();

        // cap=3 → fetch_limit = 3*CHUNK_OVERFETCH+1 = 13 raw rows, enough
        // to reach threads 2/3 past thread 1's 5 chunks.
        let (ids, truncated) = repo
            .collect_vector_ids_capped(&[1.0, 0.0, 0.0, 0.0], None, 3)
            .await
            .unwrap();
        assert!(
            ids.contains(&1) && ids.contains(&2) && ids.contains(&3),
            "all 3 distinct threads must be collected; got {ids:?}"
        );
        assert!(
            !truncated,
            "3 distinct threads <= cap and stream not clipped → not truncated"
        );
    }
}
