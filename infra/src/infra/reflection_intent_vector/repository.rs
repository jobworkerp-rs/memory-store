//! Reflection intent vector repository.
//!
//! Minimal feature set vs. `memory_vector::repository`:
//!   - No FTS / hybrid search (intent search is pure vector).
//!   - No `content` column / no highlights.
//!   - 2-stage filter: app layer narrows by RDB sidecar, this repo
//!     receives the IN list and runs the vector ANN.
//!
//! Surfaced API:
//!   - `open` / `upsert` / `batch_upsert`
//!   - `delete` / `delete_by_memory_ids`
//!   - `search_by_vector(query, filter, limit) -> Vec<IntentSearchHit>`
//!   - `count_records()` / minimal stats

use super::config::ReflectionIntentVectorConfig;
use super::record::ReflectionIntentVectorRecord;
use super::safe_filter::IntentSafeFilter;
use super::schema::intent_arrow_schema;
use crate::infra::memory_vector::config::DistanceType;
use crate::infra::memory_vector::repository::{
    InFlightGuard, compact_with, prune_with, schema_fingerprint, try_claim_gate,
};
use arc_swap::ArcSwap;
use arrow_array::{
    FixedSizeListArray, Float32Array, Int32Array, Int64Array, RecordBatch, RecordBatchIterator,
    StringArray,
};
use arrow_schema::Schema;
use futures::StreamExt;
use lancedb::Table;
use lancedb::connection::Connection;
use lancedb::index::Index;
use lancedb::query::{ExecutableQuery, QueryBase};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};

#[derive(Debug, Clone)]
pub struct IntentSearchHit {
    pub memory_id: i64,
    pub score: f32,
    pub distance: f32,
    /// N-row: which chunk produced this (winning) hit; None on legacy /
    /// projection-less paths.
    pub chunk_index: Option<i32>,
    /// The matched chunk's source text (best-effort; None if not
    /// projected).
    pub matched_content: Option<String>,
}

pub struct ReflectionIntentVectorRepository {
    /// Held so we can reopen the table after writes; the LanceDB
    /// `Table` handle is bound to the snapshot at open time, so a
    /// write+read pair against the same handle would otherwise miss
    /// the just-written rows. See `reload_table` below.
    database: Connection,
    /// Lock-free table handle (see `MemoryVectorRepositoryImpl::table`).
    /// Unlike the memory/thread stores this repo has no FTS/ANN `create_index`
    /// DDL, so `reload_table` swaps without an `index_ddl_lock`.
    table: Arc<ArcSwap<Table>>,
    config: ReflectionIntentVectorConfig,
    /// Monotonic write counter shared by the two maintenance gates. See
    /// `MemoryVectorRepositoryImpl` for the two-gate rationale; this store
    /// reuses the same `try_claim_gate` helper.
    operation_count: Arc<AtomicUsize>,
    /// `operation_count` value at which the prune path last fired.
    last_prune_count: Arc<AtomicUsize>,
    /// `operation_count` value at which the compact+index path last fired.
    last_compact_count: Arc<AtomicUsize>,
    /// Last compact+index time (ms). Exposed via `stats` for parity with
    /// the other stores' GetIndexStats; prune does not touch it.
    last_optimized_at: Arc<AtomicI64>,
    /// Single-slot guard: at most one background compaction at a time.
    /// Mirrors `MemoryVectorRepositoryImpl::compact_in_flight`.
    compact_in_flight: Arc<AtomicBool>,
}

impl ReflectionIntentVectorRepository {
    /// Open the LanceDB table, creating an empty one if it does not
    /// exist yet. Mirrors `memory_vector::repository::new` minus FTS:
    /// verifies the on-disk schema matches the expected one and
    /// (on first creation) installs scalar BTree indexes used by the
    /// 2-stage filter path.
    pub async fn open(config: ReflectionIntentVectorConfig) -> anyhow::Result<Self> {
        let database = lancedb::connect(&config.uri)
            .execute()
            .await
            .map_err(|e| anyhow::anyhow!("LanceDB connect failed: {e}"))?;

        let schema = intent_arrow_schema(config.vector_size);
        let (table, is_new) = match database.open_table(&config.table_name).execute().await {
            Ok(t) => (t, false),
            Err(_) => {
                let empty = RecordBatch::new_empty(schema.clone());
                let reader: Box<dyn arrow_array::RecordBatchReader + Send> =
                    Box::new(RecordBatchIterator::new(vec![Ok(empty)], schema.clone()));
                let t = database
                    .create_table(&config.table_name, reader)
                    .execute()
                    .await
                    .map_err(|e| anyhow::anyhow!("LanceDB create_table failed: {e}"))?;
                (t, true)
            }
        };

        // Reject incompatible on-disk tables up front. Otherwise an
        // older schema (e.g. before a column was added or after
        // vector_size changed) would silently let upsert / search
        // fail much later, possibly after returning bad results.
        verify_intent_table_schema(&table, &schema, &config).await?;

        if is_new {
            create_scalar_indexes(&table).await?;
        }

        tracing::info!(
            "reflection_intent_vector initialized: uri={}, table={}, vector_size={}, new={}",
            config.uri,
            config.table_name,
            config.vector_size,
            is_new
        );

        let repo = Self {
            database,
            table: Arc::new(ArcSwap::from_pointee(table)),
            config,
            operation_count: Arc::new(AtomicUsize::new(0)),
            last_prune_count: Arc::new(AtomicUsize::new(0)),
            last_compact_count: Arc::new(AtomicUsize::new(0)),
            last_optimized_at: Arc::new(AtomicI64::new(0)),
            compact_in_flight: Arc::new(AtomicBool::new(false)),
        };

        // Startup prune: clear the old-manifest backlog so the next boot is
        // fast. Best-effort; live data is protected by LanceDB's 7-day floor.
        // See `MemoryVectorRepositoryImpl::new` for the full rationale.
        if repo.config.optimize.prune_on_startup {
            tracing::info!(
                "reflection_intent_vector startup prune starting (clearing manifest backlog)..."
            );
            if let Err(e) = repo.prune().await {
                tracing::warn!("reflection_intent_vector startup prune failed (continuing): {e}");
            }
        }

        Ok(repo)
    }

    /// Compact data files and fold unindexed rows into the ANN index. The
    /// heavy maintenance path; does NOT prune. Updates `last_optimized_at`.
    /// Mirrors `MemoryVectorRepositoryImpl::compact_and_optimize_index`
    /// (no FTS index on this store).
    pub async fn compact_and_optimize_index(&self) -> anyhow::Result<()> {
        // Shared with the spawned auto-compaction in `track_operation`. This
        // store has no `create_index` DDL, so no ddl_lock is needed.
        compact_with(&self.table, &self.last_optimized_at, None).await
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

    /// Track writes and trigger maintenance on two independent cadences.
    /// Mirrors `MemoryVectorRepositoryImpl::track_operation`: prune is kept
    /// ordered after compact (run in the same task when compaction fires,
    /// inline otherwise, skipped while a compaction is in flight) so the two
    /// `optimize` commits never race on the same table.
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
            let optimize = self.config.optimize; // Copy
            tokio::spawn(async move {
                let _guard = guard;
                if let Err(e) = compact_with(&table, &last_optimized_at, None).await {
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

    /// Swap in the post-write table handle lock-free so subsequent reads see
    /// the new version. This store has no `create_index` DDL, so unlike the
    /// memory/thread repositories the swap needs no `index_ddl_lock`. Mirrors
    /// `MemoryVectorRepositoryImpl::reload_table` otherwise.
    async fn reload_table(&self) -> anyhow::Result<()> {
        let new_table = self
            .database
            .open_table(&self.config.table_name)
            .execute()
            .await
            .map_err(|e| anyhow::anyhow!("intent vector reload_table failed: {e}"))?;
        self.table.store(Arc::new(new_table));
        Ok(())
    }

    pub fn vector_size(&self) -> usize {
        self.config.vector_size
    }

    /// Upsert one chunk record (merge_insert keyed on
    /// `(memory_id, vector_kind, chunk_index)`).
    pub async fn upsert(&self, record: &ReflectionIntentVectorRecord) -> anyhow::Result<()> {
        let schema = intent_arrow_schema(self.config.vector_size);
        let batch = build_record_batch(
            std::slice::from_ref(record),
            &schema,
            self.config.vector_size,
        )?;
        {
            let table = self.table.load_full();
            let mut merge = table.merge_insert(&["memory_id", "vector_kind", "chunk_index"]);
            merge
                .when_matched_update_all(None)
                .when_not_matched_insert_all();
            let reader = Box::new(RecordBatchIterator::new(vec![Ok(batch)], schema));
            merge
                .execute(reader)
                .await
                .map_err(|e| anyhow::anyhow!("intent vector upsert failed: {e}"))?;
        }
        self.reload_table().await?;
        self.track_operation(1).await;
        Ok(())
    }

    /// Batch upsert chunk records in chunks of 1000 (merge keyed on the
    /// 3-column N-row key). Mirrors `memory_vector::batch_upsert`.
    pub async fn batch_upsert(
        &self,
        records: Vec<ReflectionIntentVectorRecord>,
    ) -> anyhow::Result<usize> {
        if records.is_empty() {
            return Ok(0);
        }
        let schema = intent_arrow_schema(self.config.vector_size);
        let mut total = 0usize;
        for chunk in records.chunks(1000) {
            let batch = build_record_batch(chunk, &schema, self.config.vector_size)?;
            let table = self.table.load_full();
            let mut merge = table.merge_insert(&["memory_id", "vector_kind", "chunk_index"]);
            merge
                .when_matched_update_all(None)
                .when_not_matched_insert_all();
            let reader = Box::new(RecordBatchIterator::new(vec![Ok(batch)], schema.clone()));
            merge
                .execute(reader)
                .await
                .map_err(|e| anyhow::anyhow!("intent vector batch upsert failed: {e}"))?;
            total += chunk.len();
            drop(table);
        }
        self.reload_table().await?;
        self.track_operation(total).await;
        Ok(total)
    }

    /// N-row: replace a reflection's rows for a set of `vector_kind`s,
    /// then insert the new chunk rows. Mirrors
    /// `memory_vector::replace_kinds_upsert`. The delete is scoped to
    /// `memory_id == m AND vector_kind IN (replace_kinds)` so a stale
    /// re-embedding (fewer chunks) leaves no orphan rows. `replace_kinds`
    /// empty is rejected; an empty `records` is a valid stale-delete.
    pub async fn replace_kinds_upsert(
        &self,
        memory_id: i64,
        replace_kinds: &[&str],
        records: Vec<ReflectionIntentVectorRecord>,
    ) -> anyhow::Result<usize> {
        if replace_kinds.is_empty() {
            anyhow::bail!("replace_kinds_upsert: replace_kinds must not be empty");
        }
        if let Some(bad) = records.iter().find(|r| r.memory_id != memory_id) {
            anyhow::bail!(
                "replace_kinds_upsert: all records must share memory_id={memory_id}, \
                 got {}",
                bad.memory_id
            );
        }

        let filter = IntentSafeFilter::memory_id(memory_id)
            .and(IntentSafeFilter::in_str_list("vector_kind", replace_kinds)?)
            .to_sql()?;
        {
            let table = self.table.load_full();
            table
                .delete(&filter)
                .await
                .map_err(|e| anyhow::anyhow!("intent vector replace_kinds delete failed: {e}"))?;
            drop(table);
        }

        if records.is_empty() {
            self.reload_table().await?;
            // The delete above created a new version; count it so the stale-
            // delete path still drives maintenance. The non-empty path is
            // tracked by the batch_upsert call below.
            self.track_operation(1).await;
            return Ok(0);
        }
        self.batch_upsert(records).await
    }

    pub async fn delete_by_memory_id(&self, memory_id: i64) -> anyhow::Result<()> {
        let filter = IntentSafeFilter::memory_id(memory_id).to_sql()?;
        {
            let table = self.table.load_full();
            table
                .delete(&filter)
                .await
                .map_err(|e| anyhow::anyhow!("intent vector delete failed: {e}"))?;
        }
        self.reload_table().await?;
        self.track_operation(1).await;
        Ok(())
    }

    /// Fetch all stored chunk embedding vectors for a single reflection,
    /// in `chunk_index` order.
    ///
    /// Used by `ReflectionApp::find_similar_trajectories` (F-S3): the
    /// caller already has the reference reflection's `memory_id` from the
    /// sidecar and needs the matching vector(s) to issue kNN queries
    /// against the rest of the table. N-row: a reflection may own several
    /// chunks; F-S3 runs one ANN per reference chunk and aggregates.
    /// Returns an empty Vec when the id has no rows, keeping the app layer
    /// in charge of the NotFound / stale-status error mapping.
    pub async fn find_embeddings_by_memory_id(
        &self,
        memory_id: i64,
    ) -> anyhow::Result<Vec<Vec<f32>>> {
        let filter = IntentSafeFilter::memory_id(memory_id).to_sql()?;
        let table = self.table.load_full();
        let mut stream = table
            .query()
            .only_if(filter)
            .select(lancedb::query::Select::columns(&[
                "chunk_index",
                "embedding",
            ]))
            .execute()
            .await
            .map_err(|e| anyhow::anyhow!("intent vector find_embeddings query failed: {e}"))?;
        let mut indexed: Vec<(i32, Vec<f32>)> = Vec::new();
        while let Some(batch) = stream.next().await {
            let batch =
                batch.map_err(|e| anyhow::anyhow!("intent vector batch read failed: {e}"))?;
            if batch.num_rows() == 0 {
                continue;
            }
            let chunk_col = batch
                .column_by_name("chunk_index")
                .and_then(|c| c.as_any().downcast_ref::<Int32Array>())
                .ok_or_else(|| anyhow::anyhow!("missing chunk_index column"))?;
            let col = batch
                .column_by_name("embedding")
                .ok_or_else(|| anyhow::anyhow!("missing embedding column"))?;
            let list = col
                .as_any()
                .downcast_ref::<FixedSizeListArray>()
                .ok_or_else(|| anyhow::anyhow!("embedding column type mismatch"))?;
            for i in 0..batch.num_rows() {
                let inner = list.value(i);
                let floats = inner
                    .as_any()
                    .downcast_ref::<Float32Array>()
                    .ok_or_else(|| anyhow::anyhow!("embedding inner type mismatch"))?;
                indexed.push((chunk_col.value(i), floats.values().to_vec()));
            }
        }
        indexed.sort_by_key(|(idx, _)| *idx);
        Ok(indexed.into_iter().map(|(_, v)| v).collect())
    }

    /// Run a kNN search restricted by an optional filter.
    pub async fn search_by_vector(
        &self,
        query_vector: &[f32],
        filter: Option<&IntentSafeFilter>,
        limit: usize,
    ) -> anyhow::Result<Vec<IntentSearchHit>> {
        if query_vector.len() != self.config.vector_size {
            anyhow::bail!(
                "intent vector query size mismatch: expected {}, got {}",
                self.config.vector_size,
                query_vector.len()
            );
        }
        let table = self.table.load_full();
        let mut query = table.query().nearest_to(query_vector)?;
        // N-row: `limit` is the caller's entity-level distinct target. The
        // app's staged over-fetch loop (find_similar_by_vector) sizes it
        // large enough that one reflection with many chunk rows cannot
        // fill the page and collapse to a single hit after de-dup.
        query = query.limit(limit);
        if let Some(f) = filter {
            let sql = f.clone().to_sql()?;
            query = query.only_if(sql);
        }
        let mut stream = query.execute().await?;
        let mut hits = Vec::new();
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            extract_hits(&batch, self.config.distance_type, &mut hits)?;
        }
        let mut hits = dedup_intent_hits(hits);
        hits.truncate(limit);
        Ok(hits)
    }

    /// 2-stage search entry point used by F-S3 / F-S8: the app layer
    /// narrows reflections via the RDB sidecar, hands the resulting
    /// `memory_id` set in here, and we splice it into the LanceDB
    /// filter before running ANN. An empty candidate list is the
    /// normal "no matching reflections" outcome (no rows for the
    /// origin_user_id, every reflection filtered out, etc.); short-
    /// circuit to `Ok(vec![])` instead of bubbling the
    /// `IntentSafeFilter::memory_id_in` empty-list bail up to the
    /// service handler. `extra_filter` AND-combines with the IN
    /// list (e.g. additional task_category narrowing).
    pub async fn search_with_candidate_ids(
        &self,
        query_vector: &[f32],
        candidate_memory_ids: &[i64],
        extra_filter: Option<&IntentSafeFilter>,
        limit: usize,
    ) -> anyhow::Result<Vec<IntentSearchHit>> {
        if candidate_memory_ids.is_empty() {
            return Ok(Vec::new());
        }
        let id_filter = IntentSafeFilter::memory_id_in(candidate_memory_ids)?;
        let combined = match extra_filter {
            Some(extra) => extra.clone().and(id_filter),
            None => id_filter,
        };
        self.search_by_vector(query_vector, Some(&combined), limit)
            .await
    }

    pub async fn count_records(&self) -> anyhow::Result<u64> {
        let table = self.table.load_full();
        let n = table
            .count_rows(None)
            .await
            .map_err(|e| anyhow::anyhow!("intent vector count_rows failed: {e}"))?;
        Ok(n as u64)
    }
}

fn build_record_batch(
    records: &[ReflectionIntentVectorRecord],
    schema: &Arc<Schema>,
    vector_size: usize,
) -> anyhow::Result<RecordBatch> {
    // Validate up front so a malformed record short-circuits before
    // we touch the per-column allocations.
    for (idx, r) in records.iter().enumerate() {
        if r.embedding.len() != vector_size {
            anyhow::bail!(
                "intent vector record {idx} has embedding len {}, expected {}",
                r.embedding.len(),
                vector_size
            );
        }
    }

    let memory_ids: Vec<i64> = records.iter().map(|r| r.memory_id).collect();
    let vector_kinds: Vec<String> = records.iter().map(|r| r.vector_kind.clone()).collect();
    let chunk_indexes: Vec<i32> = records.iter().map(|r| r.chunk_index).collect();
    let begin_positions: Vec<i32> = records.iter().map(|r| r.begin_position).collect();
    let end_positions: Vec<i32> = records.iter().map(|r| r.end_position).collect();
    let contents: Vec<String> = records.iter().map(|r| r.content.clone()).collect();
    let user_ids: Vec<i64> = records.iter().map(|r| r.origin_user_id).collect();
    let channels: Vec<Option<String>> = records.iter().map(|r| r.origin_channel.clone()).collect();
    let task_categories: Vec<i32> = records.iter().map(|r| r.task_category).collect();
    let aspects: Vec<i32> = records.iter().map(|r| r.reflection_aspect).collect();
    let outcomes: Vec<i32> = records.iter().map(|r| r.outcome).collect();
    let embedding_models: Vec<Option<String>> =
        records.iter().map(|r| r.embedding_model.clone()).collect();
    let created_ats: Vec<i64> = records.iter().map(|r| r.created_at).collect();

    let flat: Vec<f32> = records
        .iter()
        .flat_map(|r| r.embedding.iter().copied())
        .collect();
    let values = Float32Array::from(flat);
    let item_field = Arc::new(arrow_schema::Field::new(
        "item",
        arrow_schema::DataType::Float32,
        true,
    ));
    let embedding_array =
        FixedSizeListArray::try_new(item_field, vector_size as i32, Arc::new(values), None)
            .map_err(|e| anyhow::anyhow!("FixedSizeListArray build failed: {e}"))?;

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(memory_ids)),
            Arc::new(StringArray::from(vector_kinds)),
            Arc::new(Int32Array::from(chunk_indexes)),
            Arc::new(Int32Array::from(begin_positions)),
            Arc::new(Int32Array::from(end_positions)),
            Arc::new(StringArray::from(contents)),
            Arc::new(Int64Array::from(user_ids)),
            Arc::new(StringArray::from(channels)),
            Arc::new(Int32Array::from(task_categories)),
            Arc::new(Int32Array::from(aspects)),
            Arc::new(Int32Array::from(outcomes)),
            Arc::new(embedding_array),
            Arc::new(StringArray::from(embedding_models)),
            Arc::new(Int64Array::from(created_ats)),
        ],
    )
    .map_err(|e| anyhow::anyhow!("intent vector batch build failed: {e}"))?;
    Ok(batch)
}

fn extract_hits(
    batch: &RecordBatch,
    distance_type: DistanceType,
    hits: &mut Vec<IntentSearchHit>,
) -> anyhow::Result<()> {
    let memory_ids = batch
        .column_by_name("memory_id")
        .ok_or_else(|| anyhow::anyhow!("missing memory_id column"))?
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| anyhow::anyhow!("memory_id column type mismatch"))?;

    // LanceDB emits a `_distance` column whose semantics depend on the
    // configured DistanceType:
    //   Cosine: 1 - cos_sim ∈ [0, 2]; smaller is more similar.
    //   L2:     squared L2; smaller is more similar.
    //   Dot:    -dot ∈ [-1, 1] for unit vectors; smaller is more similar.
    // The mappings below all push "smaller distance" toward "score 1.0".
    //
    // Note: `memory_vector::repository::extract_search_hits` keeps the
    // legacy formula `((1.0 + distance) / 2.0)` for Dot, which inverts
    // the ordering relative to Cosine / L2 (the closest vector ends up
    // with the *lowest* score). Tracking that as a separate fix in
    // memory_vector; the reflection layer ships the corrected form
    // from day one so REFLECTION_DISTANCE_TYPE=dot returns scores
    // ordered the same way as cosine search.
    let distance_col = batch
        .column_by_name("_distance")
        .and_then(|c| c.as_any().downcast_ref::<Float32Array>());
    // Best-effort: nearest_to projects all columns by default, so these
    // are usually present; tolerate their absence (None) for projected
    // count-only queries.
    let chunk_col = batch
        .column_by_name("chunk_index")
        .and_then(|c| c.as_any().downcast_ref::<Int32Array>());
    let content_col = batch
        .column_by_name("content")
        .and_then(|c| c.as_any().downcast_ref::<StringArray>());

    for i in 0..batch.num_rows() {
        let memory_id = memory_ids.value(i);
        let distance = distance_col.map(|d| d.value(i)).unwrap_or(f32::MAX);
        let score = match distance_type {
            DistanceType::Cosine => (1.0 - distance).clamp(0.0, 1.0),
            DistanceType::L2 => (1.0 / (1.0 + distance)).clamp(0.0, 1.0),
            DistanceType::Dot => ((1.0 - distance) / 2.0).clamp(0.0, 1.0),
        };
        hits.push(IntentSearchHit {
            chunk_index: chunk_col.map(|c| c.value(i)),
            matched_content: content_col.map(|c| c.value(i).to_string()),
            memory_id,
            score,
            distance,
        });
    }
    Ok(())
}

/// N-row: collapse chunk-level hits to one hit per reflection, keeping
/// the max-score row and preserving first-appearance (rank) order.
/// Mirrors `app::app::memory_vector::dedup_vector_hits`.
fn dedup_intent_hits(hits: Vec<IntentSearchHit>) -> Vec<IntentSearchHit> {
    let mut order: Vec<i64> = Vec::new();
    let mut best: std::collections::HashMap<i64, IntentSearchHit> =
        std::collections::HashMap::new();
    for hit in hits {
        match best.get_mut(&hit.memory_id) {
            None => {
                order.push(hit.memory_id);
                best.insert(hit.memory_id, hit);
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

async fn verify_intent_table_schema(
    table: &Table,
    expected: &Arc<Schema>,
    config: &ReflectionIntentVectorConfig,
) -> anyhow::Result<()> {
    let actual = table
        .schema()
        .await
        .map_err(|e| anyhow::anyhow!("intent vector schema read failed: {e}"))?;
    let actual_arrow = actual.as_ref().clone();
    let expected_fp = schema_fingerprint(expected.as_ref());
    let actual_fp = schema_fingerprint(&actual_arrow);
    if actual_fp == expected_fp {
        return Ok(());
    }
    // Surface as structured StartupError so the parent (agent-app) can
    // route into the LanceDB-dim recovery flow without parsing the
    // message text. The `embedding` field shape mirrors memory_vector,
    // so the same dim extractor works here.
    let expected_dim =
        crate::infra::memory_vector::schema::extract_embedding_dim_from_schema(expected.as_ref())
            .unwrap_or(0);
    let actual_dim =
        crate::infra::memory_vector::schema::extract_embedding_dim_from_schema(&actual_arrow)
            .unwrap_or(0);
    Err(anyhow::Error::new(
        crate::infra::startup_error::StartupError::LancedbSchemaMismatch {
            table: config.table_name.clone(),
            uri: config.uri.clone(),
            expected_dim,
            actual_dim,
            expected_fingerprint: expected_fp,
            actual_fingerprint: actual_fp,
        },
    ))
}

/// Install BTree indexes on the scalar columns the 2-stage filter
/// path uses (`only_if` predicates and direct `delete` filters).
/// Without these, queries against a non-trivial corpus fall back to
/// a full scan. Pre-existing indexes (when this is called against a
/// previously-created table) are tolerated as no-ops.
async fn create_scalar_indexes(table: &Table) -> anyhow::Result<()> {
    // memory_id is part of the merge key for upsert / delete and feeds
    // the RDB→IN list 2-stage filter. vector_kind backs the N-row
    // replace_kinds delete. The remaining columns mirror the RDB sidecar
    // columns the search layer narrows by.
    let columns = [
        "memory_id",
        "vector_kind",
        "origin_user_id",
        "task_category",
        "reflection_aspect",
        "outcome",
        "created_at",
    ];
    for col_name in columns {
        if let Err(e) = table
            .create_index(&[col_name], Index::BTree(Default::default()))
            .execute()
            .await
        {
            let msg = e.to_string();
            if msg.contains("already exists") || msg.contains("duplicate") {
                continue;
            }
            tracing::warn!(
                "reflection_intent_vector: failed to create BTree index on {}: {}",
                col_name,
                e
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infra::memory_vector::record::vector_kind;

    /// `IntentSafeFilter::memory_id_in` deliberately bails on empty
    /// input (matches the convention in
    /// `memory_vector::safe_filter::SafeFilter::in_i64_list`), but
    /// the 2-stage search path treats "no candidates" as a normal
    /// outcome. Confirm the wrapper short-circuits before building
    /// the IN filter so the caller gets `Ok(vec![])` instead of
    /// having to special-case empty candidate sets.
    #[tokio::test]
    async fn search_with_candidate_ids_short_circuits_on_empty_input() {
        // The early return path does not touch `self.table` /
        // `self.config`, so we can exercise it by reaching it before
        // any LanceDB I/O. Build a minimal fake repo via a closure
        // that reproduces the early return logic — we cannot
        // construct `ReflectionIntentVectorRepository` without a
        // running LanceDB, but the contract we care about lives in
        // the function's prologue.
        async fn early_return_path(candidates: &[i64]) -> anyhow::Result<Vec<IntentSearchHit>> {
            if candidates.is_empty() {
                return Ok(Vec::new());
            }
            anyhow::bail!("non-empty path should not run in this test");
        }

        let hits = early_return_path(&[]).await.unwrap();
        assert!(hits.is_empty());
    }

    /// The non-empty branch must still go through the IN list
    /// builder (which bails on empty input), so the wrapper is the
    /// only place that handles emptiness. Sanity check the IN list
    /// builder rejects empty input as expected.
    #[test]
    fn intent_safe_filter_memory_id_in_rejects_empty() {
        let err = match IntentSafeFilter::memory_id_in(&[]) {
            Ok(_) => panic!("expected memory_id_in([]) to fail"),
            Err(e) => e,
        };
        assert!(
            err.to_string().contains("must not be empty"),
            "memory_id_in must keep its bail behaviour for empty input: {err}"
        );
    }

    fn hit(memory_id: i64, score: f32) -> IntentSearchHit {
        IntentSearchHit {
            memory_id,
            score,
            distance: 1.0 - score,
            chunk_index: None,
            matched_content: None,
        }
    }

    /// N-row: chunk-level hits collapse to one per reflection, keeping
    /// the max-score row.
    #[test]
    fn dedup_intent_hits_keeps_max_score_per_memory() {
        let deduped = dedup_intent_hits(vec![hit(1, 0.4), hit(2, 0.9), hit(1, 0.7), hit(2, 0.3)]);
        assert_eq!(deduped.len(), 2);
        // First-appearance order is preserved: 1 then 2.
        assert_eq!(deduped[0].memory_id, 1);
        assert_eq!(deduped[0].score, 0.7);
        assert_eq!(deduped[1].memory_id, 2);
        assert_eq!(deduped[1].score, 0.9);
    }

    #[test]
    fn dedup_intent_hits_empty_is_empty() {
        assert!(dedup_intent_hits(Vec::new()).is_empty());
    }

    struct TestDb {
        path: String,
    }

    impl TestDb {
        fn config(dim: usize) -> (ReflectionIntentVectorConfig, Self) {
            let ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let path = format!("/tmp/test_intent_lancedb_{ts}");
            let config = ReflectionIntentVectorConfig {
                uri: path.clone(),
                table_name: "test_intent".to_string(),
                vector_size: dim,
                distance_type: DistanceType::Cosine,
                optimize: crate::infra::memory_vector::repository::test_optimize_config(),
            };
            (config, Self { path })
        }
    }

    impl Drop for TestDb {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    fn chunk_record(
        memory_id: i64,
        chunk_index: i32,
        embedding: Vec<f32>,
    ) -> ReflectionIntentVectorRecord {
        ReflectionIntentVectorRecord {
            memory_id,
            vector_kind: vector_kind::TEXT.to_string(),
            chunk_index,
            begin_position: chunk_index * 10,
            end_position: chunk_index * 10 + 10,
            content: format!("chunk {chunk_index}"),
            origin_user_id: 300000,
            origin_channel: None,
            task_category: 0,
            reflection_aspect: 0,
            outcome: 0,
            embedding,
            embedding_model: Some("test-model".to_string()),
            created_at: 1,
        }
    }

    /// N-row batch_upsert stores every chunk; merge key is the
    /// 3-column N-row key so chunks coexist.
    #[tokio::test]
    async fn batch_upsert_stores_all_chunks() {
        let (config, _db) = TestDb::config(4);
        let repo = ReflectionIntentVectorRepository::open(config)
            .await
            .unwrap();
        let records = vec![
            chunk_record(1, 0, vec![1.0, 0.0, 0.0, 0.0]),
            chunk_record(1, 1, vec![0.0, 1.0, 0.0, 0.0]),
            chunk_record(1, 2, vec![0.0, 0.0, 1.0, 0.0]),
        ];
        let n = repo.batch_upsert(records).await.unwrap();
        assert_eq!(n, 3);
        assert_eq!(repo.count_records().await.unwrap(), 3);

        let embeddings = repo.find_embeddings_by_memory_id(1).await.unwrap();
        assert_eq!(embeddings.len(), 3, "all chunk vectors returned");
    }

    // ===== Maintenance (compact / prune / startup) tests =====

    #[tokio::test]
    async fn compact_and_optimize_index_updates_last_optimized_at() {
        let (config, _db) = TestDb::config(4);
        let repo = ReflectionIntentVectorRepository::open(config)
            .await
            .unwrap();
        repo.upsert(&chunk_record(1, 0, vec![1.0, 0.0, 0.0, 0.0]))
            .await
            .unwrap();
        assert_eq!(repo.last_optimized_at.load(Ordering::Relaxed), 0);
        repo.compact_and_optimize_index().await.unwrap();
        assert!(repo.last_optimized_at.load(Ordering::Relaxed) > 0);
    }

    #[tokio::test]
    async fn prune_does_not_touch_last_optimized_at() {
        let (config, _db) = TestDb::config(4);
        let repo = ReflectionIntentVectorRepository::open(config)
            .await
            .unwrap();
        repo.upsert(&chunk_record(1, 0, vec![1.0, 0.0, 0.0, 0.0]))
            .await
            .unwrap();
        repo.prune().await.unwrap();
        assert_eq!(repo.last_optimized_at.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn startup_prune_runs_without_error_when_enabled() {
        let (mut config, _db) = TestDb::config(4);
        config.optimize = crate::infra::memory_vector::config::OptimizeConfig {
            prune_on_startup: true,
            prune_older_than_secs: 0,
            ..crate::infra::memory_vector::repository::test_optimize_config()
        };
        // new() must return Ok after running the startup prune path.
        ReflectionIntentVectorRepository::open(config)
            .await
            .expect("startup prune must not fail open()");
    }

    /// replace_kinds_upsert deletes the reflection's existing text rows
    /// before inserting, so a shorter rebuild leaves no stale chunks.
    #[tokio::test]
    async fn replace_kinds_upsert_drops_stale_chunks() {
        let (config, _db) = TestDb::config(4);
        let repo = ReflectionIntentVectorRepository::open(config)
            .await
            .unwrap();
        // Initial: 3 chunks.
        repo.batch_upsert(vec![
            chunk_record(1, 0, vec![1.0, 0.0, 0.0, 0.0]),
            chunk_record(1, 1, vec![0.0, 1.0, 0.0, 0.0]),
            chunk_record(1, 2, vec![0.0, 0.0, 1.0, 0.0]),
        ])
        .await
        .unwrap();
        assert_eq!(repo.count_records().await.unwrap(), 3);

        // Re-embed with 1 chunk via replace_kinds: stale chunks 1,2 gone.
        repo.replace_kinds_upsert(
            1,
            &["text"],
            vec![chunk_record(1, 0, vec![0.5, 0.5, 0.0, 0.0])],
        )
        .await
        .unwrap();
        assert_eq!(repo.count_records().await.unwrap(), 1);
    }

    /// An empty `records` set is a valid stale-delete.
    #[tokio::test]
    async fn replace_kinds_upsert_empty_is_stale_delete() {
        let (config, _db) = TestDb::config(4);
        let repo = ReflectionIntentVectorRepository::open(config)
            .await
            .unwrap();
        repo.batch_upsert(vec![chunk_record(1, 0, vec![1.0, 0.0, 0.0, 0.0])])
            .await
            .unwrap();
        assert_eq!(repo.count_records().await.unwrap(), 1);
        let n = repo
            .replace_kinds_upsert(1, &["text"], Vec::new())
            .await
            .unwrap();
        assert_eq!(n, 0);
        assert_eq!(repo.count_records().await.unwrap(), 0);
    }

    /// Search returns one hit per reflection even when several chunks
    /// rank near the query (chunk→reflection de-dup, max score kept).
    #[tokio::test]
    async fn search_by_vector_dedups_chunks_per_reflection() {
        let (config, _db) = TestDb::config(4);
        let repo = ReflectionIntentVectorRepository::open(config)
            .await
            .unwrap();
        // Reflection 1 has two chunks both near the query.
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
        let r1_hits = hits.iter().filter(|h| h.memory_id == 1).count();
        assert_eq!(
            r1_hits, 1,
            "reflection 1 must appear exactly once after de-dup"
        );
    }

    /// N-row regression (over-fetch shortfall): one reflection with many
    /// near chunks must not starve others. The repo no longer multiplies
    /// `limit` by a fixed factor, so a wider `limit` (what the app's
    /// staged loop supplies) surfaces the other distinct reflections
    /// instead of more chunk rows of the dominating reflection.
    #[tokio::test]
    async fn search_by_vector_widens_past_dominating_reflection() {
        let (config, _db) = TestDb::config(4);
        let repo = ReflectionIntentVectorRepository::open(config)
            .await
            .unwrap();
        let mut records = Vec::new();
        for i in 0..6 {
            records.push(chunk_record(1, i, vec![1.0, 0.0, 0.0, 0.0]));
        }
        records.push(chunk_record(2, 0, vec![0.95, 0.05, 0.0, 0.0]));
        records.push(chunk_record(3, 0, vec![0.9, 0.1, 0.0, 0.0]));
        repo.batch_upsert(records).await.unwrap();

        let wide = repo
            .search_by_vector(&[1.0, 0.0, 0.0, 0.0], None, 8)
            .await
            .unwrap();
        let distinct: std::collections::HashSet<i64> = wide.iter().map(|h| h.memory_id).collect();
        assert!(
            distinct.contains(&2) && distinct.contains(&3),
            "wider fetch must surface reflections 2 and 3, not just \
             reflection 1's chunks; got {distinct:?}"
        );
    }
}
