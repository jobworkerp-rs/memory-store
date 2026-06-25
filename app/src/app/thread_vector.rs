use crate::app::memory_vector::approximate_fetch_limit;
use anyhow::Result;
use infra::error::LlmMemoryError;
use infra::infra::memory_vector::repository::{AggregationStrategy, HybridOptions, HybridStrategy};
use infra::infra::thread::rdb::{ThreadRepository, ThreadRepositoryImpl};
use infra::infra::thread_label::rdb::{ThreadLabelRepository, ThreadLabelRepositoryImpl};
use infra::infra::thread_vector::record::ThreadVectorRecord;
use infra::infra::thread_vector::repository::{
    IndexStats, ThreadVectorRepositoryImpl, ThreadVectorSearchHit,
};
use infra::infra::thread_vector::safe_filter::ThreadSafeFilter;
use protobuf::llm_memory::data::{Thread, ThreadId};
use std::collections::HashMap;

/// Search result item with Thread entity and relevance scores
pub struct ThreadSearchResultItem {
    pub thread: Thread,
    pub score: f32,
    pub distance: f32,
    /// Server-computed highlight ranges against
    /// `thread.data.description`. Mirrors
    /// `MemorySearchResultItem.highlights`; empty for vector-only
    /// search, when description is absent/empty, or when
    /// `include_description = false` strips it.
    pub highlights: Vec<protobuf::llm_memory::data::HighlightField>,
}

/// App layer for thread vector search operations.
pub struct ThreadVectorAppImpl {
    thread_repo: ThreadRepositoryImpl,
    thread_label_repo: ThreadLabelRepositoryImpl,
    vector_repo: ThreadVectorRepositoryImpl,
    thread_filter_config: crate::app::thread_filter_resolver::ThreadFilterConfig,
    /// `Some` lets `redispatch_embeddings` re-enqueue thread description
    /// embedding jobs after a manual LanceDB reset. `None` (auto-embedding
    /// disabled / env-less tests) makes redispatch a FailedPrecondition.
    embedding_dispatcher: Option<
        std::sync::Arc<infra::infra::thread_vector::dispatcher::ThreadEmbeddingJobDispatcher>,
    >,
}

impl ThreadVectorAppImpl {
    pub fn new(
        thread_repo: ThreadRepositoryImpl,
        thread_label_repo: ThreadLabelRepositoryImpl,
        vector_repo: ThreadVectorRepositoryImpl,
        embedding_dispatcher: Option<
            std::sync::Arc<infra::infra::thread_vector::dispatcher::ThreadEmbeddingJobDispatcher>,
        >,
    ) -> Self {
        Self {
            thread_repo,
            thread_label_repo,
            vector_repo,
            thread_filter_config: crate::app::thread_filter_resolver::ThreadFilterConfig::from_env(
            ),
            embedding_dispatcher,
        }
    }

    // ===== Embedding management =====

    /// N-row upsert of a thread description embedding. Builds one chunk
    /// record per row (carrying the thread's description/labels/channel
    /// scalars from RDB), then replaces the thread's rows for
    /// `replace_kinds` (text). Mirrors
    /// `MemoryVectorAppImpl::batch_upsert_embeddings_rows`.
    pub async fn batch_upsert_thread_embeddings_rows(
        &self,
        thread_id: i64,
        embedding_model: Option<&str>,
        replace_kinds: &[String],
        rows: Vec<(String, i32, i32, i32, String, Vec<f32>)>,
    ) -> Result<(u32, u32, Vec<String>)> {
        if replace_kinds.is_empty() {
            return Err(LlmMemoryError::InvalidArgument(
                "batch_upsert_thread_embeddings_rows: replace_kinds must not be empty".to_string(),
            )
            .into());
        }

        let tid = ThreadId { value: thread_id };
        let Some(thread) = self.thread_repo.find(&tid).await? else {
            return Ok((
                0,
                rows.len() as u32,
                vec![format!("thread_id={thread_id}: not found in RDB")],
            ));
        };
        let Some(data) = thread.data.as_ref() else {
            return Ok((
                0,
                rows.len() as u32,
                vec![format!("thread_id={thread_id}: has no data")],
            ));
        };
        let mut labels = self
            .thread_label_repo
            .find_labels_by_thread(thread_id)
            .await?;

        let mut records = Vec::with_capacity(rows.len());
        let last = rows.len().saturating_sub(1);
        for (i, (vector_kind, chunk_index, begin, end, content, embedding)) in
            rows.into_iter().enumerate()
        {
            // Move `labels` into the final record; clone for the rest.
            let labels = if i == last {
                std::mem::take(&mut labels)
            } else {
                labels.clone()
            };
            records.push(ThreadVectorRecord::from_chunk_with_content(
                thread_id,
                data,
                labels,
                &embedding,
                embedding_model,
                &vector_kind,
                chunk_index,
                begin,
                end,
                content,
            ));
        }
        let replace_refs: Vec<&str> = replace_kinds.iter().map(String::as_str).collect();
        let success = self
            .vector_repo
            .replace_kinds_upsert(thread_id, &replace_refs, records)
            .await? as u32;
        Ok((success, 0, Vec::new()))
    }

    /// Re-enqueue thread description embedding jobs from RDB after a
    /// manual LanceDB reset. Walks threads (offset pagination) and
    /// dispatches each non-empty description to the auto-thread-embedding
    /// workflow, which writes the N-row chunks back. Mirrors
    /// `MemoryVectorAppImpl::redispatch_embeddings` (text-only — threads
    /// have no media axis). Returns `(dispatched, skipped, failed,
    /// duration_ms)`.
    pub async fn redispatch_embeddings(
        &self,
        user_id: Option<i64>,
        batch_size: Option<u32>,
    ) -> Result<(u32, u32, u32, i64)> {
        let dispatcher = self.embedding_dispatcher.as_ref().ok_or_else(|| {
            LlmMemoryError::InvalidArgument(
                "thread embedding dispatcher is not configured; cannot redispatch embeddings"
                    .to_string(),
            )
        })?;

        let page_size = batch_size.unwrap_or(500).clamp(1, 5000) as i32;
        let mut offset: i64 = 0;
        let mut dispatched = 0u32;
        let mut skipped = 0u32;
        let mut failed = 0u32;
        let start = std::time::Instant::now();

        loop {
            let threads = self
                .thread_repo
                .find_list(Some(&page_size), Some(&offset))
                .await?;
            if threads.is_empty() {
                break;
            }
            let fetched = threads.len() as i64;

            for thread in &threads {
                let Some(id) = thread.id.as_ref().map(|i| i.value) else {
                    skipped += 1;
                    continue;
                };
                // Filter by owner when requested.
                if let Some(uid) = user_id
                    && thread
                        .data
                        .as_ref()
                        .and_then(|d| d.user_id.map(|u| u.value))
                        != Some(uid)
                {
                    continue;
                }
                let description = thread
                    .data
                    .as_ref()
                    .and_then(|d| d.description.as_deref())
                    .unwrap_or("");
                if description.is_empty() {
                    skipped += 1;
                    continue;
                }
                match dispatcher.dispatch(id, description).await {
                    Ok(_) => dispatched += 1,
                    Err(e) => {
                        failed += 1;
                        tracing::warn!("thread redispatch failed for thread_id={id}: {e}");
                    }
                }
            }

            offset += fetched;
            if fetched < page_size as i64 {
                break;
            }
        }

        Ok((
            dispatched,
            skipped,
            failed,
            start.elapsed().as_millis() as i64,
        ))
    }

    // ===== Search operations =====

    pub async fn search_by_vector(
        &self,
        query_vectors: &[Vec<f32>],
        limit: usize,
        filter: Option<&ThreadSafeFilter>,
        include_description: bool,
        aggregation: Option<AggregationStrategy>,
    ) -> Result<Vec<ThreadSearchResultItem>> {
        if query_vectors.is_empty() {
            return Ok(Vec::new());
        }

        self.search_with_overfetch(limit, include_description, None, |fetch_limit| async move {
            if query_vectors.len() == 1 {
                self.vector_repo
                    .search_by_vector(&query_vectors[0], filter, fetch_limit)
                    .await
            } else {
                let mut all_hits = Vec::new();
                for qv in query_vectors {
                    let h = self
                        .vector_repo
                        .search_by_vector(qv, filter, fetch_limit)
                        .await?;
                    all_hits.push(h);
                }
                let strategy = aggregation.unwrap_or(AggregationStrategy::Average);
                Ok(aggregate_thread_scores(&all_hits, strategy, fetch_limit))
            }
        })
        .await
    }

    pub async fn search_by_text(
        &self,
        query_text: &str,
        limit: usize,
        filter: Option<&ThreadSafeFilter>,
        include_description: bool,
    ) -> Result<Vec<ThreadSearchResultItem>> {
        self.search_with_overfetch(
            limit,
            include_description,
            Some(query_text),
            |fetch_limit| async move {
                self.vector_repo
                    .search_by_text(query_text, filter, fetch_limit)
                    .await
            },
        )
        .await
    }

    pub async fn hybrid_search(
        &self,
        query_vector: &[f32],
        query_text: &str,
        limit: usize,
        filter: Option<&ThreadSafeFilter>,
        include_description: bool,
        options: &HybridOptions,
    ) -> Result<Vec<ThreadSearchResultItem>> {
        self.search_with_overfetch(
            limit,
            include_description,
            Some(query_text),
            |fetch_limit| async move {
                self.vector_repo
                    .hybrid_search(query_vector, query_text, filter, fetch_limit, options)
                    .await
            },
        )
        .await
    }

    // ===== Count search (P2, Phase 5-1 / 5-2) =====

    /// Count matches for a `ThreadVectorService::CountSearchMatches` RPC.
    /// Symmetrically mirrors `MemoryVectorAppImpl::count_search_matches`.
    /// Phase 5-1 implemented FILTER_ONLY / TEXT; Phase 5-2 added VECTOR
    /// / HYBRID with `MEMORY_COUNT_VECTOR_HARD_CAP`-bounded approximate
    /// counts. Thread-side has no `thread_filter` concept so the
    /// resolve / chunking logic is absent.
    ///
    /// `aggregation` is accepted for API symmetry with `search_by_vector`
    /// but does not change the multi-vector Count value (see
    /// `MemoryVectorAppImpl::count_vector` for the full rationale —
    /// aggregation strategies only re-rank a fixed membership set).
    #[allow(clippy::too_many_arguments)]
    pub async fn count_search_matches(
        &self,
        mode: crate::app::memory_vector::CountMode,
        filter: Option<&ThreadSafeFilter>,
        query_text: Option<&str>,
        query_vectors: &[Vec<f32>],
        aggregation: Option<AggregationStrategy>,
        hybrid_options: Option<&HybridOptions>,
    ) -> Result<crate::app::memory_vector::CountSearchOutput> {
        use crate::app::memory_vector::{CountMode, CountSearchOutput};

        let fts_hard_cap = self.thread_filter_config.fts_count_hard_cap;
        let vector_hard_cap = self.thread_filter_config.count_vector_hard_cap;

        match mode {
            CountMode::FilterOnly => {
                if filter.is_none() {
                    tracing::info!(
                        target: "thread_vector::count",
                        "FILTER_ONLY count without filter: counting all threads",
                    );
                }
                let (total, is_truncated) = self.vector_repo.count_by_filter(filter).await?;
                Ok(CountSearchOutput {
                    total,
                    is_truncated,
                    mode: CountMode::FilterOnly,
                })
            }
            CountMode::Text => {
                let q = query_text.filter(|s| !s.is_empty()).ok_or_else(|| {
                    LlmMemoryError::InvalidArgument(
                        "query_text is required and must not be empty for CountSearchMode::TEXT"
                            .to_string(),
                    )
                })?;
                let (total, is_truncated) = self
                    .vector_repo
                    .count_by_text(q, filter, fts_hard_cap)
                    .await?;
                Ok(CountSearchOutput {
                    total,
                    is_truncated,
                    mode: CountMode::Text,
                })
            }
            CountMode::Vector => {
                if query_vectors.is_empty() {
                    return Err(LlmMemoryError::InvalidArgument(
                        "query_vectors is required and must not be empty for \
                         CountSearchMode::VECTOR"
                            .to_string(),
                    )
                    .into());
                }
                let _ = aggregation;
                if query_vectors.len() == 1 {
                    let (total, is_truncated) = self
                        .vector_repo
                        .count_by_vector(&query_vectors[0], filter, vector_hard_cap)
                        .await?;
                    return Ok(CountSearchOutput {
                        total,
                        is_truncated,
                        mode: CountMode::Vector,
                    });
                }
                let overfetch_cap =
                    ((vector_hard_cap as f64 * 1.5).ceil() as u64).max(vector_hard_cap + 1);
                let futures: Vec<_> = query_vectors
                    .iter()
                    .map(|v| {
                        self.vector_repo
                            .collect_vector_ids_capped(v, filter, overfetch_cap)
                    })
                    .collect();
                let branches = futures::future::try_join_all(futures).await?;
                let mut union: std::collections::HashSet<i64> = std::collections::HashSet::new();
                let mut any_branch_truncated = false;
                for (ids, truncated) in branches {
                    any_branch_truncated |= truncated;
                    union.extend(ids);
                }
                let merged = union.len() as u64;
                let total = merged.min(vector_hard_cap);
                let is_truncated = any_branch_truncated || merged > vector_hard_cap;
                Ok(CountSearchOutput {
                    total,
                    is_truncated,
                    mode: CountMode::Vector,
                })
            }
            CountMode::Hybrid => {
                if query_vectors.is_empty() {
                    return Err(LlmMemoryError::InvalidArgument(
                        "query_vectors is required and must not be empty for \
                         CountSearchMode::HYBRID"
                            .to_string(),
                    )
                    .into());
                }
                if query_vectors.len() != 1 {
                    return Err(LlmMemoryError::InvalidArgument(
                        "CountSearchMode::HYBRID accepts exactly one query_vector".to_string(),
                    )
                    .into());
                }
                let q = query_text.filter(|s| !s.is_empty()).ok_or_else(|| {
                    LlmMemoryError::InvalidArgument(
                        "query_text is required and must not be empty for \
                         CountSearchMode::HYBRID"
                            .to_string(),
                    )
                })?;
                let options = hybrid_options.cloned().unwrap_or(HybridOptions {
                    strategy: HybridStrategy::Rrf,
                    vector_weight: None,
                    rrf_k: None,
                });
                let (total, is_truncated) = self
                    .vector_repo
                    .count_by_hybrid(
                        &query_vectors[0],
                        q,
                        filter,
                        &options,
                        vector_hard_cap,
                        fts_hard_cap,
                    )
                    .await?;
                Ok(CountSearchOutput {
                    total,
                    is_truncated,
                    mode: CountMode::Hybrid,
                })
            }
        }
    }

    /// Over-fetch from LanceDB to reach `limit` distinct threads.
    ///
    /// N-row: the repo de-dups chunk rows to one hit per thread, so a few
    /// long threads can dominate the ANN/BM25 window and leave the result
    /// short of `limit`. Stale-record drops during hydration have the same
    /// effect. Both are absorbed by staging the fetch K → 2K → 3K
    /// (`approximate_fetch_limit`, clipped at `vector_dedup_max_fetch`,
    /// floored at `limit+1`), re-counting distinct threads after de-dup +
    /// hydration, and stopping early once the cap is pinned. Mirrors
    /// `MemoryVectorAppImpl::search_with_overfetch`.
    async fn search_with_overfetch<F, Fut>(
        &self,
        limit: usize,
        include_description: bool,
        // `None` keeps highlights empty (vector-only paths); `Some` makes
        // `enrich_hits` re-tokenize the description for FTS / hybrid paths.
        query_text: Option<&str>,
        search_fn: F,
    ) -> Result<Vec<ThreadSearchResultItem>>
    where
        F: Fn(usize) -> Fut,
        Fut: std::future::Future<Output = anyhow::Result<Vec<ThreadVectorSearchHit>>>,
    {
        let cfg = &self.thread_filter_config;
        let mut results: Vec<ThreadSearchResultItem> = Vec::new();
        let mut last_fetch = 0usize;
        for attempt in 1..=3usize {
            let fetch_limit = approximate_fetch_limit(
                limit,
                cfg.vector_dedup_overfetch_k,
                attempt,
                cfg.vector_dedup_max_fetch,
            );
            // A wider attempt that cannot pull more rows than the previous
            // pass (cap pinned) would re-run de-dup + hydration on the
            // same hit set — stop instead.
            if attempt > 1 && fetch_limit <= last_fetch {
                break;
            }
            last_fetch = fetch_limit;
            let hits = search_fn(fetch_limit).await?;
            results = self
                .enrich_hits(hits, include_description, query_text, limit)
                .await?;
            if results.len() >= limit {
                break;
            }
        }
        if results.len() < limit {
            tracing::warn!(
                "thread vector de-dup short of limit: requested={} unique={} \
                 max_fetch={} (N-row: a few long threads may dominate the ANN \
                 window; safety > completeness)",
                limit,
                results.len(),
                cfg.vector_dedup_max_fetch,
            );
        }
        results.truncate(limit);
        Ok(results)
    }

    // ===== Index management =====

    /// Reconcile LanceDB and RDB:
    /// - Remove orphaned LanceDB entries (in LanceDB but not in RDB)
    /// - Detect and report missing entries (in RDB but not in LanceDB)
    ///
    /// Missing threads require re-embedding via `batch_upsert_thread_embeddings`
    /// because this function cannot generate embeddings on its own.
    ///
    /// Returns (intact_count, orphaned_count, missing_count, duration_ms).
    /// Reconcile LanceDB and RDB globally using set-difference.
    ///
    /// The `user_id` parameter is **deprecated and ignored** — rebuild always
    /// operates on the entire index. It is kept for proto backward compatibility.
    pub async fn rebuild_index(&self, user_id: Option<i64>) -> Result<(u32, u32, u32, i64)> {
        use infra::infra::thread::rdb::ThreadRepository;

        if user_id.is_some() {
            tracing::warn!(
                "rebuild_index: user_id parameter is deprecated and ignored; rebuild always operates globally"
            );
        }

        let start = std::time::Instant::now();

        // Bulk-fetch all IDs from both sides (no N+1)
        let (lance_ids, rdb_ids) = tokio::try_join!(
            self.vector_repo.get_all_thread_ids(),
            self.thread_repo.find_all_thread_ids(),
        )?;

        let lance_set: std::collections::HashSet<i64> = lance_ids.into_iter().collect();
        let rdb_set: std::collections::HashSet<i64> = rdb_ids.into_iter().collect();

        // Orphans: in LanceDB but not in RDB
        let orphan_ids: Vec<i64> = lance_set.difference(&rdb_set).copied().collect();
        let orphan_count = orphan_ids.len() as u32;
        for tid in &orphan_ids {
            self.vector_repo.delete(*tid).await?;
        }

        // Missing: in RDB but not in LanceDB
        let missing_ids: Vec<i64> = rdb_set.difference(&lance_set).copied().collect();
        let missing_count = missing_ids.len() as u32;
        for tid in &missing_ids {
            tracing::info!(
                "rebuild_index: thread {} in RDB but missing from LanceDB (needs re-embedding)",
                tid
            );
        }

        let intact = lance_set.len() as u32 - orphan_count;
        let duration_ms = start.elapsed().as_millis() as i64;
        Ok((intact, orphan_count, missing_count, duration_ms))
    }

    pub async fn get_stats(&self) -> Result<IndexStats> {
        use infra::infra::thread::rdb::ThreadRepository;
        use infra_utils::infra::rdb::UseRdbPool;

        let (mut stats, rdb_count) = tokio::try_join!(
            self.vector_repo.get_stats(),
            self.thread_repo.count_list_tx(self.thread_repo.db_pool()),
        )?;
        stats.rdb_total_records = Some(rdb_count as u64);
        Ok(stats)
    }

    pub async fn delete_thread_vector(&self, thread_id: i64) -> Result<()> {
        self.vector_repo.delete(thread_id).await?;
        Ok(())
    }

    /// Sync scalar columns (labels, channel, timestamps) in LanceDB
    /// without re-generating embeddings. N-row: re-syncs every chunk row
    /// (not just chunk 0) so LanceDB search filters that evaluate
    /// labels/channel per row stay consistent across the whole thread.
    /// Best-effort: skips if the thread has no embedding in LanceDB yet.
    pub async fn sync_thread_scalars(&self, thread_id: i64) -> Result<()> {
        let tid = ThreadId { value: thread_id };
        let thread = self.thread_repo.find(&tid).await?;
        let Some(thread) = thread else {
            tracing::warn!("sync_thread_scalars: thread_id={thread_id} not in RDB, skipping");
            return Ok(());
        };
        let Some(data) = thread.data.as_ref() else {
            return Ok(());
        };

        let labels = self
            .thread_label_repo
            .find_labels_by_thread(thread_id)
            .await?;

        self.vector_repo.sync_scalars(thread_id, data, labels).await
    }

    // ===== Internal helpers =====

    /// Hydrate search hits from RDB (with labels) and, when
    /// `query_text` is set, attach P3 highlight ranges over the
    /// thread's `description`. Stale LanceDB entries are dropped with
    /// a warning, matching the memory-side behaviour.
    async fn enrich_hits(
        &self,
        hits: Vec<ThreadVectorSearchHit>,
        include_description: bool,
        query_text: Option<&str>,
        limit: usize,
    ) -> Result<Vec<ThreadSearchResultItem>> {
        let thread_ids: Vec<i64> = hits.iter().map(|h| h.thread_id).collect();

        // Batch-fetch threads and labels in parallel
        let (threads, label_rows) = tokio::try_join!(
            self.thread_repo.find_by_ids(&thread_ids),
            self.thread_label_repo
                .find_labels_by_thread_ids(&thread_ids),
        )?;

        let mut thread_map: HashMap<i64, Thread> = threads
            .into_iter()
            .filter_map(|t| t.id.as_ref().map(|id| (id.value, t.clone())))
            .collect();
        let mut labels_map: HashMap<i64, Vec<String>> = HashMap::new();
        for row in label_rows {
            labels_map.entry(row.thread_id).or_default().push(row.label);
        }

        // Single fts_config snapshot — tokenizer settings are
        // process-wide. `include_description == false` strips the
        // description body, so highlight offsets into a stripped
        // payload would mislead the client; we surface empty
        // highlights instead.
        let fts_for_highlights = query_text
            .filter(|q| !q.is_empty() && include_description)
            .map(|q| (q, self.vector_repo.fts_config()));

        let mut results = Vec::with_capacity(hits.len().min(limit));
        for hit in hits {
            if results.len() >= limit {
                break;
            }
            match thread_map.remove(&hit.thread_id) {
                Some(mut thread) => {
                    if let Some(ref mut data) = thread.data {
                        data.labels = labels_map.remove(&hit.thread_id).unwrap_or_default();
                    }
                    // Compute highlights before stripping the description —
                    // the offsets would otherwise point into a payload the
                    // client never receives.
                    let highlights = compute_thread_highlights(&thread, fts_for_highlights);
                    if !include_description && let Some(ref mut data) = thread.data {
                        data.description = None;
                    }
                    results.push(ThreadSearchResultItem {
                        thread,
                        score: hit.score,
                        distance: hit.distance,
                        highlights,
                    });
                }
                None => {
                    tracing::warn!(
                        "Stale LanceDB thread entry: thread_id={} not in RDB",
                        hit.thread_id
                    );
                }
            }
        }

        Ok(results)
    }
}

/// Thread-side counterpart of `compute_memory_highlights`. Both
/// delegate to `compute_highlight_field` after extracting the
/// relevant string field; only the source enum and the input field
/// differ.
pub(crate) fn compute_thread_highlights(
    thread: &Thread,
    query_and_cfg: Option<(&str, &infra::infra::memory_vector::config::FtsConfig)>,
) -> Vec<protobuf::llm_memory::data::HighlightField> {
    use infra::infra::memory_vector::highlight::compute_highlight_field;
    use protobuf::llm_memory::data::HighlightSource;

    let text = thread.data.as_ref().and_then(|d| d.description.as_deref());
    compute_highlight_field(text, query_and_cfg, HighlightSource::Description)
}

/// Aggregate scores from multiple vector searches into a single ranked list.
fn aggregate_thread_scores(
    all_hits: &[Vec<ThreadVectorSearchHit>],
    strategy: AggregationStrategy,
    limit: usize,
) -> Vec<ThreadVectorSearchHit> {
    // thread_id → Vec<(vec_idx, rank, score)>
    let mut score_map: HashMap<i64, Vec<(usize, usize, f32)>> = HashMap::new();
    for (vec_idx, hits) in all_hits.iter().enumerate() {
        for (rank, hit) in hits.iter().enumerate() {
            score_map
                .entry(hit.thread_id)
                .or_default()
                .push((vec_idx, rank, hit.score));
        }
    }

    let num_vectors = all_hits.len() as f32;
    let mut merged: Vec<ThreadVectorSearchHit> = score_map
        .into_iter()
        .map(|(thread_id, entries)| {
            let aggregated = match strategy {
                AggregationStrategy::Sum => entries.iter().map(|(_, _, s)| *s).sum::<f32>(),
                AggregationStrategy::Average => {
                    entries.iter().map(|(_, _, s)| s).sum::<f32>() / num_vectors
                }
                AggregationStrategy::Max => entries
                    .iter()
                    .map(|(_, _, s)| *s)
                    .fold(f32::NEG_INFINITY, f32::max),
                AggregationStrategy::WeightedByPosition => entries
                    .iter()
                    .map(|(idx, _, s)| s / (*idx as f32 + 1.0))
                    .sum(),
                AggregationStrategy::RankFusion => entries
                    .iter()
                    .map(|(_, rank, _)| 1.0 / (*rank as f32 + 60.0))
                    .sum(),
            };
            ThreadVectorSearchHit {
                thread_id,
                score: aggregated,
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
    merged
}
