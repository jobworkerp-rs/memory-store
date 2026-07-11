//! Search RPCs for reflection memories. F-S1 (filter / paginate
//! sidecar), F-S2 (history per origin thread), F-S3 (similarity by
//! existing reflection), F-S7 (failure-signature distance ranking),
//! F-S8 (similarity by free-form intent text).

use anyhow::Result;
use std::collections::HashMap;

use infra::error::LlmMemoryError;
use infra::infra::memory::rdb::MemoryRepository;
use infra::infra::reflection::applied_target::ReflectionAppliedTargetRepository;
use infra::infra::reflection::fact::ReflectionFactRepository;
use infra::infra::reflection::failure_mode::ReflectionFailureModeRepository;
use infra::infra::reflection::failure_mode_convert;
use infra::infra::reflection::few_shot_usage::ReflectionFewShotUsageRepository;
use infra::infra::reflection::rdb::ThreadReflectionIndexRepository;
use infra::infra::reflection::rows::{
    ReflectionSortKey, ResolvedReflectionSearchFilter, ThreadReflectionIndexRow,
};
use infra::infra::reflection::tool::ReflectionToolRepository;
use infra::infra::reflection::tool_outcome::ReflectionToolOutcomeRepository;
use protobuf::llm_memory::data::AggregationStrategy;
use protobuf::llm_memory::data::{
    EmbeddingVector, FactLink, FactLinkField, FailureSignaturePatternType, HybridSearchOptions,
    HybridStrategy, Reflection, ReflectionData, ReflectionFact, ReflectionId,
    ReflectionSearchFilter, ReflectionSearchResult, ScoreSource, ThreadId, ToolOutcomeEntry,
    UserId,
};
use protobuf::llm_memory::service::ReflectionListSort;

use crate::app::memory_vector::approximate_fetch_limit;
use crate::app::reflection::ReflectionAppImpl;

/// Default page size when the request omits `limit`.
const DEFAULT_LIMIT: i64 = 50;
/// Hard cap to keep responses bounded.
const MAX_LIMIT: i64 = 500;
/// Default RDB scan cap for `match_failure_signatures`. Spec
/// §4.2.4 (`docs/thread-reflection-design.md`) prescribes ~1000 rows
/// of in-memory ranking; rows past the cap are not surfaced. Override
/// at runtime with `REFLECTION_FS_MATCH_SCAN_CAP`.
const MATCH_FAILURE_SCAN_CAP_DEFAULT: i64 = 1000;
const MATCH_FAILURE_SCAN_CAP_ENV: &str = "REFLECTION_FS_MATCH_SCAN_CAP";
const MATCH_FAILURE_SIGNATURES_TARGET: &str = "reflection.match_failure_signatures";
const SEARCH_HYBRID_TARGET: &str = "reflection.search_hybrid";

/// F-S1 — filter + sort over `thread_reflection_index` for the
/// "no query_text / no query_vectors" filter-only listing path. The
/// hybrid / vector-aware variant lives in `search_hybrid`.
///
/// `cursor_after_memory_id` is the proto `cursor_after_memory_id`
/// (memory_id desc keyset). Pagination uses `memory_id < cursor` —
/// the same path `export` walks — so resending the last `memory_id`
/// of the previous page steps forward without the OFFSET drift that
/// would happen if we plugged the Snowflake id straight into SQL
/// `OFFSET`. The handler refuses cursor + non-memory_id_desc sort
/// combinations because keyset stability would otherwise depend on
/// `created_at` ties.
pub async fn search(
    app: &ReflectionAppImpl,
    filter: Option<&ReflectionSearchFilter>,
    sort: ReflectionListSort,
    limit: Option<u32>,
    cursor_after_memory_id: Option<i64>,
) -> Result<Vec<ReflectionSearchResult>> {
    let mut resolved = filter.map(resolve_filter).unwrap_or_default();
    // Sort selection has two regimes:
    //
    // 1. Cursor-paged path (cursor present, or sort UNSPECIFIED so the
    //    next request might add a cursor): force `MemoryIdDesc`. A
    //    single-i64 `memory_id < cursor` keyset is only stable when
    //    the ORDER BY is `memory_id DESC` end-to-end — anything else
    //    (e.g. `created_at DESC, memory_id DESC`) lets a row with a
    //    smaller `memory_id` but a later `created_at` jump ahead of
    //    the cursor and disappear from the next page. Snowflake ids
    //    are time-monotonic at ms resolution, so this gives the same
    //    practical order as the old `CreatedAtDesc` default.
    // 2. Explicit sort path (caller picks SCORE_* / CREATED_* /
    //    FAILURE_FIRST): use the requested sort. The handler refuses
    //    a cursor in this case, so paging is OFFSET-based and the
    //    keyset invariant doesn't apply.
    let sort_key =
        if cursor_after_memory_id.is_some() || matches!(sort, ReflectionListSort::Unspecified) {
            ReflectionSortKey::MemoryIdDesc
        } else {
            map_sort(sort)
        };
    if let Some(cursor) = cursor_after_memory_id {
        resolved.memory_id_lt = Some(cursor);
    }
    let limit = clamp_limit(limit);

    let rows = app
        .index_repo
        .search_index(&resolved, sort_key, limit, 0)
        .await?;
    hydrate_rows(app, rows).await
}

/// F-S1 hybrid wrapper. Receives the raw proto signals from the
/// gRPC handler and decides which path runs:
///
/// 1. `query_text` + `query_vectors` both empty → caller bug; handler
///    routes filter-only requests to `search` directly. Returns
///    InvalidArgument here to fail loudly.
/// 2. `query_text` only → embed via jobworkerp, run vector path.
/// 3. `query_vectors` only → aggregate into a single vector using
///    `AggregationStrategy` (default AVERAGE), then vector path.
/// 4. both → aggregate text-embed + query_vectors into one vector,
///    then vector path; tag the result as `SCORE_HYBRID`.
///
/// `hybrid_options.strategy` selects the merge mode. RRF / WEIGHTED
/// are implemented via vector aggregation; `VECTOR_THEN_FTS` /
/// `FTS_THEN_VECTOR` return `Unimplemented` because the reflection
/// store has no FTS index (the spec implementation lives behind the
/// memory_vector path, not reflection). A warn log fires so
/// operators see the gap.
///
/// `boost_pinned_high_score` applies as a post-rank stable lift:
/// rows with `pinned=true && score>=0.7` move to the front while
/// preserving relative order.
#[allow(clippy::too_many_arguments)]
pub async fn search_hybrid(
    app: &ReflectionAppImpl,
    filter: Option<&ReflectionSearchFilter>,
    query_text: Option<&str>,
    query_vectors: &[EmbeddingVector],
    hybrid_options: Option<&HybridSearchOptions>,
    boost_pinned_high_score: bool,
    sort: ReflectionListSort,
    limit: Option<u32>,
) -> Result<Vec<ReflectionSearchResult>> {
    let _ = sort; // hybrid path is always relevance-sorted; explicit sort is ignored.
    let text_signal = query_text.map(str::trim).filter(|s| !s.is_empty());
    if text_signal.is_none() && query_vectors.is_empty() {
        return Err(LlmMemoryError::InvalidArgument(
            "search_hybrid requires at least one of query_text or query_vectors; \
             use `search` for filter-only listings"
                .into(),
        )
        .into());
    }

    // Reject the FTS-flavoured strategies up front: reflection has no
    // FTS index, so any two-stage variant that needs FTS as either
    // primary or secondary signal cannot run today.
    if let Some(opts) = hybrid_options {
        let strategy = HybridStrategy::try_from(opts.strategy).unwrap_or(HybridStrategy::Rrf);
        if matches!(
            strategy,
            HybridStrategy::VectorThenFts | HybridStrategy::FtsThenVector
        ) {
            tracing::warn!(
                target = SEARCH_HYBRID_TARGET,
                strategy = ?strategy,
                "VECTOR_THEN_FTS / FTS_THEN_VECTOR requested but reflection has no FTS index; rejecting"
            );
            return Err(LlmMemoryError::Unimplemented(format!(
                "hybrid strategy {strategy:?} requires an FTS index; reflection has none. \
                 Use RRF or WEIGHTED, which run as multi-vector aggregation over the \
                 reflection intent vector space."
            ))
            .into());
        }
    }

    // Both signal channels feed the LanceDB intent vector path, so
    // the repo must be configured before any signal can run.
    // Errors here mirror the F-S3 / F-S8 messaging so operators
    // reach for the same env variables (`MEMORY_VECTOR_ENABLED`,
    // `REFLECTION_LANCEDB_URI`).
    if app.intent_vector_repo.is_none() {
        return Err(LlmMemoryError::Unimplemented(
            "search_hybrid requires the reflection intent vector store to be configured \
             (set MEMORY_VECTOR_ENABLED=true and REFLECTION_LANCEDB_URI)."
                .into(),
        )
        .into());
    }

    let limit_u32 = limit.unwrap_or(DEFAULT_LIMIT as u32);
    let top_k = (limit_u32 as i64).clamp(1, MAX_LIMIT) as u32;
    let mut signals: Vec<Vec<f32>> = Vec::with_capacity(query_vectors.len() + 1);
    let mut text_was_present = false;
    if let Some(text) = text_signal {
        let client = app.jobworkerp_client.as_ref().ok_or_else(|| {
            LlmMemoryError::Unimplemented(
                "search_hybrid with query_text requires the jobworkerp embedding client. \
                 Set JOBWORKERP_ADDR and ensure the `memories-mm-embedding` worker is \
                 registered."
                    .into(),
            )
        })?;
        let embedded = embed_intent_text_via_jobworkerp(client, text).await?;
        signals.push(embedded);
        text_was_present = true;
    }
    for v in query_vectors {
        if v.values.is_empty() {
            continue;
        }
        signals.push(v.values.clone());
    }
    if signals.is_empty() {
        return Err(LlmMemoryError::InvalidArgument(
            "search_hybrid: every query_vector is empty and query_text resolved to nothing".into(),
        )
        .into());
    }
    let agg_strategy = AggregationStrategy::Average;
    let merged = aggregate_query_vectors(&signals, agg_strategy)?;
    let mut hits =
        find_similar_by_vector(app, std::slice::from_ref(&merged), top_k, filter, None).await?;

    // SCORE_HYBRID iff multiple distinct signals contributed
    // (text + vector, or multi vector). A single intent signal
    // stays SCORE_VECTOR for consistency with F-S3 / F-S8.
    let use_hybrid_label = text_was_present && !query_vectors.is_empty()
        || (!text_was_present && query_vectors.len() > 1);
    if use_hybrid_label {
        for r in &mut hits {
            r.score_source = ScoreSource::ScoreHybrid as i32;
        }
    }

    if boost_pinned_high_score {
        apply_pinned_high_score_boost(&mut hits);
    }

    Ok(hits)
}

/// Aggregate multiple query vectors into a single query vector by the
/// requested strategy. The reflection hybrid path collapses signals
/// to one vector before issuing the LanceDB query (rather than running
/// N separate ANN calls and merging ranks) because the intent vector
/// store is the only data plane available; running N times then RRF-
/// merging would be equivalent up to scaling for AVERAGE / SUM /
/// WEIGHTED_BY_POSITION but more expensive.
///
/// `RANK_FUSION` (proto-level RRF over multi-query rankings) is not
/// implemented in this path — pre-aggregation cannot express it. The
/// `hybrid_options.strategy = RRF` mode runs through AVERAGE because
/// reflection has only one ranking source; full N-query RRF over the
/// intent vector store is tracked as a follow-up.
fn aggregate_query_vectors(
    signals: &[Vec<f32>],
    strategy: AggregationStrategy,
) -> Result<Vec<f32>> {
    if signals.is_empty() {
        return Err(LlmMemoryError::InvalidArgument(
            "aggregate_query_vectors: empty signal list".into(),
        )
        .into());
    }
    let dim = signals[0].len();
    if dim == 0 {
        return Err(LlmMemoryError::InvalidArgument(
            "aggregate_query_vectors: first signal has zero dimension".into(),
        )
        .into());
    }
    for (i, s) in signals.iter().enumerate() {
        if s.len() != dim {
            return Err(LlmMemoryError::InvalidArgument(format!(
                "query vector dimension mismatch at index {i}: expected {dim}, got {}",
                s.len()
            ))
            .into());
        }
    }
    let mut out = vec![0.0_f32; dim];
    match strategy {
        AggregationStrategy::Sum | AggregationStrategy::Average => {
            for s in signals {
                for (o, v) in out.iter_mut().zip(s.iter()) {
                    *o += *v;
                }
            }
            if matches!(strategy, AggregationStrategy::Average) {
                let n = signals.len() as f32;
                for o in out.iter_mut() {
                    *o /= n;
                }
            }
        }
        AggregationStrategy::Max => {
            // Componentwise max over signals.
            out.copy_from_slice(&signals[0]);
            for s in &signals[1..] {
                for (o, v) in out.iter_mut().zip(s.iter()) {
                    if *v > *o {
                        *o = *v;
                    }
                }
            }
        }
        AggregationStrategy::WeightedByPosition => {
            for (idx, s) in signals.iter().enumerate() {
                let w = 1.0_f32 / (idx as f32 + 1.0);
                for (o, v) in out.iter_mut().zip(s.iter()) {
                    *o += *v * w;
                }
            }
        }
        AggregationStrategy::RankFusion => {
            // RANK_FUSION requires per-query rankings, which are not
            // available before issuing the ANN call. Fall back to
            // AVERAGE so the caller sees a reasonable aggregate
            // rather than InvalidArgument — RRF is the proto default
            // strategy and a single-vector reflection path cannot
            // emulate per-query rankings.
            tracing::warn!(
                target = SEARCH_HYBRID_TARGET,
                "RANK_FUSION aggregation requested but reflection collapses signals \
                 pre-ANN; falling back to AVERAGE"
            );
            for s in signals {
                for (o, v) in out.iter_mut().zip(s.iter()) {
                    *o += *v;
                }
            }
            let n = signals.len() as f32;
            for o in out.iter_mut() {
                *o /= n;
            }
        }
    }
    Ok(out)
}

/// F-S6 post-rank lift: pinned rows with `score >= 0.7` move to the
/// front while preserving relative order among the boosted group and
/// among the non-boosted group (stable partition). Threshold 0.7 is
/// the spec default; not user-tunable in this iteration.
fn apply_pinned_high_score_boost(hits: &mut [ReflectionSearchResult]) {
    const PIN_BOOST_THRESHOLD: f32 = 0.7;
    // sort_by_key with a bool key partitions stably (false < true);
    // negate so the boosted bucket sorts first.
    hits.sort_by_key(|r| {
        !r.reflection
            .as_ref()
            .and_then(|refl| refl.data.as_ref())
            .map(|d| d.pinned && d.score >= PIN_BOOST_THRESHOLD)
            .unwrap_or(false)
    });
}

/// F-S2 — full history for one origin thread, ordered by created_at desc.
pub async fn find_by_thread(
    app: &ReflectionAppImpl,
    thread_id: &ThreadId,
    include_history: bool,
) -> Result<Vec<Reflection>> {
    let rows = if include_history {
        app.index_repo
            .list_history_by_thread_id(thread_id.value)
            .await?
    } else {
        app.index_repo
            .find_active_by_thread_id(thread_id.value, false)
            .await?
            .into_iter()
            .collect()
    };
    let results = hydrate_rows(app, rows).await?;
    Ok(results.into_iter().filter_map(|r| r.reflection).collect())
}

/// F-S7 — ranks the filter-matched scan window by `metadata.eval.failure_signature`
/// distance and hydrates only the top-k survivors. Returns
/// `(hits, is_truncated, scanned_count)` so the RPC layer can pass
/// the scan-cap truncation signal through `MatchSignaturesResponse`
/// instead of hiding it in a server-side warn log.
pub async fn match_failure_signatures(
    app: &ReflectionAppImpl,
    indicators: &protobuf::llm_memory::data::FailureSignatureIndicators,
    pattern_type: Option<i32>,
    top_k: u32,
    filter: Option<&ReflectionSearchFilter>,
) -> Result<(Vec<ReflectionSearchResult>, bool, u32)> {
    require_positive_top_k(top_k, "MatchFailureSignatures")?;
    let top_k_usize = clamp_top_k(top_k);
    let resolved = filter.map(resolve_filter).unwrap_or_default();
    let sort_key = ReflectionSortKey::CreatedAtDesc;
    // Cap is best-effort per spec §4.2.4: rows past the cap are not
    // surfaced. Operators tune via `REFLECTION_FS_MATCH_SCAN_CAP`.
    let scan_cap = match_failure_scan_cap();
    let rows = app
        .index_repo
        .search_index(&resolved, sort_key, scan_cap, 0)
        .await?;
    let scanned_count = rows.len() as u32;
    let is_truncated = rows.len() as i64 >= scan_cap;
    if is_truncated {
        tracing::warn!(
            target = MATCH_FAILURE_SIGNATURES_TARGET,
            scan_cap,
            scanned = rows.len(),
            "match_failure_signatures hit scan cap; older rows beyond the cap are excluded from ranking — narrow the filter or raise REFLECTION_FS_MATCH_SCAN_CAP",
        );
    }
    if rows.is_empty() {
        return Ok((Vec::new(), is_truncated, scanned_count));
    }
    let input_pattern = pattern_type
        .and_then(|v| FailureSignaturePatternType::try_from(v).ok())
        .unwrap_or(FailureSignaturePatternType::FailureSignaturePatternUnspecified);

    // Score every scan-window row using metadata only, then drop the
    // metadata HashMap before hydrate so the scan_cap-sized JSON
    // payload does not stay resident through child-table fan-out.
    // (`hydrate_rows` will re-fetch the top_k memory bodies; the
    // refetch is bounded by top_k so the extra cost is small.)
    let scored = {
        let memory_ids: Vec<i64> = rows.iter().map(|r| r.memory_id).collect();
        let memories = app.memory_repo.find_by_ids(&memory_ids, false).await?;
        let mut metadata_by_id: HashMap<i64, Option<serde_json::Value>> =
            HashMap::with_capacity(memories.len());
        for m in memories {
            let Some(id) = m.id.as_ref().map(|i| i.value) else {
                continue;
            };
            let metadata = m
                .data
                .and_then(|d| parse_metadata_json(d.metadata.as_deref()));
            metadata_by_id.insert(id, metadata);
        }
        // Rows without a stored `failure_signature` are excluded:
        // `distance::distance` collapses "missing on both sides" to 0,
        // which would mask a signed row's positive distance. Spec
        // §4.2.4 only ranks rows whose signature is populated.
        let mut scored: Vec<(f32, ThreadReflectionIndexRow)> = Vec::with_capacity(rows.len());
        for row in rows {
            let metadata = metadata_by_id.get(&row.memory_id).and_then(|m| m.as_ref());
            let Some(stored_signature) = extract_signature_from_metadata(metadata) else {
                continue;
            };
            let base = super::distance::distance(
                indicators,
                &stored_signature.indicators,
                &app.norm_table,
            );
            let ranked =
                super::distance::rank_distance(base, input_pattern, stored_signature.pattern);
            scored.push((ranked, row));
        }
        scored.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
        scored
    };

    // Carry the rank distance alongside the row via a HashMap rather
    // than a parallel `Vec`: `hydrate_rows` skips rows whose
    // `memory.data` is absent (rebuild race), so `zip` over the input
    // and the hydrated output would silently misalign.
    let (selected_rows, mut distance_by_id): (Vec<_>, HashMap<i64, f32>) = {
        let mut rows = Vec::with_capacity(top_k_usize);
        let mut dists = HashMap::with_capacity(top_k_usize);
        for (dist, row) in scored.into_iter().take(top_k_usize) {
            dists.insert(row.memory_id, dist);
            rows.push(row);
        }
        (rows, dists)
    };
    let mut hydrated = hydrate_rows(app, selected_rows).await?;
    // Drop hits whose distance cannot be joined (sidecar/memory race) —
    // a default 0.0 distance would yield score=1.0 and surface a phantom
    // top match. `tracing::warn!` lets operators spot the divergence.
    hydrated.retain_mut(|h| {
        let Some(id) = h
            .reflection
            .as_ref()
            .and_then(|r| r.id.as_ref())
            .map(|i| i.value)
        else {
            tracing::warn!(
                target = MATCH_FAILURE_SIGNATURES_TARGET,
                "hydrated hit missing reflection.id; dropping",
            );
            return false;
        };
        let Some(dist) = distance_by_id.remove(&id) else {
            tracing::warn!(
                target = MATCH_FAILURE_SIGNATURES_TARGET,
                memory_id = id,
                "hydrated hit has no scored distance; dropping (sidecar/memory race)",
            );
            return false;
        };
        h.distance = dist;
        h.score = 1.0 / (1.0 + dist);
        // Override `hydrate_rows`' default (SCORE_FILTER_ONLY): F-S7
        // is a similarity-ranking RPC, not a filter-only listing.
        h.score_source = ScoreSource::ScoreVector as i32;
        true
    });
    Ok((hydrated, is_truncated, scanned_count))
}

/// Candidate cap multiplier for the RDB sidecar pre-filter that
/// feeds the LanceDB IN-list. We fetch `top_k * CANDIDATE_OVERSAMPLE`
/// candidate rows so the ANN re-ranking has enough headroom to skip
/// over rows whose vector turns out to be less relevant than their
/// recency-based RDB ordering would suggest. The cap is bounded
/// below by `top_k * 1` (when `top_k` is small enough that
/// `MAX_LIMIT` would otherwise dominate) and above by `MAX_LIMIT`.
const CANDIDATE_OVERSAMPLE: i64 = 20;

/// Staged over-fetch knobs, read from the same env names + defaults as
/// the memory / thread vector paths so operators tune one knob set. The
/// `find_similar_by_vector` staged loop widens the fetch using these
/// because the intent repo de-dups chunk rows to one hit per reflection,
/// so a reflection with many chunks can dominate the ANN window.
/// `ReflectionAppImpl` holds no `ThreadFilterConfig`, so these read env
/// directly (per-request env reads are negligible vs the ANN/RDB cost).
fn vector_dedup_overfetch_k() -> usize {
    crate::app::thread_filter_resolver::parse_env_or(
        crate::app::thread_filter_resolver::VECTOR_DEDUP_OVERFETCH_K_ENV,
        crate::app::thread_filter_resolver::VECTOR_DEDUP_OVERFETCH_K_DEFAULT,
    )
}

fn vector_dedup_max_fetch() -> usize {
    crate::app::thread_filter_resolver::parse_env_or(
        crate::app::thread_filter_resolver::VECTOR_DEDUP_MAX_FETCH_ENV,
        crate::app::thread_filter_resolver::VECTOR_DEDUP_MAX_FETCH_DEFAULT,
    )
}

/// F-S3 — vector similarity search seeded by an existing reflection.
///
/// Two reference forms:
///   - `Thread(tid)`: pick the *active* reflection on that origin
///     thread (latest reflection with `intent_embedding_status=OK`).
///     A thread with no reflection or one whose newest reflection
///     has not finished its intent embedding NotFounds explicitly —
///     spec §4.2.3 / fixpoint #29 forbids silent empty-stream
///     fallback.
///   - `Reflection(rid)`: explicit version pin. NotFound if the id
///     is unknown OR if its `intent_embedding_status != OK` (the
///     stored vector is meaningless / stale).
///
/// The reference embedding is fetched directly from LanceDB
/// (`find_embedding_by_memory_id`); we never re-embed
/// `metadata.eval.task_intent` because the stored vector is the
/// canonical query — re-embedding would drift across model changes.
pub async fn find_similar_trajectories(
    app: &ReflectionAppImpl,
    reference: super::TrajectoryReference,
    top_k: u32,
    filter: Option<&ReflectionSearchFilter>,
) -> Result<Vec<ReflectionSearchResult>> {
    require_positive_top_k(top_k, "FindSimilarTrajectories")?;
    let intent_repo = app.intent_vector_repo.as_ref().ok_or_else(|| {
        LlmMemoryError::Unimplemented(
            "FindSimilarTrajectories requires the reflection intent vector store \
             to be configured (set MEMORY_VECTOR_ENABLED=true and \
             REFLECTION_LANCEDB_URI)."
                .into(),
        )
    })?;

    // 1. Resolve the reference memory_id and assert intent embedding readiness.
    let reference_memory_id = resolve_reference_memory_id(app, &reference).await?;

    // 2. Fetch the stored vector(s) for that reflection. N-row: a
    //    reflection may own several intent chunks; all are used as
    //    reference query vectors and aggregated downstream.
    let reference_vectors = intent_repo
        .find_embeddings_by_memory_id(reference_memory_id)
        .await
        .map_err(|e| {
            LlmMemoryError::OtherError(format!(
                "intent vector lookup failed for reflection {reference_memory_id}: {e:#}"
            ))
        })?;
    if reference_vectors.is_empty() {
        return Err(LlmMemoryError::NotFound(format!(
            "reflection {reference_memory_id} has intent_embedding_status=OK in the \
             sidecar but no row in the LanceDB intent table. Run \
             RedispatchReflectionEmbeddings(kind=INTENT) to repair."
        ))
        .into());
    }

    // 3. Self-exclusion: the reference reflection itself is
    //    always the top hit (distance 0); strip it from the
    //    candidate set so the response reflects "other similar
    //    trajectories", not "this same trajectory".
    find_similar_by_vector(
        app,
        &reference_vectors,
        top_k,
        filter,
        Some(reference_memory_id),
    )
    .await
}

/// F-S8 — vector similarity search seeded by free-form intent text.
///
/// Embeds the query text synchronously via the shared
/// `memories-mm-embedding` worker (same model as stored intent
/// vectors, otherwise cosine distance is meaningless), then takes
/// the same 2-stage filter + ANN path as F-S3. The synchronous
/// embed roundtrip costs ~50–200ms against a warm GPU worker and is
/// the same model the stored vectors were produced with, so query
/// vectors live in the same space as the index.
pub async fn find_similar_by_intent_text(
    app: &ReflectionAppImpl,
    intent_text: &str,
    top_k: u32,
    filter: Option<&ReflectionSearchFilter>,
) -> Result<Vec<ReflectionSearchResult>> {
    require_positive_top_k(top_k, "FindSimilarByIntentText")?;
    if app.intent_vector_repo.is_none() {
        return Err(LlmMemoryError::Unimplemented(
            "FindSimilarByIntentText requires the reflection intent vector store \
             to be configured (set MEMORY_VECTOR_ENABLED=true and \
             REFLECTION_LANCEDB_URI)."
                .into(),
        )
        .into());
    }
    let client = app.jobworkerp_client.as_ref().ok_or_else(|| {
        // `MEMORY_REFLECTION_DISPATCH_ENABLED` is intentionally NOT
        // referenced here — that flag gates Generate / the post-
        // finalize dispatchers, not F-S8. The shared jobworkerp
        // client is constructed whenever `JOBWORKERP_ADDR` is set
        // (see `module.rs::reflection_jobworkerp_client`), so the
        // operator-facing fix is to set that env and register the
        // embedding worker.
        LlmMemoryError::Unimplemented(
            "FindSimilarByIntentText requires the jobworkerp embedding client. Set \
             JOBWORKERP_ADDR and ensure the `memories-mm-embedding` worker is registered."
                .into(),
        )
    })?;

    let trimmed = intent_text.trim();
    if trimmed.is_empty() {
        return Err(LlmMemoryError::InvalidArgument(
            "FindSimilarByIntentText requires non-empty intent_text".into(),
        )
        .into());
    }

    let query_vector = embed_intent_text_via_jobworkerp(client, trimmed).await?;
    find_similar_by_vector(
        app,
        std::slice::from_ref(&query_vector),
        top_k,
        filter,
        None,
    )
    .await
}

/// Shared 2-stage filter + ANN + hydrate path. Both F-S3 and F-S8
/// converge here once they have a query vector. `exclude_memory_id`
/// strips the reference reflection from F-S3 results (irrelevant
/// for F-S8 which has no concrete reference).
///
/// Filter routing: when every requested predicate is a column the
/// LanceDB schema carries (`origin_user_id`, `task_category`,
/// `reflection_aspect`, `outcome`), we skip the RDB pre-narrow and
/// query LanceDB directly across the full corpus — otherwise the
/// recency-ordered RDB cap could silently drop older rows whose
/// vector distance would rank them in `top_k`. For predicates that
/// only the sidecar knows about (prompt_version / experiment / failure
/// modes / etc.) the RDB pre-narrow is still required; that path
/// remains capped at `candidate_cap` and emits a warning when the cap
/// is hit.
async fn find_similar_by_vector(
    app: &ReflectionAppImpl,
    query_vectors: &[Vec<f32>],
    top_k: u32,
    filter: Option<&ReflectionSearchFilter>,
    exclude_memory_id: Option<i64>,
) -> Result<Vec<ReflectionSearchResult>> {
    use infra::infra::reflection::rdb::EMBEDDING_STATUS_OK_VALUE;

    if query_vectors.is_empty() {
        return Ok(Vec::new());
    }

    let intent_repo = app
        .intent_vector_repo
        .as_ref()
        .expect("find_similar_by_vector entered without intent_vector_repo; caller must gate this");

    let resolved = filter.map(resolve_filter).unwrap_or_default();
    // Callers (`find_similar_trajectories` / `find_similar_by_intent_text`)
    // already reject top_k == 0 with InvalidArgument so a non-positive
    // value cannot reach here; clamp keeps the upper bound enforced.
    let top_k_i64 = (top_k as i64).clamp(1, MAX_LIMIT);

    // 1. Pick the search strategy. LanceDB-only is the preferred path
    //    because the RDB narrow's recency cap silently excludes older
    //    rows from ranking.
    //
    //    `base_cap` overfetches by `CANDIDATE_OVERSAMPLE` (capped at
    //    `MAX_LIMIT * 4`) so the sidecar `intent_embedding_status`
    //    re-validation in step 2 cannot silently drop the response below
    //    `top_k`. N-row: the intent repo de-dups chunk rows to one hit
    //    per reflection, so a reflection with many chunks can fill the ANN
    //    window; the staged loop below widens the LanceDB fetch (K → 2K →
    //    3K, clipped at `vector_dedup_max_fetch` and the absolute
    //    `MAX_LIMIT*4` ceiling) until enough distinct reflections survive
    //    re-validation. Mirrors `MemoryVectorAppImpl::search_with_overfetch`.
    let base_cap = (top_k_i64 * CANDIDATE_OVERSAMPLE).clamp(top_k_i64, MAX_LIMIT * 4) as usize;
    let dedup_k = vector_dedup_overfetch_k();
    let max_fetch = vector_dedup_max_fetch().min((MAX_LIMIT * 4) as usize);
    let intent_filter = build_intent_filter(&resolved, exclude_memory_id)?;
    let top_k_usize = top_k_i64 as usize;

    // The RDB sidecar narrow is the expensive scan, so fetch it ONCE at
    // the largest staged cap; only the LanceDB ANN + aggregate +
    // re-validation loop below widens against this fixed candidate set.
    let candidate_ids: Option<Vec<i64>> = if requires_sidecar_narrow(&resolved) {
        // 1a. RDB narrow: predicates the LanceDB schema cannot answer
        //     force us back to recency-capped candidate fetching.
        let narrow_cap = approximate_fetch_limit(base_cap, dedup_k, 3, max_fetch) as i64;
        let mut narrow = resolved.clone();
        if narrow.intent_embedding_status.is_none() {
            narrow.intent_embedding_status =
                Some(protobuf::llm_memory::data::EmbeddingStatus::Ok as i32);
        }
        let candidate_rows = app
            .index_repo
            .search_index(&narrow, ReflectionSortKey::CreatedAtDesc, narrow_cap, 0)
            .await?;
        if candidate_rows.is_empty() {
            return Ok(Vec::new());
        }
        if candidate_rows.len() as i64 >= narrow_cap {
            tracing::warn!(
                target = "reflection.find_similar_by_vector",
                candidate_cap = narrow_cap,
                "candidate cap hit during sidecar narrow; older rows beyond the cap are \
                 excluded from vector ranking. Tighten the filter or move predicates onto \
                 LanceDB schema columns (origin_user_id / task_category / reflection_aspect / outcome).",
            );
        }
        let ids: Vec<i64> = candidate_rows
            .iter()
            .map(|r| r.memory_id)
            .filter(|id| Some(*id) != exclude_memory_id)
            .collect();
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        Some(ids)
    } else {
        // 1b. LanceDB-only: every predicate is push-down-friendly, so
        //     hand the query straight to ANN over the full corpus.
        None
    };

    // Staged ANN + aggregate + sidecar re-validation. The shortfall is
    // evaluated AFTER re-validation (stale `intent_embedding_status` rows
    // are dropped there), so a wider attempt only runs when distinct
    // OK-status reflections fall short of `top_k`.
    let mut ordered_rows: Vec<ThreadReflectionIndexRow> = Vec::new();
    let mut score_by_id: HashMap<i64, (f32, f32)> = HashMap::new();
    let mut last_fetch = 0usize;
    for attempt in 1..=3usize {
        let fetch_cap = approximate_fetch_limit(base_cap, dedup_k, attempt, max_fetch);
        if attempt > 1 && fetch_cap <= last_fetch {
            break;
        }
        last_fetch = fetch_cap;

        // Run ANN per query vector and aggregate per reflection. A
        // reference reflection (F-S3) may own several intent chunks → one
        // ANN per chunk, Average-aggregated (single-vector F-S8 is a
        // passthrough).
        let mut per_vector_hits: Vec<
            Vec<infra::infra::reflection_intent_vector::repository::IntentSearchHit>,
        > = Vec::with_capacity(query_vectors.len());
        for qv in query_vectors {
            let hits = match &candidate_ids {
                Some(ids) => intent_repo
                    .search_with_candidate_ids(qv, ids, intent_filter.as_ref(), fetch_cap)
                    .await
                    .map_err(|e| {
                        LlmMemoryError::OtherError(format!(
                            "reflection intent vector search failed: {e:#}"
                        ))
                    })?,
                None => intent_repo
                    .search_by_vector(qv, intent_filter.as_ref(), fetch_cap)
                    .await
                    .map_err(|e| {
                        LlmMemoryError::OtherError(format!(
                            "reflection intent vector search failed: {e:#}"
                        ))
                    })?,
            };
            per_vector_hits.push(hits);
        }

        let hits = aggregate_intent_hits(per_vector_hits, fetch_cap);
        if hits.is_empty() {
            break;
        }

        // Bulk-fetch sidecar rows for every hit and re-validate
        // `intent_embedding_status`: a redispatch / rebuild in flight can
        // drift the sidecar to FAILED/PENDING after the LanceDB row was
        // written, and those vectors must not contribute to ranking.
        let hit_ids: Vec<i64> = hits.iter().map(|h| h.memory_id).collect();
        let sidecar_rows = app.index_repo.find_by_memory_ids(&hit_ids).await?;
        let mut row_by_id: HashMap<i64, ThreadReflectionIndexRow> =
            HashMap::with_capacity(sidecar_rows.len());
        for r in sidecar_rows {
            row_by_id.insert(r.memory_id, r);
        }

        ordered_rows = Vec::with_capacity(top_k_usize.min(hits.len()));
        score_by_id = HashMap::with_capacity(top_k_usize);
        for hit in &hits {
            if ordered_rows.len() >= top_k_usize {
                break;
            }
            let Some(row) = row_by_id.remove(&hit.memory_id) else {
                continue;
            };
            if row.intent_embedding_status != EMBEDDING_STATUS_OK_VALUE {
                continue;
            }
            score_by_id.insert(hit.memory_id, (hit.score, hit.distance));
            ordered_rows.push(row);
        }

        if ordered_rows.len() >= top_k_usize {
            break;
        }
    }
    if ordered_rows.is_empty() {
        return Ok(Vec::new());
    }
    if ordered_rows.len() < top_k_usize {
        tracing::warn!(
            target = "reflection.find_similar_by_vector",
            requested = top_k_usize,
            unique = ordered_rows.len(),
            max_fetch,
            "reflection vector de-dup short of top_k (N-row: a few \
             multi-chunk reflections may dominate the ANN window; \
             safety > completeness)",
        );
    }
    let mut results = hydrate_rows(app, ordered_rows).await?;
    for r in &mut results {
        if let Some(refl) = &r.reflection
            && let Some(id) = refl.id.as_ref()
            && let Some((score, distance)) = score_by_id.get(&id.value)
        {
            r.score = *score;
            r.distance = *distance;
            r.score_source = protobuf::llm_memory::data::ScoreSource::ScoreVector as i32;
        }
    }
    Ok(results)
}

/// Aggregate per-vector intent hit lists into one ranked list per
/// reflection using the Average strategy (the canonical multi-vector
/// default, matching `memory_vector`'s `AggregationStrategy::Average`).
/// The score is the mean over the query vectors that returned the
/// reflection, divided by the total vector count so a reflection matched
/// by only some chunks is penalised. `distance` keeps the best (smallest)
/// observed value. Single-vector input is a passthrough.
fn aggregate_intent_hits(
    per_vector_hits: Vec<Vec<infra::infra::reflection_intent_vector::repository::IntentSearchHit>>,
    limit: usize,
) -> Vec<infra::infra::reflection_intent_vector::repository::IntentSearchHit> {
    use infra::infra::reflection_intent_vector::repository::IntentSearchHit;

    if per_vector_hits.len() == 1 {
        let mut hits = per_vector_hits.into_iter().next().unwrap();
        hits.truncate(limit);
        return hits;
    }

    let num_vectors = per_vector_hits.len() as f32;
    let mut order: Vec<i64> = Vec::new();
    let mut acc: HashMap<i64, (f32, IntentSearchHit)> = HashMap::new();
    for hits in per_vector_hits {
        for hit in hits {
            match acc.get_mut(&hit.memory_id) {
                None => {
                    order.push(hit.memory_id);
                    acc.insert(hit.memory_id, (hit.score, hit));
                }
                Some((sum, best)) => {
                    *sum += hit.score;
                    if hit.distance < best.distance {
                        *best = hit;
                    }
                }
            }
        }
    }

    let mut merged: Vec<IntentSearchHit> = order
        .into_iter()
        .filter_map(|id| acc.remove(&id))
        .map(|(sum, mut hit)| {
            hit.score = sum / num_vectors;
            hit
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

fn build_intent_filter(
    resolved: &ResolvedReflectionSearchFilter,
    exclude_memory_id: Option<i64>,
) -> Result<Option<infra::infra::reflection_intent_vector::safe_filter::IntentSafeFilter>> {
    use infra::infra::reflection_intent_vector::safe_filter::IntentSafeFilter;

    // The LanceDB schema mirrors a subset of the sidecar columns:
    // origin_user_id, task_category, reflection_aspect, outcome,
    // memory_id. The remaining filter columns (e.g. prompt_version,
    // failure_modes) are RDB-only and already applied during the
    // candidate narrow above.
    let mut filters: Vec<IntentSafeFilter> = Vec::new();
    if let Some(uid) = resolved.origin_user_id {
        filters.push(IntentSafeFilter::origin_user_id(uid));
    }
    if !resolved.task_categories.is_empty() {
        filters.push(IntentSafeFilter::task_categories(
            &resolved.task_categories,
        )?);
    }
    if let Some(aspect) = resolved.reflection_aspect {
        filters.push(IntentSafeFilter::reflection_aspect(aspect));
    }
    if !resolved.outcomes.is_empty() {
        filters.push(IntentSafeFilter::outcomes(&resolved.outcomes)?);
    }
    if let Some(exclude) = exclude_memory_id {
        filters.push(IntentSafeFilter::memory_id_not(exclude));
    }

    Ok(filters.into_iter().reduce(|acc, f| acc.and(f)))
}

/// Decide whether `find_similar_by_vector` needs the recency-capped
/// RDB pre-narrow. Returns true iff the caller filter touches a column
/// the LanceDB intent schema does NOT mirror — those predicates can
/// only be enforced against the sidecar. When every requested
/// predicate is push-down friendly (origin_user_id / task_categories /
/// reflection_aspect / outcomes), we skip the narrow and run ANN
/// across the full corpus, then re-validate `intent_embedding_status`
/// per hit.
fn requires_sidecar_narrow(resolved: &ResolvedReflectionSearchFilter) -> bool {
    resolved.origin_channel.is_some()
        || resolved.origin_thread_id.is_some()
        || resolved.score_min.is_some()
        || resolved.score_max.is_some()
        || !resolved.failure_modes_all.is_empty()
        || !resolved.failure_modes_any.is_empty()
        || !resolved.tools_used_all.is_empty()
        || !resolved.tools_used_any.is_empty()
        || resolved.prompt_version.is_some()
        || resolved.target_model_version.is_some()
        || resolved.experiment_id.is_some()
        || resolved.experiment_variant.is_some()
        || resolved.pinned.is_some()
        || resolved.dataset_quality.is_some()
        // `intent_embedding_status` lives on the sidecar but is implicit
        // (always OK) when a row exists in LanceDB. Callers that
        // explicitly pin a non-OK status need the sidecar narrow to
        // honour that — but doing so against the intent vector store
        // is nonsensical (no row → no result). Send those through the
        // narrow path so the error mode stays consistent with the rest
        // of the filter axes.
        || matches!(
            resolved.intent_embedding_status,
            Some(s) if s != protobuf::llm_memory::data::EmbeddingStatus::Ok as i32,
        )
        || resolved.summary_embedding_status.is_some()
        || resolved.created_after.is_some()
        || resolved.created_before.is_some()
        || resolved.is_recurrence.is_some()
        || resolved.memory_id_lt.is_some()
}

async fn resolve_reference_memory_id(
    app: &ReflectionAppImpl,
    reference: &super::TrajectoryReference,
) -> Result<i64> {
    use infra::infra::reflection::rdb::ThreadReflectionIndexRepository;
    use protobuf::llm_memory::data::EmbeddingStatus;

    let ok_status = EmbeddingStatus::Ok as i32;
    match reference {
        super::TrajectoryReference::Thread(tid) => {
            // Spec §3.7.1 defines `active reflection` as the
            // `created_at`-max row AND `intent_embedding_status=OK` —
            // both conditions, no fallback to an older OK row. The
            // infra helper's `require_intent_ok=true` mode returns the
            // newest OK row, which would happily silently use a stale
            // reflection when the newest reflection on the thread is
            // still PENDING / FAILED. Fetch the newest unconditionally
            // and reject when its intent embedding has not finished.
            let row = app
                .index_repo
                .find_active_by_thread_id(tid.value, false)
                .await?
                .ok_or_else(|| {
                    LlmMemoryError::NotFound(format!("no reflection on thread {}", tid.value))
                })?;
            if row.intent_embedding_status != ok_status {
                return Err(LlmMemoryError::NotFound(format!(
                    "newest reflection on thread {} has intent_embedding_status != OK \
                     (current: {}); spec §3.7.1 forbids using an older row as a stand-in",
                    tid.value, row.intent_embedding_status
                ))
                .into());
            }
            Ok(row.memory_id)
        }
        super::TrajectoryReference::Reflection(rid) => {
            let row = app
                .index_repo
                .find_by_memory_id(rid.value)
                .await?
                .ok_or_else(|| {
                    LlmMemoryError::NotFound(format!("reflection {} not found", rid.value))
                })?;
            if row.intent_embedding_status != ok_status {
                return Err(LlmMemoryError::NotFound(format!(
                    "reflection {} has intent_embedding_status != OK (current: {}); cannot be \
                     used as a similarity reference",
                    rid.value, row.intent_embedding_status
                ))
                .into());
            }
            Ok(row.memory_id)
        }
    }
}

async fn embed_intent_text_via_jobworkerp(
    client: &std::sync::Arc<jobworkerp_client::client::wrapper::JobworkerpClientWrapper>,
    intent_text: &str,
) -> Result<Vec<f32>> {
    // Mirror the `generateEmbedding` step in
    // `workflows/auto-reflection-intent-embedding.yaml`. We call the
    // worker directly (no workflow wrapper) because we are not
    // persisting the vector — just using it as a query key. The query
    // is short, so embed_text returns a single chunk; we read
    // embeddings[0].
    //
    // The worker name comes from `mm_embedding_worker_name()`, which
    // reads the same `MEMORY_MM_EMBEDDING_WORKER` env var that the intent
    // storage workflow YAML expands via its `%{...}` placeholder. That
    // shared env var is the single source of truth, so the F-S8 query and
    // the stored intent vectors are guaranteed to be produced by the same
    // worker (and thus the same model space) — no cross-model distance,
    // no worker-not-found from a query/storage name drift.
    //
    // We deliberately do NOT route through
    // `EmbeddingDispatcherCore::query_embed_text`: that path lives behind
    // a dispatcher (intent/summary) which is only constructed when
    // MEMORY_REFLECTION_DISPATCH_ENABLED=true. F-S8 search must stay
    // independent of that kill switch (see the `intent_dispatcher` vs
    // `jobworkerp_client` split in `app/src/module.rs`), so it uses the
    // always-available shared client directly.
    let worker_name = infra::infra::embedding_dispatch::mm_embedding_worker_name();
    let args = infra::infra::embedding_dispatch::query_embed_text_arguments(intent_text);
    let output = client
        .execute_worker_by_name(&worker_name, args, Some("embed_text"))
        .await
        .map_err(|e| {
            LlmMemoryError::OtherError(format!(
                "FindSimilarByIntentText: embedding worker failed: {e:#}"
            ))
        })?;
    let values = output
        .pointer("/embeddings/0/values")
        .and_then(|v| v.as_array())
        .ok_or_else(|| {
            LlmMemoryError::OtherError(format!(
                "embedding worker output missing /embeddings/0/values: {output}"
            ))
        })?;
    let vec: Result<Vec<f32>> = values
        .iter()
        .map(|v| {
            v.as_f64().map(|f| f as f32).ok_or_else(|| {
                LlmMemoryError::OtherError(format!("embedding value is not a number: {v}")).into()
            })
        })
        .collect();
    vec
}

/// Reject `top_k == 0` with `InvalidArgument`. proto3 plain scalars
/// default to 0 when the client omits the field, so an unwary caller
/// would otherwise either get an empty result or pay for a default
/// page-size ANN search plus per-hit hydration silently. Forcing the
/// caller to spell out the page size keeps the three top-k RPCs
/// (`MatchFailureSignatures`, `FindSimilarTrajectories`,
/// `FindSimilarByIntentText`) consistent with each other.
fn require_positive_top_k(top_k: u32, rpc_name: &str) -> Result<()> {
    if top_k == 0 {
        return Err(LlmMemoryError::InvalidArgument(format!(
            "{rpc_name} requires top_k >= 1 (proto3 omitted scalars default to 0 which is ambiguous)"
        ))
        .into());
    }
    Ok(())
}

/// Clamp an already-validated `top_k` to `MAX_LIMIT`. Callers that
/// need the bounded value (e.g. `match_failure_signatures`) call this
/// after `require_positive_top_k`; callers that hand `top_k` straight
/// to `find_similar_by_vector` rely on the clamp there.
fn clamp_top_k(top_k: u32) -> usize {
    (top_k as i64).clamp(1, MAX_LIMIT) as usize
}

fn clamp_limit(limit: Option<u32>) -> i64 {
    let raw = limit.unwrap_or(DEFAULT_LIMIT as u32) as i64;
    raw.clamp(1, MAX_LIMIT)
}

/// Read the failure-signature scan cap from env, falling back to the
/// spec default. Invalid / non-positive values fall back silently so
/// a misconfigured deploy does not break the RPC.
fn match_failure_scan_cap() -> i64 {
    std::env::var(MATCH_FAILURE_SCAN_CAP_ENV)
        .ok()
        .and_then(|s| s.parse::<i64>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(MATCH_FAILURE_SCAN_CAP_DEFAULT)
}

fn map_sort(sort: ReflectionListSort) -> ReflectionSortKey {
    match sort {
        ReflectionListSort::ScoreDesc => ReflectionSortKey::ScoreDesc,
        ReflectionListSort::ScoreAsc => ReflectionSortKey::ScoreAsc,
        ReflectionListSort::CreatedAsc => ReflectionSortKey::CreatedAtAsc,
        // CREATED_DESC, UNSPECIFIED, RELEVANCE (filter-only fallback),
        // FAILURE_FIRST (preset; full implementation is a separate
        // search path) all collapse to the same desc-by-created_at
        // ordering for the sidecar-only filter listing.
        _ => ReflectionSortKey::CreatedAtDesc,
    }
}

pub(crate) fn resolve_filter(f: &ReflectionSearchFilter) -> ResolvedReflectionSearchFilter {
    ResolvedReflectionSearchFilter {
        origin_user_id: f.origin_user_id.as_ref().map(|u| u.value),
        origin_channel: f.origin_channel.clone(),
        origin_thread_id: f.origin_thread_id.as_ref().map(|t| t.value),
        outcomes: f.outcomes.clone(),
        score_min: f.score_min.map(|v| v as f64),
        score_max: f.score_max.map(|v| v as f64),
        task_categories: f.task_categories.clone(),
        reflection_aspect: f.reflection_aspect,
        // proto carries FailureMode enum wire values; the sidecar query
        // matches on the short DB keys. The helper drops Unspecified /
        // out-of-range discriminants and substitutes a zero-match
        // sentinel when a non-empty filter resolves to nothing, so
        // "filter requested but unresolvable" stays a zero-result query
        // instead of silently degrading to "no filter applied".
        failure_modes_all: failure_mode_convert::i32_slice_to_db_keys(&f.failure_modes),
        failure_modes_any: failure_mode_convert::i32_slice_to_db_keys(&f.failure_modes_match_any),
        tools_used_all: f.tools_used.clone(),
        tools_used_any: f.tools_used_match_any.clone(),
        prompt_version: f.prompt_version.clone(),
        target_model_version: f.target_model_version.clone(),
        experiment_id: f.experiment_id.clone(),
        experiment_variant: f.experiment_variant.clone(),
        pinned: f.pinned,
        dataset_quality: f.dataset_quality,
        summary_embedding_status: f.summary_embedding_status,
        intent_embedding_status: f.intent_embedding_status,
        created_after: f.created_after,
        created_before: f.created_before,
        // is_recurrence is row-only state, not exposed in the proto filter.
        is_recurrence: None,
        // memory_id_lt is set by callers driving keyset pagination
        // (e.g. `export::export`); proto filter has no slot for it.
        memory_id_lt: None,
    }
}

/// Hydrates sidecar rows into full `ReflectionSearchResult` envelopes.
///
/// Output order matches the input `rows` Vec. Rows whose `memory.data`
/// is absent are silently skipped (`continue` on a `None` from
/// `memory_by_id.remove`), which can collapse the response below
/// `rows.len()`. Callers that pair the output with a parallel
/// distance/score Vec must therefore not zip — join on
/// `reflection.id.value` instead.
pub(crate) async fn hydrate_rows(
    app: &ReflectionAppImpl,
    rows: Vec<ThreadReflectionIndexRow>,
) -> Result<Vec<ReflectionSearchResult>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    let memory_ids: Vec<i64> = rows.iter().map(|r| r.memory_id).collect();
    let (memories, failure_modes, tools, tool_outcomes, facts, applied_targets, few_shot_usages) =
        tokio::try_join!(
            app.memory_repo.find_by_ids(&memory_ids, false),
            app.failure_mode_repo.list_by_memory_ids(&memory_ids),
            app.tool_repo.list_by_memory_ids(&memory_ids),
            app.tool_outcome_repo.list_by_memory_ids(&memory_ids),
            app.fact_repo.list_by_memory_ids(&memory_ids),
            app.applied_target_repo.list_by_memory_ids(&memory_ids),
            app.few_shot_usage_repo.list_by_memory_ids(&memory_ids),
        )?;

    let mut memory_by_id: HashMap<i64, protobuf::llm_memory::data::Memory> =
        HashMap::with_capacity(memories.len());
    for m in memories {
        let Some(id) = m.id.as_ref().map(|i| i.value) else {
            continue;
        };
        memory_by_id.insert(id, m);
    }
    let mut failure_modes_by_id = group_by_memory_id(failure_modes, |r| r.memory_id);
    let mut tools_by_id = group_by_memory_id(tools, |r| r.memory_id);
    let mut tool_outcomes_by_id = group_by_memory_id(tool_outcomes, |r| r.memory_id);
    let mut facts_by_id = group_by_memory_id(facts, |r| r.memory_id);
    let mut applied_targets_by_id = group_by_memory_id(applied_targets, |r| r.memory_id);
    let mut few_shot_usages_by_id = group_by_memory_id(few_shot_usages, |r| r.memory_id);

    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let Some(memory) = memory_by_id.remove(&row.memory_id) else {
            continue;
        };
        let Some(memory_data) = memory.data else {
            continue;
        };
        let metadata_json = parse_metadata_json(memory_data.metadata.as_deref());

        // `reflection_failure_mode.mode` is the controlled-vocabulary
        // short key. Any out-of-vocabulary row would predate the enum
        // migration; running the documented backfill (see
        // docs/thread-reflection-guide.md) leaves only resolvable keys.
        // Tolerate stragglers by skipping with a warn — the entry is
        // not surfaced via the proto enum surface, but no read fails.
        let raw_modes = failure_modes_by_id
            .remove(&row.memory_id)
            .map(|v| v.into_iter().map(|r| r.mode).collect::<Vec<_>>())
            .unwrap_or_default();
        let mut failure_modes: Vec<i32> = Vec::with_capacity(raw_modes.len());
        for m in raw_modes {
            match failure_mode_convert::from_db_name(&m) {
                Some(fm) => failure_modes.push(fm as i32),
                None => tracing::warn!(
                    target = "reflection.hydrate",
                    memory_id = row.memory_id,
                    mode = %m,
                    "skipping out-of-vocabulary failure_mode row; \
                     run the enum migration backfill",
                ),
            }
        }
        let tools_used = tools_by_id
            .remove(&row.memory_id)
            .map(|v| v.into_iter().map(|r| r.tool).collect::<Vec<_>>())
            .unwrap_or_default();
        let tool_outcomes = tool_outcomes_by_id
            .remove(&row.memory_id)
            .map(|v| {
                v.into_iter()
                    .map(|r| ToolOutcomeEntry {
                        tool: r.tool,
                        contribution: r.contribution,
                        error_kind: if r.error_kind.is_empty() {
                            None
                        } else {
                            Some(r.error_kind)
                        },
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let facts = facts_by_id
            .remove(&row.memory_id)
            .map(|v| {
                v.into_iter()
                    .map(|r| ReflectionFact {
                        turn_index: r.turn_index.max(0) as u32,
                        anchor_memory_id: Some(protobuf::llm_memory::data::MemoryId {
                            value: r.fact_memory_id,
                        }),
                        kind: r.fact_kind,
                        weight: r.weight.map(|w| w as f32),
                        note: r.note,
                        links: r
                            .links_json
                            .as_deref()
                            .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok())
                            .and_then(|v| v.as_array().cloned())
                            .map(|arr| {
                                arr.into_iter()
                                    .filter_map(|val| {
                                        let field = val
                                            .get("field")
                                            .and_then(|f| f.as_str())
                                            .and_then(parse_fact_link_field)?;
                                        let index = val.get("index").and_then(|i| i.as_u64())?;
                                        Some(FactLink {
                                            field: field as i32,
                                            index: index as u32,
                                        })
                                    })
                                    .collect()
                            })
                            .unwrap_or_default(),
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let applied_targets = applied_targets_by_id
            .remove(&row.memory_id)
            .map(|v| v.into_iter().map(|r| r.target).collect::<Vec<_>>())
            .unwrap_or_default();
        let few_shot_used_in_threads = few_shot_usages_by_id
            .remove(&row.memory_id)
            .map(|v| {
                v.into_iter()
                    .map(|r| r.used_in_thread_id)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        let EvalFields {
            summary,
            task_intent,
            mitigation_hint,
            success_factors,
            lessons,
            key_decisions,
            failure_modes_other,
            failure_signature,
        } = extract_eval_fields(metadata_json.as_ref());

        let reflection_data = ReflectionData {
            origin_thread_id: Some(ThreadId {
                value: row.origin_thread_id,
            }),
            origin_user_id: Some(UserId {
                value: row.origin_user_id,
            }),
            origin_channel: row.origin_channel.clone(),
            outcome: row.outcome,
            score: row.score as f32,
            score_self: row.score_self as f32,
            score_heuristic: row.score_heuristic as f32,
            summary: summary.unwrap_or_else(|| memory_data.content.clone()),
            task_intent: task_intent.unwrap_or_default(),
            task_category: row.task_category,
            reflection_aspect: row.reflection_aspect,
            failure_modes,
            failure_modes_other,
            success_factors,
            lessons,
            key_decisions,
            tools_used,
            mitigation_hint,
            mitigation_fingerprint: row.mitigation_fingerprint.clone(),
            failure_signature,
            is_recurrence: row.is_recurrence,
            facts,
            tool_outcomes,
            previous_reflection_id: row
                .previous_reflection_id
                .map(|v| ReflectionId { value: v }),
            pinned: row.pinned,
            applied_targets,
            few_shot_used_in_threads,
            dataset_quality: row.dataset_quality,
            summary_embedding_status: row.summary_embedding_status,
            summary_embedding_error: row.summary_embedding_error.clone(),
            intent_embedding_status: row.intent_embedding_status,
            intent_embedding_error: row.intent_embedding_error.clone(),
            reflector_id: extract_reflector_id(metadata_json.as_ref()).unwrap_or_default(),
            prompt_version: row.prompt_version.clone(),
            target_model_version: row.target_model_version.clone(),
            experiment_id: row.experiment_id.clone(),
            experiment_variant: row.experiment_variant.clone(),
            created_at: row.created_at,
            updated_at: row.updated_at,
        };

        // sidecar.thread_id is the aggregate (reflection-owner) thread,
        // sidecar.origin_thread_id is the trajectory under analysis.
        // ReflectionSearchResult surfaces both so callers never confuse
        // representative-thread mismatch (spec §3.6.1 / fixpoint #35).
        let aggregate_thread_id = ThreadId {
            value: row.thread_id,
        };
        let origin_thread_id = ThreadId {
            value: row.origin_thread_id,
        };

        out.push(ReflectionSearchResult {
            reflection: Some(Reflection {
                id: Some(ReflectionId {
                    value: row.memory_id,
                }),
                data: Some(reflection_data),
            }),
            origin_thread_id: Some(origin_thread_id),
            aggregate_thread_id: Some(aggregate_thread_id),
            // Default to the filter-only contract (top-level `score`
            // mirrors `thread_reflection_index.score` and the source
            // tag is `SCORE_FILTER_ONLY`). Relevance-ranked callers
            // (`find_similar_by_vector`, `match_failure_signatures`)
            // overwrite both fields after the bulk hydrate, so the
            // default never reaches the wire for those paths.
            score: row.score as f32,
            distance: 0.0,
            score_source: ScoreSource::ScoreFilterOnly as i32,
        });
    }
    Ok(out)
}

fn group_by_memory_id<T, F>(rows: Vec<T>, key: F) -> HashMap<i64, Vec<T>>
where
    F: Fn(&T) -> i64,
{
    let mut map: HashMap<i64, Vec<T>> = HashMap::new();
    for r in rows {
        let id = key(&r);
        map.entry(id).or_default().push(r);
    }
    map
}

/// `memory.metadata` is stored as JSON text; malformed payloads degrade
/// to None so callers can fall back to sidecar fields rather than fail
/// the whole search response.
fn parse_metadata_json(metadata: Option<&str>) -> Option<serde_json::Value> {
    let s = metadata?;
    match serde_json::from_str(s) {
        Ok(v) => Some(v),
        Err(e) => {
            tracing::debug!(
                target = "reflection.parse_metadata_json",
                error = %e,
                preview = %s.chars().take(80).collect::<String>(),
                "malformed metadata JSON; degrading to None",
            );
            None
        }
    }
}

struct StoredSignature {
    pattern: FailureSignaturePatternType,
    indicators: protobuf::llm_memory::data::FailureSignatureIndicators,
}

/// Pure read of a stored signature from raw metadata JSON. F-S7 scores
/// before hydrate so the child-table fan-out only fires for the
/// top-k survivors.
fn extract_signature_from_metadata(
    metadata: Option<&serde_json::Value>,
) -> Option<StoredSignature> {
    let sig_value = metadata?
        .pointer("/eval/failure_signature")
        .filter(|v| !v.is_null())?;
    let parsed = parse_failure_signature(sig_value)?;
    let pattern = FailureSignaturePatternType::try_from(parsed.pattern_type)
        .unwrap_or(FailureSignaturePatternType::FailureSignaturePatternUnspecified);
    Some(StoredSignature {
        pattern,
        indicators: parsed.indicators.unwrap_or_default(),
    })
}

/// Tuple returned by `extract_eval_fields`. The fields populate
/// `ReflectionData` from the `metadata.eval` JSON written at finalize
/// time (`build_memory_data::build_eval_view`).
struct EvalFields {
    summary: Option<String>,
    task_intent: Option<String>,
    mitigation_hint: Option<String>,
    success_factors: Vec<String>,
    lessons: Vec<String>,
    key_decisions: Vec<String>,
    failure_modes_other: Vec<String>,
    failure_signature: Option<protobuf::llm_memory::data::FailureSignature>,
}

fn extract_eval_fields(metadata: Option<&serde_json::Value>) -> EvalFields {
    let Some(meta) = metadata else {
        return EvalFields {
            summary: None,
            task_intent: None,
            mitigation_hint: None,
            success_factors: vec![],
            lessons: vec![],
            key_decisions: vec![],
            failure_modes_other: vec![],
            failure_signature: None,
        };
    };
    let eval = meta.pointer("/eval");
    let summary = eval
        .and_then(|v| v.get("summary"))
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let task_intent = eval
        .and_then(|v| v.get("task_intent"))
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let mitigation_hint = eval
        .and_then(|v| v.get("mitigation_hint"))
        .and_then(|v| v.as_str())
        .map(str::to_string);
    let success_factors = string_array(eval, "success_factors");
    let lessons = string_array(eval, "lessons");
    let key_decisions = string_array(eval, "key_decisions");
    let failure_modes_other = string_array(eval, "failure_modes_other");
    let failure_signature = eval
        .and_then(|v| v.get("failure_signature"))
        .filter(|v| !v.is_null())
        .and_then(parse_failure_signature);

    EvalFields {
        summary,
        task_intent,
        mitigation_hint,
        success_factors,
        lessons,
        key_decisions,
        failure_modes_other,
        failure_signature,
    }
}

fn string_array(scope: Option<&serde_json::Value>, key: &str) -> Vec<String> {
    scope
        .and_then(|v| v.get(key))
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(str::to_string))
                .collect()
        })
        .unwrap_or_default()
}

fn parse_failure_signature(
    v: &serde_json::Value,
) -> Option<protobuf::llm_memory::data::FailureSignature> {
    use protobuf::llm_memory::data::FailureSignature;
    let pattern = v
        .get("pattern_type")
        .and_then(|v| v.as_str())
        .and_then(FailureSignaturePatternType::from_str_name)
        .unwrap_or(FailureSignaturePatternType::FailureSignaturePatternUnspecified);
    let indicators = v
        .get("indicators")
        .filter(|v| !v.is_null())
        .map(parse_indicators)
        .unwrap_or_default();
    let trigger_threshold = v
        .get("trigger_threshold")
        .and_then(|v| v.as_object())
        .map(|m| {
            m.iter()
                .filter_map(|(k, val)| val.as_f64().map(|f| (k.clone(), f)))
                .collect()
        })
        .unwrap_or_default();
    let evidence_turn_indices = v
        .get("evidence_turn_indices")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|x| x.as_u64().map(|n| n as u32))
                .collect()
        })
        .unwrap_or_default();
    Some(FailureSignature {
        pattern_type: pattern as i32,
        indicators: Some(indicators),
        trigger_threshold,
        evidence_turn_indices,
    })
}

fn parse_indicators(
    v: &serde_json::Value,
) -> protobuf::llm_memory::data::FailureSignatureIndicators {
    use protobuf::llm_memory::data::FailureSignatureIndicators;
    FailureSignatureIndicators {
        same_tool_repeated_count: v
            .get("same_tool_repeated_count")
            .and_then(|x| x.as_u64())
            .map(|n| n as u32),
        same_tool_name: v
            .get("same_tool_name")
            .and_then(|x| x.as_str())
            .map(str::to_string),
        consecutive_errors: v
            .get("consecutive_errors")
            .and_then(|x| x.as_u64())
            .map(|n| n as u32),
        no_state_change_turns: v
            .get("no_state_change_turns")
            .and_then(|x| x.as_u64())
            .map(|n| n as u32),
        tool_calls_per_turn_ratio: v
            .get("tool_calls_per_turn_ratio")
            .and_then(|x| x.as_f64())
            .map(|n| n as f32),
        compact_boundary_count: v
            .get("compact_boundary_count")
            .and_then(|x| x.as_u64())
            .map(|n| n as u32),
        user_clarification_count: v
            .get("user_clarification_count")
            .and_then(|x| x.as_u64())
            .map(|n| n as u32),
        turn_count_at_detection: v
            .get("turn_count_at_detection")
            .and_then(|x| x.as_u64())
            .map(|n| n as u32),
        elapsed_ms_at_detection: v.get("elapsed_ms_at_detection").and_then(|x| x.as_u64()),
    }
}

fn extract_reflector_id(metadata: Option<&serde_json::Value>) -> Option<String> {
    metadata?
        .pointer("/reflector/id")?
        .as_str()
        .map(str::to_string)
}

fn parse_fact_link_field(name: &str) -> Option<FactLinkField> {
    FactLinkField::from_str_name(name)
}

#[cfg(test)]
mod top_k_guard_tests {
    //! Pin the proto3-default-zero rejection. The previous shape was
    //! inconsistent (`match_failure_signatures` treated 0 as
    //! unlimited; `find_similar_by_vector` mapped 0 to DEFAULT_LIMIT)
    //! and exposed callers to silent high-cost searches when they
    //! forgot to set the field. The helper centralises the contract
    //! so a future regression cannot bring back either silent path.

    use super::{MAX_LIMIT, clamp_top_k, require_positive_top_k};

    #[test]
    fn zero_top_k_is_invalid_argument() {
        let err = require_positive_top_k(0, "TestRpc").unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("top_k >= 1") && msg.contains("TestRpc"),
            "expected InvalidArgument mentioning the RPC and 1-or-more requirement; got: {msg}",
        );
    }

    #[test]
    fn positive_top_k_passes_through() {
        assert!(require_positive_top_k(1, "X").is_ok());
        assert!(require_positive_top_k(42, "X").is_ok());
    }

    #[test]
    fn clamp_caps_at_max_limit() {
        assert_eq!(clamp_top_k(1), 1);
        assert_eq!(clamp_top_k(42), 42);
        let over = MAX_LIMIT as u32 + 100;
        assert_eq!(clamp_top_k(over), MAX_LIMIT as usize);
    }
}

#[cfg(test)]
mod scan_cap_tests {
    use super::*;

    /// Guard the spec §4.2.4 default (1000) and the env override
    /// contract for `match_failure_signatures`. The env var is process
    /// global; we restore it after each step to avoid leaking state to
    /// sibling tests under `--test-threads=1`.
    #[test]
    fn match_failure_scan_cap_honours_env_and_default() {
        let restore = std::env::var(MATCH_FAILURE_SCAN_CAP_ENV).ok();
        // SAFETY: the harness pins single-threaded execution
        // (`--test-threads=1`), so flipping a process-global env var
        // here cannot race other tests.
        unsafe {
            std::env::remove_var(MATCH_FAILURE_SCAN_CAP_ENV);
        }
        assert_eq!(match_failure_scan_cap(), MATCH_FAILURE_SCAN_CAP_DEFAULT);

        unsafe {
            std::env::set_var(MATCH_FAILURE_SCAN_CAP_ENV, "2500");
        }
        assert_eq!(match_failure_scan_cap(), 2500);

        // Non-positive / non-numeric inputs collapse to default so a
        // misconfigured deploy never starves the RPC.
        unsafe {
            std::env::set_var(MATCH_FAILURE_SCAN_CAP_ENV, "0");
        }
        assert_eq!(match_failure_scan_cap(), MATCH_FAILURE_SCAN_CAP_DEFAULT);
        unsafe {
            std::env::set_var(MATCH_FAILURE_SCAN_CAP_ENV, "-1");
        }
        assert_eq!(match_failure_scan_cap(), MATCH_FAILURE_SCAN_CAP_DEFAULT);
        unsafe {
            std::env::set_var(MATCH_FAILURE_SCAN_CAP_ENV, "abc");
        }
        assert_eq!(match_failure_scan_cap(), MATCH_FAILURE_SCAN_CAP_DEFAULT);

        // Restore prior value so unrelated tests see a clean env.
        unsafe {
            match restore {
                Some(v) => std::env::set_var(MATCH_FAILURE_SCAN_CAP_ENV, v),
                None => std::env::remove_var(MATCH_FAILURE_SCAN_CAP_ENV),
            }
        }
    }

    /// Guard the staged over-fetch knob defaults and the shared env
    /// contract. The intent path reads the SAME env names + defaults as
    /// `ThreadFilterConfig` (one knob set for all vector stores).
    /// Non-numeric inputs collapse to the default; `0` passes through
    /// (`approximate_fetch_limit` clamps it with `.max(1)`), matching the
    /// `parse_env_or` contract memory / thread already use.
    #[test]
    fn vector_dedup_knobs_honour_env_and_default() {
        use crate::app::thread_filter_resolver::{
            VECTOR_DEDUP_MAX_FETCH_ENV, VECTOR_DEDUP_OVERFETCH_K_ENV,
        };
        let restore_k = std::env::var(VECTOR_DEDUP_OVERFETCH_K_ENV).ok();
        let restore_m = std::env::var(VECTOR_DEDUP_MAX_FETCH_ENV).ok();
        // SAFETY: `--test-threads=1` pins single-threaded execution.
        unsafe {
            std::env::remove_var(VECTOR_DEDUP_OVERFETCH_K_ENV);
            std::env::remove_var(VECTOR_DEDUP_MAX_FETCH_ENV);
        }
        assert_eq!(vector_dedup_overfetch_k(), 4);
        assert_eq!(vector_dedup_max_fetch(), 10_000);

        unsafe {
            std::env::set_var(VECTOR_DEDUP_OVERFETCH_K_ENV, "8");
            std::env::set_var(VECTOR_DEDUP_MAX_FETCH_ENV, "2000");
        }
        assert_eq!(vector_dedup_overfetch_k(), 8);
        assert_eq!(vector_dedup_max_fetch(), 2000);

        // Non-numeric collapses to default (parse failure).
        unsafe {
            std::env::set_var(VECTOR_DEDUP_OVERFETCH_K_ENV, "abc");
            std::env::set_var(VECTOR_DEDUP_MAX_FETCH_ENV, "abc");
        }
        assert_eq!(vector_dedup_overfetch_k(), 4);
        assert_eq!(vector_dedup_max_fetch(), 10_000);

        unsafe {
            match restore_k {
                Some(v) => std::env::set_var(VECTOR_DEDUP_OVERFETCH_K_ENV, v),
                None => std::env::remove_var(VECTOR_DEDUP_OVERFETCH_K_ENV),
            }
            match restore_m {
                Some(v) => std::env::set_var(VECTOR_DEDUP_MAX_FETCH_ENV, v),
                None => std::env::remove_var(VECTOR_DEDUP_MAX_FETCH_ENV),
            }
        }
    }
}

#[cfg(test)]
mod hybrid_helpers_tests {
    //! Pure-function tests for the F-S1 hybrid wrapper helpers.
    //! Coverage of the full `search_hybrid` entry point lives in the
    //! integration tests under `app/src/app/reflection/tests.rs`;
    //! these unit tests pin the aggregation math and post-rank boost
    //! contracts independently.

    use super::*;
    use protobuf::llm_memory::data::{Reflection, ReflectionData};

    fn vec3(x: f32, y: f32, z: f32) -> Vec<f32> {
        vec![x, y, z]
    }

    #[test]
    fn aggregate_vectors_average_is_componentwise_mean() {
        let signals = vec![vec3(1.0, 2.0, 3.0), vec3(3.0, 6.0, 9.0)];
        let out = aggregate_query_vectors(&signals, AggregationStrategy::Average).unwrap();
        assert_eq!(out, vec3(2.0, 4.0, 6.0));
    }

    #[test]
    fn aggregate_vectors_sum_adds_componentwise() {
        let signals = vec![vec3(1.0, 2.0, 3.0), vec3(0.5, -1.0, 4.0)];
        let out = aggregate_query_vectors(&signals, AggregationStrategy::Sum).unwrap();
        assert_eq!(out, vec3(1.5, 1.0, 7.0));
    }

    #[test]
    fn aggregate_vectors_max_picks_componentwise_max() {
        let signals = vec![vec3(1.0, 5.0, 2.0), vec3(3.0, 4.0, 6.0)];
        let out = aggregate_query_vectors(&signals, AggregationStrategy::Max).unwrap();
        assert_eq!(out, vec3(3.0, 5.0, 6.0));
    }

    #[test]
    fn aggregate_vectors_weighted_by_position_uses_reciprocal_rank() {
        // 1.0 * (1, 2, 3) + 0.5 * (4, 8, 12) = (3, 6, 9)
        let signals = vec![vec3(1.0, 2.0, 3.0), vec3(4.0, 8.0, 12.0)];
        let out =
            aggregate_query_vectors(&signals, AggregationStrategy::WeightedByPosition).unwrap();
        assert_eq!(out, vec3(3.0, 6.0, 9.0));
    }

    #[test]
    fn aggregate_vectors_rejects_dimension_mismatch() {
        let signals = vec![vec3(1.0, 2.0, 3.0), vec![1.0, 2.0]];
        let err = aggregate_query_vectors(&signals, AggregationStrategy::Average).unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("dimension mismatch"),
            "expected dimension-mismatch error, got: {msg}"
        );
    }

    #[test]
    fn aggregate_vectors_rejects_empty_input() {
        let signals: Vec<Vec<f32>> = vec![];
        let err = aggregate_query_vectors(&signals, AggregationStrategy::Average).unwrap_err();
        assert!(format!("{err:#}").contains("empty"));
    }

    fn hit(id: i64, score: f32, pinned: bool) -> ReflectionSearchResult {
        let data = ReflectionData {
            score,
            pinned,
            ..Default::default()
        };
        ReflectionSearchResult {
            reflection: Some(Reflection {
                id: Some(ReflectionId { value: id }),
                data: Some(data),
            }),
            origin_thread_id: None,
            aggregate_thread_id: None,
            score,
            distance: 0.0,
            score_source: ScoreSource::ScoreVector as i32,
        }
    }

    #[test]
    fn boost_pinned_high_score_lifts_pinned_above_threshold() {
        // Order before boost: [a(0.5 unpinned), b(0.9 pinned), c(0.8 unpinned), d(0.75 pinned)]
        // Expected after: pinned+score>=0.7 first in original order [b, d], then [a, c].
        let mut hits = vec![
            hit(1, 0.5, false),
            hit(2, 0.9, true),
            hit(3, 0.8, false),
            hit(4, 0.75, true),
        ];
        apply_pinned_high_score_boost(&mut hits);
        let order: Vec<i64> = hits
            .iter()
            .map(|h| h.reflection.as_ref().unwrap().id.as_ref().unwrap().value)
            .collect();
        assert_eq!(order, vec![2, 4, 1, 3]);
    }

    #[test]
    fn boost_pinned_high_score_keeps_low_score_pinned_in_place() {
        // Pinned but below 0.7 threshold must NOT be boosted.
        let mut hits = vec![hit(1, 0.95, false), hit(2, 0.6, true), hit(3, 0.75, true)];
        apply_pinned_high_score_boost(&mut hits);
        let order: Vec<i64> = hits
            .iter()
            .map(|h| h.reflection.as_ref().unwrap().id.as_ref().unwrap().value)
            .collect();
        // Only id=3 is pinned-and-high; id=2 is pinned-but-low so it stays in the rest group.
        assert_eq!(order, vec![3, 1, 2]);
    }
}
