//! Aggregate RPCs:
//!   * `aggregate_scores` — dual-backend dynamic GROUP BY with p50/p95
//!     percentiles (Postgres `percentile_cont`, SQLite nearest-rank).
//!   * `aggregate_lessons` — token-frequency histogram over
//!     `metadata.eval.lessons`, bounded by `MAX_LESSONS_SCAN`.
//!   * `aggregate_tool_contributions` — multi-axis pivot with
//!     per-bucket `recurrence_count` (rows with `is_recurrence=true`)
//!     and `contribution_share` (per-tool normalised, only when
//!     `group_by_tool=true`).

use anyhow::Result;
use infra::error::LlmMemoryError;
use infra::infra::memory::rdb::MemoryRepository;
use infra::infra::reflection::failure_mode_convert;
use infra::infra::reflection::rdb::ThreadReflectionIndexRepository;
use infra::infra::reflection::rows::{ScoresAggregateAxis, ScoresAggregateBucket};
use infra::infra::reflection::stats::{MAX_GROUPS_FOR_INMEM_PERCENTILE, ReflectionStatsRepository};
use protobuf::llm_memory::data::ReflectionSearchFilter;
use protobuf::llm_memory::service::{
    AggregateFailureModesResponse, AggregateLessonsResponse, AggregateScoresEntry,
    AggregateScoresGroupBy, AggregateScoresResponse, AggregateToolContributionsResponse,
    CoOccurrenceEntry, FailureModeAggregateEntry, LessonAggregateEntry,
    RebuildDerivedStatsResponse, ScoresGroupKey, ToolContributionAggregateEntry,
    ToolContributionStatEntry, ToolContributionStatsResponse, ToolOutcomeStatEntry,
    ToolOutcomeStatsResponse,
};
use std::collections::HashMap;

use crate::app::reflection::ReflectionAppImpl;
use crate::app::reflection::search::resolve_filter;

/// F-A1 — `AggregateFailureModes` with optional filter.
///
/// Issues two queries against the shared `build_filter_where` engine:
///   1. per-`mode` counts (sum of `is_recurrence=true` bucketed as
///      `recurrence_count`).
///   2. per-pair co-occurrence counts (each unordered pair appears
///      once, capped to a defensive ceiling on response size; the
///      cap-hit is surfaced as `is_truncated` on the response).
///
/// Both queries reuse the resolved filter so the predicate semantics
/// match `search_index` and aggregate `count` ≤ `search_index.count`
/// for the same filter. Co-occurrence rows are folded into the per-
/// `mode` entry as `co_occurring` so the proto contract stays a
/// single response (no streaming, no separate RPC).
pub async fn aggregate_failure_modes(
    app: &ReflectionAppImpl,
    filter: Option<&ReflectionSearchFilter>,
    include_co_occurrence: bool,
) -> Result<AggregateFailureModesResponse> {
    let resolved = filter.map(resolve_filter).unwrap_or_default();
    // Per-mode is always issued. The co-occurrence query is opt-in
    // via the proto `include_co_occurrence` flag (the join is a
    // self-join on `reflection_failure_mode` and is the more expensive
    // of the two even with the LIMIT cap). When requested we run the
    // two queries concurrently so wall-clock matches the worst leg
    // instead of the sum.
    // `is_truncated` reflects whether the pair-cap was reached.
    // Per-mode rows are not capped (the dictionary is small), so
    // only the co-occurrence query can drive truncation. The cap
    // value itself stays inside `stats.rs`; the infra method
    // returns the flag alongside the rows.
    let (per_mode, pairs, is_truncated) = if include_co_occurrence {
        let (per_mode, (pairs, is_truncated)) = tokio::try_join!(
            app.stats_repo.aggregate_failure_modes(&resolved),
            app.stats_repo
                .aggregate_failure_mode_co_occurrence(&resolved),
        )?;
        (per_mode, pairs, is_truncated)
    } else {
        let per_mode = app.stats_repo.aggregate_failure_modes(&resolved).await?;
        (per_mode, Vec::new(), false)
    };
    if per_mode.is_empty() {
        return Ok(AggregateFailureModesResponse {
            entries: Vec::new(),
            is_truncated,
        });
    }

    // Fold the pair table into a per-mode look-up. Empty when
    // `include_co_occurrence=false`. The SQL emits `mode_a < mode_b`
    // so each pair appears once; mirror it into both halves so
    // `entries[X].co_occurring` lists every peer mode regardless of
    // lexical ordering. Any DB string outside the 16-key controlled
    // vocabulary is a pre-migration straggler (see
    // docs/thread-reflection-migration.md): skip with a warn instead of
    // surfacing a fake bucket.
    let mut co_by_mode: HashMap<i32, Vec<CoOccurrenceEntry>> = HashMap::new();
    for p in pairs {
        let (Some(a), Some(b)) = (
            failure_mode_convert::from_db_name(&p.mode_a),
            failure_mode_convert::from_db_name(&p.mode_b),
        ) else {
            tracing::warn!(
                target = "reflection.aggregate",
                mode_a = %p.mode_a,
                mode_b = %p.mode_b,
                "skipping out-of-vocabulary co-occurrence pair; \
                 run the enum migration backfill",
            );
            continue;
        };
        let count = p.count as u64;
        co_by_mode
            .entry(a as i32)
            .or_default()
            .push(CoOccurrenceEntry {
                mode: b as i32,
                count,
            });
        co_by_mode
            .entry(b as i32)
            .or_default()
            .push(CoOccurrenceEntry {
                mode: a as i32,
                count,
            });
    }

    let entries: Vec<FailureModeAggregateEntry> = per_mode
        .into_iter()
        .filter_map(|r| {
            let Some(mode) = failure_mode_convert::from_db_name(&r.mode) else {
                tracing::warn!(
                    target = "reflection.aggregate",
                    mode = %r.mode,
                    "skipping out-of-vocabulary aggregate row; \
                     run the enum migration backfill",
                );
                return None;
            };
            let mut co_occurring = co_by_mode.remove(&(mode as i32)).unwrap_or_default();
            // Stable ordering for the proto consumer (count desc,
            // then mode asc) — mirrors the SQL ORDER BY contract.
            co_occurring.sort_by(|a, b| b.count.cmp(&a.count).then_with(|| a.mode.cmp(&b.mode)));
            Some(FailureModeAggregateEntry {
                mode: mode as i32,
                count: r.count as u64,
                recurrence_count: r.recurrence_count.max(0) as u64,
                co_occurring,
            })
        })
        .collect();

    Ok(AggregateFailureModesResponse {
        entries,
        is_truncated,
    })
}

/// F-A5 — direct read against the `tool_outcome_stats` derived table.
pub async fn get_tool_outcome_stats(
    app: &ReflectionAppImpl,
    origin_user_id: i64,
    tool: &str,
) -> Result<ToolOutcomeStatsResponse> {
    let rows = app
        .stats_repo
        .list_tool_outcome_stats(origin_user_id, tool)
        .await?;
    Ok(ToolOutcomeStatsResponse {
        entries: rows
            .into_iter()
            .map(|r| ToolOutcomeStatEntry {
                outcome: r.outcome,
                count: r.count as u64,
                last_updated_at: r.last_updated_at,
            })
            .collect(),
    })
}

/// F-A7 — `tool_contribution_stats` direct read.
pub async fn get_tool_contribution_stats(
    app: &ReflectionAppImpl,
    origin_user_id: i64,
    tool: &str,
) -> Result<ToolContributionStatsResponse> {
    let rows = app
        .stats_repo
        .list_tool_contribution_stats(origin_user_id, tool)
        .await?;
    Ok(ToolContributionStatsResponse {
        entries: rows
            .into_iter()
            .map(|r| ToolContributionStatEntry {
                contribution: r.contribution,
                error_kind: normalize_error_kind(r.error_kind),
                count: r.count as u64,
                last_updated_at: r.last_updated_at,
            })
            .collect(),
    })
}

/// F-A6 — RebuildDerivedStats. `origin_user_id=None` rebuilds all rows;
/// the two boolean flags select which derived table(s) to refresh.
pub async fn rebuild_derived_stats(
    app: &ReflectionAppImpl,
    origin_user_id: Option<i64>,
    rebuild_tool_outcome: bool,
    rebuild_tool_contribution: bool,
) -> Result<RebuildDerivedStatsResponse> {
    let started = std::time::Instant::now();
    let mut tool_outcome_rows = 0u64;
    let mut tool_contribution_rows = 0u64;
    if rebuild_tool_outcome {
        tool_outcome_rows = app
            .stats_repo
            .rebuild_tool_outcome_stats(origin_user_id)
            .await?;
    }
    if rebuild_tool_contribution {
        tool_contribution_rows = app
            .stats_repo
            .rebuild_tool_contribution_stats(origin_user_id)
            .await?;
    }
    Ok(RebuildDerivedStatsResponse {
        tool_outcome_rows_rebuilt: tool_outcome_rows,
        tool_contribution_rows_rebuilt: tool_contribution_rows,
        duration_ms: started.elapsed().as_millis() as i64,
    })
}

/// F-A2 — `AggregateScores` (dynamic GROUP BY + p50/p95 percentiles).
///
/// Translates the proto `AggregateScoresGroupBy` enum into the infra
/// `ScoresAggregateAxis` enum, dedupes the axis list (proto is
/// `repeated`), validates `time_bucket_seconds` when the TIME_BUCKET
/// axis is requested, and forwards to the stats repo. The repo
/// returns `ScoresAggregateBucket` with optional axis fields set only
/// for grouped axes; this layer maps each bucket to the proto
/// `ScoresGroupKey` shape using the same axis set.
pub async fn aggregate_scores(
    app: &ReflectionAppImpl,
    filter: Option<&ReflectionSearchFilter>,
    group_by: &[AggregateScoresGroupBy],
    time_bucket_seconds: Option<u32>,
) -> Result<AggregateScoresResponse> {
    let mut axes: Vec<ScoresAggregateAxis> = Vec::with_capacity(group_by.len());
    let mut seen: std::collections::HashSet<ScoresAggregateAxis> = std::collections::HashSet::new();
    for g in group_by {
        if let Some(a) = map_axis(*g)
            && seen.insert(a)
        {
            axes.push(a);
        }
    }
    // TIME_BUCKET without a positive bucket size is meaningless: the
    // integer-math expression collapses to zero (every reflection
    // lands in the same bucket), which is almost certainly a caller
    // bug. Reject up front rather than silently emit a single time
    // bucket of zero.
    if axes.contains(&ScoresAggregateAxis::TimeBucket) && time_bucket_seconds.unwrap_or(0) == 0 {
        return Err(LlmMemoryError::InvalidArgument(
            "AggregateScores: time_bucket_seconds must be > 0 when TIME_BUCKET is in group_by"
                .into(),
        )
        .into());
    }

    let resolved = filter.map(resolve_filter).unwrap_or_default();
    let buckets = app
        .stats_repo
        .aggregate_scores(
            &resolved,
            &axes,
            time_bucket_seconds,
            MAX_GROUPS_FOR_INMEM_PERCENTILE,
        )
        .await?;

    Ok(AggregateScoresResponse {
        entries: buckets.into_iter().map(bucket_to_entry).collect(),
    })
}

fn map_axis(g: AggregateScoresGroupBy) -> Option<ScoresAggregateAxis> {
    match g {
        AggregateScoresGroupBy::Unspecified => None,
        AggregateScoresGroupBy::OriginUser => Some(ScoresAggregateAxis::OriginUserId),
        AggregateScoresGroupBy::OriginChannel => Some(ScoresAggregateAxis::OriginChannel),
        AggregateScoresGroupBy::TargetModelVersion => Some(ScoresAggregateAxis::TargetModelVersion),
        AggregateScoresGroupBy::TaskCategory => Some(ScoresAggregateAxis::TaskCategory),
        AggregateScoresGroupBy::ReflectionAspect => Some(ScoresAggregateAxis::ReflectionAspect),
        AggregateScoresGroupBy::Outcome => Some(ScoresAggregateAxis::Outcome),
        AggregateScoresGroupBy::ExperimentId => Some(ScoresAggregateAxis::ExperimentId),
        AggregateScoresGroupBy::ExperimentVariant => Some(ScoresAggregateAxis::ExperimentVariant),
        AggregateScoresGroupBy::TimeBucket => Some(ScoresAggregateAxis::TimeBucket),
        AggregateScoresGroupBy::SummaryEmbeddingStatus => {
            Some(ScoresAggregateAxis::SummaryEmbeddingStatus)
        }
        AggregateScoresGroupBy::IntentEmbeddingStatus => {
            Some(ScoresAggregateAxis::IntentEmbeddingStatus)
        }
    }
}

fn bucket_to_entry(b: ScoresAggregateBucket) -> AggregateScoresEntry {
    AggregateScoresEntry {
        key: Some(ScoresGroupKey {
            origin_user_id: b.origin_user_id,
            origin_channel: b.origin_channel,
            target_model_version: b.target_model_version,
            task_category: b.task_category,
            reflection_aspect: b.reflection_aspect,
            outcome: b.outcome,
            experiment_id: b.experiment_id,
            experiment_variant: b.experiment_variant,
            time_bucket_start: b.time_bucket_start,
            summary_embedding_status: b.summary_embedding_status,
            intent_embedding_status: b.intent_embedding_status,
        }),
        count: b.count.max(0) as u64,
        score_avg: b.score_avg as f32,
        score_min: b.score_min as f32,
        score_max: b.score_max as f32,
        score_p50: b.score_p50 as f32,
        score_p95: b.score_p95 as f32,
    }
}

/// F-A3 — `AggregateLessons`. Token-frequency histogram over
/// `metadata.eval.lessons` array entries.
///
/// Pipeline:
///   1. `index_repo.search_index` collects every sidecar row matching
///      the filter, up to `MAX_LESSONS_SCAN` for defence.
///   2. `memory_repo.find_by_ids` bulk-fetches the memory bodies so
///      we can read `metadata.eval.lessons` once.
///   3. Each lesson string is whitespace-tokenised, lowercased, and
///      ASCII-filtered to a bag of tokens.
///   4. Frequencies are accumulated in a `HashMap<String, u64>`,
///      then sorted desc and truncated to `top_n` (default 100).
///
/// `MAX_LESSONS_SCAN` (100_000) bounds the in-memory hydration: a
/// truly extreme filter would otherwise read the entire reflection
/// corpus into the process for token counting.
pub async fn aggregate_lessons(
    app: &ReflectionAppImpl,
    filter: Option<&ReflectionSearchFilter>,
    top_n: Option<u32>,
) -> Result<AggregateLessonsResponse> {
    let resolved = filter.map(resolve_filter).unwrap_or_default();
    use infra::infra::reflection::rows::ReflectionSortKey;
    let rows = app
        .index_repo
        .search_index(
            &resolved,
            ReflectionSortKey::CreatedAtDesc,
            MAX_LESSONS_SCAN,
            0,
        )
        .await?;
    if rows.len() as i64 >= MAX_LESSONS_SCAN {
        return Err(LlmMemoryError::InvalidArgument(format!(
            "AggregateLessons: filter matches >= {MAX_LESSONS_SCAN} rows; narrow the filter \
             so the in-memory token frequency stays bounded"
        ))
        .into());
    }
    if rows.is_empty() {
        return Ok(AggregateLessonsResponse {
            entries: Vec::new(),
        });
    }

    let memory_ids: Vec<i64> = rows.iter().map(|r| r.memory_id).collect();
    let memories = app.memory_repo.find_by_ids(&memory_ids, false).await?;

    let mut counts: HashMap<String, u64> = HashMap::new();
    for m in memories {
        let Some(data) = m.data else {
            continue;
        };
        let Some(metadata_text) = data.metadata else {
            continue;
        };
        let parsed: serde_json::Value = match serde_json::from_str(&metadata_text) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let lessons = parsed
            .get("eval")
            .and_then(|e| e.get("lessons"))
            .and_then(|l| l.as_array());
        let Some(lessons) = lessons else {
            continue;
        };
        for lesson in lessons {
            let Some(text) = lesson.as_str() else {
                continue;
            };
            for tok in tokenize_lesson(text) {
                *counts.entry(tok).or_insert(0) += 1;
            }
        }
    }

    let top = top_n.unwrap_or(DEFAULT_TOP_N_LESSONS) as usize;
    let entries = top_n_by_frequency(counts, top);

    Ok(AggregateLessonsResponse {
        entries: entries
            .into_iter()
            .map(|(token_or_phrase, frequency)| LessonAggregateEntry {
                token_or_phrase,
                frequency,
            })
            .collect(),
    })
}

/// Pick the top-`n` `(token, frequency)` pairs by descending
/// frequency (ties broken by ascending token). Uses a min-heap so
/// the cost stays O(N log n) — when the distinct-token count blows
/// past `n` (default 100) on a wide corpus, a full sort over N
/// items would dominate the response.
fn top_n_by_frequency(counts: HashMap<String, u64>, n: usize) -> Vec<(String, u64)> {
    if n == 0 || counts.is_empty() {
        return Vec::new();
    }
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;

    // The heap holds `(freq, Reverse(token))` so the smallest
    // element under BinaryHeap::peek is the one we drop when full.
    // Ties prefer the lexicographically smaller token to match the
    // previous `then_with(a.0.cmp(&b.0))` ordering.
    let mut heap: BinaryHeap<Reverse<(u64, Reverse<String>)>> = BinaryHeap::with_capacity(n + 1);
    for (token, freq) in counts {
        heap.push(Reverse((freq, Reverse(token))));
        if heap.len() > n {
            heap.pop();
        }
    }
    let mut entries: Vec<(String, u64)> = heap
        .into_iter()
        .map(|Reverse((freq, Reverse(token)))| (token, freq))
        .collect();
    // Final ordering: desc by frequency, asc by token.
    entries.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    entries
}

/// Defensive cap on the row scan that feeds `aggregate_lessons`. The
/// memory bodies are read in-process; 100k rows of metadata JSON is
/// the soft ceiling before the in-memory cost (~10–50 MB of strings)
/// becomes noticeable. Operators who legitimately need a wider scan
/// should split the request by filter axes first.
const MAX_LESSONS_SCAN: i64 = 100_000;

/// Default `top_n` for `aggregate_lessons` when the caller omits it.
const DEFAULT_TOP_N_LESSONS: u32 = 100;

/// Tokenise a single lesson string. Minimal CJK-tolerant rule:
/// split on Unicode whitespace, lower-case ASCII, drop tokens that
/// contain no alphanumeric chars (so punctuation-only "fragments"
/// like "—" don't appear in the histogram). A more sophisticated
/// tokeniser (lindera / unicode-normalisation NFKC fold) can swap
/// in here without changing the call site.
fn tokenize_lesson(text: &str) -> Vec<String> {
    text.split_whitespace()
        .filter_map(|raw| {
            let lower = raw.to_lowercase();
            if lower
                .chars()
                .any(|c| c.is_ascii_alphanumeric() || !c.is_ascii())
            {
                Some(lower)
            } else {
                None
            }
        })
        .collect()
}

/// F-A7 — `AggregateToolContributions` multi-axis pivot.
///
/// The four group_by booleans select which of `(tool, contribution,
/// error_kind, origin_user_id)` axes are pivoted; axes that are off
/// collapse to a constant ('' / 0 / `UNSPECIFIED`) in the response so
/// callers can distinguish "this axis is grouped on" (Some value)
/// from "this axis is collapsed" (None / default). All four off is a
/// legitimate "give me the total row" pattern (the request looks
/// like grand-total counts).
///
/// Filter semantics are identical to F-A1 and `search_index`:
/// `resolve_filter` flattens the proto filter into the shared
/// `ResolvedReflectionSearchFilter` and the SQL reuses
/// `build_filter_where` so predicate behaviour stays consistent.
///
/// `recurrence_count` (rows with `is_recurrence=true`) and
/// `contribution_share` (count / per-tool total when grouping by
/// tool) are populated per bucket on the response.
pub async fn aggregate_tool_contributions(
    app: &ReflectionAppImpl,
    filter: Option<&ReflectionSearchFilter>,
    group_by_tool: bool,
    group_by_contribution: bool,
    group_by_error_kind: bool,
    group_by_origin_user: bool,
) -> Result<AggregateToolContributionsResponse> {
    let resolved = filter.map(resolve_filter).unwrap_or_default();
    let rows = app
        .stats_repo
        .aggregate_tool_contributions(
            &resolved,
            group_by_tool,
            group_by_contribution,
            group_by_error_kind,
            group_by_origin_user,
        )
        .await?;
    // `contribution_share` is `count / total_for_tool` — meaningful
    // only when `group_by_tool=true`. Without the tool axis the
    // denominator is ambiguous (per-error_kind? grand total?), so
    // we surface `None` for the share in that case and leave the
    // calculation to the caller's downstream pivot.
    let per_tool_total: HashMap<String, u64> = if group_by_tool {
        let mut m: HashMap<String, u64> = HashMap::new();
        for r in &rows {
            *m.entry(r.tool.clone()).or_insert(0) += r.count.max(0) as u64;
        }
        m
    } else {
        HashMap::new()
    };

    let entries = rows
        .into_iter()
        .map(|r| {
            let count_u = r.count.max(0) as u64;
            // Clamp to [0, count] defensively even though
            // `is_recurrence=true` is a strict subset of the matched
            // rows — a SQL backend that returns a stray NULL or an
            // out-of-range SUM should not panic the response builder.
            let recurrence = r.recurrence_count.clamp(0, r.count.max(0)) as u64;
            let share = if group_by_tool {
                per_tool_total
                    .get(&r.tool)
                    .filter(|t| **t > 0)
                    .map(|total| count_u as f64 / *total as f64)
            } else {
                None
            };
            ToolContributionAggregateEntry {
                // Map collapsed-axis placeholders back to `None` so the
                // proto consumer sees "this axis wasn't grouped on".
                // The SQL emits '' for unrouped string axes and 0 for
                // unrouped int axes (see stats.rs::aggregate_tool_contributions).
                tool: if group_by_tool { Some(r.tool) } else { None },
                contribution: if group_by_contribution {
                    Some(r.contribution)
                } else {
                    None
                },
                // `reflection_tool_outcome.error_kind` uses '' as the
                // sentinel for "no error" (NULL would force every
                // aggregate query through COALESCE). Normalise back to
                // None on the way out so the proto contract matches the
                // sibling APIs (`get_tool_contribution_stats`,
                // `hydrate_rows::tool_outcomes`); otherwise clients have
                // to special-case `Some("")` for this RPC alone.
                error_kind: if group_by_error_kind {
                    normalize_error_kind(r.error_kind)
                } else {
                    None
                },
                origin_user_id: if group_by_origin_user {
                    Some(r.origin_user_id)
                } else {
                    None
                },
                count: count_u,
                recurrence_count: Some(recurrence),
                contribution_share: share,
            }
        })
        .collect();
    Ok(AggregateToolContributionsResponse { entries })
}

/// Normalise the storage-layer empty-string sentinel used by
/// `reflection_tool_outcome.error_kind` back to `None`. The schema
/// stores '' rather than NULL so aggregate SQL stays NULL-free, but
/// the proto contract treats absent and "" as identical: every
/// reflection-search RPC that exposes `error_kind` collapses '' to
/// `None`. Centralised so a future schema flip to NULL only needs to
/// change one site.
fn normalize_error_kind(raw: String) -> Option<String> {
    if raw.is_empty() { None } else { Some(raw) }
}

#[cfg(test)]
mod normalize_error_kind_tests {
    //! Pin the empty-string → None contract: a regression here makes
    //! `AggregateToolContributions` and `get_tool_contribution_stats`
    //! disagree, forcing every client to special-case `Some("")` for
    //! one of them.

    use super::normalize_error_kind;

    #[test]
    fn empty_collapses_to_none() {
        assert_eq!(normalize_error_kind(String::new()), None);
    }

    #[test]
    fn non_empty_kept_verbatim() {
        assert_eq!(
            normalize_error_kind("timeout".into()),
            Some("timeout".into())
        );
        // Whitespace-only is a real (if odd) error_kind label —
        // treat it as data, not as the sentinel. The DB column has
        // no NOT NULL trim constraint, so callers can distinguish
        // "  " from "" if they so choose.
        assert_eq!(normalize_error_kind("  ".into()), Some("  ".into()));
    }
}
