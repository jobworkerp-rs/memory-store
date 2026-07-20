//! Shared P5 thread_filter resolver.
//!
//! Used by both the RDB list/Count path
//! (`MemoryApp::find_memory_list_by_condition`) and the LanceDB
//! pre-filter inside `MemoryVectorAppImpl`. Sharing one implementation
//! and one set of `MEMORY_THREAD_FILTER_*` env knobs avoids quiet
//! desync between the LanceDB pre-filter and the RDB IN-list.

use anyhow::Result;
use infra::error::LlmMemoryError;
use infra::infra::thread::rdb::{ThreadRepository, ThreadRepositoryImpl};
use infra::infra::thread_label::rdb::{ThreadLabelRepository, ThreadLabelRepositoryImpl};
use infra::infra::thread_memory::rdb::{ThreadMemoryRepository, ThreadMemoryRepositoryImpl};
use protobuf::llm_memory::data::{LabelMatchMode, ThreadSearchFilter};
use std::collections::HashSet;

/// Tunables for the P5 thread_filter resolve pipeline. Every field maps
/// 1:1 to the env vars documented in `dot.env`. Defaults are deliberately
/// conservative (silent over-fetch is not allowed): callers that hit a
/// hard limit get a `failed_precondition` so the UI can surface the
/// offending size instead of returning a quietly truncated result.
#[derive(Clone, Debug)]
pub struct ThreadFilterConfig {
    /// Cap on a single per-route intermediate buffer (labels-only or
    /// non-label-only). Exists to defend against a runaway query before
    /// the app layer can intersect the two routes; the post-intersection
    /// limit (`max_thread_ids`) is applied separately and is what
    /// callers usually feel.
    pub intermediate_hard_limit: i64,
    /// Cap on the resolved (labels ∩ non-label) thread_id set. A request
    /// that exceeds this value is rejected with `failed_precondition`.
    pub max_thread_ids: i64,
    /// Inline threshold for the FTS / Count `IN (...)` clause — values
    /// below this go on the wire as a flat IN list, beyond it the server
    /// rejects the request unless the search-side approximate fallback
    /// is opted in via `approximate_mode`.
    pub fts_max_inline_ids: usize,
    /// Absolute upper bound on memory_ids handed to FTS / Count, even
    /// with `approximate_mode = true`. Acts as the hard ceiling for the
    /// chunked Count path (50 chunks × 1000 ids = 50_000 by default).
    pub fts_max_total_ids: usize,
    /// Whether the search-side FTS path is allowed to fall back to the
    /// two-phase (over-fetch + INTERSECT) pipeline when N exceeds
    /// `fts_max_inline_ids`. Default `false` so silent precision loss
    /// requires an explicit operator opt-in. Count never honours this
    /// flag (counts must be exact or an explicit error).
    pub fts_approximate_mode: bool,
    /// Threshold above which the ANN (vector / hybrid) path switches
    /// from `prefilter` to `postfilter` to avoid blowing up the IN list
    /// inside LanceDB's planner.
    pub prefilter_threshold: usize,
    /// Multiplier applied to `limit` to compute the over-fetch size used
    /// by the FTS approximate-mode two-phase pipeline. The pipeline
    /// retries up to 3 times with K, K*2, K*3 (spec §P5「FTS 経路」近似
    /// モード「最大 3 回」). Default 8 mirrors the spec example.
    pub fts_overfetch_k: usize,
    /// Cap on rows the Count TEXT path reads from the LanceDB FTS
    /// stream before declaring `is_truncated = true`. Resolved at
    /// startup so per-request paths can hit the value via the cached
    /// config without touching `std::env`.
    pub fts_count_hard_cap: u64,
    /// Cap on rows the Count VECTOR / HYBRID paths read from the LanceDB
    /// ANN stream (and from the FTS branch in HYBRID `Rrf` / `Weighted`
    /// merging) before declaring `is_truncated = true`. ANN is more
    /// selective than FTS so the default (1000) is two orders of
    /// magnitude smaller than `fts_count_hard_cap`; the UI shifts to
    /// "N+ 表示" once the cap is hit.
    pub count_vector_hard_cap: u64,
    /// Image memory Phase 4: over-fetch base multiplier for the vector
    /// memory_id de-dup. With the N-row schema one memory owns many
    /// chunk rows, so a fixed-ratio over-fetch can be wholly consumed by
    /// a few long memories and fall short of `limit` after de-dup
    /// (design 3/3 §13.3.1, the same problem `fts_overfetch_k` solves on
    /// the FTS side). Same role as `fts_overfetch_k`.
    pub vector_dedup_overfetch_k: usize,
    /// Image memory Phase 4: absolute upper bound on rows the vector
    /// de-dup over-fetch will pull from LanceDB ANN, across all retry
    /// attempts (runaway guard). Reaching it returns fewer than `limit`
    /// with a shortfall warn — safety > completeness (design 3/3
    /// §13.3.1, mirrors `fts_max_total_ids`).
    pub vector_dedup_max_fetch: usize,
}

/// Env names + defaults for the N-row vector de-dup over-fetch knobs.
/// Shared with the reflection intent search path (which has no
/// `ThreadFilterConfig`) so both surfaces read the exact same env vars
/// and defaults. `approximate_fetch_limit` clamps `k` with `.max(1)`, so
/// no `>= 1` filter is needed here.
pub const VECTOR_DEDUP_OVERFETCH_K_ENV: &str = "MEMORY_VECTOR_DEDUP_OVERFETCH_K";
pub const VECTOR_DEDUP_OVERFETCH_K_DEFAULT: usize = 4;
pub const VECTOR_DEDUP_MAX_FETCH_ENV: &str = "MEMORY_VECTOR_DEDUP_MAX_FETCH";
pub const VECTOR_DEDUP_MAX_FETCH_DEFAULT: usize = 10_000;

/// Parse an env var or fall back to a default. Shared between
/// `ThreadFilterConfig::from_env` and the thread_vector count path so
/// both sides agree on the parse-or-default contract for
/// `MEMORY_FTS_COUNT_HARD_CAP` and similar knobs.
pub fn parse_env_or<T: std::str::FromStr>(key: &str, default: T) -> T {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse::<T>().ok())
        .unwrap_or(default)
}

impl ThreadFilterConfig {
    pub fn from_env() -> Self {
        Self {
            intermediate_hard_limit: parse_env_or(
                "MEMORY_THREAD_FILTER_INTERMEDIATE_HARD_LIMIT",
                1_000_000,
            ),
            max_thread_ids: parse_env_or("MEMORY_THREAD_FILTER_MAX_THREAD_IDS", 100_000),
            fts_max_inline_ids: parse_env_or("MEMORY_THREAD_FILTER_FTS_MAX_INLINE_IDS", 5_000),
            fts_max_total_ids: parse_env_or("MEMORY_THREAD_FILTER_FTS_MAX_TOTAL_IDS", 50_000),
            fts_approximate_mode: parse_env_or("MEMORY_THREAD_FILTER_FTS_APPROXIMATE_MODE", false),
            prefilter_threshold: parse_env_or("MEMORY_THREAD_FILTER_PREFILTER_THRESHOLD", 200),
            fts_overfetch_k: parse_env_or("MEMORY_THREAD_FILTER_FTS_OVERFETCH_K", 8),
            fts_count_hard_cap: parse_env_or("MEMORY_FTS_COUNT_HARD_CAP", 50_000),
            count_vector_hard_cap: parse_env_or("MEMORY_COUNT_VECTOR_HARD_CAP", 1_000),
            vector_dedup_overfetch_k: parse_env_or(
                VECTOR_DEDUP_OVERFETCH_K_ENV,
                VECTOR_DEDUP_OVERFETCH_K_DEFAULT,
            ),
            vector_dedup_max_fetch: parse_env_or(
                VECTOR_DEDUP_MAX_FETCH_ENV,
                VECTOR_DEDUP_MAX_FETCH_DEFAULT,
            ),
        }
    }
}

/// True iff `filter` populates any of the seven non-label conditions —
/// the `find_thread_ids_by_filter` route is skipped entirely when this
/// returns false, which keeps the labels-only fast path on a single SQL.
pub fn has_non_label_filters(f: &ThreadSearchFilter) -> bool {
    f.user_id.is_some()
        || f.channel.is_some()
        || f.created_after.is_some()
        || f.created_before.is_some()
        || f.updated_after.is_some()
        || f.updated_before.is_some()
        || !f.memory_kinds.is_empty()
}

/// True iff the non-label route is *actually needed* on top of the
/// labels route. When `labels` is set the labels route applies
/// `user_id` itself, so running a user_id-only non-label query becomes
/// pure overhead — and worse, can trip `intermediate_hard_limit` for
/// users who own a lot of threads even though the labels route would
/// resolve cleanly.
pub fn needs_other_route(f: &ThreadSearchFilter) -> bool {
    let other_than_user_id = f.channel.is_some()
        || f.created_after.is_some()
        || f.created_before.is_some()
        || f.updated_after.is_some()
        || f.updated_before.is_some()
        || !f.memory_kinds.is_empty();
    if !f.labels.is_empty() {
        // labels route already AND-applies user_id; only run the other
        // route when there is genuinely a non-user_id condition.
        other_than_user_id
    } else {
        // No labels: user_id is the only place user_id can be applied,
        // so keep the original behaviour.
        has_non_label_filters(f)
    }
}

/// Resolve `MemorySearchFilter.thread_filter` into the union of
/// memory_ids attached to any thread that matches the predicate.
///
/// Returns:
/// * `Ok(None)` — the filter has no fields set (caller should leave
///   the search un-narrowed by thread).
/// * `Ok(Some(vec![]))` — the filter matched zero threads (caller
///   should short-circuit the search to an empty result instead of
///   running the vector / FTS query).
/// * `Ok(Some(ids))` — pass `ids` to `SafeFilter::in_i64_list("memory_id", &ids)`
///   (vector path) or to the RDB IN-list (P8 list/Count path), and AND-combine
///   with the rest of the filter.
/// * `Err(LlmMemoryError::FailedPrecondition)` — the labels-only or
///   non-label-only intermediate buffer overflowed
///   (`MEMORY_THREAD_FILTER_INTERMEDIATE_HARD_LIMIT`), or the
///   intersected set exceeded `MEMORY_THREAD_FILTER_MAX_THREAD_IDS`.
///
/// `_memory_user_id` is currently unused (the memory-side `user_id`
/// is enforced by the outer filter); keeping it on the signature
/// reserves a hook for future combined-evaluation policies without
/// touching every call site.
pub async fn resolve_memory_ids_from_thread_filter(
    cfg: &ThreadFilterConfig,
    thread_label_repo: &ThreadLabelRepositoryImpl,
    thread_repo: &ThreadRepositoryImpl,
    thread_memory_repo: &ThreadMemoryRepositoryImpl,
    filter: &ThreadSearchFilter,
    _memory_user_id: Option<i64>,
) -> Result<Option<Vec<i64>>> {
    // Route 1: labels (delegates user_id matching to the label repo).
    let labels_set: Option<HashSet<i64>> = if !filter.labels.is_empty() {
        let match_all = filter.label_match_mode() == LabelMatchMode::LabelAll;
        let ids = thread_label_repo
            .find_thread_ids_by_labels(
                &filter.labels,
                match_all,
                filter.user_id,
                Some((cfg.intermediate_hard_limit as i32).saturating_add(1)),
                None,
            )
            .await?;
        if ids.len() as i64 > cfg.intermediate_hard_limit {
            return Err(LlmMemoryError::FailedPrecondition(format!(
                "thread_filter.labels matched more than {} threads (intermediate hard limit). \
                 Tighten the label set or combine with channel / time filters.",
                cfg.intermediate_hard_limit
            ))
            .into());
        }
        Some(ids.into_iter().collect())
    } else {
        None
    };

    // Route 2: the seven non-label conditions. user_id is also routed
    // through here so it can be evaluated standalone, but if `labels`
    // is also set the labels route already AND-applies user_id at the
    // SQL level (`thread_label_repo::find_thread_ids_by_labels`) — in
    // that case running an extra user_id-only query that pulls every
    // thread the user owns is not just redundant but can trip
    // `intermediate_hard_limit` on a heavy user when the labels route
    // would have resolved comfortably.
    let other_set: Option<HashSet<i64>> = if needs_other_route(filter) {
        let ids = thread_repo
            .find_thread_ids_by_filter(
                filter.user_id,
                filter.channel.as_deref(),
                filter.created_after,
                filter.created_before,
                filter.updated_after,
                filter.updated_before,
                &filter.memory_kinds,
                cfg.intermediate_hard_limit,
            )
            .await?;
        if ids.len() as i64 > cfg.intermediate_hard_limit {
            return Err(LlmMemoryError::FailedPrecondition(format!(
                "thread_filter non-label conditions matched more than {} threads \
                 (intermediate hard limit). Add a labels filter or narrow the time range.",
                cfg.intermediate_hard_limit
            ))
            .into());
        }
        Some(ids.into_iter().collect())
    } else {
        None
    };

    // Intersect / take whichever is populated. Two unset routes means
    // an empty `ThreadSearchFilter` was provided, which the caller
    // should treat as "no thread-side narrowing".
    let resolved: HashSet<i64> = match (labels_set, other_set) {
        (Some(a), Some(b)) => a.intersection(&b).copied().collect(),
        (Some(s), None) | (None, Some(s)) => s,
        (None, None) => return Ok(None),
    };

    if resolved.len() as i64 > cfg.max_thread_ids {
        return Err(LlmMemoryError::FailedPrecondition(format!(
            "thread_filter resolved to {} threads (limit: {}). \
             Add more selective conditions to the thread_filter.",
            resolved.len(),
            cfg.max_thread_ids
        ))
        .into());
    }

    if resolved.is_empty() {
        return Ok(Some(Vec::new()));
    }

    // Empty thread_ids would short-circuit above; safe to query the junction.
    let thread_ids: Vec<i64> = resolved.into_iter().collect();
    // `fts_max_total_ids` is the absolute upper bound on the
    // memory_id set, applied **before** materializing the entire
    // junction. A small number of huge threads (one common label,
    // a power user) could otherwise produce millions of memory_ids
    // and only get rejected by the FTS ceiling after the allocate /
    // sort / dedup work was already done. Treating this cap as
    // route-agnostic also fixes the gap on the ANN path: vector
    // searches now share the same hard ceiling instead of relying
    // on a `prefilter_threshold` warn that does not bound memory.
    let memory_id_cap = cfg.fts_max_total_ids;
    let mut memory_ids = thread_memory_repo
        .find_memory_ids_by_thread_ids(&thread_ids, memory_id_cap)
        .await?;
    if memory_ids.len() > memory_id_cap {
        return Err(LlmMemoryError::FailedPrecondition(format!(
            "thread_filter resolved to more than {} memory_ids \
             (MEMORY_THREAD_FILTER_FTS_MAX_TOTAL_IDS). The matching \
             threads contain too many memories — narrow the thread_filter \
             with additional labels, channel, or time-range conditions.",
            memory_id_cap
        ))
        .into());
    }
    memory_ids.sort_unstable();
    memory_ids.dedup();
    Ok(Some(memory_ids))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `has_non_label_filters` is the only piece of resolve logic that
    /// can run pure (no DB), so it gets a unit test that pins the
    /// 7-condition matrix without spinning up the test pool.
    #[test]
    fn has_non_label_filters_recognises_each_field() {
        let empty = ThreadSearchFilter::default();
        assert!(!has_non_label_filters(&empty));

        let only_labels = ThreadSearchFilter {
            labels: vec!["x".into()],
            ..Default::default()
        };
        assert!(
            !has_non_label_filters(&only_labels),
            "labels alone must NOT enable the non-label route — it has its own resolver"
        );

        for f in [
            ThreadSearchFilter {
                user_id: Some(1),
                ..Default::default()
            },
            ThreadSearchFilter {
                channel: Some("c".into()),
                ..Default::default()
            },
            ThreadSearchFilter {
                created_after: Some(1),
                ..Default::default()
            },
            ThreadSearchFilter {
                created_before: Some(1),
                ..Default::default()
            },
            ThreadSearchFilter {
                updated_after: Some(1),
                ..Default::default()
            },
            ThreadSearchFilter {
                updated_before: Some(1),
                ..Default::default()
            },
            ThreadSearchFilter {
                memory_kinds: vec![1],
                ..Default::default()
            },
        ] {
            assert!(
                has_non_label_filters(&f),
                "expected non-label route to fire for {:?}",
                f
            );
        }
    }

    /// `needs_other_route` decides whether the non-label SQL route runs
    /// when both `labels` and `user_id` are populated. The whole point of
    /// P2-B is that `labels` alone already AND-applies user_id (the
    /// thread_label SQL has `AND t.user_id = ?`), so a redundant
    /// user_id-only thread scan is wasted — and worse, can trip
    /// `intermediate_hard_limit` for users who own many threads.
    #[test]
    fn needs_other_route_skips_user_id_only_when_labels_set() {
        // labels + user_id only → non-label route NOT needed.
        let f = ThreadSearchFilter {
            labels: vec!["rust".into()],
            user_id: Some(1),
            ..Default::default()
        };
        assert!(
            !needs_other_route(&f),
            "labels+user_id should resolve via the labels route alone (user_id is AND-applied there)"
        );

        // labels + a real non-label condition → non-label route still
        // needed (labels route can't filter on channel / time range).
        for non_user_id in [
            ThreadSearchFilter {
                labels: vec!["rust".into()],
                user_id: Some(1),
                channel: Some("dev".into()),
                ..Default::default()
            },
            ThreadSearchFilter {
                labels: vec!["rust".into()],
                created_after: Some(1),
                ..Default::default()
            },
            ThreadSearchFilter {
                labels: vec!["rust".into()],
                updated_before: Some(2),
                ..Default::default()
            },
            ThreadSearchFilter {
                labels: vec!["rust".into()],
                memory_kinds: vec![1],
                ..Default::default()
            },
        ] {
            assert!(
                needs_other_route(&non_user_id),
                "expected non-label route to fire for {:?}",
                non_user_id
            );
        }

        // No labels: behaves like has_non_label_filters.
        let no_labels_user = ThreadSearchFilter {
            user_id: Some(1),
            ..Default::default()
        };
        assert!(needs_other_route(&no_labels_user));

        let no_labels_empty = ThreadSearchFilter::default();
        assert!(!needs_other_route(&no_labels_empty));
    }
}
