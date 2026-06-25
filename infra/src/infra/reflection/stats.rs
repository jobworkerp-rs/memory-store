//! Derived statistics: `tool_outcome_stats` and
//! `tool_contribution_stats`. Updated incrementally from the finalize
//! transaction (spec N-11) and rebuilt by F-A6.
//!
//! `tool_contribution_stats.error_kind` uses '' (empty string) in
//! place of NULL inside the PK so the sqlite/postgres semantics agree
//! without driver-specific NULL-in-PK handling.
//!
//! Phase F (F-A1 / F-A7) added the dynamic-filter aggregate methods
//! `aggregate_failure_modes` and `aggregate_tool_contributions`,
//! which reuse `build_filter_where` / `FilterBind` from `super::rdb`
//! so the predicate set stays in lockstep with `search_index`.

use super::rdb::{FilterBind, build_filter_where};
use super::rows::{
    FailureModeAggregateRow, FailureModeCoOccurrenceRow, ResolvedReflectionSearchFilter,
    ScoresAggregateAxis, ScoresAggregateBucket, ToolContributionAggregateRow,
    ToolContributionStatsRow, ToolOutcomeStatsRow,
};
use crate::error::LlmMemoryError;
use crate::sql::p;
use anyhow::Result;
use async_trait::async_trait;
use infra_utils::infra::rdb::{Rdb, RdbPool, UseRdbPool};
use sqlx::Executor;

// -------- tool_outcome_stats --------

#[cfg(feature = "postgres")]
const UPSERT_TOS_SQL: &str = concat!(
    "INSERT INTO tool_outcome_stats \
     (origin_user_id, tool, outcome, count, last_updated_at) VALUES (",
    p!(1),
    ",",
    p!(2),
    ",",
    p!(3),
    ",",
    p!(4),
    ",",
    p!(5),
    ") ON CONFLICT (origin_user_id, tool, outcome) DO UPDATE SET \
       count = tool_outcome_stats.count + EXCLUDED.count, \
       last_updated_at = EXCLUDED.last_updated_at;"
);

#[cfg(not(feature = "postgres"))]
const UPSERT_TOS_SQL: &str = concat!(
    "INSERT INTO tool_outcome_stats \
     (origin_user_id, tool, outcome, count, last_updated_at) VALUES (",
    p!(1),
    ",",
    p!(2),
    ",",
    p!(3),
    ",",
    p!(4),
    ",",
    p!(5),
    ") ON CONFLICT (origin_user_id, tool, outcome) DO UPDATE SET \
       count = tool_outcome_stats.count + excluded.count, \
       last_updated_at = excluded.last_updated_at;"
);

const LIST_TOS_BY_USER_TOOL_SQL: &str = concat!(
    "SELECT origin_user_id, tool, outcome, count, last_updated_at \
     FROM tool_outcome_stats WHERE origin_user_id = ",
    p!(1),
    " AND tool = ",
    p!(2),
    " ORDER BY outcome ASC;"
);

const REBUILD_TOS_DELETE_ALL_SQL: &str = "DELETE FROM tool_outcome_stats;";
const REBUILD_TOS_DELETE_BY_USER_SQL: &str = concat!(
    "DELETE FROM tool_outcome_stats WHERE origin_user_id = ",
    p!(1),
    ";"
);

const REBUILD_TOS_INSERT_ALL_SQL: &str = "INSERT INTO tool_outcome_stats (origin_user_id, tool, outcome, count, last_updated_at) \
     SELECT tri.origin_user_id, rt.tool, tri.outcome, COUNT(*) AS count, \
       COALESCE(MAX(tri.created_at), 0) AS last_updated_at \
     FROM thread_reflection_index tri \
     JOIN reflection_tool rt ON rt.memory_id = tri.memory_id \
     GROUP BY tri.origin_user_id, rt.tool, tri.outcome;";

const REBUILD_TOS_INSERT_BY_USER_SQL: &str = concat!(
    "INSERT INTO tool_outcome_stats (origin_user_id, tool, outcome, count, last_updated_at) \
     SELECT tri.origin_user_id, rt.tool, tri.outcome, COUNT(*) AS count, \
       COALESCE(MAX(tri.created_at), 0) AS last_updated_at \
     FROM thread_reflection_index tri \
     JOIN reflection_tool rt ON rt.memory_id = tri.memory_id \
     WHERE tri.origin_user_id = ",
    p!(1),
    " GROUP BY tri.origin_user_id, rt.tool, tri.outcome;"
);

// -------- tool_contribution_stats --------

#[cfg(feature = "postgres")]
const UPSERT_TCS_SQL: &str = concat!(
    "INSERT INTO tool_contribution_stats \
     (origin_user_id, tool, contribution, error_kind, count, last_updated_at) VALUES (",
    p!(1),
    ",",
    p!(2),
    ",",
    p!(3),
    ",",
    p!(4),
    ",",
    p!(5),
    ",",
    p!(6),
    ") ON CONFLICT (origin_user_id, tool, contribution, error_kind) DO UPDATE SET \
       count = tool_contribution_stats.count + EXCLUDED.count, \
       last_updated_at = EXCLUDED.last_updated_at;"
);

#[cfg(not(feature = "postgres"))]
const UPSERT_TCS_SQL: &str = concat!(
    "INSERT INTO tool_contribution_stats \
     (origin_user_id, tool, contribution, error_kind, count, last_updated_at) VALUES (",
    p!(1),
    ",",
    p!(2),
    ",",
    p!(3),
    ",",
    p!(4),
    ",",
    p!(5),
    ",",
    p!(6),
    ") ON CONFLICT (origin_user_id, tool, contribution, error_kind) DO UPDATE SET \
       count = tool_contribution_stats.count + excluded.count, \
       last_updated_at = excluded.last_updated_at;"
);

const LIST_TCS_BY_USER_TOOL_SQL: &str = concat!(
    "SELECT origin_user_id, tool, contribution, error_kind, count, last_updated_at \
     FROM tool_contribution_stats WHERE origin_user_id = ",
    p!(1),
    " AND tool = ",
    p!(2),
    " ORDER BY contribution ASC, error_kind ASC;"
);

const REBUILD_TCS_DELETE_ALL_SQL: &str = "DELETE FROM tool_contribution_stats;";
const REBUILD_TCS_DELETE_BY_USER_SQL: &str = concat!(
    "DELETE FROM tool_contribution_stats WHERE origin_user_id = ",
    p!(1),
    ";"
);

const REBUILD_TCS_INSERT_ALL_SQL: &str = "INSERT INTO tool_contribution_stats \
       (origin_user_id, tool, contribution, error_kind, count, last_updated_at) \
     SELECT tri.origin_user_id, rto.tool, rto.contribution, \
       COALESCE(rto.error_kind, '') AS error_kind, \
       COUNT(*) AS count, \
       COALESCE(MAX(tri.created_at), 0) AS last_updated_at \
     FROM thread_reflection_index tri \
     JOIN reflection_tool_outcome rto ON rto.memory_id = tri.memory_id \
     GROUP BY tri.origin_user_id, rto.tool, rto.contribution, COALESCE(rto.error_kind, '');";

const REBUILD_TCS_INSERT_BY_USER_SQL: &str = concat!(
    "INSERT INTO tool_contribution_stats \
       (origin_user_id, tool, contribution, error_kind, count, last_updated_at) \
     SELECT tri.origin_user_id, rto.tool, rto.contribution, \
       COALESCE(rto.error_kind, '') AS error_kind, \
       COUNT(*) AS count, \
       COALESCE(MAX(tri.created_at), 0) AS last_updated_at \
     FROM thread_reflection_index tri \
     JOIN reflection_tool_outcome rto ON rto.memory_id = tri.memory_id \
     WHERE tri.origin_user_id = ",
    p!(1),
    " GROUP BY tri.origin_user_id, rto.tool, rto.contribution, COALESCE(rto.error_kind, '');"
);

#[async_trait]
#[allow(clippy::too_many_arguments)]
pub trait ReflectionStatsRepository: UseRdbPool + Send + Sync {
    /// Increment by `delta` (typically 1 per reflection insert) for the
    /// matching key. Idempotent under retries iff the caller wraps the
    /// finalize tx as a single attempt.
    async fn upsert_tool_outcome_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        origin_user_id: i64,
        tool: &str,
        outcome: i32,
        delta: i64,
        last_updated_at: i64,
    ) -> Result<()> {
        sqlx::query::<Rdb>(UPSERT_TOS_SQL)
            .bind(origin_user_id)
            .bind(tool)
            .bind(outcome)
            .bind(delta)
            .bind(last_updated_at)
            .execute(tx)
            .await
            .map(|_| ())
            .map_err(|e| LlmMemoryError::DBError(e).into())
    }

    async fn upsert_tool_contribution_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        origin_user_id: i64,
        tool: &str,
        contribution: i32,
        error_kind: &str,
        delta: i64,
        last_updated_at: i64,
    ) -> Result<()> {
        sqlx::query::<Rdb>(UPSERT_TCS_SQL)
            .bind(origin_user_id)
            .bind(tool)
            .bind(contribution)
            .bind(error_kind)
            .bind(delta)
            .bind(last_updated_at)
            .execute(tx)
            .await
            .map(|_| ())
            .map_err(|e| LlmMemoryError::DBError(e).into())
    }

    async fn list_tool_outcome_stats(
        &self,
        origin_user_id: i64,
        tool: &str,
    ) -> Result<Vec<ToolOutcomeStatsRow>> {
        sqlx::query_as::<Rdb, ToolOutcomeStatsRow>(LIST_TOS_BY_USER_TOOL_SQL)
            .bind(origin_user_id)
            .bind(tool)
            .fetch_all(self.db_pool())
            .await
            .map_err(|e| LlmMemoryError::DBError(e).into())
    }

    async fn list_tool_contribution_stats(
        &self,
        origin_user_id: i64,
        tool: &str,
    ) -> Result<Vec<ToolContributionStatsRow>> {
        sqlx::query_as::<Rdb, ToolContributionStatsRow>(LIST_TCS_BY_USER_TOOL_SQL)
            .bind(origin_user_id)
            .bind(tool)
            .fetch_all(self.db_pool())
            .await
            .map_err(|e| LlmMemoryError::DBError(e).into())
    }

    /// Two-step rebuild for F-A6. When `origin_user_id` is `Some`,
    /// both the DELETE and the INSERT-SELECT are scoped to that user;
    /// scoping only the DELETE would let the INSERT-SELECT recompute
    /// every user and clash with the rows we left untouched. Returns
    /// the number of stats rows the rebuild touched (max of the
    /// DELETE and INSERT counts; informational only).
    async fn rebuild_tool_outcome_stats(&self, origin_user_id: Option<i64>) -> Result<u64> {
        rebuild_stats_scoped(
            self.db_pool(),
            origin_user_id,
            REBUILD_TOS_DELETE_ALL_SQL,
            REBUILD_TOS_DELETE_BY_USER_SQL,
            REBUILD_TOS_INSERT_ALL_SQL,
            REBUILD_TOS_INSERT_BY_USER_SQL,
        )
        .await
    }

    async fn rebuild_tool_contribution_stats(&self, origin_user_id: Option<i64>) -> Result<u64> {
        rebuild_stats_scoped(
            self.db_pool(),
            origin_user_id,
            REBUILD_TCS_DELETE_ALL_SQL,
            REBUILD_TCS_DELETE_BY_USER_SQL,
            REBUILD_TCS_INSERT_ALL_SQL,
            REBUILD_TCS_INSERT_BY_USER_SQL,
        )
        .await
    }

    // ============================================================
    // F-A1 — AggregateFailureModes
    // ============================================================

    /// Per-`mode` count + `is_recurrence` bucket count over the
    /// filtered sidecar. `filter` reuses the same shape as
    /// `search_index` so the predicate semantics match across
    /// search / aggregate. Empty result is a normal outcome (no
    /// reflections matched the filter); the caller distinguishes
    /// that from "feature not yet implemented" because this method
    /// returns `Ok(...)` rather than the previous `Unimplemented`
    /// stub.
    async fn aggregate_failure_modes(
        &self,
        filter: &ResolvedReflectionSearchFilter,
    ) -> Result<Vec<FailureModeAggregateRow>> {
        let (where_clause, binds) = build_filter_where(filter, 1);
        let sql = format!(
            "SELECT rfm.mode AS mode, \
                    COUNT(*) AS count, \
                    SUM(CASE WHEN tri.is_recurrence THEN 1 ELSE 0 END) AS recurrence_count \
             FROM thread_reflection_index tri \
             JOIN reflection_failure_mode rfm ON rfm.memory_id = tri.memory_id\
             {where_clause} \
             GROUP BY rfm.mode \
             ORDER BY count DESC, rfm.mode ASC"
        );
        let q = sqlx::query_as::<Rdb, FailureModeAggregateRow>(sqlx::AssertSqlSafe(sql));
        bind_filter_values_as(q, &binds)
            .fetch_all(self.db_pool())
            .await
            .map_err(|e| LlmMemoryError::DBError(e).into())
    }

    /// Per-pair `(mode_a, mode_b)` co-occurrence count over the
    /// filtered sidecar. `mode_a < mode_b` is enforced in SQL so each
    /// unordered pair appears exactly once. The pair set is capped at
    /// `CO_OCCURRENCE_PAIR_CAP` to bound the response size when a
    /// permissive filter matches the whole table; the natural pair
    /// count is ~100 (~15-entry failure_mode dictionary).
    ///
    /// Returns `(rows, is_truncated)` so the cap stays a private
    /// infra concern — the app layer surfaces `is_truncated` on the
    /// proto response without ever comparing against the constant.
    async fn aggregate_failure_mode_co_occurrence(
        &self,
        filter: &ResolvedReflectionSearchFilter,
    ) -> Result<(Vec<FailureModeCoOccurrenceRow>, bool)> {
        let (where_clause, binds) = build_filter_where(filter, 1);
        let sql = format!(
            "SELECT a.mode AS mode_a, b.mode AS mode_b, COUNT(*) AS count \
             FROM reflection_failure_mode a \
             JOIN reflection_failure_mode b ON a.memory_id = b.memory_id AND a.mode < b.mode \
             JOIN thread_reflection_index tri ON tri.memory_id = a.memory_id\
             {where_clause} \
             GROUP BY a.mode, b.mode \
             ORDER BY count DESC, a.mode ASC, b.mode ASC \
             LIMIT {CO_OCCURRENCE_PAIR_CAP}"
        );
        let q = sqlx::query_as::<Rdb, FailureModeCoOccurrenceRow>(sqlx::AssertSqlSafe(sql));
        let rows = bind_filter_values_as(q, &binds)
            .fetch_all(self.db_pool())
            .await
            .map_err(LlmMemoryError::DBError)?;
        let is_truncated = rows.len() as i64 >= CO_OCCURRENCE_PAIR_CAP;
        Ok((rows, is_truncated))
    }

    // ============================================================
    // F-A7 — AggregateToolContributions (multi-axis pivot)
    // ============================================================

    /// Pivot `reflection_tool_outcome` joined with the filtered
    /// `thread_reflection_index` by any subset of `{tool, contribution,
    /// error_kind, origin_user_id}`. Axes that are not requested
    /// collapse to a constant ('' / 0) so the FromRow shape stays
    /// uniform — the proto-side handler reads only the axes that the
    /// request asked for and ignores the constants.
    async fn aggregate_tool_contributions(
        &self,
        filter: &ResolvedReflectionSearchFilter,
        group_by_tool: bool,
        group_by_contribution: bool,
        group_by_error_kind: bool,
        group_by_origin_user: bool,
    ) -> Result<Vec<ToolContributionAggregateRow>> {
        // Build the SELECT / GROUP BY columns dynamically.
        // Constants for ungrouped axes:
        //   tool             -> '' (TEXT)
        //   contribution     -> 0  (INT)
        //   error_kind       -> '' (TEXT)
        //   origin_user_id   -> 0  (INT)
        let tool_expr = if group_by_tool { "rto.tool" } else { "''" };
        let contribution_expr = if group_by_contribution {
            "rto.contribution"
        } else {
            "0"
        };
        // error_kind is stored as NOT NULL DEFAULT '' so we wrap the
        // grouped form in `COALESCE` defensively even though it should
        // already be non-null.
        let error_kind_expr = if group_by_error_kind {
            "COALESCE(rto.error_kind, '')"
        } else {
            "''"
        };
        // Postgres infers untyped `0` as INT4, but `origin_user_id` is
        // BIGINT (i64) and the FromRow reads it as i64 — cast the constant
        // so the column type matches when the axis is collapsed. SQLite
        // accepts `CAST(0 AS BIGINT)` (BIGINT aliases to INTEGER).
        let origin_user_expr = if group_by_origin_user {
            "tri.origin_user_id"
        } else {
            "CAST(0 AS BIGINT)"
        };

        let mut group_by_parts: Vec<&str> = Vec::new();
        if group_by_tool {
            group_by_parts.push("rto.tool");
        }
        if group_by_contribution {
            group_by_parts.push("rto.contribution");
        }
        if group_by_error_kind {
            group_by_parts.push("COALESCE(rto.error_kind, '')");
        }
        if group_by_origin_user {
            group_by_parts.push("tri.origin_user_id");
        }
        // GROUP BY with no axes collapses every matching row to a
        // single bucket, which is a legitimate "give me a totals row"
        // pattern. Postgres allows an empty GROUP BY; sqlite does too
        // (omitting the clause). Emit nothing when the parts list is
        // empty.
        let group_by_clause = if group_by_parts.is_empty() {
            String::new()
        } else {
            format!(" GROUP BY {}", group_by_parts.join(", "))
        };

        let (where_clause, binds) = build_filter_where(filter, 1);
        // `recurrence_count` is the subset of `count` whose sidecar
        // row carries `is_recurrence=true`. SUM(CASE …) keeps the
        // query a single GROUP BY pass (no second SELECT or JOIN),
        // and the result is i64 so the app layer can normalise back
        // to `Option<u64>` per the proto contract.
        let sql = format!(
            "SELECT {tool_expr} AS tool, \
                    {contribution_expr} AS contribution, \
                    {error_kind_expr} AS error_kind, \
                    {origin_user_expr} AS origin_user_id, \
                    COUNT(*) AS count, \
                    SUM(CASE WHEN tri.is_recurrence THEN 1 ELSE 0 END) AS recurrence_count \
             FROM thread_reflection_index tri \
             JOIN reflection_tool_outcome rto ON rto.memory_id = tri.memory_id\
             {where_clause}\
             {group_by_clause} \
             ORDER BY count DESC"
        );
        let q = sqlx::query_as::<Rdb, ToolContributionAggregateRow>(sqlx::AssertSqlSafe(sql));
        bind_filter_values_as(q, &binds)
            .fetch_all(self.db_pool())
            .await
            .map_err(|e| LlmMemoryError::DBError(e).into())
    }

    // ============================================================
    // F-A2 — AggregateScores
    // ============================================================

    /// Dynamic GROUP BY over `thread_reflection_index.score` with
    /// percentile (p50 / p95) on top of avg/min/max/count. Backends
    /// diverge:
    ///   * Postgres has `percentile_cont` so the whole aggregate fits
    ///     into one SELECT.
    ///   * SQLite has no percentile aggregate, so we run a second
    ///     scan that returns one row per matching reflection (per
    ///     group key + score), sorted, and pick the percentile in
    ///     application code. The defensive `cap` bounds the in-memory
    ///     buffer when a permissive filter / coarse axis selection
    ///     blows the group count past expectations.
    ///
    /// `time_bucket_seconds` is required when `ScoresAggregateAxis::TimeBucket`
    /// is in `group_by`; callers (app layer) enforce that contract,
    /// since the infra layer treats a `None` bucket size as "every
    /// row in bucket zero" — a useless aggregation, but not an SQL
    /// error.
    async fn aggregate_scores(
        &self,
        filter: &ResolvedReflectionSearchFilter,
        group_by: &[ScoresAggregateAxis],
        time_bucket_seconds: Option<u32>,
        cap: usize,
    ) -> Result<Vec<ScoresAggregateBucket>> {
        aggregate_scores_impl(self.db_pool(), filter, group_by, time_bucket_seconds, cap).await
    }
}

/// `aggregate_scores` body. Kept out of the trait so the helper types
/// (`AxisColumns`, `FilterBind`) stay private to the infra module —
/// the trait method itself only forwards plain Rust types.
async fn aggregate_scores_impl(
    pool: &RdbPool,
    filter: &ResolvedReflectionSearchFilter,
    group_by: &[ScoresAggregateAxis],
    time_bucket_seconds: Option<u32>,
    cap: usize,
) -> Result<Vec<ScoresAggregateBucket>> {
    let time_bucket_ms: i64 = time_bucket_seconds.unwrap_or(0) as i64 * 1000;
    // App-layer rejects this combination already, but the infra path
    // is reachable from internal admin tools that bypass the app
    // dispatch — defend so the bucket expression cannot silently
    // collapse every row into the same `time_bucket_start = 0` row.
    if group_by.contains(&ScoresAggregateAxis::TimeBucket) && time_bucket_ms == 0 {
        return Err(LlmMemoryError::InvalidArgument(
            "aggregate_scores: time_bucket_seconds must be > 0 when TimeBucket is in group_by"
                .into(),
        )
        .into());
    }
    let (where_clause, binds) = build_filter_where(filter, 1);
    // The SELECT-time TIME_BUCKET expression is portable integer
    // math: floor-divide created_at by the bucket width, then
    // multiply back. SQLite and Postgres agree on integer / and *.
    let time_bucket_expr = if time_bucket_ms > 0 {
        format!("((tri.created_at / {time_bucket_ms}) * {time_bucket_ms})")
    } else {
        "0".to_string()
    };
    let axes = AxisColumns::new(group_by, &time_bucket_expr);

    #[cfg(feature = "postgres")]
    {
        aggregate_scores_postgres(pool, &axes, &where_clause, &binds, cap).await
    }
    #[cfg(not(feature = "postgres"))]
    {
        aggregate_scores_sqlite(pool, &axes, &where_clause, &binds, cap).await
    }
}

#[cfg(feature = "postgres")]
async fn aggregate_scores_postgres(
    pool: &RdbPool,
    axes: &AxisColumns<'_>,
    where_clause: &str,
    binds: &[FilterBind],
    cap: usize,
) -> Result<Vec<ScoresAggregateBucket>> {
    let select = axes.select_columns_prefix();
    let group_by_clause = axes.group_by_clause();
    let sql = format!(
        "SELECT {select}\
                COUNT(*) AS bucket_count, \
                COALESCE(AVG(tri.score), 0.0) AS score_avg, \
                COALESCE(MIN(tri.score), 0.0) AS score_min, \
                COALESCE(MAX(tri.score), 0.0) AS score_max, \
                COALESCE(percentile_cont(0.5) WITHIN GROUP (ORDER BY tri.score), 0.0) AS score_p50, \
                COALESCE(percentile_cont(0.95) WITHIN GROUP (ORDER BY tri.score), 0.0) AS score_p95 \
         FROM thread_reflection_index tri\
         {where_clause}\
         {group_by_clause}"
    );
    let q = sqlx::query_as::<Rdb, AggregateScoresFullRow>(sqlx::AssertSqlSafe(sql));
    let rows = bind_filter_values_as(q, binds)
        .fetch_all(pool)
        .await
        .map_err(LlmMemoryError::DBError)?;
    if rows.len() > cap {
        return Err(LlmMemoryError::InvalidArgument(format!(
            "aggregate_scores group count {} exceeds cap {}",
            rows.len(),
            cap
        ))
        .into());
    }
    Ok(rows.into_iter().map(|r| r.into_bucket(axes)).collect())
}

#[cfg(not(feature = "postgres"))]
async fn aggregate_scores_sqlite(
    pool: &RdbPool,
    axes: &AxisColumns<'_>,
    where_clause: &str,
    binds: &[FilterBind],
    cap: usize,
) -> Result<Vec<ScoresAggregateBucket>> {
    // Stage A: count / avg / min / max per group. The same WHERE
    // clause is reused unchanged with the same bind sequence in
    // Stage B; we run them sequentially so the bind borrow flows
    // through both queries without cloning into 'static.
    let select = axes.select_columns_prefix();
    let group_by_clause = axes.group_by_clause();
    let agg_sql = format!(
        "SELECT {select}\
                COUNT(*) AS bucket_count, \
                COALESCE(AVG(tri.score), 0.0) AS score_avg, \
                COALESCE(MIN(tri.score), 0.0) AS score_min, \
                COALESCE(MAX(tri.score), 0.0) AS score_max \
         FROM thread_reflection_index tri\
         {where_clause}\
         {group_by_clause}"
    );
    let q = sqlx::query_as::<Rdb, AggregateScoresAggRow>(sqlx::AssertSqlSafe(agg_sql));
    let agg_rows = bind_filter_values_as(q, binds)
        .fetch_all(pool)
        .await
        .map_err(LlmMemoryError::DBError)?;
    if agg_rows.len() > cap {
        return Err(LlmMemoryError::InvalidArgument(format!(
            "aggregate_scores group count {} exceeds cap {}",
            agg_rows.len(),
            cap
        ))
        .into());
    }
    if agg_rows.is_empty() {
        return Ok(Vec::new());
    }

    // Stage B: re-scan the same filter, but return one row per
    // matching reflection with the group key columns plus the
    // raw score, sorted by group key + score asc so app-side
    // percentile picking is a single pass.
    let key_select = axes.select_columns_prefix();
    let order_by = axes.order_by_clause();
    let raw_sql = format!(
        "SELECT {key_select}tri.score AS score \
         FROM thread_reflection_index tri\
         {where_clause} \
         {order_by}"
    );
    let q = sqlx::query_as::<Rdb, AggregateScoresRawRow>(sqlx::AssertSqlSafe(raw_sql));
    let raw_rows = bind_filter_values_as(q, binds)
        .fetch_all(pool)
        .await
        .map_err(LlmMemoryError::DBError)?;

    // Walk both result streams in lockstep: ORDER BY in Stage B
    // matches GROUP BY in Stage A, so the score buffer for each
    // group fills monotonically and we can pick percentiles
    // without a HashMap. The shared WHERE binds guarantee the row
    // sets line up exactly.
    let mut out: Vec<ScoresAggregateBucket> = Vec::with_capacity(agg_rows.len());
    let mut raw_iter = raw_rows.into_iter().peekable();
    for agg in agg_rows {
        let mut scores: Vec<f64> = Vec::with_capacity(agg.bucket_count as usize);
        while let Some(peek) = raw_iter.peek() {
            if !agg.same_group(peek) {
                break;
            }
            let row = raw_iter.next().expect("peek then next");
            scores.push(row.score);
        }
        // Percentile via the nearest-rank method on the sorted
        // score vector. SQLite's ORDER BY tri.score ASC keeps
        // the vector pre-sorted, so we skip the in-app sort.
        let p50 = nearest_rank_percentile(&scores, 0.5);
        let p95 = nearest_rank_percentile(&scores, 0.95);
        out.push(agg.into_bucket(axes, p50, p95));
    }
    Ok(out)
}

/// Cap on the number of `(mode_a, mode_b)` rows the co-occurrence
/// query returns. With a 15-entry failure_mode dictionary the
/// natural pair count is ~100; the cap exists to bound pathological
/// outputs when an extended dictionary or a permissive filter
/// blows past expectations. The infra method itself reports
/// `is_truncated` on the way out so callers do not import this
/// constant.
const CO_OCCURRENCE_PAIR_CAP: i64 = 1000;

/// Apply the dynamic-filter binds to a `query_as` builder. Three
/// aggregate paths share the same bind-loop shape; expressing the
/// match as a free function avoids triplicating it (the sqlx
/// `QueryAs` trait bounds make a generic method too noisy to be
/// worth the abstraction).
fn bind_filter_values_as<'q, T>(
    q: sqlx::query::QueryAs<'q, Rdb, T, <Rdb as sqlx::Database>::Arguments>,
    binds: &'q [FilterBind],
) -> sqlx::query::QueryAs<'q, Rdb, T, <Rdb as sqlx::Database>::Arguments> {
    let mut q = q;
    for b in binds {
        q = match b {
            FilterBind::I64(x) => q.bind(*x),
            FilterBind::I32(x) => q.bind(*x),
            FilterBind::F64(x) => q.bind(*x),
            FilterBind::Bool(x) => q.bind(*x),
            FilterBind::Str(s) => q.bind(s.as_str()),
        };
    }
    q
}

async fn rebuild_stats_scoped(
    pool: &RdbPool,
    origin_user_id: Option<i64>,
    delete_all: &'static str,
    delete_by_user: &'static str,
    insert_all: &'static str,
    insert_by_user: &'static str,
) -> Result<u64> {
    // Wrap DELETE + INSERT in a single transaction so a failure
    // between the two leaves the existing stats untouched. Otherwise
    // a crash after the DELETE would leave the scope (the user, or
    // the whole table) with empty / partial stats and skew
    // subsequent GetToolOutcomeStats / GetToolContributionStats
    // results until the next successful rebuild.
    let mut tx = pool.begin().await.map_err(LlmMemoryError::DBError)?;
    let deleted = match origin_user_id {
        None => sqlx::query::<Rdb>(delete_all)
            .execute(&mut *tx)
            .await
            .map_err(LlmMemoryError::DBError)?
            .rows_affected(),
        Some(uid) => sqlx::query::<Rdb>(delete_by_user)
            .bind(uid)
            .execute(&mut *tx)
            .await
            .map_err(LlmMemoryError::DBError)?
            .rows_affected(),
    };
    let inserted = match origin_user_id {
        None => sqlx::query::<Rdb>(insert_all)
            .execute(&mut *tx)
            .await
            .map_err(LlmMemoryError::DBError)?
            .rows_affected(),
        Some(uid) => sqlx::query::<Rdb>(insert_by_user)
            .bind(uid)
            .execute(&mut *tx)
            .await
            .map_err(LlmMemoryError::DBError)?
            .rows_affected(),
    };
    tx.commit().await.map_err(LlmMemoryError::DBError)?;
    Ok(deleted.max(inserted))
}

/// Maximum number of distinct groups the SQLite path materialises
/// before refusing the request. The all-axes-on selection over a
/// large filter could otherwise read the entire reflection table
/// into memory just to bucket it. 10_000 is well past the natural
/// ceiling for a single tenant (the spec budgets analytics dashboards
/// at thousands of groups, not millions) and well below the
/// out-of-memory wall.
pub const MAX_GROUPS_FOR_INMEM_PERCENTILE: usize = 10_000;

/// Per-axis bookkeeping for `aggregate_scores`. Holds the SQL
/// expressions for each axis (either the real column reference or a
/// constant placeholder for axes the request did not ask for) so the
/// SELECT, GROUP BY, and ORDER BY clauses stay in lockstep without
/// repeating the toggle logic three times.
///
/// `time_bucket_expr` is computed once at the call site (the integer
/// math depends on `time_bucket_seconds`) and reused for whichever
/// fragment needs it.
struct AxisColumns<'a> {
    /// Original axis order from the request, deduplicated at the app
    /// layer. Used to drive the SELECT order so the FromRow column
    /// indices remain deterministic; the GROUP BY / ORDER BY clauses
    /// also reuse this order.
    axes: Vec<ScoresAggregateAxis>,
    /// Membership view computed once at construction time so the
    /// per-row `into_bucket` mappers stay allocation-free even on
    /// the cap-sized (10_000-bucket) SQLite path.
    active: std::collections::HashSet<ScoresAggregateAxis>,
    time_bucket_expr: &'a str,
}

impl<'a> AxisColumns<'a> {
    fn new(group_by: &[ScoresAggregateAxis], time_bucket_expr: &'a str) -> Self {
        // Dedupe while preserving insertion order. The proto enum is
        // `repeated`, so duplicates can slip through; the app layer
        // already strips them but defending here keeps the SQL builder
        // independent.
        let mut active: std::collections::HashSet<ScoresAggregateAxis> =
            std::collections::HashSet::with_capacity(group_by.len());
        let mut axes: Vec<ScoresAggregateAxis> = Vec::with_capacity(group_by.len());
        for a in group_by {
            if active.insert(*a) {
                axes.push(*a);
            }
        }
        Self {
            axes,
            active,
            time_bucket_expr,
        }
    }

    /// Column expression for `axis` (real column for grouped axes;
    /// constant placeholder for collapsed ones).
    fn expr(&self, axis: ScoresAggregateAxis) -> &str {
        match axis {
            ScoresAggregateAxis::OriginUserId => "tri.origin_user_id",
            ScoresAggregateAxis::OriginChannel => "tri.origin_channel",
            ScoresAggregateAxis::TargetModelVersion => "tri.target_model_version",
            ScoresAggregateAxis::TaskCategory => "tri.task_category",
            ScoresAggregateAxis::ReflectionAspect => "tri.reflection_aspect",
            ScoresAggregateAxis::Outcome => "tri.outcome",
            ScoresAggregateAxis::ExperimentId => "tri.experiment_id",
            ScoresAggregateAxis::ExperimentVariant => "tri.experiment_variant",
            ScoresAggregateAxis::TimeBucket => self.time_bucket_expr,
            ScoresAggregateAxis::SummaryEmbeddingStatus => "tri.summary_embedding_status",
            ScoresAggregateAxis::IntentEmbeddingStatus => "tri.intent_embedding_status",
        }
    }

    fn alias(axis: ScoresAggregateAxis) -> &'static str {
        match axis {
            ScoresAggregateAxis::OriginUserId => "axis_origin_user_id",
            ScoresAggregateAxis::OriginChannel => "axis_origin_channel",
            ScoresAggregateAxis::TargetModelVersion => "axis_target_model_version",
            ScoresAggregateAxis::TaskCategory => "axis_task_category",
            ScoresAggregateAxis::ReflectionAspect => "axis_reflection_aspect",
            ScoresAggregateAxis::Outcome => "axis_outcome",
            ScoresAggregateAxis::ExperimentId => "axis_experiment_id",
            ScoresAggregateAxis::ExperimentVariant => "axis_experiment_variant",
            ScoresAggregateAxis::TimeBucket => "axis_time_bucket",
            ScoresAggregateAxis::SummaryEmbeddingStatus => "axis_summary_embedding_status",
            ScoresAggregateAxis::IntentEmbeddingStatus => "axis_intent_embedding_status",
        }
    }

    /// `expr AS alias, expr AS alias, ...` for the SELECT projection.
    /// With no axes (the "totals row" pattern), emits an empty string;
    /// callers must guard the trailing comma to avoid `SELECT , COUNT(*)`
    /// syntax errors.
    fn select_columns_with_alias(&self) -> String {
        self.axes
            .iter()
            .map(|a| format!("{} AS {}", self.expr(*a), Self::alias(*a)))
            .collect::<Vec<_>>()
            .join(", ")
    }

    /// `select_columns_with_alias` with a trailing `, ` when non-empty
    /// so SELECT projections that append `COUNT(*) AS ...` directly
    /// don't need to special-case the empty case.
    fn select_columns_prefix(&self) -> String {
        let s = self.select_columns_with_alias();
        if s.is_empty() {
            String::new()
        } else {
            format!("{s}, ")
        }
    }

    fn group_by_clause(&self) -> String {
        if self.axes.is_empty() {
            // Empty GROUP BY collapses every match into one row, which
            // is the "totals" pattern. Both backends accept omitting
            // the clause for that.
            String::new()
        } else {
            let cols = self
                .axes
                .iter()
                .map(|a| self.expr(*a).to_string())
                .collect::<Vec<_>>()
                .join(", ");
            format!(" GROUP BY {cols}")
        }
    }

    #[cfg(not(feature = "postgres"))]
    fn order_by_clause(&self) -> String {
        if self.axes.is_empty() {
            // SQLite path needs to read scores in order even without
            // group keys; the single bucket is then sorted by score.
            " ORDER BY tri.score ASC".to_string()
        } else {
            let cols = self
                .axes
                .iter()
                .map(|a| self.expr(*a).to_string())
                .collect::<Vec<_>>()
                .join(", ");
            format!(" ORDER BY {cols}, tri.score ASC")
        }
    }
}

/// Nearest-rank percentile pick. `scores` must be sorted ascending.
/// For an empty input, returns 0.0 (caller short-circuits on empty
/// buckets before reaching here; this branch is just for safety).
/// Only the SQLite path needs this — Postgres uses `percentile_cont`
/// in SQL.
#[cfg(not(feature = "postgres"))]
fn nearest_rank_percentile(scores: &[f64], p: f64) -> f64 {
    if scores.is_empty() {
        return 0.0;
    }
    let n = scores.len();
    // ceil(p * n) - 1 is the zero-based index for the nearest-rank
    // method. Clamp to [0, n-1] so floating-point quirks at p=0 / p=1
    // can't index out of bounds.
    let rank = (p * n as f64).ceil() as isize - 1;
    let idx = rank.clamp(0, n as isize - 1) as usize;
    scores[idx]
}

// Helper FromRow types for `aggregate_scores`. Each variant has the
// same 11 axis columns followed by the per-backend payload (full
// percentile row for Postgres, raw score for SQLite Stage B, agg
// columns without percentile for SQLite Stage A). Splitting the
// struct per stage keeps each column count manageable; sqlx wires
// columns by name when `#[derive(FromRow)]` is used so the alias
// names defined in `AxisColumns::alias` line up automatically.

#[cfg(feature = "postgres")]
#[derive(sqlx::FromRow, Debug)]
#[allow(dead_code)]
struct AggregateScoresFullRow {
    #[sqlx(default)]
    axis_origin_user_id: Option<i64>,
    #[sqlx(default)]
    axis_origin_channel: Option<String>,
    #[sqlx(default)]
    axis_target_model_version: Option<String>,
    #[sqlx(default)]
    axis_task_category: Option<i32>,
    #[sqlx(default)]
    axis_reflection_aspect: Option<i32>,
    #[sqlx(default)]
    axis_outcome: Option<i32>,
    #[sqlx(default)]
    axis_experiment_id: Option<String>,
    #[sqlx(default)]
    axis_experiment_variant: Option<String>,
    #[sqlx(default)]
    axis_time_bucket: Option<i64>,
    #[sqlx(default)]
    axis_summary_embedding_status: Option<i32>,
    #[sqlx(default)]
    axis_intent_embedding_status: Option<i32>,
    bucket_count: i64,
    score_avg: f64,
    score_min: f64,
    score_max: f64,
    score_p50: f64,
    score_p95: f64,
}

#[cfg(feature = "postgres")]
impl AggregateScoresFullRow {
    fn into_bucket(self, axes: &AxisColumns<'_>) -> ScoresAggregateBucket {
        let active = &axes.active;
        ScoresAggregateBucket {
            origin_user_id: active
                .contains(&ScoresAggregateAxis::OriginUserId)
                .then_some(self.axis_origin_user_id.unwrap_or(0)),
            origin_channel: if active.contains(&ScoresAggregateAxis::OriginChannel) {
                self.axis_origin_channel
            } else {
                None
            },
            target_model_version: if active.contains(&ScoresAggregateAxis::TargetModelVersion) {
                self.axis_target_model_version
            } else {
                None
            },
            task_category: active
                .contains(&ScoresAggregateAxis::TaskCategory)
                .then_some(self.axis_task_category.unwrap_or(0)),
            reflection_aspect: active
                .contains(&ScoresAggregateAxis::ReflectionAspect)
                .then_some(self.axis_reflection_aspect.unwrap_or(0)),
            outcome: active
                .contains(&ScoresAggregateAxis::Outcome)
                .then_some(self.axis_outcome.unwrap_or(0)),
            experiment_id: if active.contains(&ScoresAggregateAxis::ExperimentId) {
                self.axis_experiment_id
            } else {
                None
            },
            experiment_variant: if active.contains(&ScoresAggregateAxis::ExperimentVariant) {
                self.axis_experiment_variant
            } else {
                None
            },
            time_bucket_start: active
                .contains(&ScoresAggregateAxis::TimeBucket)
                .then_some(self.axis_time_bucket.unwrap_or(0)),
            summary_embedding_status: active
                .contains(&ScoresAggregateAxis::SummaryEmbeddingStatus)
                .then_some(self.axis_summary_embedding_status.unwrap_or(0)),
            intent_embedding_status: active
                .contains(&ScoresAggregateAxis::IntentEmbeddingStatus)
                .then_some(self.axis_intent_embedding_status.unwrap_or(0)),
            count: self.bucket_count,
            score_avg: self.score_avg,
            score_min: self.score_min,
            score_max: self.score_max,
            score_p50: self.score_p50,
            score_p95: self.score_p95,
        }
    }
}

#[cfg(not(feature = "postgres"))]
#[derive(sqlx::FromRow, Debug)]
#[allow(dead_code)]
struct AggregateScoresAggRow {
    #[sqlx(default)]
    axis_origin_user_id: Option<i64>,
    #[sqlx(default)]
    axis_origin_channel: Option<String>,
    #[sqlx(default)]
    axis_target_model_version: Option<String>,
    #[sqlx(default)]
    axis_task_category: Option<i32>,
    #[sqlx(default)]
    axis_reflection_aspect: Option<i32>,
    #[sqlx(default)]
    axis_outcome: Option<i32>,
    #[sqlx(default)]
    axis_experiment_id: Option<String>,
    #[sqlx(default)]
    axis_experiment_variant: Option<String>,
    #[sqlx(default)]
    axis_time_bucket: Option<i64>,
    #[sqlx(default)]
    axis_summary_embedding_status: Option<i32>,
    #[sqlx(default)]
    axis_intent_embedding_status: Option<i32>,
    bucket_count: i64,
    score_avg: f64,
    score_min: f64,
    score_max: f64,
}

#[cfg(not(feature = "postgres"))]
#[derive(sqlx::FromRow, Debug)]
#[allow(dead_code)]
struct AggregateScoresRawRow {
    #[sqlx(default)]
    axis_origin_user_id: Option<i64>,
    #[sqlx(default)]
    axis_origin_channel: Option<String>,
    #[sqlx(default)]
    axis_target_model_version: Option<String>,
    #[sqlx(default)]
    axis_task_category: Option<i32>,
    #[sqlx(default)]
    axis_reflection_aspect: Option<i32>,
    #[sqlx(default)]
    axis_outcome: Option<i32>,
    #[sqlx(default)]
    axis_experiment_id: Option<String>,
    #[sqlx(default)]
    axis_experiment_variant: Option<String>,
    #[sqlx(default)]
    axis_time_bucket: Option<i64>,
    #[sqlx(default)]
    axis_summary_embedding_status: Option<i32>,
    #[sqlx(default)]
    axis_intent_embedding_status: Option<i32>,
    score: f64,
}

#[cfg(not(feature = "postgres"))]
impl AggregateScoresAggRow {
    /// Same-group test: every axis key field must match the next raw
    /// row's corresponding field. Axes not in the request collapse to
    /// `None` on both sides (the SELECT projection omits them) and
    /// trivially match. SQL `ORDER BY` keeps the raw stream aligned
    /// with the agg stream's group order.
    fn same_group(&self, raw: &AggregateScoresRawRow) -> bool {
        self.axis_origin_user_id == raw.axis_origin_user_id
            && self.axis_origin_channel == raw.axis_origin_channel
            && self.axis_target_model_version == raw.axis_target_model_version
            && self.axis_task_category == raw.axis_task_category
            && self.axis_reflection_aspect == raw.axis_reflection_aspect
            && self.axis_outcome == raw.axis_outcome
            && self.axis_experiment_id == raw.axis_experiment_id
            && self.axis_experiment_variant == raw.axis_experiment_variant
            && self.axis_time_bucket == raw.axis_time_bucket
            && self.axis_summary_embedding_status == raw.axis_summary_embedding_status
            && self.axis_intent_embedding_status == raw.axis_intent_embedding_status
    }

    fn into_bucket(
        self,
        axes: &AxisColumns<'_>,
        score_p50: f64,
        score_p95: f64,
    ) -> ScoresAggregateBucket {
        let active = &axes.active;
        ScoresAggregateBucket {
            origin_user_id: active
                .contains(&ScoresAggregateAxis::OriginUserId)
                .then_some(self.axis_origin_user_id.unwrap_or(0)),
            origin_channel: if active.contains(&ScoresAggregateAxis::OriginChannel) {
                self.axis_origin_channel
            } else {
                None
            },
            target_model_version: if active.contains(&ScoresAggregateAxis::TargetModelVersion) {
                self.axis_target_model_version
            } else {
                None
            },
            task_category: active
                .contains(&ScoresAggregateAxis::TaskCategory)
                .then_some(self.axis_task_category.unwrap_or(0)),
            reflection_aspect: active
                .contains(&ScoresAggregateAxis::ReflectionAspect)
                .then_some(self.axis_reflection_aspect.unwrap_or(0)),
            outcome: active
                .contains(&ScoresAggregateAxis::Outcome)
                .then_some(self.axis_outcome.unwrap_or(0)),
            experiment_id: if active.contains(&ScoresAggregateAxis::ExperimentId) {
                self.axis_experiment_id
            } else {
                None
            },
            experiment_variant: if active.contains(&ScoresAggregateAxis::ExperimentVariant) {
                self.axis_experiment_variant
            } else {
                None
            },
            time_bucket_start: active
                .contains(&ScoresAggregateAxis::TimeBucket)
                .then_some(self.axis_time_bucket.unwrap_or(0)),
            summary_embedding_status: active
                .contains(&ScoresAggregateAxis::SummaryEmbeddingStatus)
                .then_some(self.axis_summary_embedding_status.unwrap_or(0)),
            intent_embedding_status: active
                .contains(&ScoresAggregateAxis::IntentEmbeddingStatus)
                .then_some(self.axis_intent_embedding_status.unwrap_or(0)),
            count: self.bucket_count,
            score_avg: self.score_avg,
            score_min: self.score_min,
            score_max: self.score_max,
            score_p50,
            score_p95,
        }
    }
}

pub struct ReflectionStatsRepositoryImpl {
    pool: &'static RdbPool,
}

impl ReflectionStatsRepositoryImpl {
    pub fn new(pool: &'static RdbPool) -> Self {
        Self { pool }
    }
}

impl UseRdbPool for ReflectionStatsRepositoryImpl {
    fn db_pool(&self) -> &RdbPool {
        self.pool
    }
}

impl ReflectionStatsRepository for ReflectionStatsRepositoryImpl {}

#[cfg(test)]
mod tests {
    //! F-A2 unit tests. Each test owns a unique `origin_user_id` so
    //! parallel runs (or leftover rows from a crashed earlier run)
    //! cannot pollute the aggregate. `fixture_sidecar` already
    //! populates an FK-satisfying memory row via `insert_index_tx`'s
    //! caller contract.

    use super::*;
    use crate::infra::reflection::rdb::{
        ThreadReflectionIndexRepository, ThreadReflectionIndexRepositoryImpl,
    };
    use crate::infra::reflection::rows::{
        ResolvedReflectionSearchFilter, ThreadReflectionIndexRow,
    };
    use crate::infra::reflection::test_support::{fixture_sidecar, setup_pool};
    use crate::sql::p;
    use infra_utils::infra::rdb::Rdb;
    use infra_utils::infra::test::TEST_RUNTIME;

    /// FK to `memory.id` exists via `insert_index_tx` invariants in
    /// the surrounding child-table tests. The aggregate path only
    /// reads `thread_reflection_index`, so we skip seeding `memory`
    /// here and rely on FK being deferred or the sidecar happily
    /// living with a synthetic memory_id under sqlite's lax FK.
    async fn ensure_sidecar_raw(pool: &'static RdbPool, row: &ThreadReflectionIndexRow) {
        let index_repo = ThreadReflectionIndexRepositoryImpl::new(pool);
        // Reuse the standard memory creator pattern from the child-
        // table tests: seed via fixture if a memory row is required.
        // For the aggregate, sqlite FK enforcement is off by default
        // in this project's pool config, so a synthetic memory_id is
        // accepted. Postgres tests would need a real memory row, but
        // the postgres CI job runs the higher-level app tests that
        // already cover the FK side; here we only assert SQL behaviour.
        let mut tx = pool.begin().await.expect("tx begin");
        index_repo
            .insert_index_tx(&mut *tx, row)
            .await
            .expect("insert sidecar");
        tx.commit().await.expect("tx commit");
    }

    async fn cleanup(pool: &RdbPool, memory_ids: &[i64]) {
        for id in memory_ids {
            let _ = sqlx::query::<Rdb>(concat!(
                "DELETE FROM thread_reflection_index WHERE memory_id = ",
                p!(1)
            ))
            .bind(id)
            .execute(pool)
            .await;
        }
    }

    #[test]
    fn run_aggregate_scores_no_axes_returns_totals_row() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            let user_id: i64 = 999_910_001;
            // Use memory_id values that are far from any concurrent
            // test's range; the test_support helper allocates ids by
            // its own convention.
            let ids: Vec<i64> = (0..4).map(|i| 9_910_001_000_i64 + i).collect();
            cleanup(pool, &ids).await;
            for (i, id) in ids.iter().enumerate() {
                let mut row = fixture_sidecar(*id);
                row.origin_user_id = user_id;
                row.score = 0.1 * (i as f64 + 1.0);
                ensure_sidecar_raw(pool, &row).await;
            }

            let repo = ReflectionStatsRepositoryImpl::new(pool);
            let filter = ResolvedReflectionSearchFilter {
                origin_user_id: Some(user_id),
                ..Default::default()
            };
            let buckets = repo
                .aggregate_scores(&filter, &[], None, MAX_GROUPS_FOR_INMEM_PERCENTILE)
                .await?;
            assert_eq!(buckets.len(), 1, "totals row only");
            assert_eq!(buckets[0].count, 4);
            assert!((buckets[0].score_avg - 0.25).abs() < 1e-5);
            // Axis Options must be `None` when no axis is grouped on.
            assert!(buckets[0].origin_user_id.is_none());
            assert!(buckets[0].time_bucket_start.is_none());

            cleanup(pool, &ids).await;
            Ok(())
        })
    }

    #[test]
    fn run_aggregate_scores_cap_exceeded_rejects() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            let user_id: i64 = 999_910_002;
            // Seed 5 rows over distinct task_category values so the
            // GROUP BY produces 5 buckets. cap=2 must fail.
            let ids: Vec<i64> = (0..5).map(|i| 9_910_002_000_i64 + i).collect();
            cleanup(pool, &ids).await;
            for (i, id) in ids.iter().enumerate() {
                let mut row = fixture_sidecar(*id);
                row.origin_user_id = user_id;
                // task_category is i32 so the distinct values produce
                // distinct buckets.
                row.task_category = (i as i32) + 100;
                ensure_sidecar_raw(pool, &row).await;
            }

            let repo = ReflectionStatsRepositoryImpl::new(pool);
            let filter = ResolvedReflectionSearchFilter {
                origin_user_id: Some(user_id),
                ..Default::default()
            };
            let err = repo
                .aggregate_scores(&filter, &[ScoresAggregateAxis::TaskCategory], None, 2)
                .await
                .expect_err("cap=2 with 5 distinct buckets must error");
            let msg = format!("{err}");
            assert!(msg.contains("exceeds cap"), "error names the cap: {msg}");

            cleanup(pool, &ids).await;
            Ok(())
        })
    }

    #[test]
    fn run_aggregate_scores_filter_binds_match_search_path() -> anyhow::Result<()> {
        // The aggregate reuses `build_filter_where`, so any filter
        // axis the search path accepts must also work here. Seed
        // rows split by `outcome` and verify that filtering by
        // outcome only picks the matching subset.
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            let user_id: i64 = 999_910_003;
            let ids: Vec<i64> = (0..4).map(|i| 9_910_003_000_i64 + i).collect();
            cleanup(pool, &ids).await;
            for (i, id) in ids.iter().enumerate() {
                let mut row = fixture_sidecar(*id);
                row.origin_user_id = user_id;
                row.outcome = if i < 2 { 1 } else { 2 };
                row.score = 0.5;
                ensure_sidecar_raw(pool, &row).await;
            }

            let repo = ReflectionStatsRepositoryImpl::new(pool);
            let filter = ResolvedReflectionSearchFilter {
                origin_user_id: Some(user_id),
                outcomes: vec![1],
                ..Default::default()
            };
            let buckets = repo
                .aggregate_scores(&filter, &[], None, MAX_GROUPS_FOR_INMEM_PERCENTILE)
                .await?;
            assert_eq!(buckets.len(), 1);
            assert_eq!(buckets[0].count, 2, "outcome=1 has two rows");

            cleanup(pool, &ids).await;
            Ok(())
        })
    }
}
