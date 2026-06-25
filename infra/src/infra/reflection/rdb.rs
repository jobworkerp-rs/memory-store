//! `thread_reflection_index` repository (sidecar table).
//!
//! Responsibilities:
//!   - INSERT a new row inside the finalize transaction.
//!   - Read by `memory_id`, by `thread_id` (active reflection / full
//!     history), and by an arbitrary list of `memory_id`s for hydration.
//!   - Patch the operational state columns (`pinned`,
//!     `*_embedding_status`, `*_embedding_error`) plus bump
//!     `updated_at`. Everything else is immutable per §3.7.
//!   - Delete by `memory_id` for cascade flows in `app::ReflectionApp`.
//!
//! This module intentionally stops short of building dynamic
//! filter SQL for `Search` / `Aggregate*`; those compose `WHERE`
//! clauses from `ReflectionSearchFilter` and live in
//! `app::reflection` so the infra layer stays free of proto
//! filter types. A small `count_by_thread` helper is exposed
//! because the active-reflection lookup (`tri_thread_created`
//! index) is the only multi-row read shape we need outside of
//! the dynamic search builder.

use super::rows::{ReflectionSortKey, ResolvedReflectionSearchFilter, ThreadReflectionIndexRow};
use crate::error::LlmMemoryError;
use crate::sql::{IN_LIST_CHUNK_SIZE, build_in_placeholders, dyn_placeholder, p};
use anyhow::Result;
use async_trait::async_trait;
use infra_utils::infra::rdb::{Rdb, RdbPool, UseRdbPool};
use sqlx::Executor;

/// Ordered SELECT column list shared by sidecar reads. Kept as a single
/// macro so additions stay synchronized between the `INSERT` SQL and the
/// `FromRow` mapping in `ThreadReflectionIndexRow`.
macro_rules! tri_columns {
    () => {
        "memory_id, thread_id, origin_thread_id, origin_user_id, origin_channel, \
         outcome, score, score_self, score_heuristic, task_category, reflection_aspect, \
         dataset_quality, summary_embedding_status, summary_embedding_error, \
         intent_embedding_status, intent_embedding_error, prompt_version, target_model_version, \
         experiment_id, experiment_variant, previous_reflection_id, pinned, is_recurrence, \
         mitigation_fingerprint, created_at, updated_at"
    };
}

const INSERT_SQL: &str = concat!(
    "INSERT INTO thread_reflection_index (memory_id, thread_id, origin_thread_id, ",
    "origin_user_id, origin_channel, outcome, score, score_self, score_heuristic, ",
    "task_category, reflection_aspect, dataset_quality, summary_embedding_status, ",
    "summary_embedding_error, intent_embedding_status, intent_embedding_error, ",
    "prompt_version, target_model_version, experiment_id, experiment_variant, ",
    "previous_reflection_id, pinned, is_recurrence, mitigation_fingerprint, ",
    "created_at, updated_at) VALUES (",
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
    ",",
    p!(7),
    ",",
    p!(8),
    ",",
    p!(9),
    ",",
    p!(10),
    ",",
    p!(11),
    ",",
    p!(12),
    ",",
    p!(13),
    ",",
    p!(14),
    ",",
    p!(15),
    ",",
    p!(16),
    ",",
    p!(17),
    ",",
    p!(18),
    ",",
    p!(19),
    ",",
    p!(20),
    ",",
    p!(21),
    ",",
    p!(22),
    ",",
    p!(23),
    ",",
    p!(24),
    ",",
    p!(25),
    ",",
    p!(26),
    ");"
);

const FIND_BY_MEMORY_ID_SQL: &str = concat!(
    "SELECT ",
    tri_columns!(),
    " FROM thread_reflection_index WHERE memory_id = ",
    p!(1),
    ";"
);

// `thread_id` here is the origin trajectory thread, not the aggregate
// container — spec §3.7.1 active-reflection / F-S2 history-by-thread
// both scope on origin_thread_id. The aggregate thread is reachable
// via the thread_memory junction.
const FIND_HISTORY_BY_THREAD_SQL: &str = concat!(
    "SELECT ",
    tri_columns!(),
    " FROM thread_reflection_index WHERE origin_thread_id = ",
    p!(1),
    " ORDER BY created_at DESC, memory_id DESC;"
);

// Powers `find_active_by_thread_id(require_intent_ok=false)` via the
// `tri_origin_thread_created` index. The strict path
// (`require_intent_ok=true`) still walks the full history in memory;
// adding intent_embedding_status to the compound index is tracked in
// spec §9.3.
const FIND_LATEST_BY_THREAD_SQL: &str = concat!(
    "SELECT ",
    tri_columns!(),
    " FROM thread_reflection_index WHERE origin_thread_id = ",
    p!(1),
    " ORDER BY created_at DESC, memory_id DESC LIMIT 1;"
);

const DELETE_BY_MEMORY_ID_SQL: &str = concat!(
    "DELETE FROM thread_reflection_index WHERE memory_id = ",
    p!(1),
    ";"
);

const UPDATE_PINNED_SQL: &str = concat!(
    "UPDATE thread_reflection_index SET pinned = ",
    p!(1),
    ", updated_at = ",
    p!(2),
    " WHERE memory_id = ",
    p!(3),
    ";"
);

const UPDATE_SUMMARY_EMBEDDING_STATUS_SQL: &str = concat!(
    "UPDATE thread_reflection_index SET summary_embedding_status = ",
    p!(1),
    ", summary_embedding_error = ",
    p!(2),
    ", updated_at = ",
    p!(3),
    " WHERE memory_id = ",
    p!(4),
    ";"
);

const UPDATE_INTENT_EMBEDDING_STATUS_SQL: &str = concat!(
    "UPDATE thread_reflection_index SET intent_embedding_status = ",
    p!(1),
    ", intent_embedding_error = ",
    p!(2),
    ", updated_at = ",
    p!(3),
    " WHERE memory_id = ",
    p!(4),
    ";"
);

const UPDATE_MITIGATION_FINGERPRINT_SQL: &str = concat!(
    "UPDATE thread_reflection_index SET mitigation_fingerprint = ",
    p!(1),
    ", updated_at = ",
    p!(2),
    " WHERE memory_id = ",
    p!(3),
    ";"
);

/// Selector for the embedding kind targeted by `update_*_status_tx`.
/// Mirrors `protobuf::llm_memory::data::EmbeddingKind` but kept as a
/// small infra-local enum so the RDB layer stays proto-free.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EmbeddingTrack {
    Summary,
    Intent,
}

/// Wire value of `protobuf::llm_memory::data::EmbeddingStatus::Ok` (= 2).
/// Used here so we do not depend on the proto enum from the infra layer.
pub const EMBEDDING_STATUS_OK_VALUE: i32 = 2;

#[async_trait]
pub trait ThreadReflectionIndexRepository: UseRdbPool + Send + Sync {
    /// Insert one sidecar row inside the caller's transaction.
    /// Spec §3.7: `created_at` and `updated_at` carry the same value
    /// at insert time; the latter is bumped via the dedicated
    /// patch helpers below for operational state changes.
    async fn insert_index_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        row: &ThreadReflectionIndexRow,
    ) -> Result<()> {
        sqlx::query::<Rdb>(INSERT_SQL)
            .bind(row.memory_id)
            .bind(row.thread_id)
            .bind(row.origin_thread_id)
            .bind(row.origin_user_id)
            .bind(&row.origin_channel)
            .bind(row.outcome)
            .bind(row.score)
            .bind(row.score_self)
            .bind(row.score_heuristic)
            .bind(row.task_category)
            .bind(row.reflection_aspect)
            .bind(row.dataset_quality)
            .bind(row.summary_embedding_status)
            .bind(&row.summary_embedding_error)
            .bind(row.intent_embedding_status)
            .bind(&row.intent_embedding_error)
            .bind(&row.prompt_version)
            .bind(&row.target_model_version)
            .bind(&row.experiment_id)
            .bind(&row.experiment_variant)
            .bind(row.previous_reflection_id)
            .bind(row.pinned)
            .bind(row.is_recurrence)
            .bind(&row.mitigation_fingerprint)
            .bind(row.created_at)
            .bind(row.updated_at)
            .execute(tx)
            .await
            .map(|_| ())
            .map_err(|e| LlmMemoryError::DBError(e).into())
    }

    async fn find_by_memory_id(&self, memory_id: i64) -> Result<Option<ThreadReflectionIndexRow>> {
        sqlx::query_as::<Rdb, ThreadReflectionIndexRow>(FIND_BY_MEMORY_ID_SQL)
            .bind(memory_id)
            .fetch_optional(self.db_pool())
            .await
            .map_err(|e| LlmMemoryError::DBError(e).into())
    }

    /// Bulk variant of `find_by_memory_id`. The order between input
    /// ids and returned rows is unspecified — callers (currently the
    /// `hydrate_rows` / `find_similar_by_vector` fan-out path) must
    /// hash-join by `row.memory_id`. Missing ids are silently absent.
    /// Chunked at `sql::IN_LIST_CHUNK_SIZE` to stay clear of the SQLite IN
    /// parameter limit (999), mirroring
    /// `MemoryRepository::find_by_ids`.
    async fn find_by_memory_ids(
        &self,
        memory_ids: &[i64],
    ) -> Result<Vec<ThreadReflectionIndexRow>> {
        if memory_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut out = Vec::with_capacity(memory_ids.len());
        for chunk in memory_ids.chunks(IN_LIST_CHUNK_SIZE) {
            let placeholders = build_in_placeholders(chunk.len(), 1);
            let sql = format!(
                "SELECT {cols} FROM thread_reflection_index \
                 WHERE memory_id IN ({placeholders});",
                cols = tri_columns!(),
            );
            let mut q = sqlx::query_as::<Rdb, ThreadReflectionIndexRow>(sqlx::AssertSqlSafe(sql));
            for id in chunk {
                q = q.bind(id);
            }
            let rows = q
                .fetch_all(self.db_pool())
                .await
                .map_err(LlmMemoryError::DBError)?;
            out.extend(rows);
        }
        Ok(out)
    }

    /// History for an *origin* trajectory thread, ordered (created_at
    /// desc, memory_id desc). Spec §3.7.1: the active reflection is
    /// the head of this list, optionally filtered by
    /// `intent_embedding_status = OK`. The aggregate (reflection-owner)
    /// thread is reachable via the `thread_memory` junction.
    async fn list_history_by_thread_id(
        &self,
        origin_thread_id: i64,
    ) -> Result<Vec<ThreadReflectionIndexRow>> {
        sqlx::query_as::<Rdb, ThreadReflectionIndexRow>(FIND_HISTORY_BY_THREAD_SQL)
            .bind(origin_thread_id)
            .fetch_all(self.db_pool())
            .await
            .map_err(|e| LlmMemoryError::DBError(e).into())
    }

    /// Spec §3.7.1: latest reflection by `created_at` for the given
    /// *origin* thread. When `require_intent_ok = false` the SQL
    /// returns the head row directly via `tri_origin_thread_created`
    /// + LIMIT 1. The strict path walks the full history in memory
    /// because no compound index covers `intent_embedding_status`
    /// (spec §9.3 future work). The in-memory scan is bounded by the
    /// per-thread reflection count, not the global table size.
    async fn find_active_by_thread_id(
        &self,
        origin_thread_id: i64,
        require_intent_ok: bool,
    ) -> Result<Option<ThreadReflectionIndexRow>> {
        if !require_intent_ok {
            return sqlx::query_as::<Rdb, ThreadReflectionIndexRow>(FIND_LATEST_BY_THREAD_SQL)
                .bind(origin_thread_id)
                .fetch_optional(self.db_pool())
                .await
                .map_err(|e| LlmMemoryError::DBError(e).into());
        }
        let history = self.list_history_by_thread_id(origin_thread_id).await?;
        Ok(history
            .into_iter()
            .find(|r| r.intent_embedding_status == EMBEDDING_STATUS_OK_VALUE))
    }

    async fn delete_by_memory_id_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        memory_id: i64,
    ) -> Result<bool> {
        sqlx::query::<Rdb>(DELETE_BY_MEMORY_ID_SQL)
            .bind(memory_id)
            .execute(tx)
            .await
            .map(|r| r.rows_affected() > 0)
            .map_err(|e| LlmMemoryError::DBError(e).into())
    }

    async fn update_pinned_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        memory_id: i64,
        pinned: bool,
        now_millis: i64,
    ) -> Result<bool> {
        sqlx::query::<Rdb>(UPDATE_PINNED_SQL)
            .bind(pinned)
            .bind(now_millis)
            .bind(memory_id)
            .execute(tx)
            .await
            .map(|r| r.rows_affected() > 0)
            .map_err(|e| LlmMemoryError::DBError(e).into())
    }

    /// F-F6 sidecar mirror. After
    /// `ReflectionAppliedTargetRepository::upsert_mitigation_tx`
    /// reports `applied=true`, the same tx must propagate the
    /// fingerprint here so search/read paths reflect the apply (spec
    /// §3.3.2 lists `mitigation_fingerprint` on the sidecar; §3.7
    /// requires `updated_at` to bump on `reflection_applied_target`
    /// writes). Returns whether the sidecar row was actually touched.
    async fn update_mitigation_fingerprint_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        memory_id: i64,
        fingerprint: &str,
        now_millis: i64,
    ) -> Result<bool> {
        sqlx::query::<Rdb>(UPDATE_MITIGATION_FINGERPRINT_SQL)
            .bind(fingerprint)
            .bind(now_millis)
            .bind(memory_id)
            .execute(tx)
            .await
            .map(|r| r.rows_affected() > 0)
            .map_err(|e| LlmMemoryError::DBError(e).into())
    }

    /// F-F8 path. `error` is `None` when transitioning to OK; otherwise
    /// it carries the failure reason. Both `PENDING -> *` and
    /// `OK <-> FAILED` transitions are idempotent at the DB level
    /// (UPDATE without conflict logic), per spec §4.1 confirming
    /// "PENDING→OK / FAILED→OK / OK→FAILED / FAILED→FAILED all allowed".
    async fn update_embedding_status_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        memory_id: i64,
        track: EmbeddingTrack,
        status: i32,
        error: Option<&str>,
        now_millis: i64,
    ) -> Result<bool> {
        let sql = match track {
            EmbeddingTrack::Summary => UPDATE_SUMMARY_EMBEDDING_STATUS_SQL,
            EmbeddingTrack::Intent => UPDATE_INTENT_EMBEDDING_STATUS_SQL,
        };
        sqlx::query::<Rdb>(sql)
            .bind(status)
            .bind(error)
            .bind(now_millis)
            .bind(memory_id)
            .execute(tx)
            .await
            .map(|r| r.rows_affected() > 0)
            .map_err(|e| LlmMemoryError::DBError(e).into())
    }

    /// Spec §3.7 / §4.1.2 F-G recurrence detection: returns `true` when
    /// at least one prior reflection in the window covers the same
    /// `(origin_user_id, task_category)` and shares any of the supplied
    /// `failure_modes`. Empty / all-unresolvable `failure_modes` is a
    /// guaranteed `false` (the SQL would otherwise reduce to an
    /// unconditional EXISTS scan).
    ///
    /// `failure_modes` arrives as proto `FailureMode` wire values
    /// (`Vec<i32>`); they are resolved to the short DB keys actually
    /// stored in `reflection_failure_mode.mode` before binding.
    /// `Unspecified` / out-of-range discriminants are dropped.
    async fn detect_recurrence(
        &self,
        origin_user_id: i64,
        task_category: i32,
        failure_modes: &[i32],
        window_millis: i64,
        now_millis: i64,
        exclude_memory_id: Option<i64>,
    ) -> Result<bool> {
        let mode_keys: Vec<&'static str> = failure_modes
            .iter()
            .filter_map(|v| super::failure_mode_convert::db_name_from_i32(*v))
            .collect();
        if mode_keys.is_empty() {
            return Ok(false);
        }
        let from_ts = now_millis - window_millis;
        // p! placeholders: [origin_user_id=1, task_category=2, from_ts=3, ...modes..., exclude?]
        let modes_sql = build_in_placeholders(mode_keys.len(), 4);
        let exclude_clause = if exclude_memory_id.is_some() {
            format!(
                " AND tri.memory_id <> {}",
                dyn_placeholder(4 + mode_keys.len())
            )
        } else {
            String::new()
        };
        let sql = format!(
            "SELECT EXISTS (\
               SELECT 1 \
                 FROM thread_reflection_index AS tri \
                 JOIN reflection_failure_mode AS rfm ON rfm.memory_id = tri.memory_id \
                WHERE tri.origin_user_id = {p1} \
                  AND tri.task_category = {p2} \
                  AND tri.created_at >= {p3} \
                  AND rfm.mode IN ({modes}){exclude}\
             ) AS hit",
            p1 = dyn_placeholder(1),
            p2 = dyn_placeholder(2),
            p3 = dyn_placeholder(3),
            modes = modes_sql,
            exclude = exclude_clause,
        );
        let mut q = sqlx::query_scalar::<Rdb, bool>(sqlx::AssertSqlSafe(sql))
            .bind(origin_user_id)
            .bind(task_category)
            .bind(from_ts);
        for m in &mode_keys {
            q = q.bind(*m);
        }
        if let Some(exclude) = exclude_memory_id {
            q = q.bind(exclude);
        }
        q.fetch_one(self.db_pool())
            .await
            .map_err(|e| LlmMemoryError::DBError(e).into())
    }

    /// Spec §4.2 search backbone. Builds a dynamic `WHERE` clause from
    /// `ResolvedReflectionSearchFilter` plus pagination/sort hints and
    /// returns matching sidecar rows. The app layer hydrates these into
    /// `ReflectionSearchResult` (joining the memory body and aggregate
    /// thread metadata).
    ///
    /// `failure_modes_all` / `tools_used_all` use correlated EXISTS for
    /// every requested code (ALL semantics); the `_any` variants reduce
    /// to a single EXISTS over an `IN (...)` list (ANY semantics).
    async fn search_index(
        &self,
        filter: &ResolvedReflectionSearchFilter,
        sort: ReflectionSortKey,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<ThreadReflectionIndexRow>> {
        let (where_sql, binds) = build_filter_where(filter, 1);
        let order_sql = render_sort_clause(sort);
        let limit_sql = format!(
            " LIMIT {} OFFSET {}",
            dyn_placeholder(binds.len() + 1),
            dyn_placeholder(binds.len() + 2),
        );
        let sql = format!(
            "SELECT {cols} FROM thread_reflection_index AS tri{where_sql}{order_sql}{limit_sql}",
            cols = tri_columns!(),
        );
        let mut q = sqlx::query_as::<Rdb, ThreadReflectionIndexRow>(sqlx::AssertSqlSafe(sql));
        for v in &binds {
            q = match v {
                FilterBind::I64(x) => q.bind(*x),
                FilterBind::I32(x) => q.bind(*x),
                FilterBind::F64(x) => q.bind(*x),
                FilterBind::Bool(x) => q.bind(*x),
                FilterBind::Str(s) => q.bind(s.as_str()),
            };
        }
        q = q.bind(limit).bind(offset);
        q.fetch_all(self.db_pool())
            .await
            .map_err(|e| LlmMemoryError::DBError(e).into())
    }

    /// Companion to `search_index` exposing the same filter for paged
    /// totals (`Search` returns total count alongside the page).
    async fn count_by_filter(&self, filter: &ResolvedReflectionSearchFilter) -> Result<i64> {
        let (where_sql, binds) = build_filter_where(filter, 1);
        let sql = format!("SELECT COUNT(*) FROM thread_reflection_index AS tri{where_sql}");
        let mut q = sqlx::query_scalar::<Rdb, i64>(sqlx::AssertSqlSafe(sql));
        for v in &binds {
            q = match v {
                FilterBind::I64(x) => q.bind(*x),
                FilterBind::I32(x) => q.bind(*x),
                FilterBind::F64(x) => q.bind(*x),
                FilterBind::Bool(x) => q.bind(*x),
                FilterBind::Str(s) => q.bind(s.as_str()),
            };
        }
        q.fetch_one(self.db_pool())
            .await
            .map_err(|e| LlmMemoryError::DBError(e).into())
    }
}

/// Bind value for the dynamic filter SQL. Owning the data here lets us
/// keep the bind sequence in lockstep with the placeholder layout that
/// `build_filter_where` emitted.
#[derive(Debug, Clone)]
pub(super) enum FilterBind {
    I64(i64),
    I32(i32),
    F64(f64),
    Bool(bool),
    Str(String),
}

fn render_sort_clause(sort: ReflectionSortKey) -> &'static str {
    match sort {
        ReflectionSortKey::CreatedAtDesc => " ORDER BY tri.created_at DESC, tri.memory_id DESC",
        ReflectionSortKey::CreatedAtAsc => " ORDER BY tri.created_at ASC, tri.memory_id ASC",
        ReflectionSortKey::ScoreDesc => " ORDER BY tri.score DESC, tri.memory_id DESC",
        ReflectionSortKey::ScoreAsc => " ORDER BY tri.score ASC, tri.memory_id ASC",
        ReflectionSortKey::MemoryIdDesc => " ORDER BY tri.memory_id DESC",
    }
}

/// Build the dynamic `WHERE ...` fragment from a resolved filter. `start`
/// is the next placeholder index ($N for postgres, ignored for sqlite).
/// Returns the rendered clause (including a leading space and the
/// `WHERE` keyword) plus the bind values in the same order placeholders
/// appear in the SQL.
// The trailing `pos += 1` in the last branch is a deliberate
// invariant ("pos always trails the next placeholder") so that adding
// a new filter axis below stays drop-in.
#[allow(unused_assignments)]
pub(super) fn build_filter_where(
    filter: &ResolvedReflectionSearchFilter,
    start: usize,
) -> (String, Vec<FilterBind>) {
    let mut clauses: Vec<String> = Vec::new();
    let mut binds: Vec<FilterBind> = Vec::new();
    let mut pos = start;

    macro_rules! push_pred {
        ($col:expr, $op:expr, $bind:expr) => {{
            clauses.push(format!("{} {} {}", $col, $op, dyn_placeholder(pos)));
            binds.push($bind);
            pos += 1;
        }};
    }

    if let Some(v) = filter.origin_user_id {
        push_pred!("tri.origin_user_id", "=", FilterBind::I64(v));
    }
    if let Some(ref v) = filter.origin_channel {
        push_pred!("tri.origin_channel", "=", FilterBind::Str(v.clone()));
    }
    if let Some(v) = filter.origin_thread_id {
        push_pred!("tri.origin_thread_id", "=", FilterBind::I64(v));
    }
    if !filter.outcomes.is_empty() {
        let placeholders = build_in_placeholders(filter.outcomes.len(), pos);
        clauses.push(format!("tri.outcome IN ({placeholders})"));
        for v in &filter.outcomes {
            binds.push(FilterBind::I32(*v));
            pos += 1;
        }
    }
    if let Some(v) = filter.score_min {
        push_pred!("tri.score", ">=", FilterBind::F64(v));
    }
    if let Some(v) = filter.score_max {
        push_pred!("tri.score", "<=", FilterBind::F64(v));
    }
    if !filter.task_categories.is_empty() {
        let placeholders = build_in_placeholders(filter.task_categories.len(), pos);
        clauses.push(format!("tri.task_category IN ({placeholders})"));
        for v in &filter.task_categories {
            binds.push(FilterBind::I32(*v));
            pos += 1;
        }
    }
    if let Some(v) = filter.reflection_aspect {
        push_pred!("tri.reflection_aspect", "=", FilterBind::I32(v));
    }
    if let Some(ref v) = filter.prompt_version {
        push_pred!("tri.prompt_version", "=", FilterBind::Str(v.clone()));
    }
    if let Some(ref v) = filter.target_model_version {
        push_pred!("tri.target_model_version", "=", FilterBind::Str(v.clone()));
    }
    if let Some(ref v) = filter.experiment_id {
        push_pred!("tri.experiment_id", "=", FilterBind::Str(v.clone()));
    }
    if let Some(ref v) = filter.experiment_variant {
        push_pred!("tri.experiment_variant", "=", FilterBind::Str(v.clone()));
    }
    if let Some(v) = filter.pinned {
        push_pred!("tri.pinned", "=", FilterBind::Bool(v));
    }
    if let Some(v) = filter.dataset_quality {
        push_pred!("tri.dataset_quality", "=", FilterBind::I32(v));
    }
    if let Some(v) = filter.summary_embedding_status {
        push_pred!("tri.summary_embedding_status", "=", FilterBind::I32(v));
    }
    if let Some(v) = filter.intent_embedding_status {
        push_pred!("tri.intent_embedding_status", "=", FilterBind::I32(v));
    }
    if let Some(v) = filter.created_after {
        push_pred!("tri.created_at", ">=", FilterBind::I64(v));
    }
    if let Some(v) = filter.created_before {
        push_pred!("tri.created_at", "<=", FilterBind::I64(v));
    }
    if let Some(v) = filter.is_recurrence {
        push_pred!("tri.is_recurrence", "=", FilterBind::Bool(v));
    }
    if let Some(v) = filter.memory_id_lt {
        push_pred!("tri.memory_id", "<", FilterBind::I64(v));
    }

    // ALL semantics: every mode must be present (one EXISTS per mode).
    for mode in &filter.failure_modes_all {
        clauses.push(format!(
            "EXISTS (SELECT 1 FROM reflection_failure_mode rfm \
             WHERE rfm.memory_id = tri.memory_id AND rfm.mode = {})",
            dyn_placeholder(pos),
        ));
        binds.push(FilterBind::Str(mode.clone()));
        pos += 1;
    }
    if !filter.failure_modes_any.is_empty() {
        let placeholders = build_in_placeholders(filter.failure_modes_any.len(), pos);
        clauses.push(format!(
            "EXISTS (SELECT 1 FROM reflection_failure_mode rfm \
             WHERE rfm.memory_id = tri.memory_id AND rfm.mode IN ({placeholders}))"
        ));
        for v in &filter.failure_modes_any {
            binds.push(FilterBind::Str(v.clone()));
            pos += 1;
        }
    }
    for tool in &filter.tools_used_all {
        clauses.push(format!(
            "EXISTS (SELECT 1 FROM reflection_tool rt \
             WHERE rt.memory_id = tri.memory_id AND rt.tool = {})",
            dyn_placeholder(pos),
        ));
        binds.push(FilterBind::Str(tool.clone()));
        pos += 1;
    }
    if !filter.tools_used_any.is_empty() {
        let placeholders = build_in_placeholders(filter.tools_used_any.len(), pos);
        clauses.push(format!(
            "EXISTS (SELECT 1 FROM reflection_tool rt \
             WHERE rt.memory_id = tri.memory_id AND rt.tool IN ({placeholders}))"
        ));
        for v in &filter.tools_used_any {
            binds.push(FilterBind::Str(v.clone()));
            pos += 1;
        }
    }

    if clauses.is_empty() {
        (String::new(), binds)
    } else {
        (format!(" WHERE {}", clauses.join(" AND ")), binds)
    }
}

/// Zero-config impl. Holds only the pool and id generator handles via
/// `UseRdbPool`. Threading IdGenerator here is unnecessary because
/// `memory_id` (the PK of this sidecar) is generated by the parent
/// `MemoryRepository` before insert.
pub struct ThreadReflectionIndexRepositoryImpl {
    pool: &'static RdbPool,
}

impl ThreadReflectionIndexRepositoryImpl {
    pub fn new(pool: &'static RdbPool) -> Self {
        Self { pool }
    }
}

impl UseRdbPool for ThreadReflectionIndexRepositoryImpl {
    fn db_pool(&self) -> &RdbPool {
        self.pool
    }
}

impl ThreadReflectionIndexRepository for ThreadReflectionIndexRepositoryImpl {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infra::reflection::aggregate_thread::{
        ThreadAggregateKeyRepository, ThreadAggregateKeyRepositoryImpl,
    };
    use crate::infra::reflection::failure_mode::{
        ReflectionFailureModeRepository, ReflectionFailureModeRepositoryImpl,
    };
    use crate::infra::reflection::stats::{
        ReflectionStatsRepository, ReflectionStatsRepositoryImpl,
    };
    use crate::infra::reflection::test_support::setup_pool;

    fn fixture_row(
        memory_id: i64,
        thread_id: i64,
        origin_user_id: i64,
    ) -> ThreadReflectionIndexRow {
        // The infra-level tests don't differentiate aggregate vs origin
        // threads (they exercise SQL shape, not Phase D semantics), so
        // both columns share the same fixture value.
        ThreadReflectionIndexRow {
            memory_id,
            thread_id,
            origin_thread_id: thread_id,
            origin_user_id,
            origin_channel: Some("test_channel".to_string()),
            outcome: 1, // SUCCESS
            score: 0.85,
            score_self: 0.9,
            score_heuristic: 0.8,
            task_category: 1,            // CODING
            reflection_aspect: 1,        // TASK_OUTCOME
            dataset_quality: 1,          // AUTO
            summary_embedding_status: 1, // PENDING
            summary_embedding_error: None,
            intent_embedding_status: 1, // PENDING
            intent_embedding_error: None,
            prompt_version: "20260510".to_string(),
            target_model_version: Some("claude-opus-4-7".to_string()),
            experiment_id: None,
            experiment_variant: None,
            previous_reflection_id: None,
            pinned: false,
            is_recurrence: false,
            mitigation_fingerprint: None,
            created_at: 1_700_000_000_000,
            updated_at: 1_700_000_000_000,
        }
    }

    async fn cleanup(pool: &RdbPool, memory_ids: &[i64]) {
        for id in memory_ids {
            // sidecar + child tables; ignore errors so tests stay
            // resilient to leftover state from previous runs.
            let _ = sqlx::query::<Rdb>(concat!(
                "DELETE FROM thread_reflection_index WHERE memory_id = ",
                p!(1)
            ))
            .bind(id)
            .execute(pool)
            .await;
            let _ = sqlx::query::<Rdb>(concat!(
                "DELETE FROM reflection_failure_mode WHERE memory_id = ",
                p!(1)
            ))
            .bind(id)
            .execute(pool)
            .await;
        }
    }

    #[test]
    fn run_index_crud_roundtrip() -> anyhow::Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            let memory_id = 9_991_001;
            cleanup(pool, &[memory_id]).await;

            let repo = ThreadReflectionIndexRepositoryImpl::new(pool);

            // Insert: take a tx, commit it.
            let mut tx = pool.begin().await?;
            repo.insert_index_tx(&mut *tx, &fixture_row(memory_id, 1, 1))
                .await?;
            tx.commit().await?;

            // Read back.
            let row = repo
                .find_by_memory_id(memory_id)
                .await?
                .expect("row should exist");
            assert_eq!(row.memory_id, memory_id);
            assert_eq!(row.thread_id, 1);
            assert_eq!(row.outcome, 1);
            assert!((row.score - 0.85).abs() < 1e-6);
            assert_eq!(row.summary_embedding_status, 1);

            // Update pinned.
            let mut tx = pool.begin().await?;
            let updated = repo
                .update_pinned_tx(&mut *tx, memory_id, true, 1_700_000_001_000)
                .await?;
            tx.commit().await?;
            assert!(updated);
            let row = repo.find_by_memory_id(memory_id).await?.unwrap();
            assert!(row.pinned);
            assert_eq!(row.updated_at, 1_700_000_001_000);

            // Update embedding status (summary -> OK).
            let mut tx = pool.begin().await?;
            repo.update_embedding_status_tx(
                &mut *tx,
                memory_id,
                EmbeddingTrack::Summary,
                2, // OK
                None,
                1_700_000_002_000,
            )
            .await?;
            tx.commit().await?;
            let row = repo.find_by_memory_id(memory_id).await?.unwrap();
            assert_eq!(row.summary_embedding_status, 2);
            assert_eq!(row.summary_embedding_error, None);

            // Embedding status FAILED with error reason.
            let mut tx = pool.begin().await?;
            repo.update_embedding_status_tx(
                &mut *tx,
                memory_id,
                EmbeddingTrack::Intent,
                3, // FAILED
                Some("test_failure"),
                1_700_000_003_000,
            )
            .await?;
            tx.commit().await?;
            let row = repo.find_by_memory_id(memory_id).await?.unwrap();
            assert_eq!(row.intent_embedding_status, 3);
            assert_eq!(row.intent_embedding_error.as_deref(), Some("test_failure"));

            // Active reflection lookup.
            let active = repo
                .find_active_by_thread_id(1, false)
                .await?
                .expect("active reflection lookup");
            assert_eq!(active.memory_id, memory_id);
            // require_intent_ok=true must filter out FAILED status.
            let strict = repo.find_active_by_thread_id(1, true).await?;
            assert!(
                strict.is_none(),
                "intent_status=FAILED must not pass strict filter"
            );

            // Cleanup.
            let mut tx = pool.begin().await?;
            let deleted = repo.delete_by_memory_id_tx(&mut *tx, memory_id).await?;
            tx.commit().await?;
            assert!(deleted);
            assert!(repo.find_by_memory_id(memory_id).await?.is_none());

            cleanup(pool, &[memory_id]).await;
            Ok(())
        })
    }

    /// Aggregate-thread idempotency: two parallel tasks racing on the
    /// same (user_id, labels_hash) must end up with one row, and the
    /// loser must surface a UniqueViolation rather than corrupting state.
    #[test]
    fn run_aggregate_thread_unique_collision() -> anyhow::Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            let user_id: i64 = 999_002;
            let labels_hash = "b".repeat(64);

            // Cleanup any leftover from previous failed runs.
            let _ = sqlx::query::<Rdb>(concat!(
                "DELETE FROM thread_aggregate_key WHERE user_id = ",
                p!(1),
                " AND labels_hash = ",
                p!(2)
            ))
            .bind(user_id)
            .bind(&labels_hash)
            .execute(pool)
            .await;

            let repo = ThreadAggregateKeyRepositoryImpl::new(pool);

            // First insert wins.
            let mut tx = pool.begin().await?;
            repo.insert_tx(&mut *tx, user_id, &labels_hash, 100, 1_700_000_000_000)
                .await?;
            tx.commit().await?;

            // Second insert with a different thread_id collides.
            let mut tx = pool.begin().await?;
            let race = repo
                .insert_tx(&mut *tx, user_id, &labels_hash, 200, 1_700_000_001_000)
                .await;
            // Roll back regardless so the test database stays clean.
            let _ = tx.rollback().await;
            assert!(
                race.is_err(),
                "duplicate (user_id, labels_hash) must collide"
            );

            // The original mapping survives.
            let existing = repo.find(user_id, &labels_hash).await?.unwrap();
            assert_eq!(existing.thread_id, 100);

            // Cleanup.
            let mut tx = pool.begin().await?;
            let _ = repo.delete_tx(&mut *tx, user_id, &labels_hash).await?;
            tx.commit().await?;
            Ok(())
        })
    }

    /// failure_mode insert is idempotent under PK conflict.
    #[test]
    fn run_failure_mode_idempotent_insert() -> anyhow::Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            let memory_id = 9_991_011;
            let _ = sqlx::query::<Rdb>(concat!(
                "DELETE FROM reflection_failure_mode WHERE memory_id = ",
                p!(1)
            ))
            .bind(memory_id)
            .execute(pool)
            .await;

            let repo = ReflectionFailureModeRepositoryImpl::new(pool);

            let mut tx = pool.begin().await?;
            repo.insert_mode_tx(&mut *tx, memory_id, "tool_misuse")
                .await?;
            // Same (memory_id, mode) - must not error.
            repo.insert_mode_tx(&mut *tx, memory_id, "tool_misuse")
                .await?;
            // Different mode - separate row.
            repo.insert_mode_tx(&mut *tx, memory_id, "loop").await?;
            tx.commit().await?;

            let modes = repo.list_by_memory_id(memory_id).await?;
            assert_eq!(modes.len(), 2);

            let mut tx = pool.begin().await?;
            repo.delete_by_memory_id_tx(&mut *tx, memory_id).await?;
            tx.commit().await?;
            Ok(())
        })
    }

    /// Stats upsert increments on conflict.
    #[test]
    fn run_stats_upsert_increment() -> anyhow::Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            let user_id: i64 = 999_003;
            // Cleanup
            let _ = sqlx::query::<Rdb>(concat!(
                "DELETE FROM tool_outcome_stats WHERE origin_user_id = ",
                p!(1)
            ))
            .bind(user_id)
            .execute(pool)
            .await;

            let repo = ReflectionStatsRepositoryImpl::new(pool);
            let mut tx = pool.begin().await?;
            repo.upsert_tool_outcome_tx(&mut *tx, user_id, "Read", 1, 1, 1_700_000_000_000)
                .await?;
            repo.upsert_tool_outcome_tx(&mut *tx, user_id, "Read", 1, 1, 1_700_000_001_000)
                .await?;
            repo.upsert_tool_outcome_tx(&mut *tx, user_id, "Read", 3, 2, 1_700_000_002_000)
                .await?;
            tx.commit().await?;

            let stats = repo.list_tool_outcome_stats(user_id, "Read").await?;
            assert_eq!(stats.len(), 2);
            // outcome=1 (SUCCESS) should be at count=2 after two inserts.
            let success = stats.iter().find(|s| s.outcome == 1).unwrap();
            assert_eq!(success.count, 2);
            // outcome=3 (FAILURE) at count=2 after one insert with delta=2.
            let failure = stats.iter().find(|s| s.outcome == 3).unwrap();
            assert_eq!(failure.count, 2);

            let _ = sqlx::query::<Rdb>(concat!(
                "DELETE FROM tool_outcome_stats WHERE origin_user_id = ",
                p!(1)
            ))
            .bind(user_id)
            .execute(pool)
            .await;
            Ok(())
        })
    }

    /// reflection_tool_outcome must keep separate rows when a
    /// (tool, contribution) pair is reported with two different
    /// error_kind values, and must collapse identical
    /// (memory_id, tool, contribution, error_kind) tuples.
    #[test]
    fn run_tool_outcome_distinguishes_error_kind() -> anyhow::Result<()> {
        use crate::infra::reflection::tool_outcome::{
            ReflectionToolOutcomeRepository, ReflectionToolOutcomeRepositoryImpl,
        };
        use infra_utils::infra::test::TEST_RUNTIME;

        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            let memory_id = 9_991_500_i64;
            let _ = sqlx::query::<Rdb>(concat!(
                "DELETE FROM reflection_tool_outcome WHERE memory_id = ",
                p!(1)
            ))
            .bind(memory_id)
            .execute(pool)
            .await;

            let repo = ReflectionToolOutcomeRepositoryImpl::new(pool);
            let mut tx = pool.begin().await?;
            // Same (tool, contribution=NEGATIVE) but two error kinds.
            repo.insert_outcome_tx(&mut *tx, memory_id, "Bash", 2, Some("permission_denied"))
                .await?;
            repo.insert_outcome_tx(&mut *tx, memory_id, "Bash", 2, Some("rate_limit"))
                .await?;
            // Identical row: must collapse via ON CONFLICT DO NOTHING.
            repo.insert_outcome_tx(&mut *tx, memory_id, "Bash", 2, Some("permission_denied"))
                .await?;
            // None / Some("") are aliases on the PK ('' substitutes for NULL).
            repo.insert_outcome_tx(&mut *tx, memory_id, "Bash", 2, None)
                .await?;
            repo.insert_outcome_tx(&mut *tx, memory_id, "Bash", 2, Some(""))
                .await?;
            tx.commit().await?;

            let rows = repo.list_by_memory_id(memory_id).await?;
            assert_eq!(
                rows.len(),
                3,
                "expected three distinct error_kind rows ('permission_denied', 'rate_limit', ''), got {rows:?}"
            );
            let kinds: std::collections::BTreeSet<&str> =
                rows.iter().map(|r| r.error_kind.as_str()).collect();
            assert!(kinds.contains("permission_denied"));
            assert!(kinds.contains("rate_limit"));
            assert!(kinds.contains(""), "blank error_kind row must be present");

            let mut tx = pool.begin().await?;
            repo.delete_by_memory_id_tx(&mut *tx, memory_id).await?;
            tx.commit().await?;
            Ok(())
        })
    }

    /// `rebuild_tool_outcome_stats(Some(uid))` must only touch the
    /// requested user's rows and must commit the DELETE+INSERT
    /// pair atomically.
    #[test]
    fn run_rebuild_tool_outcome_stats_scope_isolation() -> anyhow::Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            let target_user: i64 = 999_201;
            let other_user: i64 = 999_202;

            // Pre-clean any leftover from a previous run.
            for uid in [target_user, other_user] {
                let _ = sqlx::query::<Rdb>(concat!(
                    "DELETE FROM tool_outcome_stats WHERE origin_user_id = ",
                    p!(1)
                ))
                .bind(uid)
                .execute(pool)
                .await;
            }

            let repo = ReflectionStatsRepositoryImpl::new(pool);

            // Seed both users with one row each so we can detect leakage.
            let mut tx = pool.begin().await?;
            repo.upsert_tool_outcome_tx(&mut *tx, target_user, "Read", 1, 5, 1_700_000_000_000)
                .await?;
            repo.upsert_tool_outcome_tx(&mut *tx, other_user, "Write", 1, 7, 1_700_000_000_000)
                .await?;
            tx.commit().await?;

            // Scoped rebuild on target_user only. There are no
            // reflections in `thread_reflection_index` for either
            // user, so the INSERT-SELECT yields zero rows; the
            // assertion is that target_user's row goes away while
            // other_user's row is preserved.
            repo.rebuild_tool_outcome_stats(Some(target_user)).await?;

            let target_after = repo.list_tool_outcome_stats(target_user, "Read").await?;
            assert!(
                target_after.is_empty(),
                "scoped rebuild must drop target_user rows that have no backing reflections"
            );
            let other_after = repo.list_tool_outcome_stats(other_user, "Write").await?;
            assert_eq!(
                other_after.len(),
                1,
                "scoped rebuild must NOT touch unrelated users"
            );
            assert_eq!(other_after[0].count, 7);

            // Cleanup.
            for uid in [target_user, other_user] {
                let _ = sqlx::query::<Rdb>(concat!(
                    "DELETE FROM tool_outcome_stats WHERE origin_user_id = ",
                    p!(1)
                ))
                .bind(uid)
                .execute(pool)
                .await;
            }
            Ok(())
        })
    }

    /// Helper used by the dynamic-filter tests below: seeds one
    /// `thread_reflection_index` row plus optional failure_mode / tool
    /// rows so the EXISTS subqueries have something to match.
    #[allow(clippy::too_many_arguments)]
    async fn seed_index_with_children(
        pool: &'static RdbPool,
        memory_id: i64,
        thread_id: i64,
        origin_user_id: i64,
        outcome: i32,
        task_category: i32,
        modes: &[&str],
        tools: &[&str],
    ) -> anyhow::Result<()> {
        let mut row = fixture_row(memory_id, thread_id, origin_user_id);
        row.outcome = outcome;
        row.task_category = task_category;

        let repo = ThreadReflectionIndexRepositoryImpl::new(pool);
        let mut tx = pool.begin().await?;
        repo.insert_index_tx(&mut *tx, &row).await?;
        let fm = ReflectionFailureModeRepositoryImpl::new(pool);
        for m in modes {
            fm.insert_mode_tx(&mut *tx, memory_id, m).await?;
        }
        let tr = crate::infra::reflection::tool::ReflectionToolRepositoryImpl::new(pool);
        for t in tools {
            crate::infra::reflection::tool::ReflectionToolRepository::insert_tool_tx(
                &tr, &mut *tx, memory_id, t,
            )
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    async fn cleanup_index_and_children(pool: &RdbPool, memory_ids: &[i64]) {
        for id in memory_ids {
            let _ = sqlx::query::<Rdb>(concat!(
                "DELETE FROM reflection_failure_mode WHERE memory_id = ",
                p!(1)
            ))
            .bind(id)
            .execute(pool)
            .await;
            let _ = sqlx::query::<Rdb>(concat!(
                "DELETE FROM reflection_tool WHERE memory_id = ",
                p!(1)
            ))
            .bind(id)
            .execute(pool)
            .await;
            let _ = sqlx::query::<Rdb>(concat!(
                "DELETE FROM thread_reflection_index WHERE memory_id = ",
                p!(1)
            ))
            .bind(id)
            .execute(pool)
            .await;
        }
    }

    /// `detect_recurrence` returns `true` only when a prior reflection in
    /// the configured window matches both `(origin_user_id, task_category)`
    /// and at least one of the supplied failure modes.
    #[test]
    fn run_detect_recurrence_window_and_modes() -> anyhow::Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            let user_id: i64 = 999_301;
            let mid_recent = 9_993_001_i64;
            let mid_old = 9_993_002_i64;
            cleanup_index_and_children(pool, &[mid_recent, mid_old]).await;

            let now: i64 = 1_700_000_000_000;
            let day_ms: i64 = 86_400_000;

            // Recent: 5 days ago, mode = "tool_misuse".
            seed_index_with_children(
                pool,
                mid_recent,
                10_001,
                user_id,
                /* outcome */ 1,
                /* task_category */ 1,
                &["tool_misuse"],
                &[],
            )
            .await?;
            // Override created_at on the recent row to 5 days ago so the
            // window check is meaningful (the helper used the fixture
            // timestamp by default).
            let _ = sqlx::query::<Rdb>(concat!(
                "UPDATE thread_reflection_index SET created_at = ",
                p!(1),
                ", updated_at = ",
                p!(2),
                " WHERE memory_id = ",
                p!(3)
            ))
            .bind(now - 5 * day_ms)
            .bind(now - 5 * day_ms)
            .bind(mid_recent)
            .execute(pool)
            .await?;

            // Old: 60 days ago, mode = "loop".
            seed_index_with_children(pool, mid_old, 10_002, user_id, 1, 1, &["loop"], &[]).await?;
            let _ = sqlx::query::<Rdb>(concat!(
                "UPDATE thread_reflection_index SET created_at = ",
                p!(1),
                ", updated_at = ",
                p!(2),
                " WHERE memory_id = ",
                p!(3)
            ))
            .bind(now - 60 * day_ms)
            .bind(now - 60 * day_ms)
            .bind(mid_old)
            .execute(pool)
            .await?;

            let repo = ThreadReflectionIndexRepositoryImpl::new(pool);

            use protobuf::llm_memory::data::FailureMode;
            let tool_misuse = FailureMode::ToolMisuse as i32;
            let loop_mode = FailureMode::Loop as i32;
            // A controlled-vocabulary mode that is never seeded here, so
            // it stands in for the old "unrelated" free-text miss case.
            let unseeded = FailureMode::ScopeDrift as i32;

            // Mode hit, in-window: must return true.
            let hit_recent = repo
                .detect_recurrence(user_id, 1, &[tool_misuse], 30 * day_ms, now, None)
                .await?;
            assert!(hit_recent, "5-day-old tool_misuse must match");

            // Mode hit, but row is older than the window: must return false.
            let miss_old = repo
                .detect_recurrence(user_id, 1, &[loop_mode], 30 * day_ms, now, None)
                .await?;
            assert!(!miss_old, "60-day-old loop must fall outside 30d window");

            // ANY semantics: if any of the supplied modes hits, return true.
            let any_hit = repo
                .detect_recurrence(user_id, 1, &[unseeded, tool_misuse], 30 * day_ms, now, None)
                .await?;
            assert!(any_hit);

            // Different task_category must miss.
            let category_miss = repo
                .detect_recurrence(user_id, 99, &[tool_misuse], 30 * day_ms, now, None)
                .await?;
            assert!(!category_miss);

            // exclude_memory_id removes the only candidate.
            let self_excluded = repo
                .detect_recurrence(
                    user_id,
                    1,
                    &[tool_misuse],
                    30 * day_ms,
                    now,
                    Some(mid_recent),
                )
                .await?;
            assert!(!self_excluded);

            // Empty failure_modes is always false (avoids unconstrained
            // EXISTS scan).
            let empty = repo
                .detect_recurrence(user_id, 1, &[], 30 * day_ms, now, None)
                .await?;
            assert!(!empty);

            cleanup_index_and_children(pool, &[mid_recent, mid_old]).await;
            Ok(())
        })
    }

    /// `search_index` honours the dynamic AND/IN/EXISTS predicates and
    /// `count_by_filter` returns the same total.
    #[test]
    fn run_search_index_dynamic_filters() -> anyhow::Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            let user_id: i64 = 999_401;
            let other_user: i64 = 999_402;
            let mids: [i64; 4] = [9_994_001, 9_994_002, 9_994_003, 9_994_004];
            cleanup_index_and_children(pool, &mids).await;

            // mids[0]: outcome=SUCCESS, modes=[loop, tool_misuse], tools=[Read]
            seed_index_with_children(
                pool,
                mids[0],
                20_001,
                user_id,
                1,
                1,
                &["loop", "tool_misuse"],
                &["Read"],
            )
            .await?;
            // mids[1]: outcome=FAILURE, modes=[tool_misuse], tools=[Bash]
            seed_index_with_children(
                pool,
                mids[1],
                20_002,
                user_id,
                3,
                1,
                &["tool_misuse"],
                &["Bash"],
            )
            .await?;
            // mids[2]: outcome=SUCCESS, modes=[loop], tools=[Read]
            seed_index_with_children(pool, mids[2], 20_003, user_id, 1, 1, &["loop"], &["Read"])
                .await?;
            // mids[3]: different user; must never appear.
            seed_index_with_children(
                pool,
                mids[3],
                20_004,
                other_user,
                1,
                1,
                &["loop"],
                &["Read"],
            )
            .await?;

            let repo = ThreadReflectionIndexRepositoryImpl::new(pool);

            // origin_user_id only.
            let f = ResolvedReflectionSearchFilter {
                origin_user_id: Some(user_id),
                ..Default::default()
            };
            let rows = repo
                .search_index(&f, ReflectionSortKey::CreatedAtDesc, 100, 0)
                .await?;
            assert_eq!(rows.len(), 3);
            assert_eq!(repo.count_by_filter(&f).await?, 3);

            // outcomes IN (SUCCESS).
            let f = ResolvedReflectionSearchFilter {
                origin_user_id: Some(user_id),
                outcomes: vec![1],
                ..Default::default()
            };
            let rows = repo
                .search_index(&f, ReflectionSortKey::CreatedAtDesc, 100, 0)
                .await?;
            assert_eq!(rows.len(), 2);
            assert_eq!(repo.count_by_filter(&f).await?, 2);

            // failure_modes_all = [loop, tool_misuse] -> only mids[0].
            let f = ResolvedReflectionSearchFilter {
                origin_user_id: Some(user_id),
                failure_modes_all: vec!["loop".into(), "tool_misuse".into()],
                ..Default::default()
            };
            let rows = repo
                .search_index(&f, ReflectionSortKey::CreatedAtDesc, 100, 0)
                .await?;
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].memory_id, mids[0]);

            // failure_modes_any = [tool_misuse] -> mids[0], mids[1].
            let f = ResolvedReflectionSearchFilter {
                origin_user_id: Some(user_id),
                failure_modes_any: vec!["tool_misuse".into()],
                ..Default::default()
            };
            let rows = repo
                .search_index(&f, ReflectionSortKey::CreatedAtDesc, 100, 0)
                .await?;
            assert_eq!(rows.len(), 2);

            // tools_used_all = [Read] -> mids[0], mids[2].
            let f = ResolvedReflectionSearchFilter {
                origin_user_id: Some(user_id),
                tools_used_all: vec!["Read".into()],
                ..Default::default()
            };
            let rows = repo
                .search_index(&f, ReflectionSortKey::CreatedAtDesc, 100, 0)
                .await?;
            assert_eq!(rows.len(), 2);

            // Range filters exercise the `>=` / `<=` operator branch
            // (regression guard for the earlier `push_eq!` bug that
            // produced `tri.score >= = $N`). All seeded rows carry
            // score=0.85 and created_at=1_700_000_000_000 from the
            // shared fixture, so `0.5 <= score <= 1.0` and a window
            // around the timestamp must include them.
            let f = ResolvedReflectionSearchFilter {
                origin_user_id: Some(user_id),
                score_min: Some(0.5),
                score_max: Some(1.0),
                ..Default::default()
            };
            assert_eq!(repo.count_by_filter(&f).await?, 3);
            let f = ResolvedReflectionSearchFilter {
                origin_user_id: Some(user_id),
                score_min: Some(0.99),
                ..Default::default()
            };
            assert_eq!(repo.count_by_filter(&f).await?, 0);
            let f = ResolvedReflectionSearchFilter {
                origin_user_id: Some(user_id),
                created_after: Some(1_699_999_999_000),
                created_before: Some(1_700_000_001_000),
                ..Default::default()
            };
            assert_eq!(repo.count_by_filter(&f).await?, 3);

            // limit/offset basic check.
            let f = ResolvedReflectionSearchFilter {
                origin_user_id: Some(user_id),
                ..Default::default()
            };
            let page1 = repo
                .search_index(&f, ReflectionSortKey::CreatedAtDesc, 2, 0)
                .await?;
            let page2 = repo
                .search_index(&f, ReflectionSortKey::CreatedAtDesc, 2, 2)
                .await?;
            assert_eq!(page1.len(), 2);
            assert_eq!(page2.len(), 1);
            // No overlap between pages.
            for r1 in &page1 {
                assert!(!page2.iter().any(|r2| r2.memory_id == r1.memory_id));
            }

            // No filter at all returns every row.
            let f = ResolvedReflectionSearchFilter::default();
            let total = repo.count_by_filter(&f).await?;
            assert!(total >= 4, "expected >=4 (user + other_user), got {total}");

            cleanup_index_and_children(pool, &mids).await;
            Ok(())
        })
    }

    /// `find_by_memory_ids` is the bulk fan-out entry point used by
    /// the hydrate / find_similar_by_vector paths. Confirm it returns
    /// every present row regardless of input order and silently skips
    /// missing ids.
    #[test]
    fn run_find_by_memory_ids_returns_rows_for_present_ids() -> anyhow::Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            let ids = [9_010_001_i64, 9_010_002, 9_010_003];
            cleanup(pool, &ids).await;

            let repo = ThreadReflectionIndexRepositoryImpl::new(pool);
            for id in ids.iter() {
                let mut tx = pool.begin().await?;
                repo.insert_index_tx(&mut *tx, &fixture_row(*id, *id, 1))
                    .await?;
                tx.commit().await?;
            }

            // Query in a deliberately scrambled order — bulk fetch
            // makes no order guarantees, so callers hash-join by id.
            let rows = repo.find_by_memory_ids(&[ids[2], ids[0], ids[1]]).await?;
            assert_eq!(rows.len(), 3);
            let got: std::collections::HashSet<i64> = rows.iter().map(|r| r.memory_id).collect();
            for id in ids.iter() {
                assert!(got.contains(id));
            }
            cleanup(pool, &ids).await;
            Ok(())
        })
    }

    #[test]
    fn run_find_by_memory_ids_empty_input_returns_empty() -> anyhow::Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            let repo = ThreadReflectionIndexRepositoryImpl::new(pool);
            let rows = repo.find_by_memory_ids(&[]).await?;
            assert!(rows.is_empty());
            Ok(())
        })
    }

    #[test]
    fn run_find_by_memory_ids_handles_missing_ids_silently() -> anyhow::Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            let present = 9_010_010_i64;
            let absent = 9_010_011_i64;
            cleanup(pool, &[present, absent]).await;

            let repo = ThreadReflectionIndexRepositoryImpl::new(pool);
            let mut tx = pool.begin().await?;
            repo.insert_index_tx(&mut *tx, &fixture_row(present, present, 1))
                .await?;
            tx.commit().await?;

            let rows = repo.find_by_memory_ids(&[present, absent]).await?;
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].memory_id, present);
            cleanup(pool, &[present, absent]).await;
            Ok(())
        })
    }

    /// Duplicated input ids must not produce duplicated rows: SQL
    /// `IN (x, x, y)` collapses to a set, and that's the contract the
    /// hydrate fan-out relies on.
    #[test]
    fn run_find_by_memory_ids_dedupes_input_ids() -> anyhow::Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            let id = 9_010_020_i64;
            cleanup(pool, &[id]).await;

            let repo = ThreadReflectionIndexRepositoryImpl::new(pool);
            let mut tx = pool.begin().await?;
            repo.insert_index_tx(&mut *tx, &fixture_row(id, id, 1))
                .await?;
            tx.commit().await?;

            let rows = repo.find_by_memory_ids(&[id, id, id]).await?;
            assert_eq!(rows.len(), 1);
            cleanup(pool, &[id]).await;
            Ok(())
        })
    }
}
