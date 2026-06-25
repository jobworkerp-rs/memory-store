//! sqlx `FromRow` structs for the reflection sidecar / child tables.
//!
//! Naming: every table column is mirrored verbatim to keep the macros
//! readable. Conversion helpers to/from proto types live in the
//! `app::reflection` layer (Phase D); the infra layer stays
//! proto-agnostic so postgres/sqlite query bodies can share these
//! row types without dragging proto enums into raw SQL.

/// Sidecar row for `thread_reflection_index`. Authoritative source of
/// reflection filter / sort / aggregate columns. Operational state
/// fields (`pinned`, `*_embedding_status`, `*_embedding_error`) are
/// the only mutable columns; everything else is set on insert and
/// stays immutable per spec §3.7.
#[derive(sqlx::FromRow, Debug, Clone)]
pub struct ThreadReflectionIndexRow {
    pub memory_id: i64,
    pub thread_id: i64,
    pub origin_thread_id: i64,
    pub origin_user_id: i64,
    pub origin_channel: Option<String>,
    pub outcome: i32,
    pub score: f64,
    pub score_self: f64,
    pub score_heuristic: f64,
    pub task_category: i32,
    pub reflection_aspect: i32,
    pub dataset_quality: i32,
    pub summary_embedding_status: i32,
    pub summary_embedding_error: Option<String>,
    pub intent_embedding_status: i32,
    pub intent_embedding_error: Option<String>,
    pub prompt_version: String,
    pub target_model_version: Option<String>,
    pub experiment_id: Option<String>,
    pub experiment_variant: Option<String>,
    pub previous_reflection_id: Option<i64>,
    pub pinned: bool,
    pub is_recurrence: bool,
    pub mitigation_fingerprint: Option<String>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(sqlx::FromRow, Debug, Clone)]
pub struct ReflectionFailureModeRow {
    pub memory_id: i64,
    pub mode: String,
}

#[derive(sqlx::FromRow, Debug, Clone)]
pub struct ReflectionToolRow {
    pub memory_id: i64,
    pub tool: String,
}

/// `error_kind` is stored as NOT NULL DEFAULT '' so it can sit in
/// the PK without driver-specific NULL handling. Callers translate
/// the empty string back to "no error_kind" at the API boundary.
#[derive(sqlx::FromRow, Debug, Clone)]
pub struct ReflectionToolOutcomeRow {
    pub memory_id: i64,
    pub tool: String,
    pub contribution: i32,
    pub error_kind: String,
}

/// `links_json` is stored as JSON text; the app layer parses it into
/// the structured `[{field, index}, ...]` array. Keeping it as a raw
/// string at infra level lets sqlite (`TEXT`) and postgres (`JSONB`)
/// share the same FromRow struct.
/// `turn_index` snapshots the LLM's global turn index at finalize
/// time so search responses can surface the original anchor location
/// (`ReflectionFact.turn_index`) without re-resolving via
/// thread_memory.
#[derive(sqlx::FromRow, Debug, Clone)]
pub struct ReflectionFactRow {
    pub memory_id: i64,
    pub fact_memory_id: i64,
    pub fact_kind: i32,
    pub turn_index: i32,
    pub weight: Option<f64>,
    pub note: Option<String>,
    pub links_json: Option<String>,
}

#[derive(sqlx::FromRow, Debug, Clone)]
pub struct ReflectionAppliedTargetRow {
    pub memory_id: i64,
    pub target: String,
    pub mitigation_fingerprint: Option<String>,
    pub applied_at: i64,
}

#[derive(sqlx::FromRow, Debug, Clone)]
pub struct ReflectionFewShotUsageRow {
    pub memory_id: i64,
    pub used_in_thread_id: i64,
    pub used_at: i64,
}

#[derive(sqlx::FromRow, Debug, Clone)]
pub struct ToolOutcomeStatsRow {
    pub origin_user_id: i64,
    pub tool: String,
    pub outcome: i32,
    pub count: i64,
    pub last_updated_at: i64,
}

#[derive(sqlx::FromRow, Debug, Clone)]
pub struct ToolContributionStatsRow {
    pub origin_user_id: i64,
    pub tool: String,
    pub contribution: i32,
    pub error_kind: String,
    pub count: i64,
    pub last_updated_at: i64,
}

/// F-A1 — one bucket of the failure-mode aggregate. `recurrence_count`
/// is the subset of `count` whose sidecar has `is_recurrence=true`, so
/// `count >= recurrence_count` is an invariant. Co-occurrence rows are
/// returned separately via `FailureModeCoOccurrenceRow` to keep the
/// per-mode count query indexable.
#[derive(sqlx::FromRow, Debug, Clone)]
pub struct FailureModeAggregateRow {
    pub mode: String,
    pub count: i64,
    pub recurrence_count: i64,
}

/// F-A1 co-occurrence pair. `mode_a < mode_b` is enforced by the SQL
/// (`a.mode < b.mode` join condition) so each unordered pair appears
/// exactly once. App layer flips the pair back into the per-mode view
/// expected by the proto (`AggregateFailureModesResponse.entries[i]
/// .co_occurring`).
#[derive(sqlx::FromRow, Debug, Clone)]
pub struct FailureModeCoOccurrenceRow {
    pub mode_a: String,
    pub mode_b: String,
    pub count: i64,
}

/// F-A7 — one bucket of the dynamic tool-contribution pivot.
/// Empty-string columns ("", "", 0) appear when the corresponding
/// `group_by_*` flag was off — the SQL emits a constant placeholder so
/// the FromRow shape stays uniform across requests.
///
/// `recurrence_count` is the subset of `count` whose sidecar row
/// carries `is_recurrence=true`. The invariant
/// `count >= recurrence_count` holds — clamp the app-side mapping to
/// `[0, count]` to guard against an unusual SUM(CASE) result.
#[derive(sqlx::FromRow, Debug, Clone)]
pub struct ToolContributionAggregateRow {
    pub tool: String,
    pub contribution: i32,
    pub error_kind: String,
    pub origin_user_id: i64,
    pub count: i64,
    pub recurrence_count: i64,
}

#[derive(sqlx::FromRow, Debug, Clone)]
pub struct FailureModeDictionaryRow {
    pub mode: String,
    pub description: String,
    pub severity: i32,
    pub category: i32,
    pub default_mitigation: String,
}

#[derive(sqlx::FromRow, Debug, Clone)]
pub struct FailureSignatureIndicatorNormRow {
    pub indicator_name: String,
    pub max_value: f64,
    pub weight: f64,
}

#[derive(sqlx::FromRow, Debug, Clone)]
pub struct ThreadAggregateKeyRow {
    pub user_id: i64,
    pub labels_hash: String,
    pub thread_id: i64,
    pub created_at: i64,
}

/// Sort key consumed by `ThreadReflectionIndexRepository::search_index`.
/// Kept here next to the row types so the trait method signature stays
/// proto-free.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReflectionSortKey {
    #[default]
    CreatedAtDesc,
    CreatedAtAsc,
    ScoreDesc,
    ScoreAsc,
    /// `(memory_id DESC)` only — used by F-E1 export so the keyset
    /// cursor (`memory_id < cursor`) is compatible with the page
    /// boundaries. Other CreatedAt-based sorts mix two keys and would
    /// require a tuple cursor.
    MemoryIdDesc,
}

/// `ReflectionSearchFilter` (proto) flattened into plain Rust scalars so
/// the infra layer can compose dynamic `WHERE` clauses without depending
/// on the proto crate. `failure_modes` and `tools_used` follow ALL
/// semantics; the `_match_any` variants follow ANY semantics. Empty
/// vectors are treated as "no filter on that axis".
#[derive(Debug, Clone, Default)]
pub struct ResolvedReflectionSearchFilter {
    pub origin_user_id: Option<i64>,
    pub origin_channel: Option<String>,
    pub origin_thread_id: Option<i64>,
    pub outcomes: Vec<i32>,
    pub score_min: Option<f64>,
    pub score_max: Option<f64>,
    pub task_categories: Vec<i32>,
    pub reflection_aspect: Option<i32>,
    pub failure_modes_all: Vec<String>,
    pub failure_modes_any: Vec<String>,
    pub tools_used_all: Vec<String>,
    pub tools_used_any: Vec<String>,
    pub prompt_version: Option<String>,
    pub target_model_version: Option<String>,
    pub experiment_id: Option<String>,
    pub experiment_variant: Option<String>,
    pub pinned: Option<bool>,
    pub dataset_quality: Option<i32>,
    pub summary_embedding_status: Option<i32>,
    pub intent_embedding_status: Option<i32>,
    pub created_after: Option<i64>,
    pub created_before: Option<i64>,
    pub is_recurrence: Option<bool>,
    /// Keyset cursor: only return rows with `memory_id < memory_id_lt`.
    /// Used by F-E1 export to advance through pages without reusing
    /// `OFFSET`, which would silently shift under concurrent inserts.
    pub memory_id_lt: Option<i64>,
}

/// F-A2 — dynamic GROUP BY axis for `aggregate_scores`. Mirrors the
/// proto `AggregateScoresGroupBy` enum but lives in the infra layer
/// so the SQL builder stays proto-agnostic (the app layer translates
/// proto -> this enum).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ScoresAggregateAxis {
    OriginUserId,
    OriginChannel,
    TargetModelVersion,
    TaskCategory,
    ReflectionAspect,
    Outcome,
    ExperimentId,
    ExperimentVariant,
    TimeBucket,
    SummaryEmbeddingStatus,
    IntentEmbeddingStatus,
}

/// F-A2 — one bucket of the score aggregate. Axes that the request
/// did not group on are `None`; the SQL builder leaves their key
/// fields unset so the proto handler can map "absent axis" back to
/// the proto's `optional` semantics.
///
/// `score_p50` / `score_p95` come from `percentile_cont` on Postgres
/// and from an app-side sort on SQLite, but the shape stays the same
/// either way. Empty groups never appear (the SQL only emits a row
/// when at least one match exists).
#[derive(Debug, Clone)]
pub struct ScoresAggregateBucket {
    pub origin_user_id: Option<i64>,
    pub origin_channel: Option<String>,
    pub target_model_version: Option<String>,
    pub task_category: Option<i32>,
    pub reflection_aspect: Option<i32>,
    pub outcome: Option<i32>,
    pub experiment_id: Option<String>,
    pub experiment_variant: Option<String>,
    pub time_bucket_start: Option<i64>,
    pub summary_embedding_status: Option<i32>,
    pub intent_embedding_status: Option<i32>,
    pub count: i64,
    pub score_avg: f64,
    pub score_min: f64,
    pub score_max: f64,
    pub score_p50: f64,
    pub score_p95: f64,
}
