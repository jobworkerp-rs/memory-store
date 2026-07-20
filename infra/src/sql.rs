//! Backend-aware SQL placeholder helpers (`p!`, `p_jsonb!`,
//! `build_in_placeholders`, ...) used to assemble dynamic statements.
//!
//! SQL-injection safety contract: callers build query text only from
//! static fragments, these placeholder helpers (which emit `$N`/`?` by
//! arity, never caller data), and internal column/table identifiers —
//! every external value is supplied through `.bind()`. Statements built
//! this way are therefore safe to pass through `sqlx::AssertSqlSafe`,
//! which is why the `query*` call sites wrap their owned SQL strings in
//! it rather than relying on the `&'static str` bound.

/// Generate a parameter placeholder for the current database backend.
/// PostgreSQL uses `$N` (1-indexed), SQLite uses `?`.
#[cfg(feature = "postgres")]
macro_rules! p {
    ($n:literal) => {
        concat!("$", stringify!($n))
    };
}

#[cfg(not(feature = "postgres"))]
macro_rules! p {
    ($n:literal) => {
        "?"
    };
}

/// Generate a JSONB-cast parameter placeholder.
/// PostgreSQL: `$N::jsonb`, SQLite: `?` (no cast needed).
#[cfg(feature = "postgres")]
macro_rules! p_jsonb {
    ($n:literal) => {
        concat!("$", stringify!($n), "::jsonb")
    };
}

#[cfg(not(feature = "postgres"))]
macro_rules! p_jsonb {
    ($n:literal) => {
        "?"
    };
}

/// Build a SELECT column list for the memory table.
/// PostgreSQL: JSONB columns are cast to text via `col::text AS col`.
///
/// History:
/// - Phase 1: `system_id` removed from the schema — system prompts moved to
///   ROLE_SYSTEM memories referenced via `parent_ids`.
/// - Phase 4: `thread_id` removed — thread membership is resolved exclusively
///   through the `thread_memory` junction table, which also carries the
///   conversation `position` used by the parent_ids traversal.
#[cfg(feature = "postgres")]
macro_rules! memory_columns {
    () => {
        "id, parent_ids, user_id, content, content_type, params::text AS params, metadata::text AS metadata, created_at, updated_at, role, external_id, media_object_id, memory_kind"
    };
}

#[cfg(not(feature = "postgres"))]
macro_rules! memory_columns {
    () => {
        "id, parent_ids, user_id, content, content_type, params, metadata, created_at, updated_at, role, external_id, media_object_id, memory_kind"
    };
}

/// Build a qualified SELECT column list for the memory table (for use in JOINs).
#[cfg(feature = "postgres")]
macro_rules! memory_qualified_columns {
    () => {
        "memory.id, memory.parent_ids, memory.user_id, memory.content, memory.content_type, memory.params::text AS params, memory.metadata::text AS metadata, memory.created_at, memory.updated_at, memory.role, memory.external_id, memory.media_object_id, memory.memory_kind"
    };
}

#[cfg(not(feature = "postgres"))]
macro_rules! memory_qualified_columns {
    () => {
        "memory.id, memory.parent_ids, memory.user_id, memory.content, memory.content_type, memory.params, memory.metadata, memory.created_at, memory.updated_at, memory.role, memory.external_id, memory.media_object_id, memory.memory_kind"
    };
}

/// Build a SELECT column list for the memory_rating table.
#[cfg(feature = "postgres")]
macro_rules! memory_rating_columns {
    () => {
        "id, memory_id, user_id, rating, metadata::text AS metadata, created_at, updated_at"
    };
}

#[cfg(not(feature = "postgres"))]
macro_rules! memory_rating_columns {
    () => {
        "id, memory_id, user_id, rating, metadata, created_at, updated_at"
    };
}

/// Build a SELECT column list for the thread table.
/// PostgreSQL: `metadata` is JSONB and must be cast to text so sqlx can
/// decode into `Option<String>`. The other columns are plain SQL types.
#[cfg(feature = "postgres")]
macro_rules! thread_columns {
    () => {
        "id, default_system_memory_id, user_id, description, channel, embedding, embedding_dim, created_at, updated_at, metadata::text AS metadata, memory_kind"
    };
}

#[cfg(not(feature = "postgres"))]
macro_rules! thread_columns {
    () => {
        "id, default_system_memory_id, user_id, description, channel, embedding, embedding_dim, created_at, updated_at, metadata, memory_kind"
    };
}

/// Build a SELECT column list for the media_object table.
/// PostgreSQL: `metadata` is JSONB and must be cast to text so sqlx can
/// decode it into `Option<String>` (same rationale as `thread_columns!`).
#[cfg(feature = "postgres")]
macro_rules! media_object_columns {
    () => {
        "id, kind, media_type, byte_size, sha256, width, height, \
         duration_ms, storage_backend, storage_uri, alt, ref_count, gc_state, \
         metadata::text AS metadata, created_at, updated_at"
    };
}

#[cfg(not(feature = "postgres"))]
macro_rules! media_object_columns {
    () => {
        "id, kind, media_type, byte_size, sha256, width, height, \
         duration_ms, storage_backend, storage_uri, alt, ref_count, gc_state, \
         metadata, created_at, updated_at"
    };
}

/// Maximum number of bind parameters per `IN (...)` clause for the
/// `*_by_ids` chunked bulk reads. SQLite's compiled-in
/// `SQLITE_MAX_VARIABLE_NUMBER` defaults to 999; PostgreSQL handles
/// far more, but the unified cap keeps the call shape identical
/// across backends. Callers chunk input slices via
/// `chunks(IN_LIST_CHUNK_SIZE)` before composing the SQL.
pub const IN_LIST_CHUNK_SIZE: usize = 500;

/// Build dynamic IN clause placeholders for a given count.
/// PostgreSQL: `$1, $2, ...`, SQLite: `?, ?, ...`
/// `offset` is the starting parameter number (1-indexed, only used for PostgreSQL).
#[cfg(feature = "postgres")]
pub fn build_in_placeholders(count: usize, offset: usize) -> String {
    (0..count)
        .map(|i| format!("${}", offset + i))
        .collect::<Vec<_>>()
        .join(", ")
}

#[cfg(not(feature = "postgres"))]
pub fn build_in_placeholders(count: usize, _offset: usize) -> String {
    (0..count).map(|_| "?").collect::<Vec<_>>().join(", ")
}

/// Render a single positional placeholder for the current backend.
/// PostgreSQL: `$N`, SQLite: `?`. The `idx` is ignored on SQLite but
/// keeping a uniform call shape lets callers track parameter numbers
/// without `#[cfg]` blocks at every site.
#[cfg(feature = "postgres")]
pub fn dyn_placeholder(idx: usize) -> String {
    format!("${idx}")
}

#[cfg(not(feature = "postgres"))]
pub fn dyn_placeholder(_idx: usize) -> String {
    "?".to_string()
}

/// Escape LIKE wildcards (`%`, `_`, `\`) so the value can be embedded as a
/// literal in a `WHERE col LIKE :p ESCAPE '\\'` predicate. Both SQLite and
/// PostgreSQL honour `ESCAPE '\\'`; callers issue the bind value as
/// `format!("{escaped}%")` (or with leading `%` for suffix match).
pub fn escape_like(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}

pub(crate) use media_object_columns;
pub(crate) use memory_columns;
pub(crate) use memory_qualified_columns;
pub(crate) use memory_rating_columns;
pub(crate) use p;
pub(crate) use p_jsonb;
pub(crate) use thread_columns;
