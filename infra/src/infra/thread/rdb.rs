use super::rows::ThreadRow;
use crate::error::LlmMemoryError;
use crate::infra::IdGeneratorWrapper;
use crate::infra::UseIdGenerator;
use crate::sql::{IN_LIST_CHUNK_SIZE, build_in_placeholders, p, p_jsonb, thread_columns};
use anyhow::{Context, Result};
use async_trait::async_trait;
use infra_utils::infra::rdb::Rdb;
use infra_utils::infra::rdb::RdbPool;
use infra_utils::infra::rdb::UseRdbPool;
use itertools::Itertools;
use protobuf::llm_memory::data::{Thread, ThreadData, ThreadId, UserId};
use sqlx::Executor;

// Phase 2 of system-prompt-as-memory migration:
//   The legacy `system_prompt_id BIGINT NOT NULL` column is now
//   `default_system_memory_id BIGINT NULL`. INSERT/UPDATE bind a plain
//   `Option<i64>` (sqlx maps `None` to SQL NULL automatically).
//
// Exposed (`pub`) so cross-crate test code (app/) can reuse the same
// backend-aware placeholder string instead of hard-coding `?` — the
// production code uses `crate::sql::p!` here, so any test that bypasses
// this constant would silently break under `--features postgres`.
pub const INSERT_SQL: &str = concat!(
    "INSERT INTO thread (id, default_system_memory_id, user_id, description, channel, ",
    "embedding, embedding_dim, created_at, updated_at, metadata) VALUES (",
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
    p_jsonb!(10),
    ")"
);

const UPDATE_SQL: &str = concat!(
    "UPDATE thread SET default_system_memory_id = ",
    p!(1),
    ", user_id = ",
    p!(2),
    ", description = ",
    p!(3),
    ", channel = ",
    p!(4),
    ", embedding = ",
    p!(5),
    ", embedding_dim = ",
    p!(6),
    ", updated_at = ",
    p!(7),
    ", metadata = ",
    p_jsonb!(8),
    " WHERE id = ",
    p!(9),
    ";"
);

const DELETE_SQL: &str = concat!("DELETE FROM thread WHERE id = ", p!(1), ";");

// `thread.metadata` is JSONB on Postgres so SELECT must cast it to text
// (sqlx decodes JSONB as `Vec<u8>` otherwise and the `Option<String>`
// mapping fails). The shared `thread_columns!()` macro does the cast.
const FIND_SQL: &str = concat!(
    "SELECT ",
    thread_columns!(),
    " FROM thread WHERE id = ",
    p!(1),
    ";"
);

// PostgreSQL: row lock prevents concurrent add_memory / delete_thread race.
// SQLite does not support FOR UPDATE syntax (parse error), but its
// single-writer semantics already serialise writes, so the plain SELECT
// is sufficient there.
#[cfg(feature = "postgres")]
const FIND_FOR_UPDATE_SQL: &str = concat!(
    "SELECT ",
    thread_columns!(),
    " FROM thread WHERE id = ",
    p!(1),
    " FOR UPDATE;"
);
#[cfg(not(feature = "postgres"))]
const FIND_FOR_UPDATE_SQL: &str = concat!(
    "SELECT ",
    thread_columns!(),
    " FROM thread WHERE id = ",
    p!(1),
    ";"
);

const FIND_LIST_LIMIT_SQL: &str = concat!(
    "SELECT ",
    thread_columns!(),
    " FROM thread ORDER BY id DESC LIMIT ",
    p!(1),
    " OFFSET ",
    p!(2),
    ";"
);

const FIND_LIST_ALL_SQL: &str = concat!(
    "SELECT ",
    thread_columns!(),
    " FROM thread ORDER BY id DESC;"
);

// (P8) `FIND_BY_USER_LIMIT_SQL` / `FIND_BY_USER_ALL_SQL` were retired in
// favour of `find_by_user_id`'s dynamic SQL builder. The new builder lets
// callers add time-range filters (created_*/updated_*) and choose a sort
// key, which the legacy static strings could not express.

/// Sort key for `find_by_user_id` (and any future thread list path that
/// needs the same options). Discriminants mirror the proto
/// `ThreadListSort` enum 1:1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ThreadSort {
    #[default]
    UpdatedDesc,
    UpdatedAsc,
    CreatedDesc,
    CreatedAsc,
    IdDesc,
}

impl From<i32> for ThreadSort {
    /// Map a proto `ThreadListSort` discriminant to the infra enum.
    /// Unknown values fall through to `UpdatedDesc` so newer clients
    /// querying older servers degrade gracefully. Discriminants are
    /// pinned by `data/common.proto::ThreadListSort`.
    fn from(value: i32) -> Self {
        match value {
            2 => Self::UpdatedAsc,
            3 => Self::CreatedDesc,
            4 => Self::CreatedAsc,
            5 => Self::IdDesc,
            // 0 (UNSPECIFIED) and 1 (UPDATED_DESC) both land here.
            _ => Self::UpdatedDesc,
        }
    }
}

/// Build a `LIMIT ? OFFSET ?` (or `LIMIT $N OFFSET $N+1`) tail for a
/// dynamic SQL whose WHERE clause already consumed `next_idx - 1`
/// placeholders. The `next_idx` argument is unused on the sqlite path
/// because `?` placeholders are positional, but we keep the signature
/// uniform so the postgres / sqlite divergence stays in this one helper.
fn limit_offset_sql_fragment(next_idx: usize) -> String {
    #[cfg(feature = "postgres")]
    {
        format!(" LIMIT ${next_idx} OFFSET ${off}", off = next_idx + 1)
    }
    #[cfg(not(feature = "postgres"))]
    {
        let _ = next_idx;
        " LIMIT ? OFFSET ?".to_string()
    }
}

/// Resolve the ORDER BY expression for a `ThreadSort`. The `id` secondary
/// sort key gives the order a stable tiebreaker for paginated reads.
fn order_by_thread(sort: ThreadSort) -> &'static str {
    match sort {
        ThreadSort::UpdatedDesc => "updated_at DESC, id DESC",
        ThreadSort::UpdatedAsc => "updated_at ASC, id ASC",
        ThreadSort::CreatedDesc => "created_at DESC, id DESC",
        ThreadSort::CreatedAsc => "created_at ASC, id ASC",
        ThreadSort::IdDesc => "id DESC",
    }
}

const FIND_BY_CHANNEL_AND_USER_SQL: &str = concat!(
    "SELECT ",
    thread_columns!(),
    " FROM thread WHERE channel = ",
    p!(1),
    " AND user_id = ",
    p!(2),
    ";"
);

const UPDATE_UPDATED_AT_SQL: &str = concat!(
    "UPDATE thread SET updated_at = ",
    p!(1),
    " WHERE id = ",
    p!(2),
    ";"
);

// Count threads that reference the given memory as their default system
// prompt. Used by delete_memory to reject deletion while still referenced.
const COUNT_THREADS_BY_DEFAULT_SYSTEM_MEMORY_SQL: &str = concat!(
    "SELECT COUNT(*) FROM thread WHERE default_system_memory_id = ",
    p!(1),
);

// Clear default_system_memory_id on all threads that reference the given
// memory. Used by delete_memory to detach dangling default references
// before deleting the memory row.
const CLEAR_DEFAULT_SYSTEM_MEMORY_SQL: &str = concat!(
    "UPDATE thread SET default_system_memory_id = NULL WHERE default_system_memory_id = ",
    p!(1),
);

#[cfg(feature = "postgres")]
const FIND_THREAD_IDS_BY_DEFAULT_SYSTEM_MEMORY_FOR_UPDATE_SQL: &str = concat!(
    "SELECT id FROM thread WHERE default_system_memory_id = ",
    p!(1),
    " FOR UPDATE;"
);
#[cfg(not(feature = "postgres"))]
const FIND_THREAD_IDS_BY_DEFAULT_SYSTEM_MEMORY_FOR_UPDATE_SQL: &str = concat!(
    "SELECT id FROM thread WHERE default_system_memory_id = ",
    p!(1),
    ";"
);

const COUNT_SQL: &str = "SELECT count(*) as count FROM thread;";

/// P1 (improve-search): Lightweight projection of a Thread row used by
/// `MemoryVectorAppImpl::enrich_hits` to attach representative-thread
/// metadata (`thread_owner_user_id`, `thread_description`) to search
/// results in a single SQL. Kept narrow on purpose — full `Thread`
/// hydration would pull labels and other columns the search path does
/// not need.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadSummary {
    pub user_id: i64,
    /// `None` when the underlying `thread.description` column is NULL.
    /// The app layer maps `Some(ThreadSummary { description: None, .. })`
    /// to "thread_id / thread_owner_user_id are set, but
    /// thread_description is unset" — distinct from the orphan-thread
    /// case where the entire summary is missing from the map.
    pub description: Option<String>,
}

#[async_trait]
pub trait ThreadRepository: UseRdbPool + UseIdGenerator + Sync + Send {
    async fn create<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        thread: &ThreadData,
    ) -> Result<ThreadId> {
        let id: i64 = self.id_generator().generate_id()?;
        let (created_at, updated_at) =
            crate::infra::fill_timestamps(thread.created_at, thread.updated_at);
        let res = sqlx::query::<Rdb>(INSERT_SQL)
            .bind(id)
            .bind(thread.default_system_memory_id)
            .bind(thread.user_id.map(|u| u.value).unwrap_or(0))
            .bind(&thread.description)
            .bind(&thread.channel)
            .bind(&thread.embedding)
            .bind(thread.embedding_dim)
            .bind(created_at)
            .bind(updated_at)
            .bind(&thread.metadata)
            .execute(tx)
            .await
            .map_err(LlmMemoryError::DBError)?;
        if res.rows_affected() > 0 {
            Ok(ThreadId { value: id })
        } else {
            Err(LlmMemoryError::RuntimeError(format!(
                "Cannot insert thread (logic error?): {:?}",
                thread
            ))
            .into())
        }
    }

    async fn update<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        id: &ThreadId,
        thread: &ThreadData,
    ) -> Result<bool> {
        let updated_at = crate::infra::fill_updated_at(thread.updated_at);
        sqlx::query(UPDATE_SQL)
            .bind(thread.default_system_memory_id)
            .bind(thread.user_id.map(|u| u.value).unwrap_or(0))
            .bind(&thread.description)
            .bind(&thread.channel)
            .bind(&thread.embedding)
            .bind(thread.embedding_dim)
            .bind(updated_at)
            .bind(&thread.metadata)
            .bind(id.value)
            .execute(tx)
            .await
            .map(|r| r.rows_affected() > 0)
            .map_err(LlmMemoryError::DBError)
            .context(format!("error in update: id = {}", id.value))
    }

    async fn delete(&self, id: &ThreadId) -> Result<bool> {
        self.delete_tx(self.db_pool(), id).await
    }

    async fn delete_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        id: &ThreadId,
    ) -> Result<bool> {
        let del = sqlx::query::<Rdb>(DELETE_SQL)
            .bind(id.value)
            .execute(tx)
            .await
            .map(|r| r.rows_affected() > 0)
            .map_err(LlmMemoryError::DBError)?;
        Ok(del)
    }

    async fn find(&self, id: &ThreadId) -> Result<Option<Thread>> {
        self.find_row_tx(self.db_pool(), id)
            .await
            .map(|r| r.map(|r2| r2.to_proto()))
    }

    async fn find_row_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        id: &ThreadId,
    ) -> Result<Option<ThreadRow>> {
        sqlx::query_as::<Rdb, ThreadRow>(FIND_SQL)
            .bind(id.value)
            .fetch_optional(tx)
            .await
            .map_err(LlmMemoryError::DBError)
            .context(format!("error in find: id = {}", id.value))
    }

    /// Fetch a thread row while acquiring a row-level lock (PostgreSQL).
    /// On SQLite the lock is a no-op; single-writer semantics serialize.
    async fn find_row_for_update_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        id: &ThreadId,
    ) -> Result<Option<ThreadRow>> {
        sqlx::query_as::<Rdb, ThreadRow>(FIND_FOR_UPDATE_SQL)
            .bind(id.value)
            .fetch_optional(tx)
            .await
            .map_err(LlmMemoryError::DBError)
            .context(format!("error in find_for_update: id = {}", id.value))
    }

    async fn find_list(&self, limit: Option<&i32>, offset: Option<&i64>) -> Result<Vec<Thread>> {
        self.find_row_list_tx(self.db_pool(), limit, offset)
            .await
            .map(|r| r.into_iter().map(|r2| r2.to_proto()).collect_vec())
    }

    /// Fetch all thread IDs (lightweight — no data columns).
    async fn find_all_thread_ids(&self) -> Result<Vec<i64>> {
        let rows: Vec<(i64,)> = sqlx::query_as("SELECT id FROM thread")
            .fetch_all(self.db_pool())
            .await
            .map_err(LlmMemoryError::DBError)
            .context("find_all_thread_ids")?;
        Ok(rows.into_iter().map(|(id,)| id).collect())
    }

    /// Batch-fetch threads by IDs. Returns found threads (missing IDs silently skipped).
    async fn find_by_ids(&self, ids: &[i64]) -> Result<Vec<Thread>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut all_rows = Vec::with_capacity(ids.len());
        // Chunk to avoid SQLite parameter limits
        for chunk in ids.chunks(IN_LIST_CHUNK_SIZE) {
            let placeholders = build_in_placeholders(chunk.len(), 1);
            let sql = format!(
                "SELECT {} FROM thread WHERE id IN ({}) ORDER BY id DESC",
                thread_columns!(),
                placeholders
            );
            let mut query = sqlx::query_as::<_, ThreadRow>(sqlx::AssertSqlSafe(sql));
            for id in chunk {
                query = query.bind(id);
            }
            let rows = query
                .fetch_all(self.db_pool())
                .await
                .map_err(LlmMemoryError::DBError)
                .context("find_by_ids")?;
            all_rows.extend(rows);
        }
        Ok(all_rows.into_iter().map(|r| r.to_proto()).collect())
    }

    /// P1 (improve-search): Batch-fetch `(thread_id, user_id, description)`
    /// triples for the representative-thread set surfaced by
    /// `ThreadMemoryRepository::find_positions_for_memories`. Threads that
    /// were concurrently deleted (cascade race) are silently absent from
    /// the returned map — callers MUST treat absence as "orphan thread"
    /// and unset all 5 P1 fields.
    ///
    /// Empty input short-circuits. Chunk size 500 mirrors `find_by_ids`
    /// (well below the 900-parameter SQLite ceiling).
    async fn find_thread_summaries(
        &self,
        thread_ids: &[i64],
    ) -> Result<std::collections::HashMap<i64, ThreadSummary>> {
        if thread_ids.is_empty() {
            return Ok(std::collections::HashMap::new());
        }
        let mut out: std::collections::HashMap<i64, ThreadSummary> =
            std::collections::HashMap::with_capacity(thread_ids.len());
        for chunk in thread_ids.chunks(IN_LIST_CHUNK_SIZE) {
            let placeholders = build_in_placeholders(chunk.len(), 1);
            let sql = format!(
                "SELECT id, user_id, description FROM thread WHERE id IN ({})",
                placeholders
            );
            let mut query =
                sqlx::query_as::<_, (i64, i64, Option<String>)>(sqlx::AssertSqlSafe(sql));
            for tid in chunk {
                query = query.bind(tid);
            }
            let rows = query
                .fetch_all(self.db_pool())
                .await
                .map_err(LlmMemoryError::DBError)
                .context("find_thread_summaries")?;
            for (tid, uid, desc) in rows {
                out.insert(
                    tid,
                    ThreadSummary {
                        user_id: uid,
                        description: desc,
                    },
                );
            }
        }
        Ok(out)
    }

    async fn find_row_list_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        limit: Option<&i32>,
        offset: Option<&i64>,
    ) -> Result<Vec<ThreadRow>> {
        if let Some(l) = limit {
            sqlx::query_as::<_, ThreadRow>(FIND_LIST_LIMIT_SQL)
                .bind(l)
                .bind(offset.unwrap_or(&0i64))
                .fetch_all(tx)
        } else {
            sqlx::query_as::<_, ThreadRow>(FIND_LIST_ALL_SQL).fetch_all(tx)
        }
        .await
        .map_err(LlmMemoryError::DBError)
        .context(format!("error in find_list: ({:?}, {:?})", limit, offset))
    }

    /// (P8) Find threads owned by `user_id` with optional time-range
    /// filters and an explicit sort key. The static SQL constants used
    /// before P8 only supported the hard-coded `ORDER BY updated_at DESC`
    /// pattern; the dynamic builder here produces equivalent SQL when the
    /// caller passes the defaults (`None * 4, ThreadSort::default()`).
    #[allow(clippy::too_many_arguments)]
    async fn find_by_user_id(
        &self,
        user_id: UserId,
        limit: Option<&i32>,
        offset: Option<&i64>,
        created_after: Option<i64>,
        created_before: Option<i64>,
        updated_after: Option<i64>,
        updated_before: Option<i64>,
        sort: ThreadSort,
    ) -> Result<Vec<Thread>> {
        let mut clauses: Vec<String> = Vec::new();
        let mut placeholder_idx: usize = 1;
        let mut push_clause = |column: &str, op: &str| {
            #[cfg(feature = "postgres")]
            let placeholder = format!("${placeholder_idx}");
            #[cfg(not(feature = "postgres"))]
            let placeholder = "?".to_string();
            clauses.push(format!("{column} {op} {placeholder}"));
            placeholder_idx += 1;
        };
        // user_id is required for this entrypoint, so the WHERE clause is
        // never empty.
        push_clause("user_id", "=");
        if created_after.is_some() {
            push_clause("created_at", ">");
        }
        if created_before.is_some() {
            push_clause("created_at", "<=");
        }
        if updated_after.is_some() {
            push_clause("updated_at", ">");
        }
        if updated_before.is_some() {
            push_clause("updated_at", "<=");
        }
        let where_sql = format!(" WHERE {}", clauses.join(" AND "));
        let order_sql = order_by_thread(sort);
        let limit_sql = limit
            .map(|_| limit_offset_sql_fragment(placeholder_idx))
            .unwrap_or_default();
        let sql = format!(
            "SELECT {} FROM thread{where_sql} ORDER BY {order_sql}{limit_sql}",
            thread_columns!()
        );

        let mut query = sqlx::query_as::<Rdb, ThreadRow>(sqlx::AssertSqlSafe(sql));
        query = query.bind(user_id.value);
        if let Some(v) = created_after {
            query = query.bind(v);
        }
        if let Some(v) = created_before {
            query = query.bind(v);
        }
        if let Some(v) = updated_after {
            query = query.bind(v);
        }
        if let Some(v) = updated_before {
            query = query.bind(v);
        }
        if let Some(l) = limit {
            query = query.bind(*l as i64).bind(*offset.unwrap_or(&0i64));
        }

        query
            .fetch_all(self.db_pool())
            .await
            .map(|r| r.into_iter().map(|r2| r2.to_proto()).collect_vec())
            .map_err(LlmMemoryError::DBError)
            .context(format!(
                "error in find_by_user_id: ({:?}, {:?})",
                user_id, limit
            ))
    }

    /// Find threads by channel and user_id (no pagination — expected result set is small).
    async fn find_by_channel_and_user_id(
        &self,
        channel: &str,
        user_id: &UserId,
    ) -> Result<Vec<Thread>> {
        self.find_by_channel_and_user_id_tx(self.db_pool(), channel, user_id)
            .await
    }

    /// Tx-bound variant of `find_by_channel_and_user_id` — runs inside an
    /// existing transaction so the caller can resolve a channel-keyed
    /// thread atomically with subsequent inserts. Used by the batch
    /// importer to upsert by `(user_id, channel)` without a separate
    /// non-transactional read.
    async fn find_by_channel_and_user_id_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        channel: &str,
        user_id: &UserId,
    ) -> Result<Vec<Thread>> {
        sqlx::query_as::<_, ThreadRow>(FIND_BY_CHANNEL_AND_USER_SQL)
            .bind(channel)
            .bind(user_id.value)
            .fetch_all(tx)
            .await
            .map(|r| r.into_iter().map(|r2| r2.to_proto()).collect_vec())
            .map_err(LlmMemoryError::DBError)
            .context(format!(
                "error in find_by_channel_and_user_id_tx: ({}, {:?})",
                channel, user_id
            ))
    }

    /// Generic thread-id resolver for the P5 thread_filter feature. Combines
    /// the seven non-label conditions (`user_id`, `channel`, `created_*`,
    /// `updated_*`) with AND in a single SQL statement and returns just the
    /// matching thread ids — labels are resolved through
    /// `find_thread_ids_by_labels` and intersected by the app layer
    /// (see `MemoryVectorAppImpl::resolve_memory_ids_from_thread_filter`).
    ///
    /// Direct backend-specific SQL is built dynamically because each
    /// optional bound translates to a different placeholder index. The
    /// `LIMIT max + 1` idiom lets the caller distinguish "exactly max" from
    /// "more than max" without a separate count query — used to enforce
    /// `MEMORY_THREAD_FILTER_INTERMEDIATE_HARD_LIMIT` in the app layer.
    ///
    /// Kept independent from `find_by_channel_and_user_id`: the latter is a
    /// UI-side helper that returns full `Thread` rows under a fixed
    /// `(user_id, channel)` shape, while this method is a generic resolver
    /// that returns ids only.
    #[allow(clippy::too_many_arguments)]
    async fn find_thread_ids_by_filter(
        &self,
        user_id: Option<i64>,
        channel: Option<&str>,
        created_after: Option<i64>,
        created_before: Option<i64>,
        updated_after: Option<i64>,
        updated_before: Option<i64>,
        max: i64,
    ) -> Result<Vec<i64>> {
        let mut clauses: Vec<String> = Vec::new();
        let mut placeholder_idx: usize = 1;
        let mut push_clause = |column: &str, op: &str| {
            #[cfg(feature = "postgres")]
            let placeholder = format!("${placeholder_idx}");
            #[cfg(not(feature = "postgres"))]
            let placeholder = "?".to_string();
            clauses.push(format!("{column} {op} {placeholder}"));
            placeholder_idx += 1;
        };
        if user_id.is_some() {
            push_clause("user_id", "=");
        }
        if channel.is_some() {
            push_clause("channel", "=");
        }
        if created_after.is_some() {
            push_clause("created_at", ">");
        }
        if created_before.is_some() {
            push_clause("created_at", "<");
        }
        if updated_after.is_some() {
            push_clause("updated_at", ">");
        }
        if updated_before.is_some() {
            push_clause("updated_at", "<");
        }

        let where_sql = if clauses.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", clauses.join(" AND "))
        };
        // `+ 1` overfetch lets the caller reject "exceeds max" without a
        // separate count query.
        #[cfg(feature = "postgres")]
        let limit_clause = format!(" LIMIT ${placeholder_idx}");
        #[cfg(not(feature = "postgres"))]
        let limit_clause = " LIMIT ?".to_string();
        let sql = format!("SELECT id FROM thread{where_sql}{limit_clause}");

        let mut query = sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(sql));
        if let Some(uid) = user_id {
            query = query.bind(uid);
        }
        if let Some(ch) = channel {
            query = query.bind(ch.to_string());
        }
        if let Some(v) = created_after {
            query = query.bind(v);
        }
        if let Some(v) = created_before {
            query = query.bind(v);
        }
        if let Some(v) = updated_after {
            query = query.bind(v);
        }
        if let Some(v) = updated_before {
            query = query.bind(v);
        }
        query = query.bind(max.saturating_add(1));

        query
            .fetch_all(self.db_pool())
            .await
            .map_err(LlmMemoryError::DBError)
            .context("error in find_thread_ids_by_filter")
    }

    async fn update_updated_at_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        id: &ThreadId,
        updated_at: i64,
    ) -> Result<bool> {
        sqlx::query(UPDATE_UPDATED_AT_SQL)
            .bind(updated_at)
            .bind(id.value)
            .execute(tx)
            .await
            .map(|r| r.rows_affected() > 0)
            .map_err(LlmMemoryError::DBError)
            .context(format!("error in update_updated_at: id = {}", id.value))
    }

    /// Count how many threads reference the given memory as their
    /// `default_system_memory_id`. Used by `delete_memory` to reject
    /// deletion of a system prompt that is still in active use.
    async fn count_threads_by_default_system_memory_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        memory_id: i64,
    ) -> Result<i64> {
        sqlx::query_scalar::<Rdb, i64>(COUNT_THREADS_BY_DEFAULT_SYSTEM_MEMORY_SQL)
            .bind(memory_id)
            .fetch_one(tx)
            .await
            .map_err(LlmMemoryError::DBError)
            .context(format!(
                "error in count_threads_by_default_system_memory: memory_id={}",
                memory_id
            ))
    }

    /// Clear `default_system_memory_id` on every thread that references the
    /// given memory. Returns the number of threads updated.
    async fn clear_default_system_memory_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        memory_id: i64,
    ) -> Result<u64> {
        sqlx::query(CLEAR_DEFAULT_SYSTEM_MEMORY_SQL)
            .bind(memory_id)
            .execute(tx)
            .await
            .map(|r| r.rows_affected())
            .map_err(LlmMemoryError::DBError)
            .context(format!(
                "error in clear_default_system_memory: memory_id={}",
                memory_id
            ))
    }

    /// Lock and return all thread ids that currently reference `memory_id`
    /// through `default_system_memory_id`.
    ///
    /// `delete_memory` uses this before it locks the memory row so that the
    /// thread->memory lock order matches `update_thread` on PostgreSQL.
    async fn find_thread_ids_by_default_system_memory_for_update_tx<
        'c,
        E: Executor<'c, Database = Rdb>,
    >(
        &self,
        tx: E,
        memory_id: i64,
    ) -> Result<Vec<i64>> {
        sqlx::query_scalar::<Rdb, i64>(FIND_THREAD_IDS_BY_DEFAULT_SYSTEM_MEMORY_FOR_UPDATE_SQL)
            .bind(memory_id)
            .fetch_all(tx)
            .await
            .map_err(LlmMemoryError::DBError)
            .context(format!(
                "error in find_thread_ids_by_default_system_memory_for_update: memory_id={}",
                memory_id
            ))
    }

    async fn count_list_tx<'c, E: Executor<'c, Database = Rdb>>(&self, tx: E) -> Result<i64> {
        sqlx::query_scalar(COUNT_SQL)
            .fetch_one(tx)
            .await
            .map_err(LlmMemoryError::DBError)
            .context("error in count_list".to_string())
    }
}

pub struct ThreadRepositoryImpl {
    id_generator: IdGeneratorWrapper,
    pool: &'static RdbPool,
}

pub trait UseThreadRepository {
    fn thread_repository(&self) -> &ThreadRepositoryImpl;
}

impl ThreadRepositoryImpl {
    pub fn new(id_generator: IdGeneratorWrapper, pool: &'static RdbPool) -> Self {
        Self { id_generator, pool }
    }
}

impl UseRdbPool for ThreadRepositoryImpl {
    fn db_pool(&self) -> &RdbPool {
        self.pool
    }
}

impl UseIdGenerator for ThreadRepositoryImpl {
    fn id_generator(&self) -> &IdGeneratorWrapper {
        &self.id_generator
    }
}

impl ThreadRepository for ThreadRepositoryImpl {}

#[cfg(test)]
mod test {
    use super::ThreadRepository;
    use super::ThreadRepositoryImpl;
    use super::ThreadSort;
    use super::ThreadSummary;
    use crate::infra::memory::rdb::{MemoryRepository, MemoryRepositoryImpl};
    use anyhow::Context;
    use anyhow::Result;
    use infra_utils::infra::rdb::RdbPool;
    use infra_utils::infra::rdb::UseRdbPool;
    use protobuf::llm_memory::data::MemoryData;
    use protobuf::llm_memory::data::Thread;
    use protobuf::llm_memory::data::ThreadData;
    use protobuf::llm_memory::data::UserId;

    async fn _test_repository(pool: &'static RdbPool) -> Result<()> {
        let repository = ThreadRepositoryImpl::new(crate::test_helper::shared_id_generator(), pool);
        let db = repository.db_pool();
        let data = Some(ThreadData {
            default_system_memory_id: Some(2),
            user_id: Some(UserId { value: 3 }),
            description: Some("test thread".to_string()),
            channel: Some("discord".to_string()),
            embedding: None,
            embedding_dim: None,
            created_at: 9,
            updated_at: 10,
            labels: vec![],
            metadata: None,
        });

        let mut tx = db.begin().await.context("error in test")?;
        let id = repository.create(&mut *tx, &data.clone().unwrap()).await?;
        assert!(id.value > 0);
        tx.commit().await.context("error in test create commit")?;

        let id1 = id;
        let expect = Thread {
            id: Some(id1),
            data,
        };

        // find
        let found = repository.find(&id1).await?;
        assert_eq!(Some(&expect), found.as_ref());

        // update (created_at in update data is ignored — original value preserved)
        tx = db.begin().await.context("error in test")?;
        let update = ThreadData {
            default_system_memory_id: Some(3),
            user_id: Some(UserId { value: 4 }),
            description: Some("updated thread".to_string()),
            channel: Some("slack".to_string()),
            embedding: None,
            embedding_dim: None,
            created_at: 10,
            updated_at: 11,
            labels: vec![],
            metadata: None,
        };
        let updated = repository
            .update(&mut *tx, &expect.id.unwrap(), &update)
            .await?;
        assert!(updated);
        tx.commit().await.context("error in test update commit")?;

        // find after update — created_at should be preserved from original insert (9)
        let found = repository.find(&expect.id.unwrap()).await?;
        let found_data = found.unwrap().data.unwrap();
        assert_eq!(found_data.created_at, 9);
        assert_eq!(
            found_data,
            ThreadData {
                created_at: 9,
                ..update.clone()
            }
        );

        // update_updated_at
        tx = db.begin().await.context("error in test")?;
        repository
            .update_updated_at_tx(&mut *tx, &expect.id.unwrap(), 99)
            .await?;
        tx.commit()
            .await
            .context("error in test update_updated_at commit")?;
        let found = repository.find(&expect.id.unwrap()).await?;
        assert_eq!(99, found.unwrap().data.unwrap().updated_at);

        let count = repository.count_list_tx(repository.db_pool()).await?;
        assert_eq!(1, count);

        // find_list
        let list = repository.find_list(None, None).await?;
        assert_eq!(1, list.len());

        // find_by_user_id
        let list = repository
            .find_by_user_id(
                UserId { value: 4 },
                None,
                None,
                None,
                None,
                None,
                None,
                ThreadSort::default(),
            )
            .await?;
        assert_eq!(1, list.len());

        // delete record
        tx = db.begin().await.context("error in test")?;
        let del = repository.delete_tx(&mut *tx, &expect.id.unwrap()).await?;
        tx.commit().await.context("error in test delete commit")?;
        assert!(del, "delete error");
        Ok(())
    }

    async fn _test_cascade_delete(pool: &'static RdbPool) -> Result<()> {
        use crate::infra::thread_memory::rdb::{
            ThreadMemoryRepository, ThreadMemoryRepositoryImpl,
        };

        let id_gen = crate::test_helper::shared_id_generator();
        let thread_repo = ThreadRepositoryImpl::new(id_gen.clone(), pool);
        let memory_repo = MemoryRepositoryImpl::new(id_gen, pool);
        let tm_repo = ThreadMemoryRepositoryImpl::new(pool);
        let db = thread_repo.db_pool();

        // Create a thread
        let thread_data = ThreadData {
            default_system_memory_id: None,
            user_id: Some(UserId { value: 1 }),
            description: Some("cascade test".to_string()),
            channel: None,
            embedding: None,
            embedding_dim: None,
            created_at: 100,
            updated_at: 100,
            labels: vec![],
            metadata: None,
        };
        let mut tx = db.begin().await.context("begin")?;
        let thread_id = thread_repo.create(&mut *tx, &thread_data).await?;
        tx.commit().await.context("commit thread")?;

        // Create memories and register them in the thread_memory junction,
        // which is the sole source of truth for thread membership after Phase 4.
        for i in 0..3 {
            let mem = MemoryData {
                parent_ids: vec![],
                user_id: Some(UserId { value: 1 }),
                content: format!("memory {}", i),
                content_type: 0,
                params: None,
                metadata: None,
                created_at: 100,
                updated_at: 100,
                role: 0,
                external_id: None,
                media_object_id: None,
                thread_ids: Vec::new(),
            };
            let mut tx = db.begin().await.context("begin mem")?;
            let mid = memory_repo.create(&mut *tx, &mem).await?;
            tm_repo
                .insert_tx(&mut *tx, thread_id.value, mid.value, i, 100)
                .await?;
            tx.commit().await.context("commit mem")?;
        }

        // Verify memories exist via the junction-backed reader.
        let mems = memory_repo
            .find_by_thread_id(thread_id.value, None, None, &[], &[], false)
            .await?;
        assert_eq!(3, mems.len());

        // Cascade delete via the junction-first path: walk the junction to
        // discover member memory ids, then delete the memories, the junction
        // rows, and finally the thread itself.
        let mut tx = db.begin().await.context("begin cascade")?;
        let mem_ids = tm_repo
            .find_memory_ids_by_thread_tx(&mut *tx, thread_id.value)
            .await?;
        assert_eq!(3, mem_ids.len());
        for mid in &mem_ids {
            memory_repo.delete_tx(&mut *tx, mid).await?;
        }
        tm_repo
            .delete_by_thread_tx(&mut *tx, thread_id.value)
            .await?;
        let thread_deleted = thread_repo.delete_tx(&mut *tx, &thread_id).await?;
        assert!(thread_deleted);
        tx.commit().await.context("commit cascade")?;

        // Verify all gone
        let mems = memory_repo
            .find_by_thread_id(thread_id.value, None, None, &[], &[], false)
            .await?;
        assert!(mems.is_empty());
        let thread = thread_repo.find(&thread_id).await?;
        assert!(thread.is_none());

        Ok(())
    }

    async fn _test_updated_at_auto_fill_on_update(pool: &'static RdbPool) -> Result<()> {
        let repository = ThreadRepositoryImpl::new(crate::test_helper::shared_id_generator(), pool);
        let db = repository.db_pool();

        let data = ThreadData {
            default_system_memory_id: Some(1),
            user_id: Some(UserId { value: 1 }),
            description: Some("update timestamp test".to_string()),
            channel: None,
            embedding: None,
            embedding_dim: None,
            created_at: 100,
            updated_at: 100,
            labels: vec![],
            metadata: None,
        };

        let mut tx = db.begin().await.context("begin")?;
        let id = repository.create(&mut *tx, &data).await?;
        tx.commit().await.context("commit")?;

        // Update with updated_at=0 should auto-fill server timestamp
        let update_data = ThreadData {
            updated_at: 0,
            created_at: 0,
            description: Some("updated thread".to_string()),
            ..data.clone()
        };
        let before = command_utils::util::datetime::now_millis();
        let mut tx = db.begin().await.context("begin")?;
        repository.update(&mut *tx, &id, &update_data).await?;
        tx.commit().await.context("commit")?;
        let after = command_utils::util::datetime::now_millis();

        let found = repository.find(&id).await?.expect("should exist");
        let found_data = found.data.unwrap();
        assert!(
            found_data.updated_at >= before && found_data.updated_at <= after,
            "updated_at should be server timestamp, got {}",
            found_data.updated_at
        );
        // created_at should be preserved from original insert, not changed by update
        assert_eq!(
            found_data.created_at, 100,
            "created_at should be preserved from original insert, got {}",
            found_data.created_at
        );

        // cleanup
        repository.delete(&id).await?;
        Ok(())
    }

    async fn _test_created_at_auto_fill(pool: &'static RdbPool) -> Result<()> {
        let repository = ThreadRepositoryImpl::new(crate::test_helper::shared_id_generator(), pool);
        let db = repository.db_pool();

        // created_at=0 should be auto-filled by server
        let data = ThreadData {
            default_system_memory_id: Some(1),
            user_id: Some(UserId { value: 1 }),
            description: Some("auto timestamp test".to_string()),
            channel: None,
            embedding: None,
            embedding_dim: None,
            created_at: 0,
            updated_at: 0,
            labels: vec![],
            metadata: None,
        };

        let before = command_utils::util::datetime::now_millis();
        let mut tx = db.begin().await.context("begin")?;
        let id = repository.create(&mut *tx, &data).await?;
        tx.commit().await.context("commit")?;
        let after = command_utils::util::datetime::now_millis();

        let found = repository.find(&id).await?.expect("should exist");
        let found_data = found.data.unwrap();
        assert!(
            found_data.created_at >= before && found_data.created_at <= after,
            "created_at should be server timestamp, got {}",
            found_data.created_at
        );
        assert!(
            found_data.updated_at >= before && found_data.updated_at <= after,
            "updated_at should be server timestamp, got {}",
            found_data.updated_at
        );

        // Client-supplied non-zero value should be preserved
        let data2 = ThreadData {
            created_at: 12345,
            updated_at: 67890,
            description: Some("explicit timestamp test".to_string()),
            ..data.clone()
        };
        let mut tx = db.begin().await.context("begin")?;
        let id2 = repository.create(&mut *tx, &data2).await?;
        tx.commit().await.context("commit")?;

        let found2 = repository.find(&id2).await?.expect("should exist");
        let found_data2 = found2.data.unwrap();
        assert_eq!(found_data2.created_at, 12345);
        assert_eq!(found_data2.updated_at, 67890);

        // cleanup
        repository.delete(&id).await?;
        repository.delete(&id2).await?;
        Ok(())
    }

    fn setup_pool() -> impl std::future::Future<Output = &'static RdbPool> {
        use infra_utils::infra::test::setup_test_rdb_from;
        async {
            if cfg!(feature = "postgres") {
                let pool = setup_test_rdb_from("sql/postgres").await;
                sqlx::query("TRUNCATE TABLE thread, memory, thread_memory CASCADE;")
                    .execute(pool)
                    .await
                    .unwrap();
                pool
            } else {
                let pool = setup_test_rdb_from("sql/sqlite").await;
                sqlx::query("DELETE FROM thread_memory;")
                    .execute(pool)
                    .await
                    .unwrap();
                sqlx::query("DELETE FROM thread;")
                    .execute(pool)
                    .await
                    .unwrap();
                sqlx::query("DELETE FROM memory;")
                    .execute(pool)
                    .await
                    .unwrap();
                pool
            }
        }
    }

    #[test]
    fn run_test() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let rdb_pool = setup_pool().await;
            _test_repository(rdb_pool).await
        })
    }

    #[test]
    fn run_cascade_delete_test() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let rdb_pool = setup_pool().await;
            _test_cascade_delete(rdb_pool).await
        })
    }

    #[test]
    fn run_created_at_auto_fill_test() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let rdb_pool = setup_pool().await;
            _test_created_at_auto_fill(rdb_pool).await
        })
    }

    #[test]
    fn run_updated_at_auto_fill_on_update_test() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let rdb_pool = setup_pool().await;
            _test_updated_at_auto_fill_on_update(rdb_pool).await
        })
    }

    /// Round-trip is JSON-equivalent, not byte-identical: PostgreSQL
    /// stores `metadata` as JSONB which canonicalises whitespace, may
    /// reorder keys, and drops duplicate keys. SQLite stores it as TEXT
    /// and is byte-identical, but the proto contract is the weaker
    /// JSON-equivalent guarantee so the test pins the weaker invariant.
    async fn _test_metadata_round_trip(pool: &'static RdbPool) -> Result<()> {
        let repository = ThreadRepositoryImpl::new(crate::test_helper::shared_id_generator(), pool);
        let db = repository.db_pool();

        // Whitespace deliberately included so a byte-identical assert
        // would fail on Postgres. The same JSON document survives JSONB
        // canonicalisation as long as we compare parsed values.
        let initial_metadata = Some(r#"{ "git" : { "last_commit" : "deadbeef" } }"#.to_string());
        let data = ThreadData {
            default_system_memory_id: None,
            user_id: Some(UserId { value: 1 }),
            description: Some("metadata round trip".to_string()),
            channel: None,
            embedding: None,
            embedding_dim: None,
            created_at: 100,
            updated_at: 100,
            labels: vec![],
            metadata: initial_metadata.clone(),
        };

        let assert_json_eq =
            |stored: &Option<String>, expected: &Option<String>, label: &str| match (
                stored, expected,
            ) {
                (Some(s), Some(e)) => {
                    let sv: serde_json::Value = serde_json::from_str(s).unwrap_or_else(|err| {
                        panic!("{label}: stored value is not valid JSON: {err}: {s:?}")
                    });
                    let ev: serde_json::Value = serde_json::from_str(e)
                        .unwrap_or_else(|err| panic!("{label}: expected literal not JSON: {err}"));
                    assert_eq!(sv, ev, "{label}: JSON-equivalent round-trip");
                }
                (None, None) => {}
                (a, b) => panic!("{label}: presence mismatch: stored={a:?} expected={b:?}"),
            };

        let mut tx = db.begin().await.context("begin")?;
        let id = repository.create(&mut *tx, &data).await?;
        tx.commit().await.context("commit")?;

        let found = repository.find(&id).await?.expect("created thread");
        assert_json_eq(&found.data.unwrap().metadata, &initial_metadata, "create");

        // Update metadata to a different value.
        let updated_metadata = Some(r#"{"git":{"last_branch":"main"}}"#.to_string());
        let updated = ThreadData {
            metadata: updated_metadata.clone(),
            updated_at: 200,
            ..data.clone()
        };
        let mut tx = db.begin().await.context("begin update")?;
        repository.update(&mut *tx, &id, &updated).await?;
        tx.commit().await.context("commit update")?;

        let found = repository.find(&id).await?.expect("updated thread");
        assert_json_eq(&found.data.unwrap().metadata, &updated_metadata, "update");

        // Update with metadata = None clears the column.
        let cleared = ThreadData {
            metadata: None,
            updated_at: 300,
            ..updated.clone()
        };
        let mut tx = db.begin().await.context("begin clear")?;
        repository.update(&mut *tx, &id, &cleared).await?;
        tx.commit().await.context("commit clear")?;

        let found = repository.find(&id).await?.expect("cleared thread");
        assert_eq!(found.data.unwrap().metadata, None);

        repository.delete(&id).await?;
        Ok(())
    }

    #[test]
    fn run_metadata_round_trip_test() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let rdb_pool = setup_pool().await;
            _test_metadata_round_trip(rdb_pool).await
        })
    }

    /// Helper for the P5 `find_thread_ids_by_filter` tests below: insert a
    /// fresh thread with explicit attribute values (created_at / updated_at
    /// in particular) and return its id. Calling `repository.create` would
    /// auto-fill the timestamps via `fill_timestamps`, defeating the
    /// time-range tests.
    #[allow(clippy::too_many_arguments)]
    async fn insert_thread_raw(
        pool: &'static RdbPool,
        id: i64,
        user_id: i64,
        channel: Option<&str>,
        description: Option<&str>,
        created_at: i64,
        updated_at: i64,
    ) -> Result<()> {
        sqlx::query::<infra_utils::infra::rdb::Rdb>(super::INSERT_SQL)
            .bind(id)
            .bind(None::<i64>) // default_system_memory_id
            .bind(user_id)
            .bind(description)
            .bind(channel)
            .bind(None::<Vec<u8>>) // embedding
            .bind(None::<i32>) // embedding_dim
            .bind(created_at)
            .bind(updated_at)
            .bind(None::<String>) // metadata
            .execute(pool)
            .await?;
        Ok(())
    }

    /// P5: `find_thread_ids_by_filter` must combine all seven non-label
    /// conditions with AND and return only ids (no full Thread fetch).
    /// Tests cover individual fields, combined fields, the empty-filter
    /// case (= all threads under `max + 1`), and the `max + 1` overfetch
    /// idiom that the app layer uses to detect "exceeds max".
    async fn _test_find_thread_ids_by_filter(pool: &'static RdbPool) -> Result<()> {
        let repo = ThreadRepositoryImpl::new(crate::test_helper::shared_id_generator(), pool);

        // Five threads with distinct attribute combinations.
        insert_thread_raw(pool, 9_100_001, 1, Some("alpha"), Some("a1"), 100, 200).await?;
        insert_thread_raw(pool, 9_100_002, 1, Some("beta"), Some("a2"), 150, 250).await?;
        insert_thread_raw(pool, 9_100_003, 2, Some("alpha"), Some("b1"), 100, 200).await?;
        insert_thread_raw(pool, 9_100_004, 2, Some("alpha"), Some("b2"), 300, 400).await?;
        insert_thread_raw(pool, 9_100_005, 3, None, Some("c1"), 50, 60).await?;

        let max = 100i64;
        let to_set = |v: Vec<i64>| v.into_iter().collect::<std::collections::HashSet<_>>();

        // user_id only.
        let by_user = repo
            .find_thread_ids_by_filter(Some(1), None, None, None, None, None, max)
            .await?;
        assert_eq!(to_set(by_user), to_set(vec![9_100_001, 9_100_002]));

        // channel only.
        let by_channel = repo
            .find_thread_ids_by_filter(None, Some("alpha"), None, None, None, None, max)
            .await?;
        assert_eq!(
            to_set(by_channel),
            to_set(vec![9_100_001, 9_100_003, 9_100_004])
        );

        // user_id + channel (intersection).
        let by_uc = repo
            .find_thread_ids_by_filter(Some(2), Some("alpha"), None, None, None, None, max)
            .await?;
        assert_eq!(to_set(by_uc), to_set(vec![9_100_003, 9_100_004]));

        // created_after — strictly greater (not >=).
        let after_100 = repo
            .find_thread_ids_by_filter(None, None, Some(100), None, None, None, max)
            .await?;
        assert_eq!(to_set(after_100), to_set(vec![9_100_002, 9_100_004]));

        // created_before + updated_after combined.
        let combo = repo
            .find_thread_ids_by_filter(None, None, None, Some(200), Some(100), None, max)
            .await?;
        assert_eq!(to_set(combo), to_set(vec![9_100_001, 9_100_002, 9_100_003]));

        // No conditions returns every thread (capped by max + 1).
        let none = repo
            .find_thread_ids_by_filter(None, None, None, None, None, None, max)
            .await?;
        assert_eq!(none.len(), 5);

        // `max + 1` overfetch: ask for max=2 against 5 rows, get 3 (the
        // caller uses len > max to detect "exceeds max").
        let over = repo
            .find_thread_ids_by_filter(None, None, None, None, None, None, 2)
            .await?;
        assert_eq!(over.len(), 3);

        // Cleanup
        #[cfg(feature = "postgres")]
        let cleanup_sql = "DELETE FROM thread WHERE id >= $1 AND id < $2";
        #[cfg(not(feature = "postgres"))]
        let cleanup_sql = "DELETE FROM thread WHERE id >= ? AND id < ?";
        sqlx::query::<infra_utils::infra::rdb::Rdb>(cleanup_sql)
            .bind(9_100_000_i64)
            .bind(9_200_000_i64)
            .execute(pool)
            .await?;
        Ok(())
    }

    #[test]
    fn run_find_thread_ids_by_filter_test() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let rdb_pool = setup_pool().await;
            _test_find_thread_ids_by_filter(rdb_pool).await
        })
    }

    /// P1 (improve-search): `find_thread_summaries` must
    ///   (a) return one entry per matched id (with the thread's user_id +
    ///       description),
    ///   (b) preserve `description = None` for threads whose description
    ///       column is NULL (orphan-vs-placeholder distinction),
    ///   (c) silently omit ids that don't exist (orphan thread case).
    async fn _test_find_thread_summaries(pool: &'static RdbPool) -> Result<()> {
        let repo = ThreadRepositoryImpl::new(crate::test_helper::shared_id_generator(), pool);

        let id_with_desc: i64 = 9_200_001;
        let id_with_null_desc: i64 = 9_200_002;
        let id_missing: i64 = 9_200_999; // not inserted (orphan)

        insert_thread_raw(pool, id_with_desc, 11, None, Some("hello"), 1000, 2000).await?;
        insert_thread_raw(pool, id_with_null_desc, 12, None, None, 1100, 2100).await?;

        // Empty input short-circuits.
        let empty = repo.find_thread_summaries(&[]).await?;
        assert!(empty.is_empty());

        let map = repo
            .find_thread_summaries(&[id_with_desc, id_with_null_desc, id_missing])
            .await?;
        assert_eq!(map.len(), 2, "missing id must be silently absent");
        assert_eq!(
            map.get(&id_with_desc),
            Some(&ThreadSummary {
                user_id: 11,
                description: Some("hello".to_string()),
            })
        );
        assert_eq!(
            map.get(&id_with_null_desc),
            Some(&ThreadSummary {
                user_id: 12,
                description: None,
            })
        );

        // Cleanup
        #[cfg(feature = "postgres")]
        let cleanup_sql = "DELETE FROM thread WHERE id >= $1 AND id < $2";
        #[cfg(not(feature = "postgres"))]
        let cleanup_sql = "DELETE FROM thread WHERE id >= ? AND id < ?";
        sqlx::query::<infra_utils::infra::rdb::Rdb>(cleanup_sql)
            .bind(9_200_000_i64)
            .bind(9_300_000_i64)
            .execute(pool)
            .await?;
        Ok(())
    }

    #[test]
    fn run_find_thread_summaries_test() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let rdb_pool = setup_pool().await;
            _test_find_thread_summaries(rdb_pool).await
        })
    }
}
