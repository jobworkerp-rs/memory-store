use super::rows::{MemoryRow, MemoryWithOptionalPositionRow, MemoryWithPositionRow};
use crate::error::LlmMemoryError;
use crate::infra::IdGeneratorWrapper;
use crate::infra::UseIdGenerator;
use crate::sql::{IN_LIST_CHUNK_SIZE, memory_columns, memory_qualified_columns, p, p_jsonb};
use anyhow::{Context, Result};
use async_trait::async_trait;
use infra_utils::infra::rdb::Rdb;
use infra_utils::infra::rdb::RdbPool;
use infra_utils::infra::rdb::UseRdbPool;
use itertools::Itertools;
use protobuf::llm_memory::data::{Memory, MemoryData, MemoryId, ThreadId, UserId};
use sqlx::Executor;
use std::collections::HashMap;

#[derive(Debug, Clone, Copy, Default)]
pub struct UpdatedAtRange {
    pub updated_after: Option<i64>,
    pub updated_before: Option<i64>,
}

/// Symmetrical to `UpdatedAtRange`; bounds AND-combine with the rest of
/// the condition. Lower bound is strict (`>`), upper is inclusive (`<=`)
/// to match the `service/memory.proto` contract.
#[derive(Debug, Clone, Copy, Default)]
pub struct CreatedAtRange {
    pub created_after: Option<i64>,
    pub created_before: Option<i64>,
}

/// Sort key applied to `find_list_by_condition`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MemorySort {
    /// Matches the proto `MemoryListSort::Unspecified` wire default. `id`
    /// (snowflake) is the secondary key so paginated reads stay stable
    /// when `updated_at` collides.
    #[default]
    UpdatedDesc,
    UpdatedAsc,
    CreatedDesc,
    CreatedAsc,
    /// Pure `id DESC` — exposed because snowflake id ≈ creation order
    /// with monotonic guarantees, so callers can pin to a non-mutable
    /// timeline that `updated_at` cannot offer.
    IdDesc,
}

impl From<i32> for MemorySort {
    /// Map a proto `MemoryListSort` discriminant to the infra enum.
    /// Unknown values fall through to `UpdatedDesc` so newer clients
    /// querying older servers degrade gracefully. Discriminants are
    /// pinned by `data/common.proto::MemoryListSort`.
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

// -- INSERT / UPDATE / DELETE --
//
// Phase 3 note: the legacy `memory.system_id` column was dropped together
// with the `system_prompt` table. System prompts are now plain ROLE_SYSTEM
// memory rows referenced via `parent_ids`.
//
// Phase 4 note: the legacy `memory.thread_id` column was also dropped.
// Thread membership is tracked exclusively in the `thread_memory` junction
// table, which also carries the conversation `position` used by the
// parent_ids traversal at execution time.
const INSERT_SQL: &str = concat!(
    "INSERT INTO memory (id, parent_ids, user_id, content, content_type, ",
    "params, metadata, created_at, updated_at, role, external_id, media_object_id) VALUES (",
    p!(1),
    ",",
    p_jsonb!(2),
    ",",
    p!(3),
    ",",
    p!(4),
    ",",
    p!(5),
    ",",
    p_jsonb!(6),
    ",",
    p_jsonb!(7),
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
    ")"
);

const UPDATE_SQL: &str = concat!(
    "UPDATE memory SET parent_ids = ",
    p_jsonb!(1),
    ", user_id = ",
    p!(2),
    ", content = ",
    p!(3),
    ", content_type = ",
    p!(4),
    ", params = ",
    p_jsonb!(5),
    ", metadata = ",
    p_jsonb!(6),
    ", updated_at = ",
    p!(7),
    ", role = ",
    p!(8),
    ", media_object_id = ",
    p!(9),
    " WHERE id = ",
    p!(10),
    ";"
);

// content + updated_at ONLY (UpdateContentNoDispatch / caption workflow).
// Deliberately does NOT touch media_object_id / parent_ids / role so the
// caption write cannot drop other fields (design 2/3 §7.5.3).
const UPDATE_CONTENT_ONLY_SQL: &str = concat!(
    "UPDATE memory SET content = ",
    p!(1),
    ", updated_at = ",
    p!(2),
    " WHERE id = ",
    p!(3),
    ";"
);

const UPDATE_PARENT_IDS_SQL: &str = concat!(
    "UPDATE memory SET parent_ids = ",
    p_jsonb!(1),
    " WHERE id = ",
    p!(2),
    ";"
);

const DELETE_SQL: &str = concat!("DELETE FROM memory WHERE id = ", p!(1), ";");

#[cfg(feature = "postgres")]
const FIND_BY_IDS_FOR_UPDATE_SUFFIX: &str = " FOR UPDATE";

#[cfg(not(feature = "postgres"))]
const FIND_BY_IDS_FOR_UPDATE_SUFFIX: &str = "";

// -- SELECT queries --
const FIND_SQL: &str = concat!(
    "SELECT ",
    memory_columns!(),
    " FROM memory WHERE id = ",
    p!(1),
    ";"
);

const FIND_BY_EXTERNAL_ID_SQL: &str = concat!(
    "SELECT ",
    memory_columns!(),
    " FROM memory WHERE external_id = ",
    p!(1),
    ";"
);

const FIND_LIST_LIMIT_SQL: &str = concat!(
    "SELECT ",
    memory_columns!(),
    " FROM memory ORDER BY id DESC LIMIT ",
    p!(1),
    " OFFSET ",
    p!(2),
    ";"
);

const FIND_LIST_ALL_SQL: &str = concat!(
    "SELECT ",
    memory_columns!(),
    " FROM memory ORDER BY id DESC;"
);

const COUNT_SQL: &str = "SELECT count(*) as count FROM memory;";

// Uses thread_memory junction table; pivot by position (consistent with list ordering).
// The pivot position is resolved via a subquery on (thread_id, memory_id).
const FIND_SURROUNDING_BEFORE_SQL: &str = concat!(
    "SELECT ",
    memory_qualified_columns!(),
    " FROM memory INNER JOIN thread_memory tm ON tm.memory_id = memory.id",
    " WHERE tm.thread_id = ",
    p!(1),
    " AND tm.position < (SELECT position FROM thread_memory WHERE thread_id = ",
    p!(2),
    " AND memory_id = ",
    p!(3),
    ")",
    " ORDER BY tm.position DESC LIMIT ",
    p!(4),
    ";"
);

const FIND_SURROUNDING_AFTER_SQL: &str = concat!(
    "SELECT ",
    memory_qualified_columns!(),
    " FROM memory INNER JOIN thread_memory tm ON tm.memory_id = memory.id",
    " WHERE tm.thread_id = ",
    p!(1),
    " AND tm.position > (SELECT position FROM thread_memory WHERE thread_id = ",
    p!(2),
    " AND memory_id = ",
    p!(3),
    ")",
    " ORDER BY tm.position ASC LIMIT ",
    p!(4),
    ";"
);

#[async_trait]
pub trait MemoryRepository: UseRdbPool + UseIdGenerator + Sync + Send {
    async fn create<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        memory: &MemoryData,
    ) -> Result<MemoryId> {
        let id: i64 = self.id_generator().generate_id()?;
        let (created_at, updated_at) =
            crate::infra::fill_timestamps(memory.created_at, memory.updated_at);
        let res = sqlx::query::<Rdb>(INSERT_SQL)
            .bind(id)
            .bind(serde_json::to_string(
                &memory
                    .parent_ids
                    .iter()
                    .map(|p| p.value)
                    .collect::<Vec<_>>(),
            )?)
            .bind(memory.user_id.map(|u| u.value).unwrap_or(0))
            .bind(&memory.content)
            .bind(memory.content_type)
            .bind(&memory.params)
            .bind(&memory.metadata)
            .bind(created_at)
            .bind(updated_at)
            .bind(memory.role)
            .bind(&memory.external_id)
            .bind(memory.media_object_id.map(|m| m.value))
            .execute(tx)
            .await
            .map_err(map_create_memory_error)?;
        if res.rows_affected() > 0 {
            Ok(MemoryId { value: id })
        } else {
            Err(LlmMemoryError::RuntimeError(format!(
                "Cannot insert memory (logic error?): {:?}",
                memory
            ))
            .into())
        }
    }

    async fn update<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        id: &MemoryId,
        memory: &MemoryData,
    ) -> Result<bool> {
        let updated_at = crate::infra::fill_updated_at(memory.updated_at);
        sqlx::query(UPDATE_SQL)
            .bind(serde_json::to_string(
                &memory
                    .parent_ids
                    .iter()
                    .map(|p| p.value)
                    .collect::<Vec<_>>(),
            )?)
            .bind(memory.user_id.map(|u| u.value).unwrap_or(0))
            .bind(&memory.content)
            .bind(memory.content_type)
            .bind(&memory.params)
            .bind(&memory.metadata)
            .bind(updated_at)
            .bind(memory.role)
            .bind(memory.media_object_id.map(|m| m.value))
            .bind(id.value)
            .execute(tx)
            .await
            .map(|r| r.rows_affected() > 0)
            .map_err(LlmMemoryError::DBError)
            .context(format!("error in update: id = {}", id.value))
    }

    /// Partially update only the parent_ids field of a memory.
    async fn update_parent_ids<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        id: &MemoryId,
        parent_ids: &[MemoryId],
    ) -> Result<bool> {
        sqlx::query(UPDATE_PARENT_IDS_SQL)
            .bind(serde_json::to_string(
                &parent_ids.iter().map(|p| p.value).collect::<Vec<_>>(),
            )?)
            .bind(id.value)
            .execute(tx)
            .await
            .map(|r| r.rows_affected() > 0)
            .map_err(LlmMemoryError::DBError)
            .context(format!("error in update_parent_ids: id = {}", id.value))
    }

    /// Replace ONLY `content` (and `updated_at`, server-filled) in the
    /// same tx. media_object_id / parent_ids / role are untouched. Backs
    /// `MemoryService.UpdateContentNoDispatch` — the caption workflow path
    /// that must not drop other fields nor re-spawn the dispatcher
    /// (design 2/3 §7.5.3).
    async fn update_content_only<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        id: &MemoryId,
        content: &str,
    ) -> Result<bool> {
        let updated_at = crate::infra::fill_updated_at(0);
        sqlx::query(UPDATE_CONTENT_ONLY_SQL)
            .bind(content)
            .bind(updated_at)
            .bind(id.value)
            .execute(tx)
            .await
            .map(|r| r.rows_affected() > 0)
            .map_err(LlmMemoryError::DBError)
            .context(format!("error in update_content_only: id = {}", id.value))
    }

    async fn delete(&self, id: &MemoryId) -> Result<bool> {
        self.delete_tx(self.db_pool(), id).await
    }

    async fn delete_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        id: &MemoryId,
    ) -> Result<bool> {
        let del = sqlx::query::<Rdb>(DELETE_SQL)
            .bind(id.value)
            .execute(tx)
            .await
            .map(|r| r.rows_affected() > 0)
            .map_err(LlmMemoryError::DBError)?;
        Ok(del)
    }

    async fn fill_thread_ids(&self, memories: &mut [Memory]) -> Result<()> {
        if memories.is_empty() {
            return Ok(());
        }
        let memory_ids = memory_ids_from(memories);
        if memory_ids.is_empty() {
            return Ok(());
        }
        let mut rows = Vec::new();
        for chunk in memory_ids.chunks(IN_LIST_CHUNK_SIZE) {
            let placeholders = crate::sql::build_in_placeholders(chunk.len(), 1);
            let sql = format!(
                "SELECT memory_id, thread_id FROM thread_memory \
                 WHERE memory_id IN ({placeholders}) \
                 ORDER BY memory_id ASC, thread_id ASC"
            );
            let mut query = sqlx::query_as::<Rdb, (i64, i64)>(sqlx::AssertSqlSafe(sql));
            for id in chunk {
                query = query.bind(*id);
            }
            rows.extend(
                query
                    .fetch_all(self.db_pool())
                    .await
                    .map_err(LlmMemoryError::DBError)
                    .context("error in fill_thread_ids")?,
            );
        }
        apply_thread_ids(memories, rows);
        Ok(())
    }

    async fn fill_thread_ids_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Rdb>,
        memories: &mut [Memory],
    ) -> Result<()> {
        if memories.is_empty() {
            return Ok(());
        }
        let memory_ids = memory_ids_from(memories);
        if memory_ids.is_empty() {
            return Ok(());
        }
        let mut rows = Vec::new();
        for chunk in memory_ids.chunks(IN_LIST_CHUNK_SIZE) {
            let placeholders = crate::sql::build_in_placeholders(chunk.len(), 1);
            let sql = format!(
                "SELECT memory_id, thread_id FROM thread_memory \
                 WHERE memory_id IN ({placeholders}) \
                 ORDER BY memory_id ASC, thread_id ASC"
            );
            let mut query = sqlx::query_as::<Rdb, (i64, i64)>(sqlx::AssertSqlSafe(sql));
            for id in chunk {
                query = query.bind(*id);
            }
            rows.extend(
                query
                    .fetch_all(&mut **tx)
                    .await
                    .map_err(LlmMemoryError::DBError)
                    .context("error in fill_thread_ids_tx")?,
            );
        }
        apply_thread_ids(memories, rows);
        Ok(())
    }

    async fn find(&self, id: &MemoryId, fill_thread_ids: bool) -> Result<Option<Memory>> {
        let mut memory = self
            .find_row_tx(self.db_pool(), id)
            .await?
            .map(|r| r.to_proto());
        if fill_thread_ids && let Some(m) = memory.as_mut() {
            self.fill_thread_ids(std::slice::from_mut(m)).await?;
        }
        Ok(memory)
    }

    async fn find_by_external_id(&self, external_id: &str) -> Result<Option<Memory>> {
        sqlx::query_as::<Rdb, MemoryRow>(FIND_BY_EXTERNAL_ID_SQL)
            .bind(external_id)
            .fetch_optional(self.db_pool())
            .await
            .map(|r| r.map(|r2| r2.to_proto()))
            .map_err(LlmMemoryError::DBError)
            .context(format!("error in find_by_external_id: {}", external_id))
    }

    /// Bulk variant of `find_by_external_id` joined with `thread_memory`
    /// for the given thread. Each returned tuple is
    /// `(memory, Option<position>)` — `Some(p)` when the memory is
    /// attached under `thread_id`, `None` when the memory exists but
    /// lives in another thread (cross-thread collision detection).
    /// Chunks the IN list to stay within `SQLITE_MAX_VARIABLE_NUMBER`.
    /// Used by `ThreadApp::add_memories_batch`.
    async fn find_by_external_ids_with_position_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Rdb>,
        thread_id: i64,
        external_ids: &[String],
    ) -> Result<Vec<(Memory, Option<i32>)>> {
        if external_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut all = Vec::with_capacity(external_ids.len());
        for chunk in external_ids.chunks(IN_LIST_CHUNK_SIZE) {
            // Placeholder ordering must follow the SQL textual order so
            // SQLite's positional `?` binds line up: `tm.thread_id = $1`
            // appears before the IN list, so thread_id is bound FIRST.
            let thread_id_ph = crate::sql::dyn_placeholder(1);
            let placeholders = crate::sql::build_in_placeholders(chunk.len(), 2);
            let sql = format!(
                "SELECT {}, tm.position AS attached_position \
                 FROM memory \
                 LEFT JOIN thread_memory tm \
                   ON tm.memory_id = memory.id AND tm.thread_id = {thread_id_ph} \
                 WHERE memory.external_id IN ({placeholders})",
                memory_qualified_columns!()
            );
            let mut q =
                sqlx::query_as::<Rdb, MemoryWithOptionalPositionRow>(sqlx::AssertSqlSafe(sql))
                    .bind(thread_id);
            for eid in chunk {
                q = q.bind(eid);
            }
            let rows = q
                .fetch_all(&mut **tx)
                .await
                .map_err(LlmMemoryError::DBError)
                .context("error in find_by_external_ids_with_position_tx")?;
            all.extend(rows.into_iter().map(|r| r.into_proto_with_position()));
        }
        Ok(all)
    }

    async fn find_row_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        id: &MemoryId,
    ) -> Result<Option<MemoryRow>> {
        sqlx::query_as::<Rdb, MemoryRow>(FIND_SQL)
            .bind(id.value)
            .fetch_optional(tx)
            .await
            .map_err(LlmMemoryError::DBError)
            .context(format!("error in find: id = {}", id.value))
    }

    async fn find_list(
        &self,
        limit: Option<&i32>,
        offset: Option<&i64>,
        fill_thread_ids: bool,
    ) -> Result<Vec<Memory>> {
        let mut memories: Vec<Memory> = self
            .find_row_list_tx(self.db_pool(), limit, offset)
            .await?
            .into_iter()
            .map(|r| r.to_proto())
            .collect_vec();
        if fill_thread_ids {
            self.fill_thread_ids(&mut memories).await?;
        }
        Ok(memories)
    }

    async fn find_row_list_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        limit: Option<&i32>,
        offset: Option<&i64>,
    ) -> Result<Vec<MemoryRow>> {
        if let Some(l) = limit {
            sqlx::query_as::<_, MemoryRow>(FIND_LIST_LIMIT_SQL)
                .bind(l)
                .bind(offset.unwrap_or(&0i64))
                .fetch_all(tx)
        } else {
            sqlx::query_as::<_, MemoryRow>(FIND_LIST_ALL_SQL).fetch_all(tx)
        }
        .await
        .map_err(LlmMemoryError::DBError)
        .context(format!("error in find_list: ({:?}, {:?})", limit, offset))
    }

    async fn find_recent_list_by_user_id(
        &self,
        user_id: UserId,
        limit: Option<&i32>,
        updated_at_range: UpdatedAtRange,
        fill_thread_ids: bool,
    ) -> Result<Vec<Memory>> {
        let mut memories: Vec<Memory> = self
            .find_recent_row_list_by_user_id_tx(self.db_pool(), user_id, limit, updated_at_range)
            .await?
            .into_iter()
            .map(|r| r.to_proto())
            .collect_vec();
        if fill_thread_ids {
            self.fill_thread_ids(&mut memories).await?;
        }
        Ok(memories)
    }

    async fn find_recent_row_list_by_user_id_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        user_id: UserId,
        limit: Option<&i32>,
        updated_at_range: UpdatedAtRange,
    ) -> Result<Vec<MemoryRow>> {
        let mut clauses = vec!["user_id = ".to_string() + &crate::sql::build_in_placeholders(1, 1)];
        let mut binds = vec![ConditionBind::I64(user_id.value)];
        append_timestamp_range_clauses(
            &mut clauses,
            &mut binds,
            "updated_at",
            updated_at_range.updated_after,
            updated_at_range.updated_before,
        );
        let mut sql = format!(
            "SELECT {} FROM memory WHERE {} ORDER BY updated_at DESC",
            memory_columns!(),
            clauses.join(" AND ")
        );
        if limit.is_some() {
            sql.push_str(&format!(
                " LIMIT {}",
                crate::sql::build_in_placeholders(1, binds.len() + 1)
            ));
        }
        let mut query = sqlx::query_as::<_, MemoryRow>(sqlx::AssertSqlSafe(sql));
        for v in &binds {
            query = match v {
                ConditionBind::I32(n) => query.bind(*n),
                ConditionBind::I64(n) => query.bind(*n),
                ConditionBind::Str(s) => query.bind(s.as_str()),
            };
        }
        if let Some(l) = limit {
            query = query.bind(l);
        }
        query
            .fetch_all(tx)
            .await
            .map_err(LlmMemoryError::DBError)
            .context(format!(
                "error in find_recent_list_by_user_id: ({:?}, {:?}, {:?}, {:?})",
                user_id, limit, updated_at_range.updated_after, updated_at_range.updated_before
            ))
    }

    async fn count_list_tx<'c, E: Executor<'c, Database = Rdb>>(&self, tx: E) -> Result<i64> {
        sqlx::query_scalar(COUNT_SQL)
            .fetch_one(tx)
            .await
            .map_err(LlmMemoryError::DBError)
            .context("error in count_list".to_string())
    }

    /// Batch-fetch memories by a list of IDs (for vector search result hydration).
    /// Chunks IDs to stay within DB bind-parameter limits (SQLite: 999, PostgreSQL: 65535).
    ///
    /// Reads from `self.db_pool()` — use `find_by_ids_tx` if you need to read
    /// data inside an open transaction (for example, to see your own uncommitted
    /// writes or to guarantee read-time consistency with a sibling write).
    async fn find_by_ids(&self, ids: &[i64], fill_thread_ids: bool) -> Result<Vec<Memory>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut all_results = Vec::with_capacity(ids.len());
        for chunk in ids.chunks(IN_LIST_CHUNK_SIZE) {
            let placeholders = crate::sql::build_in_placeholders(chunk.len(), 1);
            let sql = format!(
                "SELECT {} FROM memory WHERE id IN ({}) ORDER BY id DESC",
                memory_columns!(),
                placeholders
            );
            let mut query = sqlx::query_as::<Rdb, MemoryRow>(sqlx::AssertSqlSafe(sql));
            for id in chunk {
                query = query.bind(id);
            }
            let rows = query
                .fetch_all(self.db_pool())
                .await
                .map_err(LlmMemoryError::DBError)
                .context("error in find_by_ids")?;
            all_results.extend(rows.iter().map(|r| r.to_proto()));
        }
        if fill_thread_ids {
            self.fill_thread_ids(&mut all_results).await?;
        }
        Ok(all_results)
    }

    /// Transaction-aware variant of `find_by_ids`.
    ///
    /// Reads inside the caller's transaction so that uncommitted writes from
    /// the same transaction are visible (and so that read-time consistency is
    /// preserved against sibling writes that may commit between calls).
    ///
    /// Chunked the same way as `find_by_ids` (`sql::IN_LIST_CHUNK_SIZE`) so it can
    /// handle conversation histories of arbitrary length without an
    /// implementation-imposed cap. We re-borrow the `&mut Transaction` on
    /// every chunk via `&mut **tx`, which works because sqlx implements
    /// `Executor` for `&mut Transaction<'_, DB>`.
    async fn find_by_ids_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Rdb>,
        ids: &[i64],
        fill_thread_ids: bool,
    ) -> Result<Vec<Memory>> {
        self.find_by_ids_internal_tx(tx, ids, false, fill_thread_ids)
            .await
    }

    /// Variant of `find_by_ids_tx` that acquires row locks on PostgreSQL.
    ///
    /// SQLite ignores row-level `FOR UPDATE`, so the query falls back to a
    /// plain SELECT there. The project already relies on SQLite's
    /// single-writer semantics for write/write serialization.
    async fn find_by_ids_for_update_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Rdb>,
        ids: &[i64],
    ) -> Result<Vec<Memory>> {
        self.find_by_ids_internal_tx(tx, ids, true, false).await
    }

    async fn find_by_ids_internal_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Rdb>,
        ids: &[i64],
        for_update: bool,
        fill_thread_ids: bool,
    ) -> Result<Vec<Memory>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut all_results = Vec::with_capacity(ids.len());
        for chunk in ids.chunks(IN_LIST_CHUNK_SIZE) {
            let placeholders = crate::sql::build_in_placeholders(chunk.len(), 1);
            let lock_suffix = if for_update {
                FIND_BY_IDS_FOR_UPDATE_SUFFIX
            } else {
                ""
            };
            let sql = format!(
                "SELECT {} FROM memory WHERE id IN ({}) ORDER BY id DESC{}",
                memory_columns!(),
                placeholders,
                lock_suffix
            );
            let mut query = sqlx::query_as::<Rdb, MemoryRow>(sqlx::AssertSqlSafe(sql));
            for id in chunk {
                query = query.bind(id);
            }
            // Re-borrow the transaction for each chunk. `&mut **tx` reaches
            // through `&mut Transaction` to a fresh `&mut Transaction`, which
            // sqlx accepts as an Executor.
            let rows = query
                .fetch_all(&mut **tx)
                .await
                .map_err(LlmMemoryError::DBError)
                .context("error in find_by_ids_tx")?;
            all_results.extend(rows.iter().map(|r| r.to_proto()));
        }
        if fill_thread_ids {
            self.fill_thread_ids_tx(tx, &mut all_results).await?;
        }
        Ok(all_results)
    }

    /// Delete the subset of `ids` that is no longer referenced by any row in
    /// `thread_memory`, returning the ids that were actually deleted.
    ///
    /// The orphan check is evaluated inside the DELETE statement itself so the
    /// decision is made against the statement snapshot rather than a stale
    /// precomputed list.
    async fn delete_orphaned_by_ids_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Rdb>,
        ids: &[i64],
    ) -> Result<Vec<i64>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut deleted_ids = Vec::new();
        for chunk in ids.chunks(IN_LIST_CHUNK_SIZE) {
            let placeholders = crate::sql::build_in_placeholders(chunk.len(), 1);
            let sql = format!(
                "DELETE FROM memory \
                 WHERE id IN ({}) \
                   AND NOT EXISTS (\
                     SELECT 1 FROM thread_memory tm WHERE tm.memory_id = memory.id\
                   ) \
                 RETURNING id",
                placeholders
            );
            let mut query = sqlx::query_scalar::<Rdb, i64>(sqlx::AssertSqlSafe(sql));
            for id in chunk {
                query = query.bind(id);
            }
            let rows = query
                .fetch_all(&mut **tx)
                .await
                .map_err(LlmMemoryError::DBError)
                .context("error in delete_orphaned_by_ids_tx")?;
            deleted_ids.extend(rows);
        }
        Ok(deleted_ids)
    }

    /// Batch-fetch memories by a list of IDs, joining with `thread_memory`
    /// to attach each memory's conversation `position` under the given
    /// `thread_id`. Results are sorted globally by `tm.position ASC`, so
    /// the caller does not need to re-sort.
    ///
    /// Memories that are not registered in the junction under `thread_id`
    /// are silently skipped: an INNER JOIN drops them. This is how Phase 4
    /// surfaces "memory exists in the DB but is not part of this thread's
    /// conversation" — the caller can compare `ids.len()` against the
    /// returned length to detect the gap and decide how to react (usually
    /// log a warning and proceed).
    ///
    /// Chunked with `sql::IN_LIST_CHUNK_SIZE` mirrors `find_by_ids_tx` so that
    /// callers can pass arbitrary-length ancestor closures without worrying
    /// about SQL bind limits (SQLite: 999, Postgres: 65535). Each chunk
    /// comes back `ORDER BY tm.position ASC` from the DB, but chunks are
    /// arbitrary slices of the caller's id list, so positions across two
    /// chunks can interleave — we therefore do a final stable sort on the
    /// full result set before returning. For inputs within a single chunk
    /// the final sort is a no-op on already-sorted data, so the overhead
    /// is negligible; for > sql::IN_LIST_CHUNK_SIZE inputs it turns the per-chunk local
    /// order into the global `position ASC` order that
    /// `ThreadApp::resolve_ancestor_closure` relies on.
    async fn find_by_ids_with_position_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Rdb>,
        thread_id: i64,
        ids: &[i64],
        fill_thread_ids: bool,
    ) -> Result<Vec<(Memory, i32)>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut all_results: Vec<(Memory, i32)> = Vec::with_capacity(ids.len());
        for chunk in ids.chunks(IN_LIST_CHUNK_SIZE) {
            // First placeholder is `thread_id`; the remaining `chunk.len()`
            // placeholders are the IN-list. Offsets start at 2 to leave room
            // for the thread_id bind on Postgres (`$1`).
            let in_placeholders = crate::sql::build_in_placeholders(chunk.len(), 2);
            let sql = format!(
                "SELECT {}, tm.position FROM memory \
                 INNER JOIN thread_memory tm ON tm.memory_id = memory.id \
                 WHERE tm.thread_id = {} AND memory.id IN ({}) \
                 ORDER BY tm.position ASC",
                memory_qualified_columns!(),
                p!(1),
                in_placeholders
            );
            let mut query = sqlx::query_as::<Rdb, MemoryWithPositionRow>(sqlx::AssertSqlSafe(sql));
            query = query.bind(thread_id);
            for id in chunk {
                query = query.bind(id);
            }
            let rows = query
                .fetch_all(&mut **tx)
                .await
                .map_err(LlmMemoryError::DBError)
                .context("error in find_by_ids_with_position_tx")?;
            all_results.extend(rows.iter().map(|r| r.to_proto_with_position()));
        }
        // Global merge pass: per-chunk ORDER BY only sorts within a chunk,
        // so once `ids.len() > sql::IN_LIST_CHUNK_SIZE` the accumulated `all_results`
        // can still carry out-of-order chunks. Using `sort_by_key` (a
        // stable sort on the `position` field) fixes the cross-chunk
        // ordering without dropping or rearranging ties.
        all_results.sort_by_key(|(_, pos)| *pos);
        if fill_thread_ids {
            let mut memories: Vec<Memory> = all_results.iter().map(|(m, _)| m.clone()).collect();
            self.fill_thread_ids_tx(tx, &mut memories).await?;
            for ((memory, _), hydrated) in all_results.iter_mut().zip(memories) {
                *memory = hydrated;
            }
        }
        Ok(all_results)
    }

    async fn find_by_thread_id(
        &self,
        thread_id: i64,
        limit: Option<&i32>,
        offset: Option<&i64>,
        roles: &[i32],
        content_types: &[i32],
        fill_thread_ids: bool,
    ) -> Result<Vec<Memory>> {
        let MemoryConditionSql {
            from_clause,
            where_clause,
            binds,
            ..
        } = build_memory_condition_sql(
            roles,
            content_types,
            None,
            Some(thread_id),
            UpdatedAtRange::default(),
            CreatedAtRange::default(),
            None,
            None,
            None,
        );
        let mut sql = format!(
            "SELECT {} FROM {from_clause}{where_clause} ORDER BY tm.position ASC",
            memory_qualified_columns!(),
        );
        let limit_offset = limit.map(|l| (*l as i64, *offset.unwrap_or(&0i64)));
        if limit_offset.is_some() {
            append_limit_offset(&mut sql, binds.len());
        }

        let mut query = bind_condition_values(
            sqlx::query_as::<Rdb, MemoryRow>(sqlx::AssertSqlSafe(sql)),
            &binds,
        );
        if let Some((l, off)) = limit_offset {
            query = query.bind(l).bind(off);
        }

        let mut memories: Vec<Memory> = query
            .fetch_all(self.db_pool())
            .await
            .map(|r| r.into_iter().map(|r2| r2.to_proto()).collect_vec())
            .map_err(LlmMemoryError::DBError)
            .context(format!(
                "error in find_by_thread_id: thread_id = {}",
                thread_id
            ))?;
        if fill_thread_ids {
            self.fill_thread_ids(&mut memories).await?;
        }
        Ok(memories)
    }

    /// Conditional list query used by MemoryService.FindListByCondition.
    /// Supports filtering by roles (OR), user_id, and thread_id (all AND).
    /// Ordering is by id DESC (matches FindList semantics).
    ///
    /// This is the replacement for SystemPromptService.FindList now that system
    /// prompts are stored as ROLE_SYSTEM memories.
    ///
    /// Thread membership is resolved through the `thread_memory` junction
    /// table rather than the legacy `memory.thread_id` column, so shared
    /// memories introduced by Phase 4 (fork/shared) will be matched by
    /// `thread_id`-scoped queries. A memory that only has the legacy column
    /// populated (e.g. created through a code path that bypasses
    /// `ThreadApp::add_memory`) will NOT appear here.
    #[allow(clippy::too_many_arguments)]
    async fn find_list_by_condition(
        &self,
        limit: Option<&i32>,
        offset: Option<&i64>,
        roles: &[i32],
        content_types: &[i32],
        user_id: Option<i64>,
        thread_id: Option<i64>,
        updated_at_range: UpdatedAtRange,
        created_at_range: CreatedAtRange,
        external_id: Option<&str>,
        external_id_prefix: Option<&str>,
        // App layer is expected to short-circuit on `Some(empty)` before
        // we get here; the `1=0` fallback in `build_memory_condition_sql`
        // is purely defensive for direct infra-level callers (tests).
        memory_id_constraint: Option<&[i64]>,
        sort: MemorySort,
        fill_thread_ids: bool,
    ) -> Result<Vec<Memory>> {
        let joined = thread_id.is_some();
        // `id_expr` is owned by `order_by_expr` here; the keyset path
        // (`find_list_by_condition_after_id`) still consumes it.
        let MemoryConditionSql {
            from_clause,
            where_clause,
            binds,
            id_expr: _,
        } = build_memory_condition_sql(
            roles,
            content_types,
            user_id,
            thread_id,
            updated_at_range,
            created_at_range,
            external_id,
            external_id_prefix,
            memory_id_constraint,
        );
        // When joining with `thread_memory` we must qualify every SELECT column
        // to avoid ambiguity, because the junction table carries several
        // same-named columns (thread_id, created_at).
        let select_cols: &str = if joined {
            memory_qualified_columns!()
        } else {
            memory_columns!()
        };
        let order_expr = order_by_expr(sort, joined);
        let mut sql =
            format!("SELECT {select_cols} FROM {from_clause}{where_clause} ORDER BY {order_expr}",);
        // limit/offset placeholders follow the WHERE binds. We append them to
        // the SQL up-front so the bind loop below can be written exactly once
        // regardless of whether pagination was requested.
        let limit_offset = limit.map(|l| (*l as i64, *offset.unwrap_or(&0i64)));
        if limit_offset.is_some() {
            append_limit_offset(&mut sql, binds.len());
        }

        let mut query = bind_condition_values(
            sqlx::query_as::<Rdb, MemoryRow>(sqlx::AssertSqlSafe(sql)),
            &binds,
        );
        if let Some((l, off)) = limit_offset {
            query = query.bind(l).bind(off);
        }

        let mut memories: Vec<Memory> = query
            .fetch_all(self.db_pool())
            .await
            .map(|rows| rows.into_iter().map(|r| r.to_proto()).collect_vec())
            .map_err(LlmMemoryError::DBError)
            .context("error in find_list_by_condition")?;
        if fill_thread_ids {
            self.fill_thread_ids(&mut memories).await?;
        }
        Ok(memories)
    }

    /// Keyset-paginated variant of [`find_list_by_condition`]. Returns up to
    /// `limit` rows whose `id > after_id`, ordered by id ASC. This avoids
    /// the performance degradation and row-skip/duplicate issues of OFFSET
    /// pagination for large result sets.
    async fn find_list_by_condition_after_id(
        &self,
        limit: i32,
        after_id: i64,
        roles: &[i32],
        user_id: Option<i64>,
        thread_id: Option<i64>,
    ) -> Result<Vec<Memory>> {
        let MemoryConditionSql {
            from_clause,
            mut where_clause,
            mut binds,
            id_expr,
        } = build_memory_condition_sql(
            roles,
            &[],
            user_id,
            thread_id,
            UpdatedAtRange::default(),
            CreatedAtRange::default(),
            None,
            None,
            None,
        );

        // Append the keyset cursor condition.
        let ph = crate::sql::build_in_placeholders(1, binds.len() + 1);
        if where_clause.is_empty() {
            where_clause = format!(" WHERE {id_expr} > {ph}");
        } else {
            where_clause.push_str(&format!(" AND {id_expr} > {ph}"));
        }
        binds.push(ConditionBind::I64(after_id));

        // LIMIT placeholder
        let limit_ph = crate::sql::build_in_placeholders(1, binds.len() + 1);
        let select_cols: &str = if thread_id.is_some() {
            memory_qualified_columns!()
        } else {
            memory_columns!()
        };
        let sql = format!(
            "SELECT {select_cols} FROM {from_clause}{where_clause} ORDER BY {id_expr} ASC LIMIT {limit_ph}",
        );

        let mut query = bind_condition_values(
            sqlx::query_as::<Rdb, MemoryRow>(sqlx::AssertSqlSafe(sql)),
            &binds,
        );
        query = query.bind(limit as i64);

        query
            .fetch_all(self.db_pool())
            .await
            .map(|rows| rows.into_iter().map(|r| r.to_proto()).collect_vec())
            .map_err(LlmMemoryError::DBError)
            .context("error in find_list_by_condition_after_id")
    }

    /// Conditional count used by MemoryService.CountByCondition.
    /// Shares its FROM/WHERE construction with find_list_by_condition, so the
    /// same junction-table semantics apply to `thread_id`-scoped counts.
    #[allow(clippy::too_many_arguments)]
    async fn count_by_condition(
        &self,
        roles: &[i32],
        content_types: &[i32],
        user_id: Option<i64>,
        thread_id: Option<i64>,
        updated_at_range: UpdatedAtRange,
        created_at_range: CreatedAtRange,
        external_id: Option<&str>,
        external_id_prefix: Option<&str>,
        memory_id_constraint: Option<&[i64]>,
    ) -> Result<i64> {
        let MemoryConditionSql {
            from_clause,
            where_clause,
            binds,
            id_expr: _,
        } = build_memory_condition_sql(
            roles,
            content_types,
            user_id,
            thread_id,
            updated_at_range,
            created_at_range,
            external_id,
            external_id_prefix,
            memory_id_constraint,
        );
        let sql = format!("SELECT count(*) FROM {from_clause}{where_clause}");
        bind_condition_values_scalar(
            sqlx::query_scalar::<Rdb, i64>(sqlx::AssertSqlSafe(sql)),
            &binds,
        )
        .fetch_one(self.db_pool())
        .await
        .map_err(LlmMemoryError::DBError)
        .context("error in count_by_condition")
    }

    /// Fetch N memories before and after a pivot point within a thread,
    /// using thread_memory.position for consistent ordering.
    async fn find_surrounding(
        &self,
        thread_id: i64,
        pivot_memory_id: i64,
        before_count: i64,
        after_count: i64,
        fill_thread_ids: bool,
    ) -> Result<(Vec<Memory>, Vec<Memory>)> {
        let mut before = sqlx::query_as::<_, MemoryRow>(FIND_SURROUNDING_BEFORE_SQL)
            .bind(thread_id)
            .bind(thread_id)
            .bind(pivot_memory_id)
            .bind(before_count)
            .fetch_all(self.db_pool())
            .await
            .map(|r| r.into_iter().map(|r2| r2.to_proto()).collect_vec())
            .map_err(LlmMemoryError::DBError)
            .context("error in find_surrounding (before)")?;
        before.reverse();

        let mut after = sqlx::query_as::<_, MemoryRow>(FIND_SURROUNDING_AFTER_SQL)
            .bind(thread_id)
            .bind(thread_id)
            .bind(pivot_memory_id)
            .bind(after_count)
            .fetch_all(self.db_pool())
            .await
            .map(|r| r.into_iter().map(|r2| r2.to_proto()).collect_vec())
            .map_err(LlmMemoryError::DBError)
            .context("error in find_surrounding (after)")?;

        if fill_thread_ids {
            self.fill_thread_ids(&mut before).await?;
            self.fill_thread_ids(&mut after).await?;
        }
        Ok((before, after))
    }

    async fn find_surrounding_tx(
        &self,
        tx: &mut sqlx::Transaction<'_, Rdb>,
        thread_id: i64,
        pivot_memory_id: i64,
        before_count: i64,
        after_count: i64,
        fill_thread_ids: bool,
    ) -> Result<(Vec<Memory>, Vec<Memory>)> {
        let mut before = sqlx::query_as::<_, MemoryRow>(FIND_SURROUNDING_BEFORE_SQL)
            .bind(thread_id)
            .bind(thread_id)
            .bind(pivot_memory_id)
            .bind(before_count)
            .fetch_all(&mut **tx)
            .await
            .map(|r| r.into_iter().map(|r2| r2.to_proto()).collect_vec())
            .map_err(LlmMemoryError::DBError)
            .context("error in find_surrounding (before)")?;
        // Reverse to position order (query returns DESC)
        before.reverse();

        let mut after = sqlx::query_as::<_, MemoryRow>(FIND_SURROUNDING_AFTER_SQL)
            .bind(thread_id)
            .bind(thread_id)
            .bind(pivot_memory_id)
            .bind(after_count)
            .fetch_all(&mut **tx)
            .await
            .map(|r| r.into_iter().map(|r2| r2.to_proto()).collect_vec())
            .map_err(LlmMemoryError::DBError)
            .context("error in find_surrounding (after)")?;

        if fill_thread_ids {
            self.fill_thread_ids_tx(tx, &mut before).await?;
            self.fill_thread_ids_tx(tx, &mut after).await?;
        }
        Ok((before, after))
    }
}

fn map_create_memory_error(e: sqlx::Error) -> LlmMemoryError {
    if is_memory_external_id_unique_violation(&e) {
        return LlmMemoryError::AlreadyExists("memory.external_id already exists".to_string());
    }
    LlmMemoryError::DBError(e)
}

fn is_memory_external_id_unique_violation(e: &sqlx::Error) -> bool {
    let sqlx::Error::Database(db) = e else {
        return false;
    };
    let Some(code) = db.code() else {
        return false;
    };
    let is_unique = match code.as_ref() {
        // SQLite: SQLITE_CONSTRAINT_UNIQUE
        "2067" => true,
        // PostgreSQL: unique_violation
        "23505" => true,
        _ => false,
    };
    if !is_unique {
        return false;
    }

    db.constraint() == Some("memory_external_id")
        || db.message().contains("memory.external_id")
        || db.message().contains("memory_external_id")
}

fn memory_ids_from(memories: &[Memory]) -> Vec<i64> {
    memories
        .iter()
        .filter_map(|m| m.id.as_ref().map(|id| id.value))
        .unique()
        .collect()
}

fn apply_thread_ids(memories: &mut [Memory], rows: Vec<(i64, i64)>) {
    let mut by_memory: HashMap<i64, Vec<i64>> = HashMap::new();
    for (memory_id, thread_id) in rows {
        by_memory.entry(memory_id).or_default().push(thread_id);
    }
    for memory in memories {
        let Some(memory_id) = memory.id.as_ref().map(|id| id.value) else {
            continue;
        };
        let Some(data) = memory.data.as_mut() else {
            continue;
        };
        data.thread_ids = by_memory
            .get(&memory_id)
            .into_iter()
            .flatten()
            .map(|value| ThreadId { value: *value })
            .collect();
    }
}

/// Bind value carried by the dynamic WHERE clause in `build_memory_condition_where`.
///
/// The `role` column is `INTEGER` (i32) while `user_id` and `thread_id` are
/// `BIGINT` (i64). sqlx requires the bound type to match the column type, so
/// the builder returns a typed enum instead of a homogenous `Vec<i64>`.
#[derive(Debug, Clone)]
enum ConditionBind {
    I32(i32),
    I64(i64),
    Str(String),
}

/// Bind all `ConditionBind` values to a `QueryAs` in order.
fn bind_condition_values<'q, O: Send + Unpin>(
    mut query: sqlx::query::QueryAs<'q, Rdb, O, <Rdb as sqlx::Database>::Arguments>,
    binds: &'q [ConditionBind],
) -> sqlx::query::QueryAs<'q, Rdb, O, <Rdb as sqlx::Database>::Arguments> {
    for v in binds {
        query = match v {
            ConditionBind::I32(n) => query.bind(*n),
            ConditionBind::I64(n) => query.bind(*n),
            ConditionBind::Str(s) => query.bind(s.as_str()),
        };
    }
    query
}

/// Bind all `ConditionBind` values to a scalar `QueryScalar` in order.
fn bind_condition_values_scalar<'q, O: Send + Unpin>(
    mut query: sqlx::query::QueryScalar<'q, Rdb, O, <Rdb as sqlx::Database>::Arguments>,
    binds: &'q [ConditionBind],
) -> sqlx::query::QueryScalar<'q, Rdb, O, <Rdb as sqlx::Database>::Arguments> {
    for v in binds {
        query = match v {
            ConditionBind::I32(n) => query.bind(*n),
            ConditionBind::I64(n) => query.bind(*n),
            ConditionBind::Str(s) => query.bind(s.as_str()),
        };
    }
    query
}

/// Append `LIMIT $n OFFSET $m` placeholders to a SQL string.
fn append_limit_offset(sql: &mut String, n_existing_binds: usize) {
    sql.push_str(&format!(
        " LIMIT {} OFFSET {}",
        crate::sql::build_in_placeholders(1, n_existing_binds + 1),
        crate::sql::build_in_placeholders(1, n_existing_binds + 2),
    ));
}

/// Assembled SQL fragments for the "by condition" memory queries.
struct MemoryConditionSql {
    /// `memory` or `memory INNER JOIN thread_memory tm ...`
    from_clause: &'static str,
    /// Either empty or starts with a leading " WHERE ".
    where_clause: String,
    /// Bind values in SQL-parameter order.
    binds: Vec<ConditionBind>,
    /// `memory.id DESC` when joined, otherwise `id DESC`.
    id_expr: &'static str,
}

/// Render a slice of i64 values as a comma-separated SQL literal list
/// suitable for `IN (...)`. Used for the `memory.id` allow-list which
/// is server-generated (snowflake) and would otherwise have to fight
/// SQLite's `SQLITE_MAX_VARIABLE_NUMBER` (default 999) cap on bound
/// parameters per statement.
fn format_i64_in_list(ids: &[i64]) -> String {
    use std::fmt::Write;
    // 20 chars covers the i64 max width plus the ", " separator.
    let mut out = String::with_capacity(ids.len() * 20);
    for (i, v) in ids.iter().enumerate() {
        if i > 0 {
            out.push_str(", ");
        }
        let _ = write!(out, "{v}");
    }
    out
}

/// Build the FROM/WHERE/ORDER fragments and bind values for the "by condition"
/// memory queries (`find_list_by_condition` / `count_by_condition`).
///
/// * `roles` → `(memory.)role IN (...)` with OR-style membership (int bindings)
/// * `content_types` → `(memory.)content_type IN (...)` with OR-style membership
/// * `user_id` → `(memory.)user_id = ?`
/// * `thread_id` → join `thread_memory` on `memory.id = tm.memory_id` and
///   filter `tm.thread_id = ?`. The junction table is the source of truth
///   for thread membership (Phase 2+), so this intentionally ignores the
///   legacy `memory.thread_id` column which is scheduled for removal in
///   Phase 4.
///
/// All present filters are combined with AND.
#[allow(clippy::too_many_arguments)]
fn build_memory_condition_sql(
    roles: &[i32],
    content_types: &[i32],
    user_id: Option<i64>,
    thread_id: Option<i64>,
    updated_at_range: UpdatedAtRange,
    created_at_range: CreatedAtRange,
    external_id: Option<&str>,
    external_id_prefix: Option<&str>,
    memory_id_constraint: Option<&[i64]>,
) -> MemoryConditionSql {
    let joined = thread_id.is_some();
    let from_clause = if joined {
        "memory INNER JOIN thread_memory tm ON tm.memory_id = memory.id"
    } else {
        "memory"
    };
    let id_expr = if joined { "memory.id" } else { "id" };
    // Column prefixes must match the FROM clause so that the generated SQL
    // is unambiguous under the JOIN (both memory and thread_memory carry a
    // `thread_id` column on Postgres/SQLite alike).
    let role_col = if joined { "memory.role" } else { "role" };
    let user_id_col = if joined { "memory.user_id" } else { "user_id" };
    let updated_at_col = if joined {
        "memory.updated_at"
    } else {
        "updated_at"
    };
    let created_at_col = if joined {
        "memory.created_at"
    } else {
        "created_at"
    };
    let external_id_col = if joined {
        "memory.external_id"
    } else {
        "external_id"
    };
    let content_type_col = if joined {
        "memory.content_type"
    } else {
        "content_type"
    };

    let mut clauses: Vec<String> = Vec::new();
    let mut binds: Vec<ConditionBind> = Vec::new();

    if !roles.is_empty() {
        let placeholders = crate::sql::build_in_placeholders(roles.len(), binds.len() + 1);
        clauses.push(format!("{role_col} IN ({placeholders})"));
        for r in roles {
            binds.push(ConditionBind::I32(*r));
        }
    }
    if !content_types.is_empty() {
        let placeholders = crate::sql::build_in_placeholders(content_types.len(), binds.len() + 1);
        clauses.push(format!("{content_type_col} IN ({placeholders})"));
        for ct in content_types {
            binds.push(ConditionBind::I32(*ct));
        }
    }
    if let Some(uid) = user_id {
        let ph = crate::sql::build_in_placeholders(1, binds.len() + 1);
        clauses.push(format!("{user_id_col} = {ph}"));
        binds.push(ConditionBind::I64(uid));
    }
    if let Some(tid) = thread_id {
        let ph = crate::sql::build_in_placeholders(1, binds.len() + 1);
        clauses.push(format!("tm.thread_id = {ph}"));
        binds.push(ConditionBind::I64(tid));
    }
    if let Some(eid) = external_id {
        let ph = crate::sql::build_in_placeholders(1, binds.len() + 1);
        clauses.push(format!("{external_id_col} = {ph}"));
        binds.push(ConditionBind::Str(eid.to_string()));
    }
    if let Some(prefix) = external_id_prefix {
        // LIKE prefix match with `\` `%` `_` escaped so callers can pass
        // arbitrary delimiters (e.g. `under_score:`). `ESCAPE '\\'` is
        // honoured by both SQLite and PostgreSQL.
        let ph = crate::sql::build_in_placeholders(1, binds.len() + 1);
        clauses.push(format!("{external_id_col} LIKE {ph} ESCAPE '\\'"));
        binds.push(ConditionBind::Str(format!(
            "{}%",
            crate::sql::escape_like(prefix)
        )));
    }
    append_timestamp_range_clauses(
        &mut clauses,
        &mut binds,
        updated_at_col,
        updated_at_range.updated_after,
        updated_at_range.updated_before,
    );
    append_timestamp_range_clauses(
        &mut clauses,
        &mut binds,
        created_at_col,
        created_at_range.created_after,
        created_at_range.created_before,
    );
    // App layer short-circuits `Some(empty)`; the `1=0` branch keeps the
    // SQL syntactically valid for direct infra-level callers (tests).
    //
    // The allow-list is rendered as inline integer literals rather than
    // bound parameters. memory.id is a server-generated snowflake i64
    // (never user input), so direct embedding has no injection risk and
    // sidesteps SQLite's `SQLITE_MAX_VARIABLE_NUMBER` (default 999) —
    // bound-parameter chunking would still trip that cap because the
    // limit is per-statement, not per-`IN` clause. PostgreSQL also
    // accepts arbitrarily long literal IN lists.
    if let Some(ids) = memory_id_constraint {
        if ids.is_empty() {
            clauses.push("1=0".to_string());
        } else {
            clauses.push(format!("{id_expr} IN ({})", format_i64_in_list(ids)));
        }
    }

    let where_clause = if clauses.is_empty() {
        String::new()
    } else {
        format!(" WHERE {}", clauses.join(" AND "))
    };

    MemoryConditionSql {
        from_clause,
        where_clause,
        binds,
        id_expr,
    }
}

/// Generic `>` / `<=` range append used by both updated_at and created_at.
/// Bounds are exclusive lower / inclusive upper to match the
/// `service/memory.proto` field comments.
fn append_timestamp_range_clauses(
    clauses: &mut Vec<String>,
    binds: &mut Vec<ConditionBind>,
    column: &str,
    after: Option<i64>,
    before: Option<i64>,
) {
    if let Some(ts) = after {
        let ph = crate::sql::build_in_placeholders(1, binds.len() + 1);
        clauses.push(format!("{column} > {ph}"));
        binds.push(ConditionBind::I64(ts));
    }
    if let Some(ts) = before {
        let ph = crate::sql::build_in_placeholders(1, binds.len() + 1);
        clauses.push(format!("{column} <= {ph}"));
        binds.push(ConditionBind::I64(ts));
    }
}

/// Resolve the ORDER BY expression for a `MemorySort`. When the query is
/// JOIN-ed against `thread_memory` (`joined = true`) the columns must be
/// qualified to stay unambiguous. The secondary `id` key gives the sort
/// a stable tiebreaker so paginated reads do not re-shuffle equal-
/// timestamp rows.
fn order_by_expr(sort: MemorySort, joined: bool) -> &'static str {
    match (sort, joined) {
        (MemorySort::UpdatedDesc, true) => "memory.updated_at DESC, memory.id DESC",
        (MemorySort::UpdatedDesc, false) => "updated_at DESC, id DESC",
        (MemorySort::UpdatedAsc, true) => "memory.updated_at ASC, memory.id ASC",
        (MemorySort::UpdatedAsc, false) => "updated_at ASC, id ASC",
        (MemorySort::CreatedDesc, true) => "memory.created_at DESC, memory.id DESC",
        (MemorySort::CreatedDesc, false) => "created_at DESC, id DESC",
        (MemorySort::CreatedAsc, true) => "memory.created_at ASC, memory.id ASC",
        (MemorySort::CreatedAsc, false) => "created_at ASC, id ASC",
        (MemorySort::IdDesc, true) => "memory.id DESC",
        (MemorySort::IdDesc, false) => "id DESC",
    }
}

pub struct MemoryRepositoryImpl {
    id_generator: IdGeneratorWrapper,
    pool: &'static RdbPool,
}

pub trait UseMemoryRepository {
    fn memory_repository(&self) -> &MemoryRepositoryImpl;
}

impl MemoryRepositoryImpl {
    pub fn new(id_generator: IdGeneratorWrapper, pool: &'static RdbPool) -> Self {
        Self { id_generator, pool }
    }
}

impl UseRdbPool for MemoryRepositoryImpl {
    fn db_pool(&self) -> &RdbPool {
        self.pool
    }
}

impl UseIdGenerator for MemoryRepositoryImpl {
    fn id_generator(&self) -> &IdGeneratorWrapper {
        &self.id_generator
    }
}

impl MemoryRepository for MemoryRepositoryImpl {}

#[cfg(test)]
mod test {
    use super::CreatedAtRange;
    use super::MemoryRepository;
    use super::MemoryRepositoryImpl;
    use super::MemorySort;
    use super::UpdatedAtRange;
    use crate::sql::p;
    use anyhow::Context;
    use anyhow::Result;
    use infra_utils::infra::rdb::RdbPool;
    use infra_utils::infra::rdb::UseRdbPool;
    use protobuf::llm_memory::data::Memory;
    use protobuf::llm_memory::data::MemoryData;
    use protobuf::llm_memory::data::MemoryId;
    use protobuf::llm_memory::data::UserId;

    async fn _test_repository(pool: &'static RdbPool) -> Result<()> {
        let repository = MemoryRepositoryImpl::new(crate::test_helper::shared_id_generator(), pool);
        let db = repository.db_pool();
        let data = Some(MemoryData {
            parent_ids: vec![MemoryId { value: 3 }, MemoryId { value: 4 }],
            user_id: Some(UserId { value: 4 }),
            content: "hoge4".to_string(),
            content_type: 6,
            params: Some("\"hoge7\"".to_string()),
            metadata: Some("\"hoge8\"".to_string()),
            created_at: 9,
            updated_at: 10,
            role: 0,
            external_id: None,
            media_object_id: None,
            thread_ids: Vec::new(),
        });

        let mut tx = db.begin().await.context("error in test")?;
        let id = repository.create(&mut *tx, &data.clone().unwrap()).await?;
        assert!(id.value > 0);
        tx.commit().await.context("error in test delete commit")?;

        let id1 = id;
        let expect = Memory {
            id: Some(id1),
            data,
            media: None,
        };

        // find
        let found = repository.find(&id1, false).await?;
        assert_eq!(Some(&expect), found.as_ref());

        // update (created_at in update data is ignored — original value preserved)
        tx = db.begin().await.context("error in test")?;
        let update = MemoryData {
            parent_ids: vec![MemoryId { value: 40 }, MemoryId { value: 50 }],
            user_id: Some(UserId { value: 5 }),
            content: "fuga4".to_string(),
            content_type: 7,
            params: Some("\"fuga7\"".to_string()),
            metadata: Some("\"fuga8\"".to_string()),
            created_at: 10,
            updated_at: 11,
            role: 0,
            external_id: None,
            media_object_id: None,
            thread_ids: Vec::new(),
        };
        let updated = repository
            .update(&mut *tx, &expect.id.unwrap(), &update)
            .await?;
        assert!(updated);
        tx.commit().await.context("error in test delete commit")?;

        // find — created_at should be preserved from original insert (9), not from update data (10)
        let found = repository.find(&expect.id.unwrap(), false).await?;
        let found_data = found.unwrap().data.unwrap();
        assert_eq!(found_data.created_at, 9);
        assert_eq!(
            found_data,
            MemoryData {
                created_at: 9,
                ..update.clone()
            }
        );
        let count = repository.count_list_tx(repository.db_pool()).await?;
        assert_eq!(1, count);

        // delete record
        tx = db.begin().await.context("error in test")?;
        let del = repository.delete_tx(&mut *tx, &expect.id.unwrap()).await?;
        tx.commit().await.context("error in test delete commit")?;
        assert!(del, "delete error");
        Ok(())
    }

    async fn _test_created_at_auto_fill(pool: &'static RdbPool) -> Result<()> {
        let repository = MemoryRepositoryImpl::new(crate::test_helper::shared_id_generator(), pool);
        let db = repository.db_pool();

        // created_at=0 should be auto-filled by server
        let data = MemoryData {
            parent_ids: vec![],
            user_id: Some(UserId { value: 1 }),
            content: "auto timestamp test".to_string(),
            content_type: 0,
            params: None,
            metadata: None,
            created_at: 0,
            updated_at: 0,
            role: 0,
            external_id: None,
            media_object_id: None,
            thread_ids: Vec::new(),
        };

        let before = command_utils::util::datetime::now_millis();
        let mut tx = db.begin().await.context("begin")?;
        let id = repository.create(&mut *tx, &data).await?;
        tx.commit().await.context("commit")?;
        let after = command_utils::util::datetime::now_millis();

        let found = repository.find(&id, false).await?.expect("should exist");
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
        let data2 = MemoryData {
            created_at: 12345,
            updated_at: 67890,
            content: "explicit timestamp test".to_string(),
            ..data.clone()
        };
        let mut tx = db.begin().await.context("begin")?;
        let id2 = repository.create(&mut *tx, &data2).await?;
        tx.commit().await.context("commit")?;

        let found2 = repository.find(&id2, false).await?.expect("should exist");
        let found_data2 = found2.data.unwrap();
        assert_eq!(found_data2.created_at, 12345);
        assert_eq!(found_data2.updated_at, 67890);

        // cleanup
        repository.delete(&id).await?;
        repository.delete(&id2).await?;
        Ok(())
    }

    async fn _test_updated_at_auto_fill_on_update(pool: &'static RdbPool) -> Result<()> {
        let repository = MemoryRepositoryImpl::new(crate::test_helper::shared_id_generator(), pool);
        let db = repository.db_pool();

        let data = MemoryData {
            parent_ids: vec![],
            user_id: Some(UserId { value: 1 }),
            content: "update timestamp test".to_string(),
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

        let mut tx = db.begin().await.context("begin")?;
        let id = repository.create(&mut *tx, &data).await?;
        tx.commit().await.context("commit")?;

        // Update with updated_at=0 should auto-fill server timestamp
        let update_data = MemoryData {
            updated_at: 0,
            created_at: 0,
            content: "updated content".to_string(),
            ..data.clone()
        };
        let before = command_utils::util::datetime::now_millis();
        let mut tx = db.begin().await.context("begin")?;
        repository.update(&mut *tx, &id, &update_data).await?;
        tx.commit().await.context("commit")?;
        let after = command_utils::util::datetime::now_millis();

        let found = repository.find(&id, false).await?.expect("should exist");
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

    async fn _test_find_by_thread_id_order(pool: &'static RdbPool) -> Result<()> {
        use crate::infra::thread_memory::rdb::{
            ThreadMemoryRepository, ThreadMemoryRepositoryImpl,
        };

        let repository = MemoryRepositoryImpl::new(crate::test_helper::shared_id_generator(), pool);
        let tm_repository = ThreadMemoryRepositoryImpl::new(pool);
        let db = repository.db_pool();

        let thread_id = 999_999;

        // Insert memories with intentionally non-sequential created_at.
        // Position determines order in the new thread_memory-based query.
        let entries = vec![
            ("third", 300i64, 2i32),
            ("first", 100i64, 0i32),
            ("second", 200i64, 1i32),
        ];
        let mut ids = vec![];
        for (content, ts, position) in &entries {
            let data = MemoryData {
                parent_ids: vec![],
                user_id: Some(UserId { value: 1 }),
                content: content.to_string(),
                content_type: 0,
                params: None,
                metadata: None,
                created_at: *ts,
                updated_at: *ts,
                role: 0,
                external_id: None,
                media_object_id: None,
                thread_ids: Vec::new(),
            };
            let mut tx = db.begin().await.context("begin")?;
            let id = repository.create(&mut *tx, &data).await?;
            tm_repository
                .insert_tx(&mut *tx, thread_id, id.value, *position, *ts)
                .await?;
            tx.commit().await.context("commit")?;
            ids.push(id);
        }

        // find_by_thread_id should return in position ascending order
        let results = repository
            .find_by_thread_id(thread_id, None, None, &[], &[], false)
            .await?;
        assert_eq!(results.len(), 3);
        assert_eq!(results[0].data.as_ref().unwrap().content, "first");
        assert_eq!(results[1].data.as_ref().unwrap().content, "second");
        assert_eq!(results[2].data.as_ref().unwrap().content, "third");

        // Also verify with limit/offset
        let results = repository
            .find_by_thread_id(thread_id, Some(&2), Some(&0), &[], &[], false)
            .await?;
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].data.as_ref().unwrap().content, "first");
        assert_eq!(results[1].data.as_ref().unwrap().content, "second");

        let results = repository
            .find_by_thread_id(thread_id, Some(&2), Some(&1), &[], &[], false)
            .await?;
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].data.as_ref().unwrap().content, "second");
        assert_eq!(results[1].data.as_ref().unwrap().content, "third");

        // cleanup
        tm_repository.delete_by_thread_tx(db, thread_id).await?;
        for id in &ids {
            repository.delete(id).await?;
        }
        Ok(())
    }

    /// Tests role and content_type filters for `find_by_thread_id`.
    async fn _test_find_by_thread_id_filters(pool: &'static RdbPool) -> Result<()> {
        use crate::infra::thread_memory::rdb::{
            ThreadMemoryRepository, ThreadMemoryRepositoryImpl,
        };

        let repository = MemoryRepositoryImpl::new(crate::test_helper::shared_id_generator(), pool);
        let tm_repository = ThreadMemoryRepositoryImpl::new(pool);
        let db = repository.db_pool();

        let thread_id = 888_888;

        // (content, role, content_type, position)
        let entries: Vec<(&str, i32, i32, i32)> = vec![
            ("sys_text", 3, 0, 0),  // ROLE_SYSTEM, TEXT
            ("user_text", 1, 0, 1), // ROLE_USER, TEXT
            ("asst_text", 2, 0, 2), // ROLE_ASSISTANT, TEXT
            ("user_img", 1, 2, 3),  // ROLE_USER, IMAGE
            ("asst_tool", 2, 1, 4), // ROLE_ASSISTANT, TOOL
        ];
        let mut ids = vec![];
        for (content, role, ct, position) in &entries {
            let data = MemoryData {
                parent_ids: vec![],
                user_id: Some(UserId { value: 1 }),
                content: content.to_string(),
                content_type: *ct,
                params: None,
                metadata: None,
                created_at: 100,
                updated_at: 100,
                role: *role,
                external_id: None,
                media_object_id: None,
                thread_ids: Vec::new(),
            };
            let mut tx = db.begin().await.context("begin")?;
            let id = repository.create(&mut *tx, &data).await?;
            tm_repository
                .insert_tx(&mut *tx, thread_id, id.value, *position, 100)
                .await?;
            tx.commit().await.context("commit")?;
            ids.push(id);
        }

        // No filters: all 5 returned
        let all = repository
            .find_by_thread_id(thread_id, None, None, &[], &[], false)
            .await?;
        assert_eq!(all.len(), 5);

        // Role filter: ROLE_USER only (role=1) -> user_text, user_img
        let user_only = repository
            .find_by_thread_id(thread_id, None, None, &[1], &[], false)
            .await?;
        assert_eq!(user_only.len(), 2);
        assert_eq!(user_only[0].data.as_ref().unwrap().content, "user_text");
        assert_eq!(user_only[1].data.as_ref().unwrap().content, "user_img");

        // Role filter OR: ROLE_USER or ROLE_ASSISTANT (1, 2) -> 4 items
        let user_asst = repository
            .find_by_thread_id(thread_id, None, None, &[1, 2], &[], false)
            .await?;
        assert_eq!(user_asst.len(), 4);

        // ContentType filter: TEXT only (0) -> sys_text, user_text, asst_text
        let text_only = repository
            .find_by_thread_id(thread_id, None, None, &[], &[0], false)
            .await?;
        assert_eq!(text_only.len(), 3);
        assert_eq!(text_only[0].data.as_ref().unwrap().content, "sys_text");
        assert_eq!(text_only[1].data.as_ref().unwrap().content, "user_text");
        assert_eq!(text_only[2].data.as_ref().unwrap().content, "asst_text");

        // ContentType filter OR: TEXT or IMAGE (0, 2) -> 4 items
        let text_or_img = repository
            .find_by_thread_id(thread_id, None, None, &[], &[0, 2], false)
            .await?;
        assert_eq!(text_or_img.len(), 4);

        // AND: ROLE_USER AND TEXT -> user_text only
        let user_text = repository
            .find_by_thread_id(thread_id, None, None, &[1], &[0], false)
            .await?;
        assert_eq!(user_text.len(), 1);
        assert_eq!(user_text[0].data.as_ref().unwrap().content, "user_text");

        // AND: ROLE_ASSISTANT AND (TEXT or TOOL) -> asst_text, asst_tool
        let asst_text_tool = repository
            .find_by_thread_id(thread_id, None, None, &[2], &[0, 1], false)
            .await?;
        assert_eq!(asst_text_tool.len(), 2);
        assert_eq!(
            asst_text_tool[0].data.as_ref().unwrap().content,
            "asst_text"
        );
        assert_eq!(
            asst_text_tool[1].data.as_ref().unwrap().content,
            "asst_tool"
        );

        // No match: ROLE_META (5) -> empty
        let no_match = repository
            .find_by_thread_id(thread_id, None, None, &[5], &[], false)
            .await?;
        assert!(no_match.is_empty());

        // Filter + limit/offset: ROLE_USER or ROLE_ASSISTANT, limit=2, offset=1
        let paged = repository
            .find_by_thread_id(thread_id, Some(&2), Some(&1), &[1, 2], &[], false)
            .await?;
        assert_eq!(paged.len(), 2);
        // Position order: user_text(1), asst_text(2), user_img(3), asst_tool(4)
        // offset=1 skips user_text -> asst_text, user_img
        assert_eq!(paged[0].data.as_ref().unwrap().content, "asst_text");
        assert_eq!(paged[1].data.as_ref().unwrap().content, "user_img");

        // cleanup
        tm_repository.delete_by_thread_tx(db, thread_id).await?;
        for id in &ids {
            repository.delete(id).await?;
        }
        Ok(())
    }

    #[test]
    fn run_find_by_thread_id_filters_test() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let rdb_pool = setup_pool().await;
            _test_find_by_thread_id_filters(rdb_pool).await
        })
    }

    fn setup_pool() -> impl std::future::Future<Output = &'static RdbPool> {
        use infra_utils::infra::test::setup_test_rdb_from;
        async {
            if cfg!(feature = "postgres") {
                let pool = setup_test_rdb_from("sql/postgres").await;
                sqlx::query("TRUNCATE TABLE memory CASCADE;")
                    .execute(pool)
                    .await
                    .unwrap();
                pool
            } else {
                let pool = setup_test_rdb_from("sql/sqlite").await;
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

    #[test]
    fn run_find_by_thread_id_order_test() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let rdb_pool = setup_pool().await;
            _test_find_by_thread_id_order(rdb_pool).await
        })
    }

    async fn _test_find_by_ids(pool: &'static RdbPool) -> Result<()> {
        let repository = MemoryRepositoryImpl::new(crate::test_helper::shared_id_generator(), pool);
        let db = repository.db_pool();

        let mut ids = Vec::new();
        for i in 0..3 {
            let data = MemoryData {
                parent_ids: vec![],
                user_id: Some(UserId { value: 1 }),
                content: format!("find_by_ids test {i}"),
                content_type: 0,
                params: None,
                metadata: None,
                created_at: (i + 1) * 100,
                updated_at: (i + 1) * 100,
                role: 0,
                external_id: None,
                media_object_id: None,
                thread_ids: Vec::new(),
            };
            let mut tx = db.begin().await.context("begin")?;
            let id = repository.create(&mut *tx, &data).await?;
            tx.commit().await.context("commit")?;
            ids.push(id.value);
        }

        // Normal case: all IDs exist
        let results = repository.find_by_ids(&ids, false).await?;
        assert_eq!(results.len(), 3);

        // Empty case
        let results = repository.find_by_ids(&[], false).await?;
        assert!(results.is_empty());

        // Partial case: some IDs don't exist
        let mixed = vec![ids[0], 999_999_999, ids[2]];
        let results = repository.find_by_ids(&mixed, false).await?;
        assert_eq!(results.len(), 2);

        // cleanup
        for id in &ids {
            repository.delete(&MemoryId { value: *id }).await?;
        }
        Ok(())
    }

    #[test]
    fn run_find_by_ids_test() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let rdb_pool = setup_pool().await;
            _test_find_by_ids(rdb_pool).await
        })
    }

    /// Exercises `find_list_by_condition` / `count_by_condition` — the
    /// replacement for SystemPromptService.FindList + Count now that system
    /// prompts are stored as ROLE_SYSTEM memories.
    ///
    /// Scenarios covered:
    /// - role filter alone (single role) returns only matching memories
    /// - combined filters (role + user_id + thread_id) narrow results correctly
    /// - pagination (limit/offset) orders by id DESC
    /// - empty filter returns all memories
    /// - count matches the length of the unlimited list
    async fn _test_find_list_by_condition(pool: &'static RdbPool) -> Result<()> {
        use crate::infra::thread_memory::rdb::{
            ThreadMemoryRepository, ThreadMemoryRepositoryImpl,
        };

        let repository = MemoryRepositoryImpl::new(crate::test_helper::shared_id_generator(), pool);
        let tm_repository = ThreadMemoryRepositoryImpl::new(pool);
        let db = repository.db_pool();

        // ROLE_SYSTEM = 3, ROLE_USER = 1, ROLE_ASSISTANT = 2
        let thread_a: i64 = 111_111;
        let thread_b: i64 = 222_222;

        // (content, role, user_id, junction_thread_id)
        // `junction_thread_id = None` means the row is not registered in the
        // thread_memory junction (free-standing ROLE_SYSTEM style).
        let entries: Vec<(&str, i32, i64, Option<i64>)> = vec![
            ("system prompt A", 3, 10, None),
            ("system prompt B", 3, 20, None),
            ("user msg in thread A", 1, 10, Some(thread_a)),
            ("assistant msg in thread A", 2, 10, Some(thread_a)),
            ("user msg in thread B", 1, 20, Some(thread_b)),
        ];

        let mut created_ids: Vec<MemoryId> = Vec::new();
        for (i, (content, role, user_id, junction_thread_id)) in entries.iter().enumerate() {
            let data = MemoryData {
                parent_ids: vec![],
                user_id: Some(UserId { value: *user_id }),
                content: content.to_string(),
                content_type: 0,
                params: None,
                metadata: None,
                created_at: 100 + i as i64,
                updated_at: 100 + i as i64,
                role: *role,
                external_id: None,
                media_object_id: None,
                thread_ids: Vec::new(),
            };
            let mut tx = db.begin().await.context("begin")?;
            let id = repository.create(&mut *tx, &data).await?;
            if let Some(tid) = junction_thread_id {
                tm_repository
                    .insert_auto_position_tx(&mut *tx, *tid, id.value, 100 + i as i64)
                    .await?;
            }
            tx.commit().await.context("commit")?;
            created_ids.push(id);
        }

        // Role filter alone — only ROLE_SYSTEM memories
        let system_only = repository
            .find_list_by_condition(
                None,
                None,
                &[3],
                &[],
                None,
                None,
                UpdatedAtRange::default(),
                CreatedAtRange::default(),
                None,
                None,
                None,
                MemorySort::default(),
                false,
            )
            .await?;
        assert_eq!(system_only.len(), 2);
        for m in &system_only {
            assert_eq!(m.data.as_ref().unwrap().role, 3);
        }
        let system_count = repository
            .count_by_condition(
                &[3],
                &[],
                None,
                None,
                UpdatedAtRange::default(),
                CreatedAtRange::default(),
                None,
                None,
                None,
            )
            .await?;
        assert_eq!(system_count, 2);

        // Role + user_id — ROLE_SYSTEM memories belonging to user 10
        let system_user10 = repository
            .find_list_by_condition(
                None,
                None,
                &[3],
                &[],
                Some(10),
                None,
                UpdatedAtRange::default(),
                CreatedAtRange::default(),
                None,
                None,
                None,
                MemorySort::default(),
                false,
            )
            .await?;
        assert_eq!(system_user10.len(), 1);
        assert_eq!(
            system_user10[0].data.as_ref().unwrap().content,
            "system prompt A"
        );
        let system_user10_count = repository
            .count_by_condition(
                &[3],
                &[],
                Some(10),
                None,
                UpdatedAtRange::default(),
                CreatedAtRange::default(),
                None,
                None,
                None,
            )
            .await?;
        assert_eq!(system_user10_count, 1);

        // thread_id filter — thread A has two memories (user + assistant)
        let thread_a_mems = repository
            .find_list_by_condition(
                None,
                None,
                &[],
                &[],
                None,
                Some(thread_a),
                UpdatedAtRange::default(),
                CreatedAtRange::default(),
                None,
                None,
                None,
                MemorySort::default(),
                false,
            )
            .await?;
        assert_eq!(thread_a_mems.len(), 2);
        let thread_a_count = repository
            .count_by_condition(
                &[],
                &[],
                None,
                Some(thread_a),
                UpdatedAtRange::default(),
                CreatedAtRange::default(),
                None,
                None,
                None,
            )
            .await?;
        assert_eq!(thread_a_count, 2);

        // Combined role OR — USER or ASSISTANT across all data = 3 memories
        let conversational = repository
            .find_list_by_condition(
                None,
                None,
                &[1, 2],
                &[],
                None,
                None,
                UpdatedAtRange::default(),
                CreatedAtRange::default(),
                None,
                None,
                None,
                MemorySort::default(),
                false,
            )
            .await?;
        assert_eq!(conversational.len(), 3);
        let conversational_count = repository
            .count_by_condition(
                &[1, 2],
                &[],
                None,
                None,
                UpdatedAtRange::default(),
                CreatedAtRange::default(),
                None,
                None,
                None,
            )
            .await?;
        assert_eq!(conversational_count, 3);

        // Pagination — limit=1 per page on ROLE_SYSTEM memories ordered
        // by `id DESC`. Pinning the sort to `IdDesc` decouples this
        // pagination test from the fixture's `updated_at` schedule so it
        // exercises the offset/limit math in isolation rather than the
        // default `updated_at DESC` tiebreaker behaviour (which has its
        // own dedicated tests).
        let first_page = repository
            .find_list_by_condition(
                Some(&1),
                Some(&0),
                &[3],
                &[],
                None,
                None,
                UpdatedAtRange::default(),
                CreatedAtRange::default(),
                None,
                None,
                None,
                MemorySort::IdDesc,
                false,
            )
            .await?;
        assert_eq!(first_page.len(), 1);
        let second_page = repository
            .find_list_by_condition(
                Some(&1),
                Some(&1),
                &[3],
                &[],
                None,
                None,
                UpdatedAtRange::default(),
                CreatedAtRange::default(),
                None,
                None,
                None,
                MemorySort::IdDesc,
                false,
            )
            .await?;
        assert_eq!(second_page.len(), 1);
        let first_id = first_page[0].id.as_ref().unwrap().value;
        let second_id = second_page[0].id.as_ref().unwrap().value;
        // Explicit id DESC ordering check — the first page must be newer
        // (larger id) than the second page, not merely different.
        assert!(
            first_id > second_id,
            "first page id ({first_id}) should be larger than second page id ({second_id}) under id DESC ordering"
        );
        // Page boundary is exclusive — no overlap.
        assert_ne!(first_id, second_id);

        // Empty filter returns everything that was just inserted
        let all = repository
            .find_list_by_condition(
                None,
                None,
                &[],
                &[],
                None,
                None,
                UpdatedAtRange::default(),
                CreatedAtRange::default(),
                None,
                None,
                None,
                MemorySort::default(),
                false,
            )
            .await?;
        assert!(all.len() >= 5);

        // --- content_types filter tests ---
        // Insert extra memories with different content_types for this section.
        let thread_ct: i64 = 777_777;
        let ct_entries: Vec<(&str, i32, i32, i64)> = vec![
            ("ct_text", 1, 0, 50),  // ROLE_USER, TEXT
            ("ct_tool", 2, 1, 51),  // ROLE_ASSISTANT, TOOL
            ("ct_image", 1, 2, 52), // ROLE_USER, IMAGE
        ];
        let mut ct_ids: Vec<MemoryId> = Vec::new();
        for (content, role, ct, uid) in &ct_entries {
            let data = MemoryData {
                parent_ids: vec![],
                user_id: Some(UserId { value: *uid }),
                content: content.to_string(),
                content_type: *ct,
                params: None,
                metadata: None,
                created_at: 500,
                updated_at: 500,
                role: *role,
                external_id: None,
                media_object_id: None,
                thread_ids: Vec::new(),
            };
            let mut tx = db.begin().await.context("begin ct")?;
            let id = repository.create(&mut *tx, &data).await?;
            tm_repository
                .insert_auto_position_tx(&mut *tx, thread_ct, id.value, 500)
                .await?;
            tx.commit().await.context("commit ct")?;
            ct_ids.push(id);
        }

        // ContentType filter: TOOL only
        let tool_only = repository
            .find_list_by_condition(
                None,
                None,
                &[],
                &[1],
                None,
                Some(thread_ct),
                UpdatedAtRange::default(),
                CreatedAtRange::default(),
                None,
                None,
                None,
                MemorySort::default(),
                false,
            )
            .await?;
        assert_eq!(tool_only.len(), 1);
        assert_eq!(tool_only[0].data.as_ref().unwrap().content, "ct_tool");

        // ContentType OR: TEXT or IMAGE
        let text_or_img = repository
            .find_list_by_condition(
                None,
                None,
                &[],
                &[0, 2],
                None,
                Some(thread_ct),
                UpdatedAtRange::default(),
                CreatedAtRange::default(),
                None,
                None,
                None,
                MemorySort::default(),
                false,
            )
            .await?;
        assert_eq!(text_or_img.len(), 2);

        // Combined: ROLE_USER AND IMAGE
        let user_img = repository
            .find_list_by_condition(
                None,
                None,
                &[1],
                &[2],
                None,
                Some(thread_ct),
                UpdatedAtRange::default(),
                CreatedAtRange::default(),
                None,
                None,
                None,
                MemorySort::default(),
                false,
            )
            .await?;
        assert_eq!(user_img.len(), 1);
        assert_eq!(user_img[0].data.as_ref().unwrap().content, "ct_image");

        // count_by_condition with content_types
        let ct_count = repository
            .count_by_condition(
                &[],
                &[0, 2],
                None,
                Some(thread_ct),
                UpdatedAtRange::default(),
                CreatedAtRange::default(),
                None,
                None,
                None,
            )
            .await?;
        assert_eq!(ct_count, 2);

        // cleanup ct
        tm_repository.delete_by_thread_tx(db, thread_ct).await?;
        for id in &ct_ids {
            repository.delete(id).await?;
        }

        // cleanup original
        for id in &created_ids {
            repository.delete(id).await?;
        }
        tm_repository.delete_by_thread_tx(db, thread_a).await?;
        tm_repository.delete_by_thread_tx(db, thread_b).await?;
        Ok(())
    }

    #[test]
    fn run_find_list_by_condition_test() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let rdb_pool = setup_pool().await;
            _test_find_list_by_condition(rdb_pool).await
        })
    }

    /// Verifies that `find_list_by_condition` / `count_by_condition` resolve
    /// thread membership through the `thread_memory` junction table.
    ///
    /// Phase 4 removed the legacy `memory.thread_id` column, so a memory that
    /// is not registered in the junction is simply invisible to thread-scoped
    /// queries regardless of how it was created. This test pins that rule so
    /// that future refactors cannot accidentally re-introduce a side channel
    /// for thread membership.
    async fn _test_find_list_by_condition_uses_thread_memory_junction(
        pool: &'static RdbPool,
    ) -> Result<()> {
        use crate::infra::thread_memory::rdb::{
            ThreadMemoryRepository, ThreadMemoryRepositoryImpl,
        };

        let repository = MemoryRepositoryImpl::new(crate::test_helper::shared_id_generator(), pool);
        let tm_repository = ThreadMemoryRepositoryImpl::new(pool);
        let db = repository.db_pool();

        let thread_j: i64 = 333_333;
        let thread_orphan: i64 = 444_444;

        // Row 1: properly attached to thread_j via thread_memory junction.
        let joined_data = MemoryData {
            parent_ids: vec![],
            user_id: Some(UserId { value: 77 }),
            content: "joined through junction".to_string(),
            content_type: 0,
            params: None,
            metadata: None,
            created_at: 1_000,
            updated_at: 1_000,
            role: 1,
            external_id: None,
            media_object_id: None,
            thread_ids: Vec::new(),
        };
        let mut tx = db.begin().await.context("begin joined")?;
        let joined_id = repository.create(&mut *tx, &joined_data).await?;
        tm_repository
            .insert_auto_position_tx(&mut *tx, thread_j, joined_id.value, 1_000)
            .await?;
        tx.commit().await.context("commit joined")?;

        // Row 2: free-standing memory with no junction entry. This models any
        // code path that creates a memory directly via `MemoryRepository::create`
        // without going through `ThreadApp::add_memory` — such rows should be
        // invisible to `thread_id`-scoped queries until they are explicitly
        // attached to a thread through the junction.
        let orphan_data = MemoryData {
            parent_ids: vec![],
            user_id: Some(UserId { value: 77 }),
            content: "not registered in junction".to_string(),
            content_type: 0,
            params: None,
            metadata: None,
            created_at: 1_001,
            updated_at: 1_001,
            role: 1,
            external_id: None,
            media_object_id: None,
            thread_ids: Vec::new(),
        };
        let mut tx = db.begin().await.context("begin orphan")?;
        let orphan_id = repository.create(&mut *tx, &orphan_data).await?;
        tx.commit().await.context("commit orphan")?;

        // thread_j query must include the junction-attached row.
        let joined_results = repository
            .find_list_by_condition(
                None,
                None,
                &[],
                &[],
                None,
                Some(thread_j),
                UpdatedAtRange::default(),
                CreatedAtRange::default(),
                None,
                None,
                None,
                MemorySort::default(),
                false,
            )
            .await?;
        assert_eq!(joined_results.len(), 1);
        assert_eq!(
            joined_results[0].id.as_ref().unwrap().value,
            joined_id.value
        );
        let joined_count = repository
            .count_by_condition(
                &[],
                &[],
                None,
                Some(thread_j),
                UpdatedAtRange::default(),
                CreatedAtRange::default(),
                None,
                None,
                None,
            )
            .await?;
        assert_eq!(joined_count, 1);

        // thread_orphan query must be empty — the junction has no entry for
        // this thread at all.
        let orphan_results = repository
            .find_list_by_condition(
                None,
                None,
                &[],
                &[],
                None,
                Some(thread_orphan),
                UpdatedAtRange::default(),
                CreatedAtRange::default(),
                None,
                None,
                None,
                MemorySort::default(),
                false,
            )
            .await?;
        assert!(
            orphan_results.is_empty(),
            "a memory not registered in the junction must not appear under any thread_id filter"
        );
        let orphan_count = repository
            .count_by_condition(
                &[],
                &[],
                None,
                Some(thread_orphan),
                UpdatedAtRange::default(),
                CreatedAtRange::default(),
                None,
                None,
                None,
            )
            .await?;
        assert_eq!(orphan_count, 0);

        // Role + thread_id AND composition still works through the join.
        let joined_by_role = repository
            .find_list_by_condition(
                None,
                None,
                &[1],
                &[],
                None,
                Some(thread_j),
                UpdatedAtRange::default(),
                CreatedAtRange::default(),
                None,
                None,
                None,
                MemorySort::default(),
                false,
            )
            .await?;
        assert_eq!(joined_by_role.len(), 1);
        // Filtering by a role that does not match must yield zero rows.
        let joined_wrong_role = repository
            .find_list_by_condition(
                None,
                None,
                &[3],
                &[],
                None,
                Some(thread_j),
                UpdatedAtRange::default(),
                CreatedAtRange::default(),
                None,
                None,
                None,
                MemorySort::default(),
                false,
            )
            .await?;
        assert!(joined_wrong_role.is_empty());

        // user_id + thread_id AND composition must resolve user_id against
        // memory.user_id (not thread_memory) — the join should not confuse
        // the column source.
        let joined_by_user = repository
            .find_list_by_condition(
                None,
                None,
                &[],
                &[],
                Some(77),
                Some(thread_j),
                UpdatedAtRange::default(),
                CreatedAtRange::default(),
                None,
                None,
                None,
                MemorySort::default(),
                false,
            )
            .await?;
        assert_eq!(joined_by_user.len(), 1);
        let joined_wrong_user = repository
            .find_list_by_condition(
                None,
                None,
                &[],
                &[],
                Some(999),
                Some(thread_j),
                UpdatedAtRange::default(),
                CreatedAtRange::default(),
                None,
                None,
                None,
                MemorySort::default(),
                false,
            )
            .await?;
        assert!(joined_wrong_user.is_empty());

        // cleanup
        tm_repository.delete_by_thread_tx(db, thread_j).await?;
        repository.delete(&joined_id).await?;
        repository.delete(&orphan_id).await?;
        Ok(())
    }

    #[test]
    fn run_find_list_by_condition_uses_thread_memory_junction_test() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let rdb_pool = setup_pool().await;
            _test_find_list_by_condition_uses_thread_memory_junction(rdb_pool).await
        })
    }

    /// `memory_id_constraint` allow-lists used to be rejected at the
    /// app layer when they exceeded SQLite's `SQLITE_MAX_VARIABLE_NUMBER`
    /// (default 999). The infra layer now renders the allow-list as
    /// inline integer literals inside `build_memory_condition_sql`, so
    /// the SQL never consumes bound-parameter slots for the IDs and
    /// arbitrarily long allow-lists go through.
    async fn _test_find_list_by_condition_large_id_constraint(
        pool: &'static RdbPool,
    ) -> Result<()> {
        let repository = MemoryRepositoryImpl::new(crate::test_helper::shared_id_generator(), pool);
        let db = repository.db_pool();
        let user_id = UserId { value: 9_001 };

        // N is chosen well above SQLite's 999 bound-parameter cap so a
        // bind-based IN list would fail outright; literal rendering must
        // still work.
        const N: usize = 1_500;
        let mut id_values: Vec<i64> = Vec::with_capacity(N);
        // Single transaction keeps the 1500-row fixture from spending
        // 1500 commits on SQLite.
        let mut tx = db.begin().await.context("begin")?;
        for i in 0..N {
            let data = MemoryData {
                parent_ids: vec![],
                user_id: Some(user_id),
                content: format!("row {i}"),
                content_type: 0,
                params: None,
                metadata: None,
                created_at: 10_000 + i as i64,
                updated_at: 10_000 + i as i64,
                role: 1,
                external_id: None,
                media_object_id: None,
                thread_ids: Vec::new(),
            };
            let id = repository.create(&mut *tx, &data).await?;
            id_values.push(id.value);
        }
        tx.commit().await.context("commit")?;

        let count = |ids: Vec<i64>| {
            let repository = &repository;
            async move {
                repository
                    .count_by_condition(
                        &[],
                        &[],
                        Some(user_id.value),
                        None,
                        UpdatedAtRange::default(),
                        CreatedAtRange::default(),
                        None,
                        None,
                        Some(&ids),
                    )
                    .await
            }
        };

        assert_eq!(count(id_values.clone()).await?, N as i64);

        // Drop the first 900 ids — the count must shrink by exactly
        // that many. Catches a regression where a buggy literal builder
        // silently truncates past some threshold.
        let trimmed: Vec<i64> = id_values.iter().skip(900).copied().collect();
        assert_eq!(count(trimmed.clone()).await?, trimmed.len() as i64);

        // Non-existent ids in the allow-list must be ignored, not error.
        let mut mixed = id_values.clone();
        mixed.extend([999_999_999_001i64, 999_999_999_002, 999_999_999_003]);
        assert_eq!(count(mixed).await?, N as i64);

        // List path with limit + offset shares the literal-IN builder;
        // verify the page is sized correctly and only contains rows
        // from the allow-list.
        let page = repository
            .find_list_by_condition(
                Some(&50),
                Some(&0),
                &[],
                &[],
                Some(user_id.value),
                None,
                UpdatedAtRange::default(),
                CreatedAtRange::default(),
                None,
                None,
                Some(&id_values),
                MemorySort::default(),
                false,
            )
            .await?;
        assert_eq!(page.len(), 50);
        let allow: std::collections::HashSet<i64> = id_values.iter().copied().collect();
        for m in &page {
            assert!(allow.contains(&m.id.as_ref().unwrap().value));
        }

        // Single bulk DELETE avoids 1500 round-trips on the cleanup
        // path. `user_id = 9_001` is unique to this test fixture.
        sqlx::query(concat!("DELETE FROM memory WHERE user_id = ", p!(1)))
            .bind(user_id.value)
            .execute(db)
            .await
            .context("cleanup")?;
        Ok(())
    }

    #[test]
    fn run_find_list_by_condition_large_id_constraint_test() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let rdb_pool = setup_pool().await;
            _test_find_list_by_condition_large_id_constraint(rdb_pool).await
        })
    }

    /// `external_id_prefix` must
    ///   (a) match only rows whose external_id starts with the prefix,
    ///   (b) not bleed into adjacent prefixes that share a leading
    ///       substring (e.g. `obsidian:` vs `obsidian-private:`),
    ///   (c) treat LIKE meta-characters (`%` `_` `\`) as literals so
    ///       callers can pass arbitrary delimiters.
    async fn _test_external_id_prefix_filter(pool: &'static RdbPool) -> Result<()> {
        let repository = MemoryRepositoryImpl::new(crate::test_helper::shared_id_generator(), pool);
        let db = repository.db_pool();
        let user_id = UserId { value: 9_555_001 };

        // Seed three groups of memories with overlapping leading text.
        // Cleanup at the end uses the dedicated user_id so we never
        // leak rows into other tests.
        let seed = [
            "obsidian:s1:abc",
            "obsidian:s2:def",
            "obsidian-private:s1:ghi",
            "under_score:s1:jkl",     // metachar literal
            "under_scoreX:s1:mno",    // would falsely match without escape
            "with\\backslash:s1:pqr", // backslash literal
        ];
        let mut tx = db.begin().await.context("begin seed")?;
        for (i, eid) in seed.iter().enumerate() {
            let mem = MemoryData {
                parent_ids: vec![],
                user_id: Some(user_id),
                content: format!("payload {i}"),
                content_type: 0,
                params: None,
                metadata: None,
                created_at: 100 + i as i64,
                updated_at: 100 + i as i64,
                role: 1,
                external_id: Some((*eid).to_string()),
                media_object_id: None,
                thread_ids: Vec::new(),
            };
            repository.create(&mut *tx, &mem).await?;
        }
        tx.commit().await.context("commit seed")?;

        let collect_eids = |list: Vec<Memory>| -> std::collections::HashSet<String> {
            list.into_iter()
                .filter_map(|m| m.data.and_then(|d| d.external_id))
                .collect()
        };

        // (a) trailing-colon prefix returns only the obsidian: group.
        let by_prefix = repository
            .find_list_by_condition(
                None,
                None,
                &[],
                &[],
                Some(user_id.value),
                None,
                UpdatedAtRange::default(),
                CreatedAtRange::default(),
                None,
                Some("obsidian:"),
                None,
                MemorySort::default(),
                false,
            )
            .await?;
        assert_eq!(
            collect_eids(by_prefix),
            ["obsidian:s1:abc".to_string(), "obsidian:s2:def".to_string()]
                .into_iter()
                .collect::<std::collections::HashSet<String>>()
        );

        // (b) `obsidian-private` is NOT included by `obsidian:` lookup.
        let count_obsidian = repository
            .count_by_condition(
                &[],
                &[],
                Some(user_id.value),
                None,
                UpdatedAtRange::default(),
                CreatedAtRange::default(),
                None,
                Some("obsidian:"),
                None,
            )
            .await?;
        assert_eq!(count_obsidian, 2);

        // (c1) underscore is treated as literal — `under_score:` must
        // NOT pick up `under_scoreX:s1:mno`.
        let by_underscore = repository
            .find_list_by_condition(
                None,
                None,
                &[],
                &[],
                Some(user_id.value),
                None,
                UpdatedAtRange::default(),
                CreatedAtRange::default(),
                None,
                Some("under_score:"),
                None,
                MemorySort::default(),
                false,
            )
            .await?;
        assert_eq!(
            collect_eids(by_underscore),
            ["under_score:s1:jkl".to_string()]
                .into_iter()
                .collect::<std::collections::HashSet<String>>()
        );

        // (c2) backslash is treated as literal in the prefix.
        let by_backslash = repository
            .find_list_by_condition(
                None,
                None,
                &[],
                &[],
                Some(user_id.value),
                None,
                UpdatedAtRange::default(),
                CreatedAtRange::default(),
                None,
                Some("with\\back"),
                None,
                MemorySort::default(),
                false,
            )
            .await?;
        assert_eq!(
            collect_eids(by_backslash),
            ["with\\backslash:s1:pqr".to_string()]
                .into_iter()
                .collect::<std::collections::HashSet<String>>()
        );

        // Cleanup
        sqlx::query(concat!("DELETE FROM memory WHERE user_id = ", p!(1)))
            .bind(user_id.value)
            .execute(db)
            .await
            .context("cleanup")?;
        Ok(())
    }

    #[test]
    fn run_external_id_prefix_filter_test() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let rdb_pool = setup_pool().await;
            _test_external_id_prefix_filter(rdb_pool).await
        })
    }

    #[test]
    fn format_i64_in_list_handles_edge_cases() {
        use super::format_i64_in_list;
        assert_eq!(format_i64_in_list(&[]), "");
        assert_eq!(format_i64_in_list(&[42]), "42");
        assert_eq!(format_i64_in_list(&[1, 2, 3]), "1, 2, 3");
        // Negative i64 must keep the sign — IDs themselves are always
        // positive, but the helper must not silently lose it if the
        // contract changes.
        assert_eq!(
            format_i64_in_list(&[-1, i64::MIN]),
            "-1, -9223372036854775808"
        );
    }

    async fn _test_updated_at_range_filters(pool: &'static RdbPool) -> Result<()> {
        use crate::infra::thread_memory::rdb::{
            ThreadMemoryRepository, ThreadMemoryRepositoryImpl,
        };

        let repository = MemoryRepositoryImpl::new(crate::test_helper::shared_id_generator(), pool);
        let tm_repository = ThreadMemoryRepositoryImpl::new(pool);
        let db = repository.db_pool();
        let thread_id = 555_555;
        let user_id = UserId { value: 42 };

        let entries = [
            ("older", 100_i64, 1_i32),
            ("boundary", 200_i64, 1_i32),
            ("newer", 300_i64, 2_i32),
        ];

        let mut created_ids: Vec<MemoryId> = Vec::new();
        for (idx, (content, updated_at, role)) in entries.iter().enumerate() {
            let data = MemoryData {
                parent_ids: vec![],
                user_id: Some(user_id),
                content: (*content).to_string(),
                content_type: 0,
                params: None,
                metadata: None,
                created_at: *updated_at,
                updated_at: *updated_at,
                role: *role,
                external_id: None,
                media_object_id: None,
                thread_ids: Vec::new(),
            };
            let mut tx = db.begin().await.context("begin updated_at_range")?;
            let id = repository.create(&mut *tx, &data).await?;
            tm_repository
                .insert_auto_position_tx(&mut *tx, thread_id, id.value, idx as i64)
                .await?;
            tx.commit().await.context("commit updated_at_range")?;
            created_ids.push(id);
        }

        let recent_after = repository
            .find_recent_list_by_user_id(
                user_id,
                None,
                UpdatedAtRange {
                    updated_after: Some(200),
                    updated_before: None,
                },
                false,
            )
            .await?;
        assert_eq!(recent_after.len(), 1);
        assert_eq!(recent_after[0].data.as_ref().unwrap().content, "newer");

        let recent_before = repository
            .find_recent_list_by_user_id(
                user_id,
                None,
                UpdatedAtRange {
                    updated_after: None,
                    updated_before: Some(200),
                },
                false,
            )
            .await?;
        assert_eq!(recent_before.len(), 2);
        assert_eq!(recent_before[0].data.as_ref().unwrap().content, "boundary");
        assert_eq!(recent_before[1].data.as_ref().unwrap().content, "older");

        let ranged = repository
            .find_list_by_condition(
                None,
                None,
                &[],
                &[],
                Some(user_id.value),
                Some(thread_id),
                UpdatedAtRange {
                    updated_after: Some(100),
                    updated_before: Some(300),
                },
                CreatedAtRange::default(),
                None,
                None,
                None,
                MemorySort::default(),
                false,
            )
            .await?;
        assert_eq!(ranged.len(), 2);
        let ranged_contents = ranged
            .iter()
            .map(|m| m.data.as_ref().unwrap().content.as_str())
            .collect::<Vec<_>>();
        assert!(ranged_contents.contains(&"newer"));
        assert!(ranged_contents.contains(&"boundary"));

        let ranged_count = repository
            .count_by_condition(
                &[],
                &[],
                Some(user_id.value),
                Some(thread_id),
                UpdatedAtRange {
                    updated_after: Some(100),
                    updated_before: Some(300),
                },
                CreatedAtRange::default(),
                None,
                None,
                None,
            )
            .await?;
        assert_eq!(ranged_count, 2);

        tm_repository.delete_by_thread_tx(db, thread_id).await?;
        for id in &created_ids {
            repository.delete(id).await?;
        }
        Ok(())
    }

    #[test]
    fn run_updated_at_range_filters_test() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let rdb_pool = setup_pool().await;
            _test_updated_at_range_filters(rdb_pool).await
        })
    }

    // ---- Phase 4: find_by_ids_with_position_tx ----

    /// Helper: insert a memory with the given content/role and attach it to
    /// `thread_id` under `position`.
    async fn _create_positioned_memory(
        repository: &MemoryRepositoryImpl,
        tm_repository: &crate::infra::thread_memory::rdb::ThreadMemoryRepositoryImpl,
        db: &RdbPool,
        thread_id: i64,
        position: i32,
        content: &str,
        role: i32,
    ) -> Result<MemoryId> {
        use crate::infra::thread_memory::rdb::ThreadMemoryRepository;
        let data = MemoryData {
            parent_ids: vec![],
            user_id: Some(UserId { value: 1 }),
            content: content.to_string(),
            content_type: 0,
            params: None,
            metadata: None,
            created_at: 1_000 + position as i64,
            updated_at: 1_000 + position as i64,
            role,
            external_id: None,
            media_object_id: None,
            thread_ids: Vec::new(),
        };
        let mut tx = db.begin().await.context("begin positioned")?;
        let id = repository.create(&mut *tx, &data).await?;
        tm_repository
            .insert_tx(
                &mut *tx,
                thread_id,
                id.value,
                position,
                1_000 + position as i64,
            )
            .await?;
        tx.commit().await.context("commit positioned")?;
        Ok(id)
    }

    /// Results are returned in `tm.position ASC` order regardless of the
    /// order `ids` was passed in.
    async fn _test_find_by_ids_with_position_returns_sorted(pool: &'static RdbPool) -> Result<()> {
        use crate::infra::thread_memory::rdb::{
            ThreadMemoryRepository, ThreadMemoryRepositoryImpl,
        };
        let repository = MemoryRepositoryImpl::new(crate::test_helper::shared_id_generator(), pool);
        let tm_repository = ThreadMemoryRepositoryImpl::new(pool);
        let db = repository.db_pool();
        let thread_id = 7_100_001;

        // Insert three memories at positions 2, 0, 1 (out of order on purpose).
        let id_at_2 = _create_positioned_memory(
            &repository,
            &tm_repository,
            db,
            thread_id,
            2,
            "at position 2",
            1,
        )
        .await?;
        let id_at_0 = _create_positioned_memory(
            &repository,
            &tm_repository,
            db,
            thread_id,
            0,
            "at position 0",
            1,
        )
        .await?;
        let id_at_1 = _create_positioned_memory(
            &repository,
            &tm_repository,
            db,
            thread_id,
            1,
            "at position 1",
            1,
        )
        .await?;

        // Query in insertion order (2, 0, 1) — result must come back sorted.
        let mut tx = db.begin().await.context("begin query")?;
        let results = repository
            .find_by_ids_with_position_tx(
                &mut tx,
                thread_id,
                &[id_at_2.value, id_at_0.value, id_at_1.value],
                false,
            )
            .await?;
        tx.commit().await.context("commit query")?;

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].1, 0);
        assert_eq!(results[1].1, 1);
        assert_eq!(results[2].1, 2);
        assert_eq!(
            results[0].0.id.as_ref().unwrap().value,
            id_at_0.value,
            "position 0 row must be id_at_0"
        );
        assert_eq!(
            results[1].0.id.as_ref().unwrap().value,
            id_at_1.value,
            "position 1 row must be id_at_1"
        );
        assert_eq!(
            results[2].0.id.as_ref().unwrap().value,
            id_at_2.value,
            "position 2 row must be id_at_2"
        );

        // cleanup
        tm_repository.delete_by_thread_tx(db, thread_id).await?;
        repository.delete(&id_at_0).await?;
        repository.delete(&id_at_1).await?;
        repository.delete(&id_at_2).await?;
        Ok(())
    }

    /// Memories attached to another thread must not leak into the query
    /// even when their IDs are passed in.
    async fn _test_find_by_ids_with_position_thread_isolation(
        pool: &'static RdbPool,
    ) -> Result<()> {
        use crate::infra::thread_memory::rdb::{
            ThreadMemoryRepository, ThreadMemoryRepositoryImpl,
        };
        let repository = MemoryRepositoryImpl::new(crate::test_helper::shared_id_generator(), pool);
        let tm_repository = ThreadMemoryRepositoryImpl::new(pool);
        let db = repository.db_pool();
        let thread_a = 7_100_010;
        let thread_b = 7_100_011;

        let id_a = _create_positioned_memory(
            &repository,
            &tm_repository,
            db,
            thread_a,
            0,
            "only in thread A",
            1,
        )
        .await?;
        let id_b = _create_positioned_memory(
            &repository,
            &tm_repository,
            db,
            thread_b,
            0,
            "only in thread B",
            1,
        )
        .await?;

        // Query under thread_a with both IDs — only id_a should come back.
        let mut tx = db.begin().await.context("begin isolation")?;
        let results = repository
            .find_by_ids_with_position_tx(&mut tx, thread_a, &[id_a.value, id_b.value], false)
            .await?;
        tx.commit().await.context("commit isolation")?;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].0.id.as_ref().unwrap().value, id_a.value);

        // Cleanup both threads.
        tm_repository.delete_by_thread_tx(db, thread_a).await?;
        tm_repository.delete_by_thread_tx(db, thread_b).await?;
        repository.delete(&id_a).await?;
        repository.delete(&id_b).await?;
        Ok(())
    }

    /// IDs that exist in `memory` but are not attached to the queried
    /// thread in `thread_memory` must be silently skipped (not error).
    async fn _test_find_by_ids_with_position_skips_unregistered(
        pool: &'static RdbPool,
    ) -> Result<()> {
        use crate::infra::thread_memory::rdb::{
            ThreadMemoryRepository, ThreadMemoryRepositoryImpl,
        };
        let repository = MemoryRepositoryImpl::new(crate::test_helper::shared_id_generator(), pool);
        let tm_repository = ThreadMemoryRepositoryImpl::new(pool);
        let db = repository.db_pool();
        let thread_id = 7_100_020;

        // Attached memory (valid).
        let attached_id =
            _create_positioned_memory(&repository, &tm_repository, db, thread_id, 0, "attached", 1)
                .await?;

        // Free-standing memory: exists in `memory` but has no junction row.
        let free_data = MemoryData {
            parent_ids: vec![],
            user_id: Some(UserId { value: 1 }),
            content: "free-standing".to_string(),
            content_type: 0,
            params: None,
            metadata: None,
            created_at: 2_000,
            updated_at: 2_000,
            role: 1,
            external_id: None,
            media_object_id: None,
            thread_ids: Vec::new(),
        };
        let mut tx = db.begin().await.context("begin free")?;
        let free_id = repository.create(&mut *tx, &free_data).await?;
        tx.commit().await.context("commit free")?;

        // Query both. Missing-from-junction IDs should drop silently,
        // not error out.
        let mut tx = db.begin().await.context("begin skip")?;
        let results = repository
            .find_by_ids_with_position_tx(
                &mut tx,
                thread_id,
                &[attached_id.value, free_id.value, 999_999_999],
                false,
            )
            .await?;
        tx.commit().await.context("commit skip")?;

        assert_eq!(
            results.len(),
            1,
            "only the attached memory should be returned"
        );
        assert_eq!(results[0].0.id.as_ref().unwrap().value, attached_id.value);

        // Cleanup
        tm_repository.delete_by_thread_tx(db, thread_id).await?;
        repository.delete(&attached_id).await?;
        repository.delete(&free_id).await?;
        Ok(())
    }

    /// Empty ID slice returns empty result without touching the DB.
    async fn _test_find_by_ids_with_position_empty(pool: &'static RdbPool) -> Result<()> {
        let repository = MemoryRepositoryImpl::new(crate::test_helper::shared_id_generator(), pool);
        let db = repository.db_pool();
        let mut tx = db.begin().await.context("begin empty")?;
        let results = repository
            .find_by_ids_with_position_tx(&mut tx, 0, &[], false)
            .await?;
        tx.commit().await.context("commit empty")?;
        assert!(results.is_empty());
        Ok(())
    }

    /// Regression pin for chunk-boundary ordering: when the input id list
    /// exceeds the internal `sql::IN_LIST_CHUNK_SIZE`, the accumulated per-chunk
    /// results must still be globally sorted by `tm.position ASC`.
    ///
    /// Before the fix, `find_by_ids_with_position_tx` only sorted within
    /// each chunk and appended the chunks verbatim, so a second chunk
    /// containing smaller positions than the first chunk produced an
    /// out-of-order final vector.
    ///
    /// We insert `N > sql::IN_LIST_CHUNK_SIZE` memories under a single thread, assign
    /// their positions in the reverse of their id order, then query in a
    /// way that puts the smallest-position ids into the second chunk.
    /// The expected result is positions 0..N-1 in that exact order.
    async fn _test_find_by_ids_with_position_sorts_across_chunks(
        pool: &'static RdbPool,
    ) -> Result<()> {
        use crate::infra::thread_memory::rdb::{
            ThreadMemoryRepository, ThreadMemoryRepositoryImpl,
        };

        let repository = MemoryRepositoryImpl::new(crate::test_helper::shared_id_generator(), pool);
        let tm_repository = ThreadMemoryRepositoryImpl::new(pool);
        let db = repository.db_pool();
        let thread_id = 7_100_100;

        // `sql::IN_LIST_CHUNK_SIZE` inside `find_by_ids_with_position_tx` is 500; 600
        // comfortably crosses the boundary and exercises the global sort
        // without making the test prohibitively slow.
        const N: usize = 600;

        // Insert N memories. We assign `position = (N - 1) - index` so
        // that the memory created first (smallest id) ends up at the
        // largest position, and the memory created last (largest id)
        // ends up at position 0. After this setup "id ASC" and
        // "position ASC" are opposite orderings.
        let mut ids: Vec<i64> = Vec::with_capacity(N);
        for i in 0..N {
            let position = (N - 1 - i) as i32;
            let id = _create_positioned_memory(
                &repository,
                &tm_repository,
                db,
                thread_id,
                position,
                &format!("row {i}"),
                1,
            )
            .await?;
            ids.push(id.value);
        }

        // Query in id-ASC order. The first chunk (first 500 ids) holds
        // the largest positions (599..100), and the second chunk (last
        // 100 ids) holds the smallest positions (99..0). Without the
        // global merge step the returned vector would start with
        // position 100 and jump to position 0.
        let mut tx = db.begin().await.context("begin chunk-sort")?;
        let results = repository
            .find_by_ids_with_position_tx(&mut tx, thread_id, &ids, false)
            .await?;
        tx.commit().await.context("commit chunk-sort")?;

        assert_eq!(results.len(), N);
        // Verify the result is globally sorted by position ASC.
        for (i, (_, pos)) in results.iter().enumerate() {
            assert_eq!(
                *pos, i as i32,
                "row at index {i} must carry position {i}, got {pos}"
            );
        }

        // Cleanup
        tm_repository.delete_by_thread_tx(db, thread_id).await?;
        for id in &ids {
            repository.delete(&MemoryId { value: *id }).await?;
        }
        Ok(())
    }

    #[test]
    fn run_find_by_ids_with_position_returns_sorted_test() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let rdb_pool = setup_pool().await;
            _test_find_by_ids_with_position_returns_sorted(rdb_pool).await
        })
    }

    #[test]
    fn run_find_by_ids_with_position_thread_isolation_test() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let rdb_pool = setup_pool().await;
            _test_find_by_ids_with_position_thread_isolation(rdb_pool).await
        })
    }

    #[test]
    fn run_find_by_ids_with_position_skips_unregistered_test() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let rdb_pool = setup_pool().await;
            _test_find_by_ids_with_position_skips_unregistered(rdb_pool).await
        })
    }

    #[test]
    fn run_find_by_ids_with_position_empty_test() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let rdb_pool = setup_pool().await;
            _test_find_by_ids_with_position_empty(rdb_pool).await
        })
    }

    #[test]
    fn run_find_by_ids_with_position_sorts_across_chunks_test() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let rdb_pool = setup_pool().await;
            _test_find_by_ids_with_position_sorts_across_chunks(rdb_pool).await
        })
    }

    // ---- Phase 2: media_object_id column + UPDATE_CONTENT_ONLY ----

    /// media_object_id round-trips through INSERT/UPDATE/FIND for both
    /// Some and None (image memory feature, design 1/3 §3.2).
    async fn _test_media_object_id_round_trip(pool: &'static RdbPool) -> Result<()> {
        let repo = MemoryRepositoryImpl::new(crate::test_helper::shared_id_generator(), pool);
        let db = repo.db_pool();

        // None on create -> None on find.
        let data = MemoryData {
            parent_ids: vec![],
            user_id: Some(UserId { value: 1 }),
            content: "no media".to_string(),
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
        let mut tx = db.begin().await?;
        let id = repo.create(&mut *tx, &data).await?;
        tx.commit().await?;
        let got = repo.find(&id, false).await?.unwrap();
        assert_eq!(got.data.as_ref().unwrap().media_object_id, None);

        // Update to Some(7) -> persisted.
        let updated = MemoryData {
            media_object_id: Some(protobuf::llm_memory::data::MediaObjectId { value: 7 }),
            thread_ids: Vec::new(),
            ..data.clone()
        };
        let mut tx = db.begin().await?;
        assert!(repo.update(&mut *tx, &id, &updated).await?);
        tx.commit().await?;
        let got = repo.find(&id, false).await?.unwrap();
        assert_eq!(
            got.data.as_ref().unwrap().media_object_id,
            Some(protobuf::llm_memory::data::MediaObjectId { value: 7 })
        );

        // Create directly with Some(42) -> persisted (boundary: large id).
        let with_media = MemoryData {
            media_object_id: Some(protobuf::llm_memory::data::MediaObjectId { value: 42 }),
            thread_ids: Vec::new(),
            content: "has media".to_string(),
            ..data.clone()
        };
        let mut tx = db.begin().await?;
        let id2 = repo.create(&mut *tx, &with_media).await?;
        tx.commit().await?;
        let got2 = repo.find(&id2, false).await?.unwrap();
        assert_eq!(
            got2.data.as_ref().unwrap().media_object_id,
            Some(protobuf::llm_memory::data::MediaObjectId { value: 42 })
        );

        repo.delete(&id).await?;
        repo.delete(&id2).await?;
        Ok(())
    }

    #[test]
    fn run_media_object_id_round_trip_test() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let rdb_pool = setup_pool().await;
            _test_media_object_id_round_trip(rdb_pool).await
        })
    }

    /// update_content_only changes ONLY content + updated_at; every other
    /// field (parent_ids, role, media_object_id, external_id) is left
    /// intact. A non-existent id returns Ok(false). (design 2/3 §7.5.3)
    async fn _test_update_content_only(pool: &'static RdbPool) -> Result<()> {
        let repo = MemoryRepositoryImpl::new(crate::test_helper::shared_id_generator(), pool);
        let db = repo.db_pool();

        let data = MemoryData {
            parent_ids: vec![MemoryId { value: 11 }],
            user_id: Some(UserId { value: 3 }),
            content: "original".to_string(),
            content_type: 2,
            // Must be valid JSON: `params`/`metadata` are JSONB on
            // postgres (bound via `p_jsonb!`), so a bare `p` fails with
            // "invalid input syntax for type json". A JSON string literal
            // is the minimal valid value (matches the other rdb tests).
            params: Some("\"p\"".to_string()),
            metadata: Some("\"m\"".to_string()),
            created_at: 500,
            updated_at: 500,
            role: 1,
            external_id: Some("ext-1".to_string()),
            media_object_id: Some(protobuf::llm_memory::data::MediaObjectId { value: 9 }),
            thread_ids: Vec::new(),
        };
        let mut tx = db.begin().await?;
        let id = repo.create(&mut *tx, &data).await?;
        tx.commit().await?;

        let mut tx = db.begin().await?;
        assert!(
            repo.update_content_only(&mut *tx, &id, "caption text")
                .await?
        );
        tx.commit().await?;

        let got = repo.find(&id, false).await?.unwrap();
        let d = got.data.as_ref().unwrap();
        assert_eq!(d.content, "caption text", "content updated");
        assert!(d.updated_at >= 500, "updated_at server-filled");
        // Everything else untouched.
        assert_eq!(d.parent_ids, vec![MemoryId { value: 11 }]);
        assert_eq!(d.role, 1);
        assert_eq!(d.content_type, 2);
        assert_eq!(d.external_id.as_deref(), Some("ext-1"));
        assert_eq!(
            d.media_object_id,
            Some(protobuf::llm_memory::data::MediaObjectId { value: 9 }),
            "media_object_id must NOT be dropped by content-only update"
        );

        // Non-existent id -> Ok(false).
        let mut tx = db.begin().await?;
        let missing = repo
            .update_content_only(&mut *tx, &MemoryId { value: 999_999_999 }, "x")
            .await?;
        tx.commit().await?;
        assert!(!missing, "update_content_only on missing id returns false");

        repo.delete(&id).await?;
        Ok(())
    }

    #[test]
    fn run_update_content_only_test() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let rdb_pool = setup_pool().await;
            _test_update_content_only(rdb_pool).await
        })
    }
}
