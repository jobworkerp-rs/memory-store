use crate::error::LlmMemoryError;
use crate::sql::{build_in_placeholders, p};
use anyhow::{Context, Result};
use async_trait::async_trait;
use infra_utils::infra::rdb::Rdb;
use infra_utils::infra::rdb::RdbPool;
use infra_utils::infra::rdb::UseRdbPool;
use protobuf::llm_memory::data::MemoryId;
use sqlx::Executor;
use std::collections::HashMap;

use super::rows::ThreadMemoryRow;

/// Chunk size for `find_memory_ids_by_thread_ids` IN-list binding.
/// Picked below the older SQLite `SQLITE_MAX_VARIABLE_NUMBER` ceiling
/// of 999 so the helper works against any backend version we support
/// without a runtime probe (PostgreSQL caps at 65_535, modern SQLite
/// at 32_766 — both safely above 900). See the method's doc-comment
/// for why this matters when MAX_THREAD_IDS = 100_000.
const THREAD_ID_IN_CHUNK_SIZE: usize = 900;

const INSERT_SQL: &str = concat!(
    "INSERT INTO thread_memory (thread_id, memory_id, position, created_at) VALUES (",
    p!(1),
    ",",
    p!(2),
    ",",
    p!(3),
    ",",
    p!(4),
    ")"
);

// Atomic insert with auto-calculated next position (race-condition safe)
const INSERT_AUTO_POSITION_SQL: &str = concat!(
    "INSERT INTO thread_memory (thread_id, memory_id, position, created_at)",
    " SELECT ",
    p!(1),
    ", ",
    p!(2),
    ", COALESCE(MAX(position), -1) + 1, ",
    p!(3),
    " FROM thread_memory WHERE thread_id = ",
    p!(4),
);

// Idempotent variant used by Phase 4 parent_ids attachment. When the same
// (thread_id, memory_id) pair already exists (because e.g. two conversations
// share a ROLE_SYSTEM prompt that the thread has already registered), the
// insert becomes a no-op instead of failing the whole add_memory transaction.
//
// SQLite: `INSERT OR IGNORE` silently drops rows that violate the PK.
// PostgreSQL: `ON CONFLICT (thread_id, memory_id) DO NOTHING` achieves the
// same semantics against the PK of `thread_memory`. Both backends still run
// the MAX(position) subquery, so a fresh attachment picks up the next position
// in order.
#[cfg(feature = "postgres")]
const INSERT_OR_IGNORE_AUTO_POSITION_SQL: &str = concat!(
    "INSERT INTO thread_memory (thread_id, memory_id, position, created_at)",
    " SELECT ",
    p!(1),
    ", ",
    p!(2),
    ", COALESCE(MAX(position), -1) + 1, ",
    p!(3),
    " FROM thread_memory WHERE thread_id = ",
    p!(4),
    " ON CONFLICT (thread_id, memory_id) DO NOTHING"
);

#[cfg(not(feature = "postgres"))]
const INSERT_OR_IGNORE_AUTO_POSITION_SQL: &str = concat!(
    "INSERT OR IGNORE INTO thread_memory (thread_id, memory_id, position, created_at)",
    " SELECT ",
    p!(1),
    ", ",
    p!(2),
    ", COALESCE(MAX(position), -1) + 1, ",
    p!(3),
    " FROM thread_memory WHERE thread_id = ",
    p!(4),
);

const DELETE_BY_THREAD_SQL: &str = concat!("DELETE FROM thread_memory WHERE thread_id = ", p!(1));

const DELETE_BY_MEMORY_SQL: &str = concat!("DELETE FROM thread_memory WHERE memory_id = ", p!(1));

const DELETE_ONE_SQL: &str = concat!(
    "DELETE FROM thread_memory WHERE thread_id = ",
    p!(1),
    " AND memory_id = ",
    p!(2)
);

const FIND_BY_THREAD_SQL: &str = concat!(
    "SELECT thread_id, memory_id, position, created_at",
    " FROM thread_memory WHERE thread_id = ",
    p!(1),
    " ORDER BY position ASC"
);

const FIND_MEMORY_IDS_BY_THREAD_SQL: &str = concat!(
    "SELECT memory_id FROM thread_memory WHERE thread_id = ",
    p!(1),
    " ORDER BY position ASC"
);

const MAX_POSITION_SQL: &str = concat!(
    "SELECT COALESCE(MAX(position), -1) FROM thread_memory WHERE thread_id = ",
    p!(1)
);

const FIND_EXCLUSIVE_MEMORY_IDS_SQL: &str = concat!(
    "SELECT tm.memory_id FROM thread_memory tm",
    " WHERE tm.thread_id = ",
    p!(1),
    " AND NOT EXISTS (",
    " SELECT 1 FROM thread_memory tm2",
    " WHERE tm2.memory_id = tm.memory_id",
    " AND tm2.thread_id != ",
    p!(2),
    ")"
);

const COUNT_REFS_SQL: &str = concat!(
    "SELECT COUNT(*) FROM thread_memory WHERE memory_id = ",
    p!(1)
);

// Lightweight existence probe for a single (thread_id, memory_id) pair —
// backs `contains_tx`. Used by callers that need to validate that a pivot
// memory belongs to a specific thread before running a thread-scoped
// operation on it.
const CONTAINS_SQL: &str = concat!(
    "SELECT 1 FROM thread_memory WHERE thread_id = ",
    p!(1),
    " AND memory_id = ",
    p!(2),
    " LIMIT 1"
);

// Read the position column for a given (thread_id, memory_id) pair.
// Backs `find_position_tx`, used by `add_memories_batch` to surface
// the assigned position back to the client without a full
// `find_by_thread_tx` scan.
const FIND_POSITION_SQL: &str = concat!(
    "SELECT position FROM thread_memory WHERE thread_id = ",
    p!(1),
    " AND memory_id = ",
    p!(2),
    " LIMIT 1"
);

// Reverse lookup: given a memory_id, return every thread that has it
// attached. Used by `GetSurroundingMemories`'s backward-compatibility
// fallback so that legacy clients which do not set thread_id can still
// succeed when the memory unambiguously belongs to a single thread.
// LIMIT 2: the caller only needs to distinguish 0 / 1 / multiple, so
// fetching at most 2 rows avoids loading all thread_ids when a shared
// system prompt is attached to many threads.
const FIND_THREADS_BY_MEMORY_SQL: &str = concat!(
    "SELECT thread_id FROM thread_memory WHERE memory_id = ",
    p!(1),
    " LIMIT 2",
);

#[async_trait]
pub trait ThreadMemoryRepository: UseRdbPool + Sync + Send {
    async fn insert_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        thread_id: i64,
        memory_id: i64,
        position: i32,
        created_at: i64,
    ) -> Result<()> {
        sqlx::query::<Rdb>(INSERT_SQL)
            .bind(thread_id)
            .bind(memory_id)
            .bind(position)
            .bind(created_at)
            .execute(tx)
            .await
            .map(|_| ())
            .map_err(LlmMemoryError::DBError)
            .context(format!(
                "error in thread_memory insert: thread={}, memory={}",
                thread_id, memory_id
            ))
    }

    /// Atomically insert with auto-calculated next position (race-condition safe).
    async fn insert_auto_position_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        thread_id: i64,
        memory_id: i64,
        created_at: i64,
    ) -> Result<()> {
        sqlx::query::<Rdb>(INSERT_AUTO_POSITION_SQL)
            .bind(thread_id)
            .bind(memory_id)
            .bind(created_at)
            .bind(thread_id)
            .execute(tx)
            .await
            .map(|_| ())
            .map_err(LlmMemoryError::DBError)
            .context(format!(
                "error in thread_memory insert_auto_position: thread={}, memory={}",
                thread_id, memory_id
            ))
    }

    /// Idempotent variant of `insert_auto_position_tx`.
    ///
    /// When `(thread_id, memory_id)` is already present in the junction this
    /// call is a silent no-op (existing row's position is preserved). Used by
    /// `ThreadApp::add_memory` to attach `parent_ids` references — shared
    /// ROLE_SYSTEM prompts in particular can already be in the junction from
    /// a previous AddMemory, and the caller should not have to branch on
    /// presence.
    async fn insert_or_ignore_auto_position_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        thread_id: i64,
        memory_id: i64,
        created_at: i64,
    ) -> Result<()> {
        sqlx::query::<Rdb>(INSERT_OR_IGNORE_AUTO_POSITION_SQL)
            .bind(thread_id)
            .bind(memory_id)
            .bind(created_at)
            .bind(thread_id)
            .execute(tx)
            .await
            .map(|_| ())
            .map_err(LlmMemoryError::DBError)
            .context(format!(
                "error in thread_memory insert_or_ignore_auto_position: thread={}, memory={}",
                thread_id, memory_id
            ))
    }

    async fn next_position_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        thread_id: i64,
    ) -> Result<i32> {
        let max: i32 = sqlx::query_scalar(MAX_POSITION_SQL)
            .bind(thread_id)
            .fetch_one(tx)
            .await
            .map_err(LlmMemoryError::DBError)
            .context("error in thread_memory max_position")?;
        Ok(max + 1)
    }

    async fn find_memory_ids_by_thread_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        thread_id: i64,
    ) -> Result<Vec<MemoryId>> {
        sqlx::query_scalar::<_, i64>(FIND_MEMORY_IDS_BY_THREAD_SQL)
            .bind(thread_id)
            .fetch_all(tx)
            .await
            .map(|ids| ids.into_iter().map(|id| MemoryId { value: id }).collect())
            .map_err(LlmMemoryError::DBError)
            .context(format!(
                "error in thread_memory find_memory_ids: thread={}",
                thread_id
            ))
    }

    async fn find_by_thread_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        thread_id: i64,
    ) -> Result<Vec<ThreadMemoryRow>> {
        sqlx::query_as::<_, ThreadMemoryRow>(FIND_BY_THREAD_SQL)
            .bind(thread_id)
            .fetch_all(tx)
            .await
            .map_err(LlmMemoryError::DBError)
            .context(format!(
                "error in thread_memory find_by_thread: thread={}",
                thread_id
            ))
    }

    /// Count how many threads reference a given memory_id.
    async fn count_refs_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        memory_id: i64,
    ) -> Result<i64> {
        sqlx::query_scalar::<_, i64>(COUNT_REFS_SQL)
            .bind(memory_id)
            .fetch_one(tx)
            .await
            .map_err(LlmMemoryError::DBError)
            .context(format!(
                "error in thread_memory count_refs: memory={}",
                memory_id
            ))
    }

    /// Whether `(thread_id, memory_id)` exists in the junction table.
    ///
    /// Cheaper than pulling the full member list: a `LIMIT 1` probe against
    /// the `PRIMARY KEY (thread_id, memory_id)` index. Used by callers that
    /// need to reject thread-scoped operations on a pivot memory before
    /// running more expensive queries on it.
    async fn contains_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        thread_id: i64,
        memory_id: i64,
    ) -> Result<bool> {
        let row: Option<i32> = sqlx::query_scalar(CONTAINS_SQL)
            .bind(thread_id)
            .bind(memory_id)
            .fetch_optional(tx)
            .await
            .map_err(LlmMemoryError::DBError)
            .context(format!(
                "error in thread_memory contains: thread={thread_id}, memory={memory_id}"
            ))?;
        Ok(row.is_some())
    }

    /// Look up `position` for a given `(thread_id, memory_id)` pair.
    /// Returns `None` when the pair is not present.
    async fn find_position_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        thread_id: i64,
        memory_id: i64,
    ) -> Result<Option<i32>> {
        sqlx::query_scalar::<_, i32>(FIND_POSITION_SQL)
            .bind(thread_id)
            .bind(memory_id)
            .fetch_optional(tx)
            .await
            .map_err(LlmMemoryError::DBError)
            .context(format!(
                "error in thread_memory find_position: thread={thread_id}, memory={memory_id}"
            ))
    }

    /// Reverse lookup from a `memory_id` to every `thread_id` that has it
    /// attached in the junction table. Used by
    /// `GetSurroundingMemories`'s backward-compat fallback: when a legacy
    /// client omits the new `thread_id` field (wire default 0), the server
    /// tries to resolve the target thread through this query and fails
    /// loudly if the memory lives in more than one thread.
    ///
    /// Returned order matches the underlying index scan and is therefore
    /// unspecified; callers that need a stable order should sort the
    /// result themselves.
    async fn find_threads_by_memory_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        memory_id: i64,
    ) -> Result<Vec<i64>> {
        sqlx::query_scalar::<_, i64>(FIND_THREADS_BY_MEMORY_SQL)
            .bind(memory_id)
            .fetch_all(tx)
            .await
            .map_err(LlmMemoryError::DBError)
            .context(format!(
                "error in thread_memory find_threads_by_memory: memory={memory_id}"
            ))
    }

    async fn delete_by_thread_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        thread_id: i64,
    ) -> Result<u64> {
        sqlx::query::<Rdb>(DELETE_BY_THREAD_SQL)
            .bind(thread_id)
            .execute(tx)
            .await
            .map(|r| r.rows_affected())
            .map_err(LlmMemoryError::DBError)
            .context(format!(
                "error in thread_memory delete_by_thread: thread={}",
                thread_id
            ))
    }

    async fn delete_by_memory_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        memory_id: i64,
    ) -> Result<u64> {
        sqlx::query::<Rdb>(DELETE_BY_MEMORY_SQL)
            .bind(memory_id)
            .execute(tx)
            .await
            .map(|r| r.rows_affected())
            .map_err(LlmMemoryError::DBError)
            .context(format!(
                "error in thread_memory delete_by_memory: memory={}",
                memory_id
            ))
    }

    /// Delete a single junction row identified by (thread_id, memory_id).
    /// Returns the number of rows affected (0 or 1).
    async fn delete_one_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        thread_id: i64,
        memory_id: i64,
    ) -> Result<u64> {
        sqlx::query::<Rdb>(DELETE_ONE_SQL)
            .bind(thread_id)
            .bind(memory_id)
            .execute(tx)
            .await
            .map(|r| r.rows_affected())
            .map_err(LlmMemoryError::DBError)
            .context(format!(
                "error in thread_memory delete_one: thread={}, memory={}",
                thread_id, memory_id
            ))
    }

    /// Resolve the union of memory_ids attached to any thread in
    /// `thread_ids`, deduplicated. Used by the P5 thread_filter feature
    /// so a single search query can be narrowed by a thread-side
    /// predicate (labels / channel / time range / owner).
    ///
    /// `SELECT DISTINCT` is intentional — a memory shared across multiple
    /// threads (e.g. a ROLE_SYSTEM prompt attached to several conversations)
    /// would otherwise be returned once per thread membership, which would
    /// make the downstream Count chunk-aggregation double-count and the
    /// search-side `IN (...)` wider than necessary. Empty input short-circuits
    /// to an empty Vec to avoid an invalid `IN ()` SQL.
    ///
    /// `max` is a memory-id-level absolute upper bound. The function
    /// stops as soon as the deduplicated set grows beyond `max`,
    /// returning `Vec::with_capacity > max` so the caller can detect
    /// the overflow and reject with `failed_precondition`. Without
    /// this guard a thread_filter that matches a small number of huge
    /// threads (one common label, one heavy user) would materialize
    /// millions of memory_ids before any downstream ceiling fires.
    /// SQL-side `LIMIT max + 1` would not help because the chunk loop
    /// still has to merge across chunks for DISTINCT correctness, so
    /// we cap on the merged set inside the loop.
    ///
    /// Chunking: thread_ids is split into batches of
    /// `THREAD_ID_IN_CHUNK_SIZE` before binding to keep us under the
    /// per-statement parameter ceiling. SQLite caps at
    /// `SQLITE_MAX_VARIABLE_NUMBER` (default 32_766 since 3.32.0,
    /// 999 on older builds); PostgreSQL caps at 65_535 (`i16` parameter
    /// count). `MEMORY_THREAD_FILTER_MAX_THREAD_IDS` defaults to 100_000,
    /// so a single bound IN list would blow up against either backend
    /// well before that ceiling. The 900 chunk fits older SQLite builds
    /// without runtime probing. The DISTINCT contract is preserved by
    /// HashSet-merging across chunks.
    async fn find_memory_ids_by_thread_ids(
        &self,
        thread_ids: &[i64],
        max: usize,
    ) -> Result<Vec<i64>> {
        if thread_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut seen: std::collections::HashSet<i64> =
            std::collections::HashSet::with_capacity(thread_ids.len());
        // Per-statement `LIMIT max + 1` shrinks the worst-case row
        // transfer cost when a single thread holds millions of rows.
        // The cap on the merged HashSet below is what enforces the
        // contract — `LIMIT` alone cannot do it because DISTINCT
        // operates per chunk.
        let per_chunk_limit: i64 = (max as i64).saturating_add(1);
        for chunk in thread_ids.chunks(THREAD_ID_IN_CHUNK_SIZE) {
            let placeholders = build_in_placeholders(chunk.len(), 1);
            let sql = format!(
                "SELECT DISTINCT memory_id FROM thread_memory \
                 WHERE thread_id IN ({placeholders}) LIMIT {per_chunk_limit}"
            );
            let mut query = sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(sql));
            for tid in chunk {
                query = query.bind(*tid);
            }
            let rows = query
                .fetch_all(self.db_pool())
                .await
                .map_err(LlmMemoryError::DBError)
                .context("error in find_memory_ids_by_thread_ids")?;
            seen.extend(rows);
            if seen.len() > max {
                // Return the over-cap set so the caller can size the
                // error message; the +1 sentinel keeps "exactly max"
                // distinguishable from "max + 1 or more".
                return Ok(seen.into_iter().collect());
            }
        }
        Ok(seen.into_iter().collect())
    }

    /// Bulk-resolve representative thread metadata
    /// for a set of memory_ids. For each memory, returns the row attached
    /// to the thread with the **smallest** thread_id (representative
    /// thread).
    ///
    /// Empty input short-circuits to an empty Vec to avoid an invalid
    /// `IN ()` SQL. Result order is unspecified — callers should not rely
    /// on input order.
    ///
    /// SQL form (JOIN with GROUP BY subquery) is chosen over a row-value
    /// `(memory_id, thread_id) IN (...)` predicate because the former
    /// (a) avoids duplicate IN bindings, (b) plays well with sqlx
    /// numbered placeholders, and (c) leaves a clean attachment point
    /// for picking up additional thread attributes in later phases.
    async fn find_positions_for_memories(
        &self,
        memory_ids: &[i64],
    ) -> Result<Vec<(i64, i64, i32)>> {
        if memory_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut all_rows: Vec<(i64, i64, i32)> = Vec::with_capacity(memory_ids.len());
        for chunk in memory_ids.chunks(THREAD_ID_IN_CHUNK_SIZE) {
            let placeholders = build_in_placeholders(chunk.len(), 1);
            let sql = format!(
                "SELECT tm.memory_id, tm.thread_id, tm.position \
                 FROM thread_memory tm \
                 JOIN ( \
                   SELECT memory_id, MIN(thread_id) AS rep_thread_id \
                   FROM thread_memory \
                   WHERE memory_id IN ({placeholders}) \
                   GROUP BY memory_id \
                 ) r \
                   ON tm.memory_id = r.memory_id \
                  AND tm.thread_id = r.rep_thread_id"
            );
            let mut query = sqlx::query_as::<_, (i64, i64, i32)>(sqlx::AssertSqlSafe(sql));
            for mid in chunk {
                query = query.bind(*mid);
            }
            let rows = query
                .fetch_all(self.db_pool())
                .await
                .map_err(LlmMemoryError::DBError)
                .context("error in find_positions_for_memories")?;
            all_rows.extend(rows);
        }
        Ok(all_rows)
    }

    /// Bulk reverse lookup from memory_id to every attached thread_id.
    /// The output values are sorted by thread_id so callers can expose a
    /// stable wire order without relying on backend-specific index scans.
    async fn find_thread_ids_by_memory_ids(
        &self,
        memory_ids: &[i64],
    ) -> Result<HashMap<i64, Vec<i64>>> {
        if memory_ids.is_empty() {
            return Ok(HashMap::new());
        }
        let mut out: HashMap<i64, Vec<i64>> = HashMap::with_capacity(memory_ids.len());
        for chunk in memory_ids.chunks(THREAD_ID_IN_CHUNK_SIZE) {
            let placeholders = build_in_placeholders(chunk.len(), 1);
            let sql = format!(
                "SELECT memory_id, thread_id FROM thread_memory \
                 WHERE memory_id IN ({placeholders}) \
                 ORDER BY memory_id ASC, thread_id ASC"
            );
            let mut query = sqlx::query_as::<_, (i64, i64)>(sqlx::AssertSqlSafe(sql));
            for mid in chunk {
                query = query.bind(*mid);
            }
            let rows = query
                .fetch_all(self.db_pool())
                .await
                .map_err(LlmMemoryError::DBError)
                .context("error in find_thread_ids_by_memory_ids")?;
            for (memory_id, thread_id) in rows {
                out.entry(memory_id).or_default().push(thread_id);
            }
        }
        Ok(out)
    }

    /// P1 (improve-search): Bulk-count `thread_memory` rows per thread.
    /// Used together with `find_positions_for_memories` to populate the
    /// `thread_total` field on `MemorySearchResult` (the "M" in `[N/M]`).
    ///
    /// Empty input short-circuits. Threads with zero rows simply do not
    /// appear as keys in the returned map.
    async fn count_by_thread_ids(
        &self,
        thread_ids: &[i64],
    ) -> Result<std::collections::HashMap<i64, i32>> {
        if thread_ids.is_empty() {
            return Ok(std::collections::HashMap::new());
        }
        let mut out: std::collections::HashMap<i64, i32> =
            std::collections::HashMap::with_capacity(thread_ids.len());
        for chunk in thread_ids.chunks(THREAD_ID_IN_CHUNK_SIZE) {
            let placeholders = build_in_placeholders(chunk.len(), 1);
            let sql = format!(
                "SELECT thread_id, COUNT(*) FROM thread_memory \
                 WHERE thread_id IN ({placeholders}) GROUP BY thread_id"
            );
            let mut query = sqlx::query_as::<_, (i64, i64)>(sqlx::AssertSqlSafe(sql));
            for tid in chunk {
                query = query.bind(*tid);
            }
            let rows = query
                .fetch_all(self.db_pool())
                .await
                .map_err(LlmMemoryError::DBError)
                .context("error in count_by_thread_ids")?;
            for (tid, n) in rows {
                // `COUNT(*)` is `bigint` (i64) on both backends; clamp to
                // i32 to match the proto field. UI display value, so a
                // saturating downcast is safe even in the absurd case
                // of >2B rows in a single thread.
                out.insert(tid, i32::try_from(n).unwrap_or(i32::MAX));
            }
        }
        Ok(out)
    }

    /// Find memory_ids that belong exclusively to this thread (not shared with others).
    async fn find_exclusive_memory_ids_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        thread_id: i64,
    ) -> Result<Vec<i64>> {
        sqlx::query_scalar::<_, i64>(FIND_EXCLUSIVE_MEMORY_IDS_SQL)
            .bind(thread_id)
            .bind(thread_id)
            .fetch_all(tx)
            .await
            .map_err(LlmMemoryError::DBError)
            .context(format!(
                "error in thread_memory find_exclusive_memory_ids: thread={}",
                thread_id
            ))
    }
}

pub struct ThreadMemoryRepositoryImpl {
    pool: &'static RdbPool,
}

impl ThreadMemoryRepositoryImpl {
    pub fn new(pool: &'static RdbPool) -> Self {
        Self { pool }
    }
}

impl UseRdbPool for ThreadMemoryRepositoryImpl {
    fn db_pool(&self) -> &RdbPool {
        self.pool
    }
}

impl ThreadMemoryRepository for ThreadMemoryRepositoryImpl {}

pub trait UseThreadMemoryRepository {
    fn thread_memory_repository(&self) -> &ThreadMemoryRepositoryImpl;
}

#[cfg(test)]
mod test {
    use super::*;
    use anyhow::{Context, Result};
    use infra_utils::infra::rdb::RdbPool;

    fn setup_pool() -> impl std::future::Future<Output = &'static RdbPool> {
        use infra_utils::infra::test::setup_test_rdb_from;
        async {
            if cfg!(feature = "postgres") {
                let pool = setup_test_rdb_from("sql/postgres").await;
                sqlx::query("TRUNCATE TABLE thread_memory CASCADE;")
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
                pool
            }
        }
    }

    async fn _test_next_position(pool: &'static RdbPool) -> Result<()> {
        let repo = ThreadMemoryRepositoryImpl::new(pool);
        let thread_id = 8_000_001;

        // Empty thread starts at position 0
        let pos = repo.next_position_tx(pool, thread_id).await?;
        assert_eq!(pos, 0, "first position should be 0");

        // Insert three entries and verify monotonic position increment
        for expected_pos in 0..3 {
            let pos = repo.next_position_tx(pool, thread_id).await?;
            assert_eq!(pos, expected_pos);
            repo.insert_tx(pool, thread_id, 100_000 + expected_pos as i64, pos, 1000)
                .await?;
        }

        // Next position should be 3
        let pos = repo.next_position_tx(pool, thread_id).await?;
        assert_eq!(pos, 3);

        // Cleanup
        repo.delete_by_thread_tx(pool, thread_id).await?;
        Ok(())
    }

    async fn _test_find_exclusive_memory_ids(pool: &'static RdbPool) -> Result<()> {
        let repo = ThreadMemoryRepositoryImpl::new(pool);
        let thread_a = 8_000_010;
        let thread_b = 8_000_011;
        let mem_shared = 900_001; // belongs to both threads
        let mem_exclusive_a = 900_002; // belongs only to thread_a
        let mem_exclusive_b = 900_003; // belongs only to thread_b

        // Setup: thread_a has [shared, exclusive_a], thread_b has [shared, exclusive_b]
        repo.insert_tx(pool, thread_a, mem_shared, 0, 1000)
            .await
            .context("insert shared to A")?;
        repo.insert_tx(pool, thread_a, mem_exclusive_a, 1, 1001)
            .await
            .context("insert exclusive to A")?;
        repo.insert_tx(pool, thread_b, mem_shared, 0, 1000)
            .await
            .context("insert shared to B")?;
        repo.insert_tx(pool, thread_b, mem_exclusive_b, 1, 1002)
            .await
            .context("insert exclusive to B")?;

        // Exclusive to thread_a should be [mem_exclusive_a] only
        let exclusive_a = repo
            .find_exclusive_memory_ids_tx(pool, thread_a)
            .await
            .context("find exclusive A")?;
        assert_eq!(exclusive_a, vec![mem_exclusive_a]);

        // Exclusive to thread_b should be [mem_exclusive_b] only
        let exclusive_b = repo
            .find_exclusive_memory_ids_tx(pool, thread_b)
            .await
            .context("find exclusive B")?;
        assert_eq!(exclusive_b, vec![mem_exclusive_b]);

        // After deleting thread_a's entries, shared becomes exclusive to thread_b
        repo.delete_by_thread_tx(pool, thread_a).await?;
        let exclusive_b_after = repo
            .find_exclusive_memory_ids_tx(pool, thread_b)
            .await
            .context("find exclusive B after A deleted")?;
        assert!(exclusive_b_after.contains(&mem_shared));
        assert!(exclusive_b_after.contains(&mem_exclusive_b));

        // Cleanup
        repo.delete_by_thread_tx(pool, thread_b).await?;
        Ok(())
    }

    async fn _test_find_thread_ids_by_memory_ids(pool: &'static RdbPool) -> Result<()> {
        let repo = ThreadMemoryRepositoryImpl::new(pool);
        let thread_a = 8_000_020;
        let thread_b = 8_000_021;
        let memory_shared = 900_020;
        let memory_single = 900_021;
        let memory_orphan = 900_022;

        repo.insert_tx(pool, thread_b, memory_shared, 0, 1000)
            .await?;
        repo.insert_tx(pool, thread_a, memory_shared, 0, 1000)
            .await?;
        repo.insert_tx(pool, thread_b, memory_single, 1, 1001)
            .await?;

        let by_memory = repo
            .find_thread_ids_by_memory_ids(&[memory_shared, memory_single, memory_orphan])
            .await?;

        assert_eq!(
            by_memory.get(&memory_shared),
            Some(&vec![thread_a, thread_b]),
            "thread_ids must be stable and sorted"
        );
        assert_eq!(by_memory.get(&memory_single), Some(&vec![thread_b]));
        assert!(
            !by_memory.contains_key(&memory_orphan),
            "unattached memory_ids should not appear in the map"
        );

        repo.delete_by_thread_tx(pool, thread_a).await?;
        repo.delete_by_thread_tx(pool, thread_b).await?;
        Ok(())
    }

    #[test]
    fn run_next_position_test() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_next_position(pool).await
        })
    }

    #[test]
    fn run_find_exclusive_memory_ids_test() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_find_exclusive_memory_ids(pool).await
        })
    }

    #[test]
    fn run_find_thread_ids_by_memory_ids_test() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_find_thread_ids_by_memory_ids(pool).await
        })
    }

    /// `insert_or_ignore_auto_position_tx` must be a silent no-op when the
    /// same `(thread_id, memory_id)` pair is re-attached, and it must leave
    /// the existing row's `position` and `created_at` untouched. Fresh
    /// attachments must still receive the next monotonic position.
    async fn _test_insert_or_ignore_auto_position(pool: &'static RdbPool) -> Result<()> {
        let repo = ThreadMemoryRepositoryImpl::new(pool);
        let thread_id = 8_000_050;
        let mem_a = 910_001;
        let mem_b = 910_002;

        // First attachment lands at position 0.
        repo.insert_or_ignore_auto_position_tx(pool, thread_id, mem_a, 1000)
            .await
            .context("initial attach A")?;

        // Re-attaching the same memory must not fail and must not advance
        // the position of the existing row.
        repo.insert_or_ignore_auto_position_tx(pool, thread_id, mem_a, 9999)
            .await
            .context("duplicate attach A")?;

        let rows_after_duplicate = repo
            .find_by_thread_tx(pool, thread_id)
            .await
            .context("find_by_thread after duplicate")?;
        assert_eq!(
            rows_after_duplicate.len(),
            1,
            "duplicate insert must not create a second row"
        );
        assert_eq!(
            rows_after_duplicate[0].position, 0,
            "existing position must be preserved"
        );
        assert_eq!(
            rows_after_duplicate[0].created_at, 1000,
            "existing created_at must be preserved"
        );

        // A fresh memory_id should land at the next monotonic position (1).
        repo.insert_or_ignore_auto_position_tx(pool, thread_id, mem_b, 1001)
            .await
            .context("attach B")?;

        let rows = repo
            .find_by_thread_tx(pool, thread_id)
            .await
            .context("find_by_thread after attach B")?;
        assert_eq!(rows.len(), 2);
        // rows come back sorted by position ASC (find_by_thread invariant).
        assert_eq!(rows[0].memory_id, mem_a);
        assert_eq!(rows[0].position, 0);
        assert_eq!(rows[1].memory_id, mem_b);
        assert_eq!(rows[1].position, 1);

        // Re-attaching memB is also a no-op.
        repo.insert_or_ignore_auto_position_tx(pool, thread_id, mem_b, 2000)
            .await
            .context("duplicate attach B")?;
        let rows_final = repo.find_by_thread_tx(pool, thread_id).await?;
        assert_eq!(rows_final.len(), 2);
        assert_eq!(rows_final[1].position, 1);
        assert_eq!(rows_final[1].created_at, 1001);

        // Cleanup
        repo.delete_by_thread_tx(pool, thread_id).await?;
        Ok(())
    }

    #[test]
    fn run_insert_or_ignore_auto_position_test() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_insert_or_ignore_auto_position(pool).await
        })
    }

    /// P5 (improve-search): when the same memory is attached to multiple
    /// threads, `find_memory_ids_by_thread_ids` must return it exactly
    /// once. This is the structural invariant that lets the Count
    /// chunk-aggregation path stay strict — without DISTINCT, a memory
    /// shared across N threads would get counted N times.
    async fn _test_find_memory_ids_by_thread_ids_distinct(pool: &'static RdbPool) -> Result<()> {
        let repo = ThreadMemoryRepositoryImpl::new(pool);
        let thread_a = 8_000_100;
        let thread_b = 8_000_101;
        let thread_c = 8_000_102;
        let mem_shared = 920_001; // attached to both A and B
        let mem_only_a = 920_002;
        let mem_only_b = 920_003;
        let mem_only_c = 920_004;

        repo.insert_tx(pool, thread_a, mem_shared, 0, 1000).await?;
        repo.insert_tx(pool, thread_a, mem_only_a, 1, 1001).await?;
        repo.insert_tx(pool, thread_b, mem_shared, 0, 1002).await?;
        repo.insert_tx(pool, thread_b, mem_only_b, 1, 1003).await?;
        repo.insert_tx(pool, thread_c, mem_only_c, 0, 1004).await?;

        // Empty input must short-circuit without an invalid `IN ()` SQL.
        let empty = repo.find_memory_ids_by_thread_ids(&[], 1_000).await?;
        assert!(empty.is_empty());

        // Single-thread lookup behaves like the existing per-thread query.
        let only_c = repo
            .find_memory_ids_by_thread_ids(&[thread_c], 1_000)
            .await?;
        assert_eq!(only_c, vec![mem_only_c]);

        // Multi-thread lookup with a shared memory: the shared id must
        // appear exactly once even though it lives in two junction rows.
        let mut both = repo
            .find_memory_ids_by_thread_ids(&[thread_a, thread_b], 1_000)
            .await?;
        both.sort();
        let mut expected = vec![mem_shared, mem_only_a, mem_only_b];
        expected.sort();
        assert_eq!(both, expected);
        // Length equality with HashSet len is the structural assert: a
        // missing DISTINCT clause would push len > 3 here.
        let dedup_len: usize = both
            .iter()
            .copied()
            .collect::<std::collections::HashSet<_>>()
            .len();
        assert_eq!(both.len(), dedup_len);

        // Cleanup
        repo.delete_by_thread_tx(pool, thread_a).await?;
        repo.delete_by_thread_tx(pool, thread_b).await?;
        repo.delete_by_thread_tx(pool, thread_c).await?;
        Ok(())
    }

    #[test]
    fn run_find_memory_ids_by_thread_ids_test() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_find_memory_ids_by_thread_ids_distinct(pool).await
        })
    }

    /// `find_memory_ids_by_thread_ids` must split very large input lists
    /// into multiple statements so that the bound parameter count never
    /// exceeds `THREAD_ID_IN_CHUNK_SIZE`. Compile-time assert keeps us
    /// from accidentally raising the constant past the older SQLite
    /// ceiling — the production fix exists specifically to stay < 999.
    const _: () = assert!(
        super::THREAD_ID_IN_CHUNK_SIZE < 999,
        "THREAD_ID_IN_CHUNK_SIZE must stay < 999 (older SQLite \
         SQLITE_MAX_VARIABLE_NUMBER) so older SQLite builds keep working",
    );

    /// End-to-end: feed an input that exceeds one chunk and contains the
    /// same shared memory inside multiple chunks. The output must still
    /// dedupe correctly — without HashSet-merging the chunked
    /// implementation would let a memory shared across the chunk
    /// boundary appear twice.
    async fn _test_find_memory_ids_by_thread_ids_chunks(pool: &'static RdbPool) -> Result<()> {
        let repo = ThreadMemoryRepositoryImpl::new(pool);
        // One more thread than the chunk size to force a second batch.
        let total_threads = super::THREAD_ID_IN_CHUNK_SIZE + 5;
        let shared_memory: i64 = 950_000_001;
        let base_thread: i64 = 8_500_000;

        for offset in 0..total_threads {
            let thread_id = base_thread + offset as i64;
            // Every thread in the input set holds the same shared memory
            // so a missing dedupe across chunks would surface as a
            // duplicate in the output.
            repo.insert_tx(pool, thread_id, shared_memory, 0, 1_000)
                .await?;
        }

        let thread_ids: Vec<i64> = (0..total_threads as i64).map(|i| base_thread + i).collect();
        let result = repo
            .find_memory_ids_by_thread_ids(&thread_ids, 1_000)
            .await?;
        assert_eq!(
            result,
            vec![shared_memory],
            "shared memory must collapse to one entry across chunk boundaries"
        );

        // Cleanup
        for offset in 0..total_threads {
            let thread_id = base_thread + offset as i64;
            repo.delete_by_thread_tx(pool, thread_id).await?;
        }
        Ok(())
    }

    #[test]
    fn run_find_memory_ids_by_thread_ids_chunks_test() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_find_memory_ids_by_thread_ids_chunks(pool).await
        })
    }

    /// `max` cap: when the merged DISTINCT set exceeds the caller's
    /// budget, the function must return without scanning further chunks
    /// and the result must hold strictly more than `max` ids so the
    /// caller can detect the overflow. Without this guard, a single
    /// thread holding millions of attachments would drag the entire
    /// junction into the resolver before any FTS / vector ceiling fires.
    async fn _test_find_memory_ids_by_thread_ids_max_cap(pool: &'static RdbPool) -> Result<()> {
        let repo = ThreadMemoryRepositoryImpl::new(pool);
        let thread_id: i64 = 8_700_000;
        let mem_base: i64 = 970_000_000;
        // Insert 5 distinct memories under one thread.
        for offset in 0..5 {
            repo.insert_tx(pool, thread_id, mem_base + offset, offset as i32, 1_000)
                .await?;
        }

        // max = 3 → result must be > 3 (the function returns the
        // over-cap set so the caller can size the failure message).
        let over = repo.find_memory_ids_by_thread_ids(&[thread_id], 3).await?;
        assert!(
            over.len() > 3,
            "over-cap call must surface > max ids; got {}",
            over.len()
        );

        // max = 5 → result fits exactly, no overflow signal.
        let exact = repo.find_memory_ids_by_thread_ids(&[thread_id], 5).await?;
        assert_eq!(exact.len(), 5);

        // max = 100 → all rows present, well under the cap.
        let under = repo
            .find_memory_ids_by_thread_ids(&[thread_id], 100)
            .await?;
        assert_eq!(under.len(), 5);

        repo.delete_by_thread_tx(pool, thread_id).await?;
        Ok(())
    }

    #[test]
    fn run_find_memory_ids_by_thread_ids_max_cap_test() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_find_memory_ids_by_thread_ids_max_cap(pool).await
        })
    }

    /// P1 (improve-search): each memory must resolve to its
    /// **smallest-thread_id** representative, regardless of input order
    /// or attachment order. Memories without a `thread_memory` row are
    /// silently absent from the result so the app layer can treat
    /// "missing" as "no thread membership".
    async fn _test_find_positions_for_memories(pool: &'static RdbPool) -> Result<()> {
        let repo = ThreadMemoryRepositoryImpl::new(pool);
        let thread_a: i64 = 8_000_200;
        let thread_b: i64 = 8_000_201;
        let mem_shared: i64 = 930_001;
        let mem_only_b: i64 = 930_002;
        let mem_no_thread: i64 = 930_003;

        // mem_shared lives in BOTH a (position 5) and b (position 0).
        // The representative is `MIN(thread_id)` = thread_a, so the
        // returned position must be 5 (the row in thread_a), not 0.
        repo.insert_tx(pool, thread_a, mem_shared, 5, 1000).await?;
        repo.insert_tx(pool, thread_b, mem_shared, 0, 1001).await?;
        // mem_only_b has a single attachment.
        repo.insert_tx(pool, thread_b, mem_only_b, 1, 1002).await?;

        // Empty input short-circuits.
        let empty = repo.find_positions_for_memories(&[]).await?;
        assert!(empty.is_empty());

        let mut rows = repo
            .find_positions_for_memories(&[mem_shared, mem_only_b, mem_no_thread])
            .await?;
        rows.sort_by_key(|r| r.0);
        assert_eq!(
            rows.len(),
            2,
            "memories without thread membership must drop"
        );
        assert_eq!(rows[0], (mem_shared, thread_a, 5));
        assert_eq!(rows[1], (mem_only_b, thread_b, 1));

        repo.delete_by_thread_tx(pool, thread_a).await?;
        repo.delete_by_thread_tx(pool, thread_b).await?;
        Ok(())
    }

    #[test]
    fn run_find_positions_for_memories_test() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_find_positions_for_memories(pool).await
        })
    }

    async fn _test_count_by_thread_ids(pool: &'static RdbPool) -> Result<()> {
        let repo = ThreadMemoryRepositoryImpl::new(pool);
        let thread_a: i64 = 8_000_300;
        let thread_b: i64 = 8_000_301;
        let thread_empty: i64 = 8_000_302;

        repo.insert_tx(pool, thread_a, 940_001, 0, 1000).await?;
        repo.insert_tx(pool, thread_a, 940_002, 1, 1001).await?;
        repo.insert_tx(pool, thread_a, 940_003, 2, 1002).await?;
        repo.insert_tx(pool, thread_b, 940_004, 0, 1003).await?;

        let empty = repo.count_by_thread_ids(&[]).await?;
        assert!(empty.is_empty());

        let counts = repo
            .count_by_thread_ids(&[thread_a, thread_b, thread_empty])
            .await?;
        assert_eq!(counts.get(&thread_a).copied(), Some(3));
        assert_eq!(counts.get(&thread_b).copied(), Some(1));
        // thread_empty has zero rows → absent key (not Some(0)).
        assert!(!counts.contains_key(&thread_empty));

        repo.delete_by_thread_tx(pool, thread_a).await?;
        repo.delete_by_thread_tx(pool, thread_b).await?;
        Ok(())
    }

    #[test]
    fn run_count_by_thread_ids_test() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            _test_count_by_thread_ids(pool).await
        })
    }
}
