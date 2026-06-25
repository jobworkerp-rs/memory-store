//! `reflection_failure_mode` child table. PRIMARY KEY (memory_id, mode)
//! makes inserts naturally deduplicating; `INSERT OR IGNORE` (sqlite)
//! and `ON CONFLICT DO NOTHING` (postgres) keep the call sites clean
//! when the same mode appears twice in LLM output.
//!
//! Multi-row insert is the responsibility of the caller (app layer);
//! this trait exposes only the single-row insert helper plus
//! list/delete operations, matching the repository style established
//! by `thread_memory::ThreadMemoryRepository`.

use super::rows::ReflectionFailureModeRow;
use crate::error::LlmMemoryError;
use crate::sql::{IN_LIST_CHUNK_SIZE, build_in_placeholders, p};
use anyhow::Result;
use async_trait::async_trait;
use infra_utils::infra::rdb::{Rdb, RdbPool, UseRdbPool};
use sqlx::Executor;

#[cfg(feature = "postgres")]
const INSERT_SQL: &str = concat!(
    "INSERT INTO reflection_failure_mode (memory_id, mode) VALUES (",
    p!(1),
    ",",
    p!(2),
    ") ON CONFLICT (memory_id, mode) DO NOTHING;"
);

#[cfg(not(feature = "postgres"))]
const INSERT_SQL: &str = concat!(
    "INSERT OR IGNORE INTO reflection_failure_mode (memory_id, mode) VALUES (",
    p!(1),
    ",",
    p!(2),
    ");"
);

const LIST_BY_MEMORY_SQL: &str = concat!(
    "SELECT memory_id, mode FROM reflection_failure_mode WHERE memory_id = ",
    p!(1),
    ";"
);

const DELETE_BY_MEMORY_SQL: &str = concat!(
    "DELETE FROM reflection_failure_mode WHERE memory_id = ",
    p!(1),
    ";"
);

#[async_trait]
pub trait ReflectionFailureModeRepository: UseRdbPool + Send + Sync {
    async fn insert_mode_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        memory_id: i64,
        mode: &str,
    ) -> Result<()> {
        sqlx::query::<Rdb>(INSERT_SQL)
            .bind(memory_id)
            .bind(mode)
            .execute(tx)
            .await
            .map(|_| ())
            .map_err(|e| LlmMemoryError::DBError(e).into())
    }

    async fn list_by_memory_id(&self, memory_id: i64) -> Result<Vec<ReflectionFailureModeRow>> {
        sqlx::query_as::<Rdb, ReflectionFailureModeRow>(LIST_BY_MEMORY_SQL)
            .bind(memory_id)
            .fetch_all(self.db_pool())
            .await
            .map_err(|e| LlmMemoryError::DBError(e).into())
    }

    /// Bulk variant of `list_by_memory_id`. Returns rows for every
    /// matching `memory_id` in `ids`; the order between input ids and
    /// returned rows is unspecified — callers should join by
    /// `row.memory_id` (the hydrate path uses a HashMap to preserve the
    /// input row order). IDs without rows are silently absent.
    async fn list_by_memory_ids(
        &self,
        memory_ids: &[i64],
    ) -> Result<Vec<ReflectionFailureModeRow>> {
        if memory_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut out = Vec::with_capacity(memory_ids.len());
        for chunk in memory_ids.chunks(IN_LIST_CHUNK_SIZE) {
            let placeholders = build_in_placeholders(chunk.len(), 1);
            let sql = format!(
                "SELECT memory_id, mode FROM reflection_failure_mode \
                 WHERE memory_id IN ({placeholders});"
            );
            let mut q = sqlx::query_as::<Rdb, ReflectionFailureModeRow>(sqlx::AssertSqlSafe(sql));
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

    async fn delete_by_memory_id_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        memory_id: i64,
    ) -> Result<()> {
        sqlx::query::<Rdb>(DELETE_BY_MEMORY_SQL)
            .bind(memory_id)
            .execute(tx)
            .await
            .map(|_| ())
            .map_err(|e| LlmMemoryError::DBError(e).into())
    }
}

pub struct ReflectionFailureModeRepositoryImpl {
    pool: &'static RdbPool,
}

impl ReflectionFailureModeRepositoryImpl {
    pub fn new(pool: &'static RdbPool) -> Self {
        Self { pool }
    }
}

impl UseRdbPool for ReflectionFailureModeRepositoryImpl {
    fn db_pool(&self) -> &RdbPool {
        self.pool
    }
}

impl ReflectionFailureModeRepository for ReflectionFailureModeRepositoryImpl {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::infra::reflection::rdb::{
        ThreadReflectionIndexRepository, ThreadReflectionIndexRepositoryImpl,
    };
    use crate::infra::reflection::test_support::{
        cleanup_child_table, fixture_sidecar, setup_pool,
    };
    use infra_utils::infra::test::TEST_RUNTIME;

    async fn cleanup(pool: &RdbPool, memory_ids: &[i64]) {
        cleanup_child_table(pool, "reflection_failure_mode", memory_ids).await;
    }

    async fn seed(pool: &'static RdbPool, memory_id: i64, modes: &[&str]) -> Result<()> {
        let index_repo = ThreadReflectionIndexRepositoryImpl::new(pool);
        let repo = ReflectionFailureModeRepositoryImpl::new(pool);
        let mut tx = pool.begin().await?;
        index_repo
            .insert_index_tx(&mut *tx, &fixture_sidecar(memory_id))
            .await?;
        for m in modes {
            repo.insert_mode_tx(&mut *tx, memory_id, m).await?;
        }
        tx.commit().await?;
        Ok(())
    }

    #[test]
    fn run_list_by_memory_ids_returns_rows_for_present_ids() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            let ids = [9_001_001_i64, 9_001_002, 9_001_003];
            cleanup(pool, &ids).await;
            seed(pool, ids[0], &["mode_a", "mode_b"]).await?;
            seed(pool, ids[1], &["mode_c"]).await?;
            seed(pool, ids[2], &[]).await?;

            let repo = ReflectionFailureModeRepositoryImpl::new(pool);
            let rows = repo.list_by_memory_ids(&ids).await?;
            // Two rows for id[0], one row for id[1], none for id[2].
            assert_eq!(rows.len(), 3);
            let mut by_id: std::collections::HashMap<i64, Vec<String>> =
                std::collections::HashMap::new();
            for r in rows {
                by_id.entry(r.memory_id).or_default().push(r.mode);
            }
            assert_eq!(by_id.get(&ids[0]).map(|v| v.len()), Some(2));
            assert_eq!(by_id.get(&ids[1]).map(|v| v.len()), Some(1));
            assert!(!by_id.contains_key(&ids[2]));

            cleanup(pool, &ids).await;
            Ok(())
        })
    }

    #[test]
    fn run_list_by_memory_ids_empty_input_returns_empty() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            let repo = ReflectionFailureModeRepositoryImpl::new(pool);
            let rows = repo.list_by_memory_ids(&[]).await?;
            assert!(rows.is_empty());
            Ok(())
        })
    }

    #[test]
    fn run_list_by_memory_ids_handles_missing_ids_silently() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            let present = 9_001_010_i64;
            let absent = 9_001_011_i64;
            cleanup(pool, &[present, absent]).await;
            seed(pool, present, &["mode_x"]).await?;

            let repo = ReflectionFailureModeRepositoryImpl::new(pool);
            let rows = repo.list_by_memory_ids(&[present, absent]).await?;
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].memory_id, present);

            cleanup(pool, &[present, absent]).await;
            Ok(())
        })
    }
}
