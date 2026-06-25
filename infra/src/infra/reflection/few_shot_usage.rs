//! `reflection_few_shot_usage` operational state table. PK
//! (memory_id, used_in_thread_id) so repeated calls from the same
//! few-shot retrieval path collapse into a single row.

use super::rows::ReflectionFewShotUsageRow;
use crate::error::LlmMemoryError;
use crate::sql::{IN_LIST_CHUNK_SIZE, build_in_placeholders, p};
use anyhow::Result;
use async_trait::async_trait;
use infra_utils::infra::rdb::{Rdb, RdbPool, UseRdbPool};
use sqlx::Executor;

#[cfg(feature = "postgres")]
const INSERT_SQL: &str = concat!(
    "INSERT INTO reflection_few_shot_usage \
     (memory_id, used_in_thread_id, used_at) VALUES (",
    p!(1),
    ",",
    p!(2),
    ",",
    p!(3),
    ") ON CONFLICT (memory_id, used_in_thread_id) DO NOTHING;"
);

#[cfg(not(feature = "postgres"))]
const INSERT_SQL: &str = concat!(
    "INSERT OR IGNORE INTO reflection_few_shot_usage \
     (memory_id, used_in_thread_id, used_at) VALUES (",
    p!(1),
    ",",
    p!(2),
    ",",
    p!(3),
    ");"
);

const LIST_BY_MEMORY_SQL: &str = concat!(
    "SELECT memory_id, used_in_thread_id, used_at \
     FROM reflection_few_shot_usage WHERE memory_id = ",
    p!(1),
    ";"
);

const DELETE_BY_MEMORY_SQL: &str = concat!(
    "DELETE FROM reflection_few_shot_usage WHERE memory_id = ",
    p!(1),
    ";"
);

#[async_trait]
pub trait ReflectionFewShotUsageRepository: UseRdbPool + Send + Sync {
    async fn record_usage_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        memory_id: i64,
        used_in_thread_id: i64,
        used_at: i64,
    ) -> Result<bool> {
        sqlx::query::<Rdb>(INSERT_SQL)
            .bind(memory_id)
            .bind(used_in_thread_id)
            .bind(used_at)
            .execute(tx)
            .await
            .map(|r| r.rows_affected() > 0)
            .map_err(|e| LlmMemoryError::DBError(e).into())
    }

    async fn list_by_memory_id(&self, memory_id: i64) -> Result<Vec<ReflectionFewShotUsageRow>> {
        sqlx::query_as::<Rdb, ReflectionFewShotUsageRow>(LIST_BY_MEMORY_SQL)
            .bind(memory_id)
            .fetch_all(self.db_pool())
            .await
            .map_err(|e| LlmMemoryError::DBError(e).into())
    }

    /// Bulk variant for the hydrate fan-out. Same contract as
    /// `ReflectionFailureModeRepository::list_by_memory_ids`.
    async fn list_by_memory_ids(
        &self,
        memory_ids: &[i64],
    ) -> Result<Vec<ReflectionFewShotUsageRow>> {
        if memory_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut out = Vec::with_capacity(memory_ids.len());
        for chunk in memory_ids.chunks(IN_LIST_CHUNK_SIZE) {
            let placeholders = build_in_placeholders(chunk.len(), 1);
            let sql = format!(
                "SELECT memory_id, used_in_thread_id, used_at \
                 FROM reflection_few_shot_usage WHERE memory_id IN ({placeholders});"
            );
            let mut q = sqlx::query_as::<Rdb, ReflectionFewShotUsageRow>(sqlx::AssertSqlSafe(sql));
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

pub struct ReflectionFewShotUsageRepositoryImpl {
    pool: &'static RdbPool,
}

impl ReflectionFewShotUsageRepositoryImpl {
    pub fn new(pool: &'static RdbPool) -> Self {
        Self { pool }
    }
}

impl UseRdbPool for ReflectionFewShotUsageRepositoryImpl {
    fn db_pool(&self) -> &RdbPool {
        self.pool
    }
}

impl ReflectionFewShotUsageRepository for ReflectionFewShotUsageRepositoryImpl {}

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
        cleanup_child_table(pool, "reflection_few_shot_usage", memory_ids).await;
    }

    async fn seed(pool: &'static RdbPool, memory_id: i64, thread_ids: &[i64]) -> Result<()> {
        let index_repo = ThreadReflectionIndexRepositoryImpl::new(pool);
        let repo = ReflectionFewShotUsageRepositoryImpl::new(pool);
        let mut tx = pool.begin().await?;
        index_repo
            .insert_index_tx(&mut *tx, &fixture_sidecar(memory_id))
            .await?;
        for tid in thread_ids {
            repo.record_usage_tx(&mut *tx, memory_id, *tid, 1_700_000_007_000)
                .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    #[test]
    fn run_list_by_memory_ids_returns_rows_for_present_ids() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            let ids = [9_006_001_i64, 9_006_002, 9_006_003];
            cleanup(pool, &ids).await;
            seed(pool, ids[0], &[101, 102]).await?;
            seed(pool, ids[1], &[103]).await?;
            seed(pool, ids[2], &[]).await?;

            let repo = ReflectionFewShotUsageRepositoryImpl::new(pool);
            let rows = repo.list_by_memory_ids(&ids).await?;
            assert_eq!(rows.len(), 3);
            let mut by_id: std::collections::HashMap<i64, usize> = std::collections::HashMap::new();
            for r in rows {
                *by_id.entry(r.memory_id).or_default() += 1;
            }
            assert_eq!(by_id.get(&ids[0]), Some(&2));
            assert_eq!(by_id.get(&ids[1]), Some(&1));
            assert!(!by_id.contains_key(&ids[2]));

            cleanup(pool, &ids).await;
            Ok(())
        })
    }

    #[test]
    fn run_list_by_memory_ids_empty_input_returns_empty() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            let repo = ReflectionFewShotUsageRepositoryImpl::new(pool);
            let rows = repo.list_by_memory_ids(&[]).await?;
            assert!(rows.is_empty());
            Ok(())
        })
    }

    #[test]
    fn run_list_by_memory_ids_handles_missing_ids_silently() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            let present = 9_006_010_i64;
            let absent = 9_006_011_i64;
            cleanup(pool, &[present, absent]).await;
            seed(pool, present, &[201]).await?;

            let repo = ReflectionFewShotUsageRepositoryImpl::new(pool);
            let rows = repo.list_by_memory_ids(&[present, absent]).await?;
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].memory_id, present);

            cleanup(pool, &[present, absent]).await;
            Ok(())
        })
    }
}
