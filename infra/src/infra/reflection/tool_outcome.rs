//! `reflection_tool_outcome` child table.
//!
//! PK is (memory_id, tool, contribution, error_kind). The earlier
//! design omitted `error_kind` from the PK and silently overwrote
//! the previous row when the LLM reported a second error_kind for
//! the same (tool, contribution); that hid useful signal from the
//! `AggregateToolContributions` / `GetToolContributionStats` stats
//! the service layer surfaces. NULL `error_kind` is normalised to
//! '' (empty string) on the way in so it can sit in the PK without
//! depending on driver-specific NULL-in-PK behaviour, matching the
//! `tool_contribution_stats` derived table.

use super::rows::ReflectionToolOutcomeRow;
use crate::error::LlmMemoryError;
use crate::sql::{IN_LIST_CHUNK_SIZE, build_in_placeholders, p};
use anyhow::Result;
use async_trait::async_trait;
use infra_utils::infra::rdb::{Rdb, RdbPool, UseRdbPool};
use sqlx::Executor;

const INSERT_SQL: &str = concat!(
    "INSERT INTO reflection_tool_outcome (memory_id, tool, contribution, error_kind) VALUES (",
    p!(1),
    ",",
    p!(2),
    ",",
    p!(3),
    ",",
    p!(4),
    ") ON CONFLICT (memory_id, tool, contribution, error_kind) DO NOTHING;"
);

const LIST_BY_MEMORY_SQL: &str = concat!(
    "SELECT memory_id, tool, contribution, error_kind \
     FROM reflection_tool_outcome WHERE memory_id = ",
    p!(1),
    ";"
);

const DELETE_BY_MEMORY_SQL: &str = concat!(
    "DELETE FROM reflection_tool_outcome WHERE memory_id = ",
    p!(1),
    ";"
);

#[async_trait]
pub trait ReflectionToolOutcomeRepository: UseRdbPool + Send + Sync {
    async fn insert_outcome_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        memory_id: i64,
        tool: &str,
        contribution: i32,
        error_kind: Option<&str>,
    ) -> Result<()> {
        // PK contains `error_kind`; normalise None to '' so the row
        // matches the same (tool, contribution, error_kind="") slot
        // that `tool_contribution_stats` increments for blank kinds.
        let normalised_kind = error_kind.unwrap_or("");
        sqlx::query::<Rdb>(INSERT_SQL)
            .bind(memory_id)
            .bind(tool)
            .bind(contribution)
            .bind(normalised_kind)
            .execute(tx)
            .await
            .map(|_| ())
            .map_err(|e| LlmMemoryError::DBError(e).into())
    }

    async fn list_by_memory_id(&self, memory_id: i64) -> Result<Vec<ReflectionToolOutcomeRow>> {
        sqlx::query_as::<Rdb, ReflectionToolOutcomeRow>(LIST_BY_MEMORY_SQL)
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
    ) -> Result<Vec<ReflectionToolOutcomeRow>> {
        if memory_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut out = Vec::with_capacity(memory_ids.len());
        for chunk in memory_ids.chunks(IN_LIST_CHUNK_SIZE) {
            let placeholders = build_in_placeholders(chunk.len(), 1);
            let sql = format!(
                "SELECT memory_id, tool, contribution, error_kind \
                 FROM reflection_tool_outcome WHERE memory_id IN ({placeholders});"
            );
            let mut q = sqlx::query_as::<Rdb, ReflectionToolOutcomeRow>(sqlx::AssertSqlSafe(sql));
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

pub struct ReflectionToolOutcomeRepositoryImpl {
    pool: &'static RdbPool,
}

impl ReflectionToolOutcomeRepositoryImpl {
    pub fn new(pool: &'static RdbPool) -> Self {
        Self { pool }
    }
}

impl UseRdbPool for ReflectionToolOutcomeRepositoryImpl {
    fn db_pool(&self) -> &RdbPool {
        self.pool
    }
}

impl ReflectionToolOutcomeRepository for ReflectionToolOutcomeRepositoryImpl {}

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
        cleanup_child_table(pool, "reflection_tool_outcome", memory_ids).await;
    }

    async fn seed(
        pool: &'static RdbPool,
        memory_id: i64,
        outcomes: &[(&str, i32, Option<&str>)],
    ) -> Result<()> {
        let index_repo = ThreadReflectionIndexRepositoryImpl::new(pool);
        let repo = ReflectionToolOutcomeRepositoryImpl::new(pool);
        let mut tx = pool.begin().await?;
        index_repo
            .insert_index_tx(&mut *tx, &fixture_sidecar(memory_id))
            .await?;
        for (tool, contrib, err) in outcomes {
            repo.insert_outcome_tx(&mut *tx, memory_id, tool, *contrib, *err)
                .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    #[test]
    fn run_list_by_memory_ids_returns_rows_for_present_ids() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            let ids = [9_003_001_i64, 9_003_002, 9_003_003];
            cleanup(pool, &ids).await;
            seed(
                pool,
                ids[0],
                &[("bash", 1, None), ("read", 2, Some("timeout"))],
            )
            .await?;
            seed(pool, ids[1], &[("grep", 1, None)]).await?;
            seed(pool, ids[2], &[]).await?;

            let repo = ReflectionToolOutcomeRepositoryImpl::new(pool);
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
            let repo = ReflectionToolOutcomeRepositoryImpl::new(pool);
            let rows = repo.list_by_memory_ids(&[]).await?;
            assert!(rows.is_empty());
            Ok(())
        })
    }

    #[test]
    fn run_list_by_memory_ids_handles_missing_ids_silently() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            let present = 9_003_010_i64;
            let absent = 9_003_011_i64;
            cleanup(pool, &[present, absent]).await;
            seed(pool, present, &[("bash", 1, None)]).await?;

            let repo = ReflectionToolOutcomeRepositoryImpl::new(pool);
            let rows = repo.list_by_memory_ids(&[present, absent]).await?;
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].memory_id, present);

            cleanup(pool, &[present, absent]).await;
            Ok(())
        })
    }
}
