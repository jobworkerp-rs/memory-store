//! `reflection_applied_target` operational state table.
//!
//! Two distinct write entry points:
//!   - F-F2 `RecordAppliedTarget` — INSERT OR IGNORE on (memory_id,
//!     target). `mitigation_fingerprint` on collision is left untouched
//!     so callers cannot accidentally rewrite an applied row that
//!     another path already recorded.
//!   - F-F6 `UpsertMitigationApplied` — UPSERT that returns whether the
//!     row was newly applied (true) or skipped because the same
//!     (target, fingerprint) was already recorded (false). Writers use
//!     this to dedup mitigation backports into system_prompt memories.

use super::rows::ReflectionAppliedTargetRow;
use crate::error::LlmMemoryError;
use crate::sql::{IN_LIST_CHUNK_SIZE, build_in_placeholders, p};
use anyhow::Result;
use async_trait::async_trait;
use infra_utils::infra::rdb::{Rdb, RdbPool, UseRdbPool};
use sqlx::Executor;

#[cfg(feature = "postgres")]
const INSERT_OR_IGNORE_SQL: &str = concat!(
    "INSERT INTO reflection_applied_target \
     (memory_id, target, mitigation_fingerprint, applied_at) VALUES (",
    p!(1),
    ",",
    p!(2),
    ",",
    p!(3),
    ",",
    p!(4),
    ") ON CONFLICT (memory_id, target) DO NOTHING;"
);

#[cfg(not(feature = "postgres"))]
const INSERT_OR_IGNORE_SQL: &str = concat!(
    "INSERT OR IGNORE INTO reflection_applied_target \
     (memory_id, target, mitigation_fingerprint, applied_at) VALUES (",
    p!(1),
    ",",
    p!(2),
    ",",
    p!(3),
    ",",
    p!(4),
    ");"
);

#[cfg(feature = "postgres")]
const UPSERT_FINGERPRINT_SQL: &str = concat!(
    "INSERT INTO reflection_applied_target \
     (memory_id, target, mitigation_fingerprint, applied_at) VALUES (",
    p!(1),
    ",",
    p!(2),
    ",",
    p!(3),
    ",",
    p!(4),
    ") ON CONFLICT (memory_id, target) DO UPDATE SET \
       mitigation_fingerprint = EXCLUDED.mitigation_fingerprint, \
       applied_at = EXCLUDED.applied_at \
     WHERE reflection_applied_target.mitigation_fingerprint IS DISTINCT FROM EXCLUDED.mitigation_fingerprint;"
);

#[cfg(not(feature = "postgres"))]
const UPSERT_FINGERPRINT_SQL: &str = concat!(
    "INSERT INTO reflection_applied_target \
     (memory_id, target, mitigation_fingerprint, applied_at) VALUES (",
    p!(1),
    ",",
    p!(2),
    ",",
    p!(3),
    ",",
    p!(4),
    ") ON CONFLICT (memory_id, target) DO UPDATE SET \
       mitigation_fingerprint = excluded.mitigation_fingerprint, \
       applied_at = excluded.applied_at \
     WHERE COALESCE(reflection_applied_target.mitigation_fingerprint, '') <> COALESCE(excluded.mitigation_fingerprint, '');"
);

const FIND_FINGERPRINT_SQL: &str = concat!(
    "SELECT mitigation_fingerprint FROM reflection_applied_target \
     WHERE memory_id = ",
    p!(1),
    " AND target = ",
    p!(2),
    ";"
);

const LIST_BY_MEMORY_SQL: &str = concat!(
    "SELECT memory_id, target, mitigation_fingerprint, applied_at \
     FROM reflection_applied_target WHERE memory_id = ",
    p!(1),
    ";"
);

const DELETE_BY_MEMORY_SQL: &str = concat!(
    "DELETE FROM reflection_applied_target WHERE memory_id = ",
    p!(1),
    ";"
);

#[async_trait]
pub trait ReflectionAppliedTargetRepository: UseRdbPool + Send + Sync {
    /// F-F2: idempotent insert. Returns true when a new row was added.
    async fn record_target_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        memory_id: i64,
        target: &str,
        mitigation_fingerprint: Option<&str>,
        applied_at: i64,
    ) -> Result<bool> {
        sqlx::query::<Rdb>(INSERT_OR_IGNORE_SQL)
            .bind(memory_id)
            .bind(target)
            .bind(mitigation_fingerprint)
            .bind(applied_at)
            .execute(tx)
            .await
            .map(|r| r.rows_affected() > 0)
            .map_err(|e| LlmMemoryError::DBError(e).into())
    }

    /// F-F6: upsert that only writes when the fingerprint differs from
    /// the existing row. Returns `applied = true` on insert or
    /// fingerprint change, `false` when the call was a no-op.
    async fn upsert_mitigation_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        memory_id: i64,
        target: &str,
        fingerprint: &str,
        applied_at: i64,
    ) -> Result<bool> {
        sqlx::query::<Rdb>(UPSERT_FINGERPRINT_SQL)
            .bind(memory_id)
            .bind(target)
            .bind(fingerprint)
            .bind(applied_at)
            .execute(tx)
            .await
            .map(|r| r.rows_affected() > 0)
            .map_err(|e| LlmMemoryError::DBError(e).into())
    }

    /// Lookup helper for F-F6: returns the persisted fingerprint when
    /// the (memory_id, target) row exists. Caller compares vs. the
    /// incoming fingerprint to choose the response semantics.
    async fn find_fingerprint(
        &self,
        memory_id: i64,
        target: &str,
    ) -> Result<Option<Option<String>>> {
        sqlx::query_as::<Rdb, (Option<String>,)>(FIND_FINGERPRINT_SQL)
            .bind(memory_id)
            .bind(target)
            .fetch_optional(self.db_pool())
            .await
            .map(|opt| opt.map(|(fp,)| fp))
            .map_err(|e| LlmMemoryError::DBError(e).into())
    }

    async fn list_by_memory_id(&self, memory_id: i64) -> Result<Vec<ReflectionAppliedTargetRow>> {
        sqlx::query_as::<Rdb, ReflectionAppliedTargetRow>(LIST_BY_MEMORY_SQL)
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
    ) -> Result<Vec<ReflectionAppliedTargetRow>> {
        if memory_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut out = Vec::with_capacity(memory_ids.len());
        for chunk in memory_ids.chunks(IN_LIST_CHUNK_SIZE) {
            let placeholders = build_in_placeholders(chunk.len(), 1);
            let sql = format!(
                "SELECT memory_id, target, mitigation_fingerprint, applied_at \
                 FROM reflection_applied_target WHERE memory_id IN ({placeholders});"
            );
            let mut q = sqlx::query_as::<Rdb, ReflectionAppliedTargetRow>(sqlx::AssertSqlSafe(sql));
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

pub struct ReflectionAppliedTargetRepositoryImpl {
    pool: &'static RdbPool,
}

impl ReflectionAppliedTargetRepositoryImpl {
    pub fn new(pool: &'static RdbPool) -> Self {
        Self { pool }
    }
}

impl UseRdbPool for ReflectionAppliedTargetRepositoryImpl {
    fn db_pool(&self) -> &RdbPool {
        self.pool
    }
}

impl ReflectionAppliedTargetRepository for ReflectionAppliedTargetRepositoryImpl {}

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
        cleanup_child_table(pool, "reflection_applied_target", memory_ids).await;
    }

    async fn seed(
        pool: &'static RdbPool,
        memory_id: i64,
        targets: &[(&str, Option<&str>)],
    ) -> Result<()> {
        let index_repo = ThreadReflectionIndexRepositoryImpl::new(pool);
        let repo = ReflectionAppliedTargetRepositoryImpl::new(pool);
        let mut tx = pool.begin().await?;
        index_repo
            .insert_index_tx(&mut *tx, &fixture_sidecar(memory_id))
            .await?;
        for (target, fp) in targets {
            repo.record_target_tx(&mut *tx, memory_id, target, *fp, 1_700_000_005_000)
                .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    #[test]
    fn run_list_by_memory_ids_returns_rows_for_present_ids() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            let ids = [9_005_001_i64, 9_005_002, 9_005_003];
            cleanup(pool, &ids).await;
            seed(
                pool,
                ids[0],
                &[("memory:1", None), ("memory:2", Some("fp"))],
            )
            .await?;
            seed(pool, ids[1], &[("memory:3", None)]).await?;
            seed(pool, ids[2], &[]).await?;

            let repo = ReflectionAppliedTargetRepositoryImpl::new(pool);
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
            let repo = ReflectionAppliedTargetRepositoryImpl::new(pool);
            let rows = repo.list_by_memory_ids(&[]).await?;
            assert!(rows.is_empty());
            Ok(())
        })
    }

    /// Fingerprint round-trip: bulk path must surface
    /// `mitigation_fingerprint` for callers that compare against
    /// pre-existing rows (F-F6 idempotency).
    #[test]
    fn run_list_by_memory_ids_returns_fingerprint_when_set() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            let memory_id = 9_005_010_i64;
            cleanup(pool, &[memory_id]).await;
            seed(pool, memory_id, &[("memory:42", Some("abc123"))]).await?;

            let repo = ReflectionAppliedTargetRepositoryImpl::new(pool);
            let rows = repo.list_by_memory_ids(&[memory_id]).await?;
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].mitigation_fingerprint.as_deref(), Some("abc123"));

            cleanup(pool, &[memory_id]).await;
            Ok(())
        })
    }
}
