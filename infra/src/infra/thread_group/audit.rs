//! `thread_group_audit` repository (design section 5 / schema header).
//!
//! Append-only merge / split history. The partial UNIQUE indexes give
//! the idempotency contracts for free:
//! - a group is consumed by *merge* at most once — a second merge of the
//!   same source fails on `thread_group_audit_merge_source_group`;
//! - a group is consumed by *split* exactly once — a retry of the same
//!   canonicalized partition must find the existing row
//!   (`find_split_by_source_group_id_tx`) and return its recorded
//!   successor ids instead of writing a second row.
//!
//! Which rule the app must apply (same partition → reuse; different
//! partition → reject) is policy and stays in the app layer.

use super::rows::{
    GROUP_AUDIT_COLUMNS, NewGroupAuditMerge, NewGroupAuditSplit, ThreadGroupAuditRow,
};
use crate::error::LlmMemoryError;
use crate::infra::{IdGeneratorWrapper, UseIdGenerator, fill_timestamps};
use crate::sql::{p, p_jsonb};
use anyhow::Result;
use async_trait::async_trait;
use infra_utils::infra::rdb::{Rdb, RdbPool, UseRdbPool};
use sqlx::Executor;

const INSERT_MERGE_SQL: &str = concat!(
    "INSERT INTO thread_group_audit \
     (id, audit_type, source_group_id, target_group_id, actor_id, reason, \
      canonical_partition, successor_group_ids, created_at) \
     VALUES (",
    p!(1),
    ", 'merge', ",
    p!(2),
    ",",
    p!(3),
    ",",
    p!(4),
    ",",
    p!(5),
    ", NULL, NULL, ",
    p!(6),
    ")"
);

const INSERT_SPLIT_SQL: &str = concat!(
    "INSERT INTO thread_group_audit \
     (id, audit_type, source_group_id, target_group_id, actor_id, reason, \
      canonical_partition, successor_group_ids, created_at) \
     VALUES (",
    p!(1),
    ", 'split', ",
    p!(2),
    ", NULL,",
    p!(3),
    ",",
    p!(4),
    ",",
    p_jsonb!(5),
    ",",
    p_jsonb!(6),
    ",",
    p!(7),
    ")"
);

const FIND_BY_SOURCE_AND_TYPE_SQL: &str = concat!(
    "SELECT ",
    GROUP_AUDIT_COLUMNS!(),
    " FROM thread_group_audit WHERE source_group_id = ",
    p!(1),
    " AND audit_type = ",
    p!(2)
);

// The group id binds twice (SQLite numbers `?` positionally, so the
// placeholders must be distinct even though PostgreSQL could reuse $1).
const DELETE_BY_ID_SQL: &str = concat!("DELETE FROM thread_group_audit WHERE id = ", p!(1));

const LIST_BY_GROUP_SQL: &str = concat!(
    "SELECT ",
    GROUP_AUDIT_COLUMNS!(),
    " FROM thread_group_audit WHERE source_group_id = ",
    p!(1),
    " OR target_group_id = ",
    p!(2),
    " ORDER BY created_at, id"
);

#[async_trait]
pub trait ThreadGroupAuditRepository: UseRdbPool + UseIdGenerator + Send + Sync {
    /// Record a merge: the redirected source group, the surviving
    /// target, and the actor / reason audit payload (one transaction
    /// with `redirect_tx`).
    async fn append_merge_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        merge: &NewGroupAuditMerge,
    ) -> Result<i64> {
        let id = self.id_generator().generate_id()?;
        let (created_at, _) = fill_timestamps(merge.created_at, merge.created_at);
        sqlx::query::<Rdb>(INSERT_MERGE_SQL)
            .bind(id)
            .bind(merge.source_group_id)
            .bind(merge.target_group_id)
            .bind(&merge.actor_id)
            .bind(&merge.reason)
            .bind(created_at)
            .execute(tx)
            .await
            .map_err(LlmMemoryError::DBError)?;
        Ok(id)
    }

    /// Record a split: canonicalized partition + successor group ids as
    /// canonical JSON (one transaction with `mark_split_tx` and the
    /// successor rows).
    async fn append_split_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        split: &NewGroupAuditSplit,
    ) -> Result<i64> {
        let id = self.id_generator().generate_id()?;
        let (created_at, _) = fill_timestamps(split.created_at, split.created_at);
        sqlx::query::<Rdb>(INSERT_SPLIT_SQL)
            .bind(id)
            .bind(split.source_group_id)
            .bind(&split.actor_id)
            .bind(&split.reason)
            .bind(&split.canonical_partition)
            .bind(&split.successor_group_ids)
            .bind(created_at)
            .execute(tx)
            .await
            .map_err(LlmMemoryError::DBError)?;
        Ok(id)
    }

    /// The single merge audit row of a source group, if it was ever
    /// merged.
    async fn find_merge_by_source_group_id(
        &self,
        source_group_id: i64,
    ) -> Result<Option<ThreadGroupAuditRow>> {
        Ok(
            sqlx::query_as::<Rdb, ThreadGroupAuditRow>(FIND_BY_SOURCE_AND_TYPE_SQL)
                .bind(source_group_id)
                .bind(crate::infra::thread_group::rows::values::audit_type::MERGE)
                .fetch_optional(self.db_pool())
                .await
                .map_err(LlmMemoryError::DBError)?,
        )
    }

    /// Split retry idempotency lookup inside a write transaction (the
    /// recorded partition decides reuse vs. rejection).
    async fn find_split_by_source_group_id_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        source_group_id: i64,
    ) -> Result<Option<ThreadGroupAuditRow>> {
        Ok(
            sqlx::query_as::<Rdb, ThreadGroupAuditRow>(FIND_BY_SOURCE_AND_TYPE_SQL)
                .bind(source_group_id)
                .bind(crate::infra::thread_group::rows::values::audit_type::SPLIT)
                .fetch_optional(tx)
                .await
                .map_err(LlmMemoryError::DBError)?,
        )
    }

    /// Full operation history involving one group as source or target,
    /// oldest first.
    async fn list_by_group_id(&self, group_id: i64) -> Result<Vec<ThreadGroupAuditRow>> {
        Ok(
            sqlx::query_as::<Rdb, ThreadGroupAuditRow>(LIST_BY_GROUP_SQL)
                .bind(group_id)
                .bind(group_id)
                .fetch_all(self.db_pool())
                .await
                .map_err(LlmMemoryError::DBError)?,
        )
    }

    /// Physical delete for inactive-history purge.
    async fn delete_by_id_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        id: i64,
    ) -> Result<bool> {
        let res = sqlx::query::<Rdb>(DELETE_BY_ID_SQL)
            .bind(id)
            .execute(tx)
            .await
            .map_err(LlmMemoryError::DBError)?;
        Ok(res.rows_affected() > 0)
    }
}

pub struct ThreadGroupAuditRepositoryImpl {
    pool: &'static RdbPool,
    id_generator: IdGeneratorWrapper,
}

impl ThreadGroupAuditRepositoryImpl {
    pub fn new(id_generator: IdGeneratorWrapper, pool: &'static RdbPool) -> Self {
        Self { pool, id_generator }
    }
}

impl UseRdbPool for ThreadGroupAuditRepositoryImpl {
    fn db_pool(&self) -> &RdbPool {
        self.pool
    }
}

impl UseIdGenerator for ThreadGroupAuditRepositoryImpl {
    fn id_generator(&self) -> &IdGeneratorWrapper {
        &self.id_generator
    }
}

impl ThreadGroupAuditRepository for ThreadGroupAuditRepositoryImpl {}

#[cfg(all(test, not(feature = "postgres")))]
mod tests {
    use super::*;
    use crate::infra::thread_group::group::{ThreadGroupRepository, ThreadGroupRepositoryImpl};
    use crate::infra::thread_group::test_support::*;
    use anyhow::Context;
    use infra_utils::infra::test::TEST_RUNTIME;

    async fn _test_audit_consumption(pool: &'static RdbPool) -> Result<()> {
        let groups =
            ThreadGroupRepositoryImpl::new(crate::test_helper::shared_id_generator(), pool);
        let repo =
            ThreadGroupAuditRepositoryImpl::new(crate::test_helper::shared_id_generator(), pool);
        let source = groups.create_tx(pool, &new_group(21)).await?;
        let target = groups.create_tx(pool, &new_group(22)).await?;

        // Merge is recorded with its actor / reason; the JSON columns
        // stay NULL for merge rows (DDL CHECK).
        let m = repo
            .append_merge_tx(pool, &new_audit_merge(source, target))
            .await?;
        let row = repo
            .find_merge_by_source_group_id(source)
            .await?
            .context("merge row")?;
        assert_eq!(row.id, m);
        assert_eq!(row.target_group_id, Some(target));
        assert!(row.canonical_partition.is_none());

        // A group is consumed once per operation kind.
        assert!(
            repo.append_merge_tx(pool, &new_audit_merge(source, target))
                .await
                .is_err()
        );

        // Split records the canonicalized partition + successors; the
        // retry lookup inside a transaction is the idempotency anchor.
        let s = repo.append_split_tx(pool, &new_audit_split(target)).await?;
        assert!(
            repo.append_split_tx(pool, &new_audit_split(target))
                .await
                .is_err()
        );
        let mut tx = pool.begin().await?;
        let row = repo
            .find_split_by_source_group_id_tx(&mut *tx, target)
            .await?
            .context("split row")?;
        tx.commit().await?;
        assert_eq!(row.id, s);
        assert_eq!(row.successor_group_ids.as_deref(), Some("[11,12]"));
        assert_eq!(
            row.canonical_partition.as_deref(),
            Some("[[\"k1\"],[\"k2\"]]")
        );

        // History view over both roles: `target` appears once as merge
        // target and once as split source.
        let history = repo.list_by_group_id(target).await?;
        assert_eq!(history.len(), 2);
        Ok(())
    }

    #[test]
    fn audit_consumption_sqlite() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let pool = setup_thread_group_pool().await;
            _test_audit_consumption(pool).await
        })
    }
}
