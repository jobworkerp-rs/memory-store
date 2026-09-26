//! ThreadGroup–Memory relationship persistence.

use crate::sql::p;
use anyhow::Result;
use infra_utils::infra::rdb::{Rdb, RdbPool};
use sqlx::{Executor, FromRow};

#[derive(Clone, Debug, Eq, PartialEq, FromRow)]
pub struct ThreadGroupMemoryRelationRow {
    pub group_id: i64,
    pub memory_id: i64,
    pub purpose: String,
    pub on_group_delete: String,
    pub created_at: i64,
}

pub struct ThreadGroupMemoryRelationRepositoryImpl {
    pool: &'static RdbPool,
}

impl ThreadGroupMemoryRelationRepositoryImpl {
    pub fn new(pool: &'static RdbPool) -> Self {
        Self { pool }
    }

    pub async fn list_orphaned(&self) -> Result<Vec<ThreadGroupMemoryRelationRow>> {
        Ok(sqlx::query_as(
            "SELECT r.group_id, r.memory_id, r.purpose, r.on_group_delete, r.created_at \
             FROM thread_group_memory_relation r \
             LEFT JOIN thread_group g ON g.id = r.group_id \
             LEFT JOIN memory m ON m.id = r.memory_id \
             WHERE g.id IS NULL OR m.id IS NULL ORDER BY r.group_id, r.memory_id",
        )
        .fetch_all(self.pool)
        .await?)
    }

    pub async fn list_legacy_summary_candidates(&self) -> Result<Vec<(i64, String)>> {
        Ok(sqlx::query_as(
            "SELECT id, external_id FROM memory \
             WHERE external_id LIKE 'thread-group-summary:%' ORDER BY id",
        )
        .fetch_all(self.pool)
        .await?)
    }

    pub async fn delete_link_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        group_id: i64,
        memory_id: i64,
    ) -> Result<u64> {
        Ok(sqlx::query(concat!(
            "DELETE FROM thread_group_memory_relation WHERE group_id = ",
            p!(1),
            " AND memory_id = ",
            p!(2)
        ))
        .bind(group_id)
        .bind(memory_id)
        .execute(tx)
        .await?
        .rows_affected())
    }

    pub async fn insert_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        group_id: i64,
        memory_id: i64,
        purpose: &str,
        on_group_delete: &str,
        created_at: i64,
    ) -> Result<()> {
        sqlx::query(concat!(
            "INSERT INTO thread_group_memory_relation \
             (group_id, memory_id, purpose, on_group_delete, created_at) VALUES (",
            p!(1),
            ",",
            p!(2),
            ",",
            p!(3),
            ",",
            p!(4),
            ",",
            p!(5),
            ")"
        ))
        .bind(group_id)
        .bind(memory_id)
        .bind(purpose)
        .bind(on_group_delete)
        .bind(created_at)
        .execute(tx)
        .await?;
        Ok(())
    }

    pub async fn list_by_group_id(
        &self,
        group_id: i64,
    ) -> Result<Vec<ThreadGroupMemoryRelationRow>> {
        self.list_by_group_id_tx(self.pool, group_id).await
    }

    pub async fn list_by_group_id_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        group_id: i64,
    ) -> Result<Vec<ThreadGroupMemoryRelationRow>> {
        Ok(sqlx::query_as(concat!(
            "SELECT group_id, memory_id, purpose, on_group_delete, created_at \
             FROM thread_group_memory_relation WHERE group_id = ",
            p!(1),
            " ORDER BY memory_id"
        ))
        .bind(group_id)
        .fetch_all(tx)
        .await?)
    }

    pub async fn list_by_memory_id(
        &self,
        memory_id: i64,
    ) -> Result<Vec<ThreadGroupMemoryRelationRow>> {
        self.list_by_memory_id_tx(self.pool, memory_id).await
    }

    pub async fn list_by_memory_id_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        memory_id: i64,
    ) -> Result<Vec<ThreadGroupMemoryRelationRow>> {
        Ok(sqlx::query_as(concat!(
            "SELECT group_id, memory_id, purpose, on_group_delete, created_at \
             FROM thread_group_memory_relation WHERE memory_id = ",
            p!(1),
            " ORDER BY group_id"
        ))
        .bind(memory_id)
        .fetch_all(tx)
        .await?)
    }

    pub async fn delete_by_group_id_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        group_id: i64,
    ) -> Result<u64> {
        Ok(sqlx::query(concat!(
            "DELETE FROM thread_group_memory_relation WHERE group_id = ",
            p!(1)
        ))
        .bind(group_id)
        .execute(tx)
        .await?
        .rows_affected())
    }

    pub async fn delete_by_memory_id_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        memory_id: i64,
    ) -> Result<u64> {
        Ok(sqlx::query(concat!(
            "DELETE FROM thread_group_memory_relation WHERE memory_id = ",
            p!(1)
        ))
        .bind(memory_id)
        .execute(tx)
        .await?
        .rows_affected())
    }
}

#[cfg(all(test, not(feature = "postgres")))]
mod tests {
    use super::*;
    use crate::infra::thread_group::test_support::setup_thread_group_pool;

    #[tokio::test]
    async fn relations_keep_retained_memories_separate_from_owned_memories() {
        let pool = setup_thread_group_pool().await;
        let repo = ThreadGroupMemoryRelationRepositoryImpl::new(pool);
        repo.insert_tx(pool, 501, 601, "reference", "retain", 100)
            .await
            .unwrap();
        repo.insert_tx(pool, 501, 602, "summary", "delete", 100)
            .await
            .unwrap();
        repo.insert_tx(pool, 502, 601, "reference", "retain", 100)
            .await
            .unwrap();

        let owned = repo.list_by_group_id(501).await.unwrap();
        assert_eq!(owned.len(), 2);
        assert_eq!(owned[0].memory_id, 601);
        assert_eq!(owned[0].on_group_delete, "retain");
        assert_eq!(owned[1].memory_id, 602);
        assert_eq!(owned[1].on_group_delete, "delete");
        assert_eq!(repo.list_by_memory_id(601).await.unwrap().len(), 2);
        assert_eq!(repo.delete_by_group_id_tx(pool, 501).await.unwrap(), 2);
        assert_eq!(repo.list_by_memory_id(601).await.unwrap().len(), 1);
    }

    #[tokio::test]
    async fn duplicate_relationship_and_invalid_delete_policy_are_rejected() {
        let pool = setup_thread_group_pool().await;
        let repo = ThreadGroupMemoryRelationRepositoryImpl::new(pool);
        repo.insert_tx(pool, 503, 603, "summary", "delete", 100)
            .await
            .unwrap();
        assert!(
            repo.insert_tx(pool, 503, 603, "summary", "delete", 101)
                .await
                .is_err()
        );
        assert!(
            repo.insert_tx(pool, 503, 604, "summary", "cascade", 100)
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn orphan_scan_only_includes_missing_group_or_memory() {
        let pool = setup_thread_group_pool().await;
        let repo = ThreadGroupMemoryRelationRepositoryImpl::new(pool);
        repo.insert_tx(pool, 9_001, 9_002, "summary", "delete", 100)
            .await
            .unwrap();
        let orphaned = repo.list_orphaned().await.unwrap();
        assert!(
            orphaned
                .iter()
                .any(|row| row.group_id == 9_001 && row.memory_id == 9_002)
        );
    }
}
