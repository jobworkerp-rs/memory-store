//! `manual_collection` + `manual_collection_member` repositories
//! (design 5.2, second half).
//!
//! Reference sets owned by one typed `user_id`, deliberately orthogonal to
//! canonical lineage: attaching / detaching never touches group
//! membership or relations, and member links are *physically* deleted on
//! detach / collection deletion / thread deletion (the app calls
//! `detach_all_by_thread_tx` inside its thread-deletion transaction;
//! re-import must not restore them).
//!
//! Default listing sort is `updated_at DESC, id ASC` per the design;
//! the composite index `(user_id, updated_at DESC, id)` backs it.

use super::rows::{
    MANUAL_COLLECTION_COLUMNS, MANUAL_COLLECTION_MEMBER_COLUMNS, ManualCollectionMemberRow,
    ManualCollectionRow, NewManualCollection,
};
use crate::error::LlmMemoryError;
use crate::infra::{IdGeneratorWrapper, UseIdGenerator, fill_timestamps, fill_updated_at};
use crate::sql::p;
use anyhow::Result;
use async_trait::async_trait;
use infra_utils::infra::rdb::{Rdb, RdbPool, UseRdbPool};
use sqlx::Executor;

const INSERT_SQL: &str = concat!(
    "INSERT INTO manual_collection (id, user_id, owner_scope, title, created_at, updated_at) \
     VALUES (",
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
    ")"
);

const FIND_BY_ID_SQL: &str = concat!(
    "SELECT ",
    MANUAL_COLLECTION_COLUMNS!(),
    " FROM manual_collection WHERE id = ",
    p!(1)
);

const LIST_BY_OWNER_SQL: &str = concat!(
    "SELECT ",
    MANUAL_COLLECTION_COLUMNS!(),
    " FROM manual_collection WHERE user_id = ",
    p!(1),
    " ORDER BY updated_at DESC, id ASC"
);

const RENAME_SQL: &str = concat!(
    "UPDATE manual_collection SET title = ",
    p!(1),
    ", updated_at = ",
    p!(2),
    " WHERE id = ",
    p!(3)
);

const TOUCH_SQL: &str = concat!(
    "UPDATE manual_collection SET updated_at = ",
    p!(1),
    " WHERE id = ",
    p!(2)
);

const DELETE_SQL: &str = concat!("DELETE FROM manual_collection WHERE id = ", p!(1));

// Idempotent attach: the (collection_id, thread_id) PK makes repeated
// attaches a no-op on both backends (SQLite supports the conflict
// target form since 3.24, sqlx bundles a newer engine).
const ATTACH_MEMBER_SQL: &str = concat!(
    "INSERT INTO manual_collection_member (collection_id, thread_id, user_id, owner_scope) \
     VALUES (",
    p!(1),
    ",",
    p!(2),
    ",",
    p!(3),
    ",",
    p!(4),
    ") ON CONFLICT (collection_id, thread_id) DO NOTHING"
);

const DETACH_MEMBER_SQL: &str = concat!(
    "DELETE FROM manual_collection_member WHERE collection_id = ",
    p!(1),
    " AND thread_id = ",
    p!(2)
);

const LIST_MEMBERS_SQL: &str = concat!(
    "SELECT ",
    MANUAL_COLLECTION_MEMBER_COLUMNS!(),
    " FROM manual_collection_member WHERE collection_id = ",
    p!(1),
    " ORDER BY thread_id"
);

const LIST_COLLECTION_IDS_BY_THREAD_SQL: &str = concat!(
    "SELECT collection_id FROM manual_collection_member WHERE thread_id = ",
    p!(1),
    " ORDER BY collection_id"
);

const DETACH_ALL_BY_THREAD_SQL: &str = concat!(
    "DELETE FROM manual_collection_member WHERE thread_id = ",
    p!(1)
);

const DETACH_ALL_BY_COLLECTION_SQL: &str = concat!(
    "DELETE FROM manual_collection_member WHERE collection_id = ",
    p!(1)
);

#[async_trait]
pub trait ManualCollectionRepository: UseRdbPool + UseIdGenerator + Send + Sync {
    async fn create_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        collection: &NewManualCollection,
    ) -> Result<i64> {
        let id = self.id_generator().generate_id()?;
        let (created_at, updated_at) =
            fill_timestamps(collection.created_at, collection.updated_at);
        sqlx::query::<Rdb>(INSERT_SQL)
            .bind(id)
            .bind(collection.user_id)
            .bind(common::thread_group_key::legacy_owner_scope(
                collection.user_id,
            ))
            .bind(&collection.title)
            .bind(created_at)
            .bind(updated_at)
            .execute(tx)
            .await
            .map_err(LlmMemoryError::DBError)?;
        Ok(id)
    }

    async fn find_by_id(&self, id: i64) -> Result<Option<ManualCollectionRow>> {
        Ok(sqlx::query_as::<Rdb, ManualCollectionRow>(FIND_BY_ID_SQL)
            .bind(id)
            .fetch_optional(self.db_pool())
            .await
            .map_err(LlmMemoryError::DBError)?)
    }

    /// Default owner listing, `updated_at DESC, id ASC`.
    async fn list_by_owner(
        &self,
        user_id: i64,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> Result<Vec<ManualCollectionRow>> {
        use crate::sql::dyn_placeholder;
        let mut sql = String::from(LIST_BY_OWNER_SQL);
        let mut next = 2usize;
        if limit.is_some() {
            sql.push_str(&format!(" LIMIT {}", dyn_placeholder(next)));
            next += 1;
        }
        if offset.is_some() {
            sql.push_str(&format!(" OFFSET {}", dyn_placeholder(next)));
        }
        let mut query =
            sqlx::query_as::<Rdb, ManualCollectionRow>(sqlx::AssertSqlSafe(sql)).bind(user_id);
        if let Some(limit) = limit {
            query = query.bind(limit);
        }
        if let Some(offset) = offset {
            query = query.bind(offset);
        }
        Ok(query
            .fetch_all(self.db_pool())
            .await
            .map_err(LlmMemoryError::DBError)?)
    }

    async fn rename_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        id: i64,
        title: &str,
        updated_at: i64,
    ) -> Result<bool> {
        let updated_at = fill_updated_at(updated_at);
        let res = sqlx::query::<Rdb>(RENAME_SQL)
            .bind(title)
            .bind(updated_at)
            .bind(id)
            .execute(tx)
            .await
            .map_err(LlmMemoryError::DBError)?;
        Ok(res.rows_affected() > 0)
    }

    /// Bump `updated_at` after a membership change so the default sort
    /// reflects recency (the app decides when a change counts).
    async fn touch_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        id: i64,
        updated_at: i64,
    ) -> Result<bool> {
        let updated_at = fill_updated_at(updated_at);
        let res = sqlx::query::<Rdb>(TOUCH_SQL)
            .bind(updated_at)
            .bind(id)
            .execute(tx)
            .await
            .map_err(LlmMemoryError::DBError)?;
        Ok(res.rows_affected() > 0)
    }

    /// Delete the collection row only; member links go through
    /// `detach_all_members_tx` in the same app transaction.
    async fn delete_tx<'c, E: Executor<'c, Database = Rdb>>(&self, tx: E, id: i64) -> Result<bool> {
        let res = sqlx::query::<Rdb>(DELETE_SQL)
            .bind(id)
            .execute(tx)
            .await
            .map_err(LlmMemoryError::DBError)?;
        Ok(res.rows_affected() > 0)
    }

    /// Idempotent attach. Returns true when a new link was created
    /// (false = already attached). `user_id` is the collection's
    /// owner; cross-owner threads are admitted by the trusted caller
    /// upstream, not validated here.
    async fn attach_member_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        collection_id: i64,
        thread_id: i64,
        user_id: i64,
    ) -> Result<bool> {
        let res = sqlx::query::<Rdb>(ATTACH_MEMBER_SQL)
            .bind(collection_id)
            .bind(thread_id)
            .bind(user_id)
            .bind(common::thread_group_key::legacy_owner_scope(user_id))
            .execute(tx)
            .await
            .map_err(LlmMemoryError::DBError)?;
        Ok(res.rows_affected() > 0)
    }

    async fn detach_member_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        collection_id: i64,
        thread_id: i64,
    ) -> Result<bool> {
        let res = sqlx::query::<Rdb>(DETACH_MEMBER_SQL)
            .bind(collection_id)
            .bind(thread_id)
            .execute(tx)
            .await
            .map_err(LlmMemoryError::DBError)?;
        Ok(res.rows_affected() > 0)
    }

    async fn list_members(&self, collection_id: i64) -> Result<Vec<ManualCollectionMemberRow>> {
        Ok(
            sqlx::query_as::<Rdb, ManualCollectionMemberRow>(LIST_MEMBERS_SQL)
                .bind(collection_id)
                .fetch_all(self.db_pool())
                .await
                .map_err(LlmMemoryError::DBError)?,
        )
    }

    /// Collections referencing one thread (member view pairing).
    async fn list_collection_ids_by_thread(&self, thread_id: i64) -> Result<Vec<i64>> {
        Ok(
            sqlx::query_scalar::<Rdb, i64>(LIST_COLLECTION_IDS_BY_THREAD_SQL)
                .bind(thread_id)
                .fetch_all(self.db_pool())
                .await
                .map_err(LlmMemoryError::DBError)?,
        )
    }

    /// Thread-deletion contract: physically remove every member link
    /// before the thread row goes away, inside the same transaction.
    /// Returns the number of links deleted.
    async fn detach_all_by_thread_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        thread_id: i64,
    ) -> Result<u64> {
        let res = sqlx::query::<Rdb>(DETACH_ALL_BY_THREAD_SQL)
            .bind(thread_id)
            .execute(tx)
            .await
            .map_err(LlmMemoryError::DBError)?;
        Ok(res.rows_affected())
    }

    /// Collection-deletion helper: remove all member links of one
    /// collection. Returns the number of links deleted.
    async fn detach_all_members_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        collection_id: i64,
    ) -> Result<u64> {
        let res = sqlx::query::<Rdb>(DETACH_ALL_BY_COLLECTION_SQL)
            .bind(collection_id)
            .execute(tx)
            .await
            .map_err(LlmMemoryError::DBError)?;
        Ok(res.rows_affected())
    }
}

pub struct ManualCollectionRepositoryImpl {
    pool: &'static RdbPool,
    id_generator: IdGeneratorWrapper,
}

impl ManualCollectionRepositoryImpl {
    pub fn new(id_generator: IdGeneratorWrapper, pool: &'static RdbPool) -> Self {
        Self { pool, id_generator }
    }
}

impl UseRdbPool for ManualCollectionRepositoryImpl {
    fn db_pool(&self) -> &RdbPool {
        self.pool
    }
}

impl UseIdGenerator for ManualCollectionRepositoryImpl {
    fn id_generator(&self) -> &IdGeneratorWrapper {
        &self.id_generator
    }
}

impl ManualCollectionRepository for ManualCollectionRepositoryImpl {}

#[cfg(all(test, not(feature = "postgres")))]
mod tests {
    use super::*;
    use crate::infra::thread_group::test_support::*;
    use anyhow::Context;
    use infra_utils::infra::test::TEST_RUNTIME;

    async fn _test_collection_membership(pool: &'static RdbPool) -> Result<()> {
        let repo =
            ManualCollectionRepositoryImpl::new(crate::test_helper::shared_id_generator(), pool);

        // Two collections with distinct updated_at: default sort is
        // `updated_at DESC, id ASC`.
        let mut older = new_collection(1);
        older.updated_at = T0 + 1;
        let mut newer = new_collection(2);
        newer.updated_at = T0 + 10;
        let c_old = repo.create_tx(pool, &older).await?;
        let c_new = repo.create_tx(pool, &newer).await?;
        let listed = repo.list_by_owner(1, None, None).await?;
        assert_eq!(
            listed.iter().map(|c| c.id).collect::<Vec<_>>(),
            vec![c_new, c_old]
        );

        // Attach is idempotent on (collection_id, thread_id).
        assert!(repo.attach_member_tx(pool, c_new, 1234, 1).await?);
        assert!(!repo.attach_member_tx(pool, c_new, 1234, 1).await?);
        assert!(repo.attach_member_tx(pool, c_new, 4321, 1).await?);
        assert_eq!(repo.list_members(c_new).await?.len(), 2);
        assert_eq!(repo.list_collection_ids_by_thread(1234).await?, vec![c_new]);

        // detach: exactly one link goes away.
        assert!(repo.detach_member_tx(pool, c_new, 1234).await?);
        assert!(!repo.detach_member_tx(pool, c_new, 1234).await?);

        // touch bumps recency so the default sort flips.
        assert!(repo.touch_tx(pool, c_old, T0 + 99).await?);
        let listed = repo.list_by_owner(1, Some(1), None).await?;
        assert_eq!(listed[0].id, c_old);

        // rename + paging.
        assert!(repo.rename_tx(pool, c_old, "renamed", T0 + 100).await?);
        assert_eq!(
            repo.find_by_id(c_old).await?.context("renamed")?.title,
            "renamed"
        );
        assert_eq!(repo.list_by_owner(1, Some(1), Some(1)).await?.len(), 1);

        // Thread deletion contract: drop all links of the thread in the
        // same transaction, before the thread row goes away.
        assert!(repo.attach_member_tx(pool, c_new, 5555, 1).await?);
        assert!(repo.attach_member_tx(pool, c_old, 5555, 1).await?);
        let mut tx = pool.begin().await?;
        assert_eq!(repo.detach_all_by_thread_tx(&mut *tx, 5555).await?, 2);
        tx.commit().await?;
        assert!(repo.list_collection_ids_by_thread(5555).await?.is_empty());

        // Collection deletion removes links separately, then the row.
        assert!(repo.attach_member_tx(pool, c_new, 6666, 1).await?);
        let mut tx = pool.begin().await?;
        assert_eq!(repo.detach_all_members_tx(&mut *tx, c_new).await?, 2);
        assert!(repo.delete_tx(&mut *tx, c_new).await?);
        tx.commit().await?;
        assert!(repo.find_by_id(c_new).await?.is_none());
        Ok(())
    }

    #[test]
    fn collection_membership_sqlite() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let pool = setup_thread_group_pool().await;
            _test_collection_membership(pool).await
        })
    }
}
