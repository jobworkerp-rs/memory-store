//! `thread_deletion_marker` repository (design 5.2 / 8.2).
//!
//! Content-free deletion bookkeeping keyed by owner-local source
//! identity. The marker answers the mandatory pre-write check on
//! import (exists? forbid_reimport?), and "consuming" it is the delete
//! of that row — always inside the caller's transaction so the import
//! write and the marker consumption commit atomically. Non-source
//! threads never appear here; they only get placeholder members.

use super::rows::{
    NewThreadDeletionMarker, SourceIdentityKey, THREAD_DELETION_MARKER_COLUMNS,
    ThreadDeletionMarkerRow,
};
use crate::error::LlmMemoryError;
use crate::sql::p;
use anyhow::Result;
use async_trait::async_trait;
use infra_utils::infra::rdb::{Rdb, RdbPool, UseRdbPool};
use sqlx::Executor;

// A re-delete of the same identity replaces the previous marker fields
// (newest actor / reason / flags win); PK conflict target keeps the
// statement single-round-trip on both backends.
const PUT_SQL: &str = concat!(
    "INSERT INTO thread_deletion_marker \
     (user_id, owner_scope, source, identity_scope, native_id, forbid_reimport, recursive, \
      actor_id, reason, deleted_at, thread_canonical_key) \
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
    ",",
    p!(7),
    ",",
    p!(8),
    ",",
    p!(9),
    ",",
    p!(10),
    ",",
    p!(11),
    ") ON CONFLICT (user_id, source, identity_scope, native_id) DO UPDATE SET \
       forbid_reimport = excluded.forbid_reimport, \
       recursive = excluded.recursive, \
       actor_id = excluded.actor_id, \
       reason = excluded.reason, \
       deleted_at = excluded.deleted_at, \
       thread_canonical_key = COALESCE(excluded.thread_canonical_key, thread_deletion_marker.thread_canonical_key)"
);

const FIND_SQL: &str = concat!(
    "SELECT ",
    THREAD_DELETION_MARKER_COLUMNS!(),
    " FROM thread_deletion_marker WHERE user_id = ",
    p!(1),
    " AND source = ",
    p!(2),
    " AND identity_scope = ",
    p!(3),
    " AND native_id = ",
    p!(4)
);

const EXISTS_SQL: &str = concat!(
    "SELECT 1 FROM thread_deletion_marker WHERE user_id = ",
    p!(1),
    " AND source = ",
    p!(2),
    " AND identity_scope = ",
    p!(3),
    " AND native_id = ",
    p!(4)
);

const DELETE_SQL: &str = concat!(
    "DELETE FROM thread_deletion_marker WHERE user_id = ",
    p!(1),
    " AND source = ",
    p!(2),
    " AND identity_scope = ",
    p!(3),
    " AND native_id = ",
    p!(4)
);

const LIST_BY_OWNER_SQL: &str = concat!(
    "SELECT ",
    THREAD_DELETION_MARKER_COLUMNS!(),
    " FROM thread_deletion_marker WHERE user_id = ",
    p!(1),
    " ORDER BY deleted_at DESC, source, identity_scope, native_id"
);

#[async_trait]
pub trait ThreadDeletionMarkerRepository: UseRdbPool + Send + Sync {
    /// Record (or overwrite) the marker for one identity, inside the
    /// deletion transaction.
    async fn put_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        marker: &NewThreadDeletionMarker<'_>,
    ) -> Result<()> {
        sqlx::query::<Rdb>(PUT_SQL)
            .bind(marker.identity.user_id)
            .bind(common::thread_group_key::legacy_owner_scope(
                marker.identity.user_id,
            ))
            .bind(marker.identity.source)
            .bind(marker.identity.identity_scope)
            .bind(marker.identity.native_id)
            .bind(marker.forbid_reimport)
            .bind(marker.recursive)
            .bind(&marker.actor_id)
            .bind(&marker.reason)
            .bind(marker.deleted_at)
            .bind(&marker.thread_canonical_key)
            .execute(tx)
            .await
            .map_err(LlmMemoryError::DBError)?;
        Ok(())
    }

    async fn find(
        &self,
        identity: &SourceIdentityKey<'_>,
    ) -> Result<Option<ThreadDeletionMarkerRow>> {
        Ok(sqlx::query_as::<Rdb, ThreadDeletionMarkerRow>(FIND_SQL)
            .bind(identity.user_id)
            .bind(identity.source)
            .bind(identity.identity_scope)
            .bind(identity.native_id)
            .fetch_optional(self.db_pool())
            .await
            .map_err(LlmMemoryError::DBError)?)
    }

    /// The 8.2 pre-write check must run inside the same identity
    /// exclusive section as the write, hence the tx read variant.
    async fn find_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        identity: &SourceIdentityKey<'_>,
    ) -> Result<Option<ThreadDeletionMarkerRow>> {
        Ok(sqlx::query_as::<Rdb, ThreadDeletionMarkerRow>(FIND_SQL)
            .bind(identity.user_id)
            .bind(identity.source)
            .bind(identity.identity_scope)
            .bind(identity.native_id)
            .fetch_optional(tx)
            .await
            .map_err(LlmMemoryError::DBError)?)
    }

    async fn exists_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        identity: &SourceIdentityKey<'_>,
    ) -> Result<bool> {
        let found = sqlx::query::<Rdb>(EXISTS_SQL)
            .bind(identity.user_id)
            .bind(identity.source)
            .bind(identity.identity_scope)
            .bind(identity.native_id)
            .fetch_optional(tx)
            .await
            .map_err(LlmMemoryError::DBError)?;
        Ok(found.is_some())
    }

    /// Consume the marker on a permitted re-import (atomic with the
    /// write transaction). Returns false when the marker is already
    /// gone.
    async fn consume_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        identity: &SourceIdentityKey<'_>,
    ) -> Result<bool> {
        let res = sqlx::query::<Rdb>(DELETE_SQL)
            .bind(identity.user_id)
            .bind(identity.source)
            .bind(identity.identity_scope)
            .bind(identity.native_id)
            .execute(tx)
            .await
            .map_err(LlmMemoryError::DBError)?;
        Ok(res.rows_affected() > 0)
    }

    /// Operator / reporting view of one owner's markers, paged newest
    /// first.
    async fn list_by_owner(
        &self,
        user_id: i64,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> Result<Vec<ThreadDeletionMarkerRow>> {
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
            sqlx::query_as::<Rdb, ThreadDeletionMarkerRow>(sqlx::AssertSqlSafe(sql)).bind(user_id);
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
}

pub struct ThreadDeletionMarkerRepositoryImpl {
    pool: &'static RdbPool,
}

impl ThreadDeletionMarkerRepositoryImpl {
    pub fn new(pool: &'static RdbPool) -> Self {
        Self { pool }
    }
}

impl UseRdbPool for ThreadDeletionMarkerRepositoryImpl {
    fn db_pool(&self) -> &RdbPool {
        self.pool
    }
}

impl ThreadDeletionMarkerRepository for ThreadDeletionMarkerRepositoryImpl {}

#[cfg(all(test, not(feature = "postgres")))]
mod tests {
    use super::*;
    use crate::infra::thread_group::test_support::*;
    use anyhow::Context;
    use infra_utils::infra::test::TEST_RUNTIME;

    async fn _test_deletion_marker_flow(pool: &'static RdbPool) -> Result<()> {
        let repo = ThreadDeletionMarkerRepositoryImpl::new(pool);
        let ident = identity(5);

        let mut tx = pool.begin().await?;
        repo.put_tx(&mut *tx, &new_deletion_marker(5)).await?;
        assert!(repo.exists_tx(&mut *tx, &ident).await?);
        let marker = repo.find_tx(&mut *tx, &ident).await?.context("marker")?;
        assert!(marker.forbid_reimport);
        assert_eq!(marker.actor_id, "operator-a");
        tx.commit().await?;

        // Re-delete overwrites the marker fields (newest actor wins).
        let mut overwrite = new_deletion_marker(5);
        overwrite.forbid_reimport = false;
        overwrite.actor_id = "operator-b".to_string();
        repo.put_tx(pool, &overwrite).await?;
        let marker = repo.find(&ident).await?.context("overwritten")?;
        assert!(!marker.forbid_reimport);
        assert_eq!(marker.actor_id, "operator-b");
        assert_eq!(repo.list_by_owner(2, Some(10), None).await?.len(), 1);

        // Consumption is atomic with the re-import transaction.
        let mut tx = pool.begin().await?;
        assert!(repo.consume_tx(&mut *tx, &ident).await?);
        assert!(!repo.exists_tx(&mut *tx, &ident).await?);
        tx.commit().await?;
        assert!(!repo.consume_tx(pool, &ident).await?);
        Ok(())
    }

    #[test]
    fn deletion_marker_flow_sqlite() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let pool = setup_thread_group_pool().await;
            _test_deletion_marker_flow(pool).await
        })
    }
}
