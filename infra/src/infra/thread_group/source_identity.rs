//! `source_thread_identity` repository (design 5.6).
//!
//! Resolved owner-local mapping only: a row exists exactly when one
//! `(owner_scope, source, identity_scope, native_id)` tuple maps to
//! exactly one thread. Pending / ambiguous / unsupported identities live
//! in observations and candidate associations and never reach this
//! table — that restriction is enforced by the `resolution_state` CHECK
//! plus the app-layer admission rule, not by anything here.
//!
//! The primary key doubles as the natural idempotency key, so all
//! writes are single-statement upserts / deletes safe to retry.

use super::rows::{SOURCE_THREAD_IDENTITY_COLUMNS, SourceIdentityKey, SourceThreadIdentityRow};
use crate::error::LlmMemoryError;
use crate::sql::p;
use anyhow::Result;
use async_trait::async_trait;
use infra_utils::infra::rdb::{Rdb, RdbPool, UseRdbPool};
use sqlx::Executor;

// INSERT ... ON CONFLICT DO UPDATE works identically on SQLite (>= 3.24)
// and PostgreSQL. Re-importing the same identity refreshes the mapping
// (a revived session may land on a new thread row) and bumps
// `last_seen_at`; `first_seen_at` is written only on the initial insert.
const UPSERT_RESOLVED_SQL: &str = concat!(
    "INSERT INTO source_thread_identity \
     (owner_scope, source, identity_scope, native_id, thread_id, resolution_state, \
      first_seen_at, last_seen_at) \
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
    ", 'resolved', ",
    p!(6),
    ",",
    p!(7),
    ") ON CONFLICT (owner_scope, source, identity_scope, native_id) DO UPDATE SET \
       thread_id = excluded.thread_id, last_seen_at = excluded.last_seen_at"
);

const FIND_RESOLVED_SQL: &str = concat!(
    "SELECT ",
    SOURCE_THREAD_IDENTITY_COLUMNS!(),
    " FROM source_thread_identity WHERE owner_scope = ",
    p!(1),
    " AND source = ",
    p!(2),
    " AND identity_scope = ",
    p!(3),
    " AND native_id = ",
    p!(4)
);

const LIST_BY_THREAD_SQL: &str = concat!(
    "SELECT ",
    SOURCE_THREAD_IDENTITY_COLUMNS!(),
    " FROM source_thread_identity WHERE thread_id = ",
    p!(1),
    " ORDER BY owner_scope, source, identity_scope, native_id"
);

const REBIND_SQL: &str = concat!(
    "UPDATE source_thread_identity SET thread_id = ",
    p!(1),
    ", last_seen_at = ",
    p!(2),
    " WHERE owner_scope = ",
    p!(3),
    " AND source = ",
    p!(4),
    " AND identity_scope = ",
    p!(5),
    " AND native_id = ",
    p!(6)
);

const DELETE_SQL: &str = concat!(
    "DELETE FROM source_thread_identity WHERE owner_scope = ",
    p!(1),
    " AND source = ",
    p!(2),
    " AND identity_scope = ",
    p!(3),
    " AND native_id = ",
    p!(4)
);

#[async_trait]
pub trait SourceThreadIdentityRepository: UseRdbPool + Send + Sync {
    /// Resolve (or re-resolve) one owner-local identity to a thread.
    /// Idempotent: repeating with the same tuple refreshes the mapping
    /// instead of failing.
    async fn upsert_resolved_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        identity: &SourceIdentityKey<'_>,
        thread_id: i64,
        seen_at: i64,
    ) -> Result<()> {
        sqlx::query::<Rdb>(UPSERT_RESOLVED_SQL)
            .bind(identity.owner_scope)
            .bind(identity.source)
            .bind(identity.identity_scope)
            .bind(identity.native_id)
            .bind(thread_id)
            .bind(seen_at)
            .bind(seen_at)
            .execute(tx)
            .await
            .map_err(LlmMemoryError::DBError)?;
        Ok(())
    }

    /// Design 8.2 pre-write check / backfill resolution lookup.
    async fn find_resolved(
        &self,
        identity: &SourceIdentityKey<'_>,
    ) -> Result<Option<SourceThreadIdentityRow>> {
        Ok(
            sqlx::query_as::<Rdb, SourceThreadIdentityRow>(FIND_RESOLVED_SQL)
                .bind(identity.owner_scope)
                .bind(identity.source)
                .bind(identity.identity_scope)
                .bind(identity.native_id)
                .fetch_optional(self.db_pool())
                .await
                .map_err(LlmMemoryError::DBError)?,
        )
    }

    async fn find_resolved_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        identity: &SourceIdentityKey<'_>,
    ) -> Result<Option<SourceThreadIdentityRow>> {
        Ok(
            sqlx::query_as::<Rdb, SourceThreadIdentityRow>(FIND_RESOLVED_SQL)
                .bind(identity.owner_scope)
                .bind(identity.source)
                .bind(identity.identity_scope)
                .bind(identity.native_id)
                .fetch_optional(tx)
                .await
                .map_err(LlmMemoryError::DBError)?,
        )
    }

    /// All identities currently mapped to one thread (deletion flows
    /// need every marker placeholder to write).
    async fn list_by_thread_id(&self, thread_id: i64) -> Result<Vec<SourceThreadIdentityRow>> {
        Ok(
            sqlx::query_as::<Rdb, SourceThreadIdentityRow>(LIST_BY_THREAD_SQL)
                .bind(thread_id)
                .fetch_all(self.db_pool())
                .await
                .map_err(LlmMemoryError::DBError)?,
        )
    }

    /// In-transaction variant so the policy delete can re-read the
    /// identity set after acquiring the identity / canonical-key locks.
    async fn list_by_thread_id_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        thread_id: i64,
    ) -> Result<Vec<SourceThreadIdentityRow>> {
        Ok(
            sqlx::query_as::<Rdb, SourceThreadIdentityRow>(LIST_BY_THREAD_SQL)
                .bind(thread_id)
                .fetch_all(tx)
                .await
                .map_err(LlmMemoryError::DBError)?,
        )
    }

    /// Point an existing mapping at a new thread row without touching
    /// `first_seen_at` (revival onto a recreated thread). Returns false
    /// when no resolved row exists — the app layer must not fabricate
    /// mappings through this call.
    async fn rebind_thread_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        identity: &SourceIdentityKey<'_>,
        thread_id: i64,
        seen_at: i64,
    ) -> Result<bool> {
        let res = sqlx::query::<Rdb>(REBIND_SQL)
            .bind(thread_id)
            .bind(seen_at)
            .bind(identity.owner_scope)
            .bind(identity.source)
            .bind(identity.identity_scope)
            .bind(identity.native_id)
            .execute(tx)
            .await
            .map_err(LlmMemoryError::DBError)?;
        Ok(res.rows_affected() > 0)
    }

    async fn delete_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        identity: &SourceIdentityKey<'_>,
    ) -> Result<bool> {
        let res = sqlx::query::<Rdb>(DELETE_SQL)
            .bind(identity.owner_scope)
            .bind(identity.source)
            .bind(identity.identity_scope)
            .bind(identity.native_id)
            .execute(tx)
            .await
            .map_err(LlmMemoryError::DBError)?;
        Ok(res.rows_affected() > 0)
    }
}

pub struct SourceThreadIdentityRepositoryImpl {
    pool: &'static RdbPool,
}

impl SourceThreadIdentityRepositoryImpl {
    pub fn new(pool: &'static RdbPool) -> Self {
        Self { pool }
    }
}

impl UseRdbPool for SourceThreadIdentityRepositoryImpl {
    fn db_pool(&self) -> &RdbPool {
        self.pool
    }
}

impl SourceThreadIdentityRepository for SourceThreadIdentityRepositoryImpl {}

#[cfg(all(test, not(feature = "postgres")))]
mod tests {
    use super::*;
    use crate::infra::thread_group::test_support::*;
    use anyhow::Context;
    use infra_utils::infra::test::TEST_RUNTIME;

    async fn _test_source_identity_mapping(pool: &'static RdbPool) -> Result<()> {
        let repo = SourceThreadIdentityRepositoryImpl::new(pool);
        let ident = identity(1);

        // Resolve, then re-resolve onto a new thread row: the upsert
        // refreshes thread_id / last_seen_at but keeps first_seen_at.
        repo.upsert_resolved_tx(pool, &ident, 500, T0).await?;
        repo.upsert_resolved_tx(pool, &ident, 501, T0 + 9).await?;
        let row = repo
            .find_resolved(&ident)
            .await?
            .context("resolved mapping")?;
        assert_eq!(row.thread_id, 501);
        assert_eq!(row.first_seen_at, T0);
        assert_eq!(row.last_seen_at, T0 + 9);
        assert_eq!(row.resolution_state, "resolved");

        assert_eq!(repo.list_by_thread_id(501).await?.len(), 1);

        // rebind only refreshes an existing row — it never fabricates
        // the mapping.
        assert!(
            !repo
                .rebind_thread_tx(pool, &identity(77), 900, T0 + 1)
                .await?
        );
        assert!(repo.rebind_thread_tx(pool, &ident, 900, T0 + 2).await?);
        assert_eq!(
            repo.find_resolved(&ident)
                .await?
                .context("rebound")?
                .thread_id,
            900
        );

        assert!(repo.delete_tx(pool, &ident).await?);
        assert!(repo.find_resolved(&ident).await?.is_none());
        assert!(!repo.delete_tx(pool, &ident).await?);
        Ok(())
    }

    #[test]
    fn source_identity_mapping_sqlite() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let pool = setup_thread_group_pool().await;
            _test_source_identity_mapping(pool).await
        })
    }
}
