//! `thread_canonical_key` repository (design 5.7).
//!
//! Live correspondence table between thread rows and their immutable
//! canonical keys. Key *derivation* (source-identity serialization v2,
//! creation UUID, backfill mapping) is app-layer work; this module only
//! stores the assignment and answers the two directions of lookup.
//! The row is deleted in the same transaction as the thread; history
//! keeps its own key copies in membership / relation endpoints.

use super::observation::is_unique_violation;
use super::rows::{THREAD_CANONICAL_KEY_COLUMNS, ThreadCanonicalKeyRow};
use crate::error::LlmMemoryError;
use crate::infra::fill_timestamps;
use crate::sql::p;
use anyhow::Result;
use async_trait::async_trait;
use infra_utils::infra::rdb::{Rdb, RdbPool, UseRdbPool};
use sqlx::Executor;

const INSERT_SQL: &str = concat!(
    "INSERT INTO thread_canonical_key (thread_id, user_id, owner_scope, key, origin, assigned_at) \
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

const FIND_BY_THREAD_SQL: &str = concat!(
    "SELECT ",
    THREAD_CANONICAL_KEY_COLUMNS!(),
    " FROM thread_canonical_key WHERE thread_id = ",
    p!(1)
);

const FIND_BY_KEY_SQL: &str = concat!(
    "SELECT ",
    THREAD_CANONICAL_KEY_COLUMNS!(),
    " FROM thread_canonical_key WHERE key = ",
    p!(1)
);

const DELETE_SQL: &str = concat!("DELETE FROM thread_canonical_key WHERE thread_id = ", p!(1));

#[async_trait]
pub trait ThreadCanonicalKeyRepository: UseRdbPool + Send + Sync {
    /// Assign a key to a live thread. Fails on UNIQUE collision either
    /// because the thread already has a key (immutable assignment) or
    /// because another live thread owns the key.
    async fn assign_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        thread_id: i64,
        user_id: i64,
        key: &str,
        origin: &str,
        assigned_at: i64,
    ) -> Result<()> {
        let (assigned_at, _) = fill_timestamps(assigned_at, assigned_at);
        sqlx::query::<Rdb>(INSERT_SQL)
            .bind(thread_id)
            .bind(user_id)
            .bind(common::thread_group_key::legacy_owner_scope(user_id))
            .bind(key)
            .bind(origin)
            .bind(assigned_at)
            .execute(tx)
            .await
            .map_err(LlmMemoryError::DBError)?;
        Ok(())
    }

    async fn find_by_thread_id(&self, thread_id: i64) -> Result<Option<ThreadCanonicalKeyRow>> {
        Ok(
            sqlx::query_as::<Rdb, ThreadCanonicalKeyRow>(FIND_BY_THREAD_SQL)
                .bind(thread_id)
                .fetch_optional(self.db_pool())
                .await
                .map_err(LlmMemoryError::DBError)?,
        )
    }

    async fn find_by_thread_id_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        thread_id: i64,
    ) -> Result<Option<ThreadCanonicalKeyRow>> {
        Ok(
            sqlx::query_as::<Rdb, ThreadCanonicalKeyRow>(FIND_BY_THREAD_SQL)
                .bind(thread_id)
                .fetch_optional(tx)
                .await
                .map_err(LlmMemoryError::DBError)?,
        )
    }

    /// Reverse lookup used when re-deriving the same source-backed key
    /// on import.
    async fn find_by_key(&self, key: &str) -> Result<Option<ThreadCanonicalKeyRow>> {
        Ok(
            sqlx::query_as::<Rdb, ThreadCanonicalKeyRow>(FIND_BY_KEY_SQL)
                .bind(key)
                .fetch_optional(self.db_pool())
                .await
                .map_err(LlmMemoryError::DBError)?,
        )
    }

    async fn find_by_key_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        key: &str,
    ) -> Result<Option<ThreadCanonicalKeyRow>> {
        Ok(
            sqlx::query_as::<Rdb, ThreadCanonicalKeyRow>(FIND_BY_KEY_SQL)
                .bind(key)
                .fetch_optional(tx)
                .await
                .map_err(LlmMemoryError::DBError)?,
        )
    }

    /// Idempotent backfill / re-import helper: assign the key to the
    /// thread, or — when the key is already owned — return the existing
    /// assignment so the caller can compare thread ids and decide
    /// (reusing a mapping is exactly the backfill re-run contract).
    ///
    /// Pool-level only: on PostgreSQL a UNIQUE violation inside the
    /// caller's transaction would abort it, so the collision path here
    /// reads through a fresh connection after the failed statement.
    async fn assign_or_find(
        &self,
        thread_id: i64,
        user_id: i64,
        key: &str,
        origin: &str,
        assigned_at: i64,
    ) -> Result<(ThreadCanonicalKeyRow, bool)> {
        let insert_err = match self
            .assign_tx(self.db_pool(), thread_id, user_id, key, origin, assigned_at)
            .await
        {
            Ok(()) => {
                let row = self.find_by_thread_id(thread_id).await?.ok_or_else(|| {
                    LlmMemoryError::RuntimeError(format!(
                        "canonical key for thread {thread_id} vanished right after assign"
                    ))
                })?;
                return Ok((row, true));
            }
            Err(e) => e,
        };
        if !is_unique_violation(&insert_err) {
            return Err(insert_err);
        }
        match self.find_by_key(key).await? {
            Some(row) => Ok((row, false)),
            // The key collision may actually be on thread_id (a live
            // assignment exists for this thread, under some key the
            // caller did not expect): return that mapping instead.
            None => match self.find_by_thread_id(thread_id).await? {
                Some(row) => Ok((row, false)),
                None => Err(insert_err),
            },
        }
    }

    /// Remove the correspondence row in the same transaction as the
    /// thread deletion (design 5.7).
    async fn delete_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        thread_id: i64,
    ) -> Result<bool> {
        let res = sqlx::query::<Rdb>(DELETE_SQL)
            .bind(thread_id)
            .execute(tx)
            .await
            .map_err(LlmMemoryError::DBError)?;
        Ok(res.rows_affected() > 0)
    }
}

pub struct ThreadCanonicalKeyRepositoryImpl {
    pool: &'static RdbPool,
}

impl ThreadCanonicalKeyRepositoryImpl {
    pub fn new(pool: &'static RdbPool) -> Self {
        Self { pool }
    }
}

impl UseRdbPool for ThreadCanonicalKeyRepositoryImpl {
    fn db_pool(&self) -> &RdbPool {
        self.pool
    }
}

impl ThreadCanonicalKeyRepository for ThreadCanonicalKeyRepositoryImpl {}

#[cfg(all(test, not(feature = "postgres")))]
mod tests {
    use super::*;
    use crate::infra::thread_group::rows::values;
    use crate::infra::thread_group::test_support::*;
    use anyhow::Context;
    use infra_utils::infra::test::TEST_RUNTIME;

    async fn _test_canonical_key_assignment(pool: &'static RdbPool) -> Result<()> {
        let repo = ThreadCanonicalKeyRepositoryImpl::new(pool);
        let thread_a = 2001;
        let thread_b = 2002;
        let k = key(31);

        repo.assign_tx(
            pool,
            thread_a,
            1,
            &k,
            values::canonical_key_origin::CREATION_UUID,
            T0,
        )
        .await?;
        // Re-assigning the same thread a second key fails on the PK…
        assert!(
            repo.assign_tx(
                pool,
                thread_a,
                1,
                &key(32),
                values::canonical_key_origin::CREATION_UUID,
                T0
            )
            .await
            .is_err()
        );
        // …and stealing the key for another thread fails on the global
        // UNIQUE index.
        assert!(
            repo.assign_tx(
                pool,
                thread_b,
                1,
                &k,
                values::canonical_key_origin::SOURCE_IDENTITY,
                T0
            )
            .await
            .is_err()
        );

        // Backfill / re-import idempotency: same (thread, key) again
        // returns the existing assignment without creating a row.
        let (row, created) = repo
            .assign_or_find(
                thread_a,
                1,
                &k,
                values::canonical_key_origin::CREATION_UUID,
                T0,
            )
            .await?;
        assert!(!created);
        assert_eq!(row.thread_id, thread_a);
        assert_eq!(row.key, k);

        // The key-collision path surfaces the *owner* row so the app
        // can decide whether this is the same session's mapping.
        let (owner, created) = repo
            .assign_or_find(
                thread_b,
                1,
                &k,
                values::canonical_key_origin::SOURCE_IDENTITY,
                T0,
            )
            .await?;
        assert!(!created);
        assert_eq!(owner.thread_id, thread_a);

        assert_eq!(
            repo.find_by_key(&k).await?.context("by key")?.thread_id,
            thread_a
        );
        assert_eq!(
            repo.find_by_thread_id(thread_a)
                .await?
                .context("by thread")?
                .key,
            k
        );

        // History keeps keys in membership rows; the correspondence
        // table itself is cleaned with the thread row.
        let mut tx = pool.begin().await?;
        assert!(repo.delete_tx(&mut *tx, thread_a).await?);
        tx.commit().await?;
        assert!(repo.find_by_thread_id(thread_a).await?.is_none());
        Ok(())
    }

    #[test]
    fn canonical_key_assignment_sqlite() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let pool = setup_thread_group_pool().await;
            _test_canonical_key_assignment(pool).await
        })
    }
}
