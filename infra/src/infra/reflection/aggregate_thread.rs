//! `thread_aggregate_key` repository. The PRIMARY KEY
//! (user_id, labels_hash) is the linchpin of the
//! "retrieve-or-create" idempotent flow used in Phase 2 of
//! `finalize_generated_reflection`: a parallel finalize racing on the
//! same (user, labels_hash) gets a UNIQUE violation, the loser falls
//! back to a SELECT, and both finalizes end up referencing the same
//! aggregate thread.

use super::rows::ThreadAggregateKeyRow;
use crate::error::LlmMemoryError;
use crate::sql::p;
use anyhow::Result;
use async_trait::async_trait;
use infra_utils::infra::rdb::{Rdb, RdbPool, UseRdbPool};
use sqlx::Executor;

const INSERT_SQL: &str = concat!(
    "INSERT INTO thread_aggregate_key (user_id, labels_hash, thread_id, created_at) VALUES (",
    p!(1),
    ",",
    p!(2),
    ",",
    p!(3),
    ",",
    p!(4),
    ");"
);

const FIND_SQL: &str = concat!(
    "SELECT user_id, labels_hash, thread_id, created_at \
     FROM thread_aggregate_key \
     WHERE user_id = ",
    p!(1),
    " AND labels_hash = ",
    p!(2),
    ";"
);

const DELETE_SQL: &str = concat!(
    "DELETE FROM thread_aggregate_key \
     WHERE user_id = ",
    p!(1),
    " AND labels_hash = ",
    p!(2),
    ";"
);

#[async_trait]
pub trait ThreadAggregateKeyRepository: UseRdbPool + Send + Sync {
    /// Try to register a new aggregate-thread mapping. On UNIQUE
    /// collision, falls back to the existing row.
    ///
    /// The caller is expected to wrap this in a short transaction
    /// (Phase 2 of finalize) so the INSERT and the eventual SELECT
    /// see a stable snapshot. Postgres rejects the INSERT with
    /// `error.kind() == sqlx::error::ErrorKind::UniqueViolation`,
    /// sqlite surfaces the same kind via `sqlx::Error::Database`.
    async fn insert_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        user_id: i64,
        labels_hash: &str,
        thread_id: i64,
        created_at: i64,
    ) -> Result<()> {
        sqlx::query::<Rdb>(INSERT_SQL)
            .bind(user_id)
            .bind(labels_hash)
            .bind(thread_id)
            .bind(created_at)
            .execute(tx)
            .await
            .map(|_| ())
            .map_err(|e| LlmMemoryError::DBError(e).into())
    }

    async fn find(&self, user_id: i64, labels_hash: &str) -> Result<Option<ThreadAggregateKeyRow>> {
        sqlx::query_as::<Rdb, ThreadAggregateKeyRow>(FIND_SQL)
            .bind(user_id)
            .bind(labels_hash)
            .fetch_optional(self.db_pool())
            .await
            .map_err(|e| LlmMemoryError::DBError(e).into())
    }

    async fn delete_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        user_id: i64,
        labels_hash: &str,
    ) -> Result<bool> {
        sqlx::query::<Rdb>(DELETE_SQL)
            .bind(user_id)
            .bind(labels_hash)
            .execute(tx)
            .await
            .map(|r| r.rows_affected() > 0)
            .map_err(|e| LlmMemoryError::DBError(e).into())
    }
}

pub struct ThreadAggregateKeyRepositoryImpl {
    pool: &'static RdbPool,
}

impl ThreadAggregateKeyRepositoryImpl {
    pub fn new(pool: &'static RdbPool) -> Self {
        Self { pool }
    }
}

impl UseRdbPool for ThreadAggregateKeyRepositoryImpl {
    fn db_pool(&self) -> &RdbPool {
        self.pool
    }
}

impl ThreadAggregateKeyRepository for ThreadAggregateKeyRepositoryImpl {}
