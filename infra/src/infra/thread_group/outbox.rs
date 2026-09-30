//! `thread_group_event_outbox` repository (design 8.3).
//!
//! Transactional domain-event outbox: rows are immutable and must be
//! written inside the same transaction as the state transition they
//! describe (`append_tx`). `event_id` is caller-owned and doubles as the
//! jobworkerp `uniq_key`, so a delivery retry reuses the same id —
//! `append_idempotent_tx` makes the writer-side retry a no-op, and
//! `find_by_event_id` gives the consumer its processed / duplicate
//! check. Purge after ack is `delete_tx`.

use super::rows::{EVENT_OUTBOX_COLUMNS, NewThreadGroupEvent, ThreadGroupEventOutboxRow};
use crate::error::LlmMemoryError;
use crate::infra::fill_timestamps;
use crate::sql::{p, p_jsonb};
use anyhow::Result;
use async_trait::async_trait;
use infra_utils::infra::rdb::{Rdb, RdbPool, UseRdbPool};
use sqlx::Executor;

const INSERT_SQL: &str = concat!(
    "INSERT INTO thread_group_event_outbox \
     (event_id, event_type, operation_id, policy_version, source, identity_scope, \
      user_id, native_id_ref, group_id, thread_id, source_confidence, \
      selection_basis, operator_decision_id, polarity, payload, created_at) \
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
    ",",
    p!(12),
    ",",
    p!(13),
    ",",
    p!(14),
    ",",
    p_jsonb!(15),
    ",",
    p!(16),
    ")"
);

const INSERT_OR_IGNORE_SQL: &str = concat!(
    "INSERT INTO thread_group_event_outbox \
     (event_id, event_type, operation_id, policy_version, source, identity_scope, \
      user_id, native_id_ref, group_id, thread_id, source_confidence, \
      selection_basis, operator_decision_id, polarity, payload, created_at) \
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
    ",",
    p!(12),
    ",",
    p!(13),
    ",",
    p!(14),
    ",",
    p_jsonb!(15),
    ",",
    p!(16),
    ") ON CONFLICT (event_id) DO NOTHING"
);

const FIND_BY_EVENT_ID_SQL: &str = concat!(
    "SELECT ",
    EVENT_OUTBOX_COLUMNS!(),
    " FROM thread_group_event_outbox WHERE event_id = ",
    p!(1)
);

// Dispatch scan order matches the (event_type, created_at) and plain
// created_at indexes; the event_id tie-break keeps paging stable inside
// one millisecond.
const LIST_OLDEST_SQL: &str = concat!(
    "SELECT ",
    EVENT_OUTBOX_COLUMNS!(),
    " FROM thread_group_event_outbox ORDER BY created_at, event_id"
);

const DELETE_SQL: &str = concat!(
    "DELETE FROM thread_group_event_outbox WHERE event_id = ",
    p!(1)
);

#[async_trait]
pub trait ThreadGroupEventOutboxRepository: UseRdbPool + Send + Sync {
    /// Strict append for the state-transition transaction. Reusing an
    /// `event_id` for a *different* event is a caller bug and fails on
    /// the PK (PostgreSQL aborts the transaction — the app must derive
    /// fresh ids on retry, or use `append_idempotent_tx` for the
    /// same-event retry case).
    async fn append_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        event: &NewThreadGroupEvent,
    ) -> Result<()> {
        let (created_at, _) = fill_timestamps(event.created_at, event.created_at);
        sqlx::query::<Rdb>(INSERT_SQL)
            .bind(&event.event_id)
            .bind(&event.event_type)
            .bind(&event.operation_id)
            .bind(&event.policy_version)
            .bind(&event.source)
            .bind(&event.identity_scope)
            .bind(event.user_id)
            .bind(&event.native_id_ref)
            .bind(event.group_id)
            .bind(event.thread_id)
            .bind(&event.source_confidence)
            .bind(&event.selection_basis)
            .bind(event.operator_decision_id)
            .bind(&event.polarity)
            .bind(&event.payload)
            .bind(created_at)
            .execute(tx)
            .await
            .map_err(LlmMemoryError::DBError)?;
        Ok(())
    }

    /// Retry-safe append: an event row with the same `event_id` already
    /// present is left untouched (the first writer's immutable row wins,
    /// mirroring "delivery retry reruns the same event"). Returns true
    /// when a new row was written.
    async fn append_idempotent_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        event: &NewThreadGroupEvent,
    ) -> Result<bool> {
        let (created_at, _) = fill_timestamps(event.created_at, event.created_at);
        let res = sqlx::query::<Rdb>(INSERT_OR_IGNORE_SQL)
            .bind(&event.event_id)
            .bind(&event.event_type)
            .bind(&event.operation_id)
            .bind(&event.policy_version)
            .bind(&event.source)
            .bind(&event.identity_scope)
            .bind(event.user_id)
            .bind(&event.native_id_ref)
            .bind(event.group_id)
            .bind(event.thread_id)
            .bind(&event.source_confidence)
            .bind(&event.selection_basis)
            .bind(event.operator_decision_id)
            .bind(&event.polarity)
            .bind(&event.payload)
            .bind(created_at)
            .execute(tx)
            .await
            .map_err(LlmMemoryError::DBError)?;
        Ok(res.rows_affected() > 0)
    }

    /// Consumer-side duplicate suppression: presence of the row means
    /// the event was recorded; the processing transaction decides
    /// "handled" state elsewhere.
    async fn find_by_event_id(&self, event_id: &str) -> Result<Option<ThreadGroupEventOutboxRow>> {
        Ok(
            sqlx::query_as::<Rdb, ThreadGroupEventOutboxRow>(FIND_BY_EVENT_ID_SQL)
                .bind(event_id)
                .fetch_optional(self.db_pool())
                .await
                .map_err(LlmMemoryError::DBError)?,
        )
    }

    /// Dispatcher scan: oldest undelivered rows first, paged.
    async fn list_oldest(
        &self,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> Result<Vec<ThreadGroupEventOutboxRow>> {
        use crate::sql::dyn_placeholder;
        let mut sql = String::from(LIST_OLDEST_SQL);
        let mut next = 1usize;
        if limit.is_some() {
            sql.push_str(&format!(" LIMIT {}", dyn_placeholder(next)));
            next += 1;
        }
        if offset.is_some() {
            sql.push_str(&format!(" OFFSET {}", dyn_placeholder(next)));
        }
        let mut query = sqlx::query_as::<Rdb, ThreadGroupEventOutboxRow>(sqlx::AssertSqlSafe(sql));
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

    /// Purge after downstream acknowledgement (rows are immutable —
    /// updates never happen here).
    async fn delete_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        event_id: &str,
    ) -> Result<bool> {
        let res = sqlx::query::<Rdb>(DELETE_SQL)
            .bind(event_id)
            .execute(tx)
            .await
            .map_err(LlmMemoryError::DBError)?;
        Ok(res.rows_affected() > 0)
    }
}

pub struct ThreadGroupEventOutboxRepositoryImpl {
    pool: &'static RdbPool,
}

impl ThreadGroupEventOutboxRepositoryImpl {
    pub fn new(pool: &'static RdbPool) -> Self {
        Self { pool }
    }
}

impl UseRdbPool for ThreadGroupEventOutboxRepositoryImpl {
    fn db_pool(&self) -> &RdbPool {
        self.pool
    }
}

impl ThreadGroupEventOutboxRepository for ThreadGroupEventOutboxRepositoryImpl {}

#[cfg(all(test, not(feature = "postgres")))]
mod tests {
    use super::*;
    use crate::infra::thread_group::test_support::*;
    use anyhow::Context;
    use infra_utils::infra::test::TEST_RUNTIME;

    async fn _test_outbox_flow(pool: &'static RdbPool) -> Result<()> {
        let repo = ThreadGroupEventOutboxRepositoryImpl::new(pool);

        // Append inside the state-transition transaction; payload text
        // round-trips unchanged.
        let event = new_event(1, "evt-1".to_string());
        let mut tx = pool.begin().await?;
        repo.append_tx(&mut *tx, &event).await?;
        tx.commit().await?;
        let row = repo.find_by_event_id("evt-1").await?.context("event row")?;
        assert_eq!(row.payload, "{\"seq\":1}");
        assert_eq!(row.polarity.as_deref(), Some("supports"));

        // A second event under the same id is a caller bug (strict
        // append fails on the PK)…
        let mut clash = new_event(2, "evt-1".to_string());
        clash.payload = "{\"seq\":2}".to_string();
        assert!(repo.append_tx(pool, &clash).await.is_err());
        // …while the retry-safe form is a no-op keeping the first row.
        assert!(!repo.append_idempotent_tx(pool, &clash).await?);
        assert_eq!(
            repo.find_by_event_id("evt-1")
                .await?
                .context("kept")?
                .payload,
            "{\"seq\":1}"
        );

        // Dispatcher scan is oldest-first with stable paging.
        repo.append_tx(pool, &new_event(2, "evt-2".to_string()))
            .await?;
        repo.append_tx(pool, &new_event(3, "evt-3".to_string()))
            .await?;
        let page = repo.list_oldest(Some(2), Some(0)).await?;
        assert_eq!(
            page.iter().map(|e| e.event_id.as_str()).collect::<Vec<_>>(),
            vec!["evt-1", "evt-2"]
        );
        let page2 = repo.list_oldest(Some(2), Some(2)).await?;
        assert_eq!(page2[0].event_id, "evt-3");

        // Ack purge.
        let mut tx = pool.begin().await?;
        assert!(repo.delete_tx(&mut *tx, "evt-1").await?);
        assert!(!repo.delete_tx(&mut *tx, "evt-1").await?);
        tx.commit().await?;
        assert!(repo.find_by_event_id("evt-1").await?.is_none());
        Ok(())
    }

    #[test]
    fn outbox_flow_sqlite() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let pool = setup_thread_group_pool().await;
            _test_outbox_flow(pool).await
        })
    }
}
