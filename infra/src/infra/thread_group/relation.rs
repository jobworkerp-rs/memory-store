//! `thread_relation` repository (design 5.3).
//!
//! Canonical parent edges with their retraction history. Storage
//! invariants come from the DDL: one *active* parent per
//! `child_thread_canonical_key` (partial UNIQUE index), self edges
//! rejected by CHECK, evidence-reference shape tied to
//! `selection_basis` by CHECK. Cycle / cardinality / identity-scope
//! policy stays in the app layer; connected-component serialization is
//! the app's transaction contract (see `lock` for the shared
//! key-locking primitive).

use super::rows::{NewThreadRelation, THREAD_RELATION_COLUMNS, ThreadRelationRow};
use crate::error::LlmMemoryError;
use crate::infra::{IdGeneratorWrapper, UseIdGenerator, fill_timestamps, fill_updated_at};
use crate::sql::{IN_LIST_CHUNK_SIZE, build_in_placeholders, dyn_placeholder, p};
use anyhow::Result;
use async_trait::async_trait;
use infra_utils::infra::rdb::{Rdb, RdbPool, UseRdbPool};
use sqlx::Executor;

const INSERT_SQL: &str = concat!(
    "INSERT INTO thread_relation \
     (id, parent_thread_id, child_thread_id, parent_thread_canonical_key, child_thread_canonical_key, \
      parent_user_id, parent_owner_scope, parent_source, parent_identity_scope, parent_native_id, \
      child_user_id, child_owner_scope, child_source, child_identity_scope, child_native_id, \
      relation_type, state, selection_basis, source_confidence, \
      selected_observation_id, selected_operator_decision_id, created_at, updated_at) \
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
    p!(15),
    ",",
    p!(16),
    ",",
    p!(17),
    ",",
    p!(18),
    ",",
    p!(19),
    ",",
    p!(20),
    ",",
    p!(21),
    ",",
    p!(22),
    ",",
    p!(23),
    ")"
);

const FIND_BY_ID_SQL: &str = concat!(
    "SELECT ",
    THREAD_RELATION_COLUMNS!(),
    " FROM thread_relation WHERE id = ",
    p!(1)
);

// The partial UNIQUE index (child key WHERE state='active') makes this
// lookup return at most one row.
const FIND_ACTIVE_BY_CHILD_KEY_SQL: &str = concat!(
    "SELECT ",
    THREAD_RELATION_COLUMNS!(),
    " FROM thread_relation WHERE child_thread_canonical_key = ",
    p!(1),
    " AND state = 'active'"
);

const SET_STATE_SQL: &str = concat!(
    "UPDATE thread_relation SET state = ",
    p!(1),
    ", updated_at = ",
    p!(2),
    " WHERE id = ",
    p!(3),
    " AND state = ",
    p!(4)
);

const DELETE_BY_ID_SQL: &str = concat!("DELETE FROM thread_relation WHERE id = ", p!(1));

// Thread deletion nulls the live endpoint while the canonical key keeps
// the lineage; revival reconnects by key (design 5.3).
const DETACH_PARENT_SQL: &str = concat!(
    "UPDATE thread_relation SET parent_thread_id = NULL, updated_at = ",
    p!(1),
    " WHERE parent_thread_id = ",
    p!(2)
);

const DETACH_CHILD_SQL: &str = concat!(
    "UPDATE thread_relation SET child_thread_id = NULL, updated_at = ",
    p!(1),
    " WHERE child_thread_id = ",
    p!(2)
);

// Revival reconnects only endpoints whose thread row had gone NULL;
// relations that already carry a thread id are left untouched.
const RECONNECT_PARENT_SQL: &str = concat!(
    "UPDATE thread_relation SET parent_thread_id = ",
    p!(1),
    ", updated_at = ",
    p!(2),
    " WHERE parent_thread_canonical_key = ",
    p!(3),
    " AND parent_thread_id IS NULL"
);

const RECONNECT_CHILD_SQL: &str = concat!(
    "UPDATE thread_relation SET child_thread_id = ",
    p!(1),
    ", updated_at = ",
    p!(2),
    " WHERE child_thread_canonical_key = ",
    p!(3),
    " AND child_thread_id IS NULL"
);

// Shared SQL assembly for the parent / child canonical-key listings.
// `column` is a hard-coded endpoint column name, never caller data.
async fn list_by_endpoint<'e, E: Executor<'e, Database = Rdb>>(
    executor: E,
    column: &'static str,
    key: &str,
    state: Option<&str>,
) -> Result<Vec<ThreadRelationRow>> {
    let cols = THREAD_RELATION_COLUMNS!();
    let mut sql = format!(
        "SELECT {cols} FROM thread_relation WHERE {column} = {}",
        dyn_placeholder(1)
    );
    if state.is_some() {
        sql.push_str(&format!(" AND state = {}", dyn_placeholder(2)));
    }
    sql.push_str(" ORDER BY id");
    let mut query = sqlx::query_as::<Rdb, ThreadRelationRow>(sqlx::AssertSqlSafe(sql)).bind(key);
    if let Some(state) = state {
        query = query.bind(state);
    }
    Ok(query
        .fetch_all(executor)
        .await
        .map_err(LlmMemoryError::DBError)?)
}

#[async_trait]
pub trait ThreadRelationRepository: UseRdbPool + UseIdGenerator + Send + Sync {
    /// Insert a canonical edge. Inserting a second *active* row for the
    /// same child canonical key fails on the partial UNIQUE index; the
    /// app layer supersedes / retracts the incumbent first (inside the
    /// same transaction on SQLite; on PostgreSQL a UNIQUE race aborts
    /// the transaction and the app retries with bounded backoff).
    async fn insert_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        relation: &NewThreadRelation,
    ) -> Result<i64> {
        let id = self.id_generator().generate_id()?;
        let (created_at, updated_at) = fill_timestamps(relation.created_at, relation.updated_at);
        sqlx::query::<Rdb>(INSERT_SQL)
            .bind(id)
            .bind(relation.parent_thread_id)
            .bind(relation.child_thread_id)
            .bind(&relation.parent_thread_canonical_key)
            .bind(&relation.child_thread_canonical_key)
            .bind(relation.parent_user_id)
            .bind(common::thread_group_key::legacy_owner_scope(
                relation.parent_user_id,
            ))
            .bind(&relation.parent_source)
            .bind(&relation.parent_identity_scope)
            .bind(&relation.parent_native_id)
            .bind(relation.child_user_id)
            .bind(common::thread_group_key::legacy_owner_scope(
                relation.child_user_id,
            ))
            .bind(&relation.child_source)
            .bind(&relation.child_identity_scope)
            .bind(&relation.child_native_id)
            .bind(&relation.relation_type)
            .bind(&relation.state)
            .bind(&relation.selection_basis)
            .bind(&relation.source_confidence)
            .bind(relation.selected_observation_id)
            .bind(relation.selected_operator_decision_id)
            .bind(created_at)
            .bind(updated_at)
            .execute(tx)
            .await
            .map_err(LlmMemoryError::DBError)?;
        Ok(id)
    }

    async fn find_by_id(&self, id: i64) -> Result<Option<ThreadRelationRow>> {
        Ok(sqlx::query_as::<Rdb, ThreadRelationRow>(FIND_BY_ID_SQL)
            .bind(id)
            .fetch_optional(self.db_pool())
            .await
            .map_err(LlmMemoryError::DBError)?)
    }

    /// Idempotent "does this child already have a canonical parent"
    /// lookup over the active-child partial UNIQUE index.
    async fn find_active_by_child_canonical_key(
        &self,
        child_thread_canonical_key: &str,
    ) -> Result<Option<ThreadRelationRow>> {
        Ok(
            sqlx::query_as::<Rdb, ThreadRelationRow>(FIND_ACTIVE_BY_CHILD_KEY_SQL)
                .bind(child_thread_canonical_key)
                .fetch_optional(self.db_pool())
                .await
                .map_err(LlmMemoryError::DBError)?,
        )
    }

    async fn find_active_by_child_canonical_key_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        child_thread_canonical_key: &str,
    ) -> Result<Option<ThreadRelationRow>> {
        Ok(
            sqlx::query_as::<Rdb, ThreadRelationRow>(FIND_ACTIVE_BY_CHILD_KEY_SQL)
                .bind(child_thread_canonical_key)
                .fetch_optional(tx)
                .await
                .map_err(LlmMemoryError::DBError)?,
        )
    }

    /// Edges with this canonical parent key, optionally pinned to one
    /// state (None = include retracted / superseded history).
    async fn list_by_parent_canonical_key(
        &self,
        parent_thread_canonical_key: &str,
        state: Option<&str>,
    ) -> Result<Vec<ThreadRelationRow>> {
        list_by_endpoint(
            self.db_pool(),
            "parent_thread_canonical_key",
            parent_thread_canonical_key,
            state,
        )
        .await
    }

    /// In-transaction variant used by membership reassignment.
    async fn list_by_parent_canonical_key_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        parent_thread_canonical_key: &str,
        state: Option<&str>,
    ) -> Result<Vec<ThreadRelationRow>> {
        list_by_endpoint(
            tx,
            "parent_thread_canonical_key",
            parent_thread_canonical_key,
            state,
        )
        .await
    }

    /// Edges with this canonical child key, optionally pinned to one
    /// state.
    async fn list_by_child_canonical_key(
        &self,
        child_thread_canonical_key: &str,
        state: Option<&str>,
    ) -> Result<Vec<ThreadRelationRow>> {
        list_by_endpoint(
            self.db_pool(),
            "child_thread_canonical_key",
            child_thread_canonical_key,
            state,
        )
        .await
    }

    /// Active edges touching any of the given live thread ids on either
    /// endpoint — the connected-component edge load the reconciliation
    /// app layer walks. Chunked for the SQLite bind cap.
    async fn list_active_by_endpoint_thread_ids(
        &self,
        thread_ids: &[i64],
    ) -> Result<Vec<ThreadRelationRow>> {
        let mut rows = Vec::new();
        for chunk in thread_ids.chunks(IN_LIST_CHUNK_SIZE) {
            if chunk.is_empty() {
                continue;
            }
            let parent_placeholders = build_in_placeholders(chunk.len(), 1);
            let child_placeholders = build_in_placeholders(chunk.len(), 1 + chunk.len());
            let cols = THREAD_RELATION_COLUMNS!();
            let sql = format!(
                "SELECT {cols} FROM thread_relation \
                 WHERE state = 'active' \
                   AND (parent_thread_id IN ({parent_placeholders}) \
                        OR child_thread_id IN ({child_placeholders})) \
                 ORDER BY id"
            );
            let mut query = sqlx::query_as::<Rdb, ThreadRelationRow>(sqlx::AssertSqlSafe(sql));
            for id in chunk {
                query = query.bind(id);
            }
            for id in chunk {
                query = query.bind(id);
            }
            rows.extend(
                query
                    .fetch_all(self.db_pool())
                    .await
                    .map_err(LlmMemoryError::DBError)?,
            );
        }
        Ok(rows)
    }

    /// State transition (retract / supersede / revive history) pinned to
    /// an expected current state. Returns false on state mismatch so a
    /// racing transaction can detect the lost race instead of writing
    /// over the new state.
    async fn set_state_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        id: i64,
        expected_state: &str,
        new_state: &str,
        updated_at: i64,
    ) -> Result<bool> {
        let updated_at = fill_updated_at(updated_at);
        let res = sqlx::query::<Rdb>(SET_STATE_SQL)
            .bind(new_state)
            .bind(updated_at)
            .bind(id)
            .bind(expected_state)
            .execute(tx)
            .await
            .map_err(LlmMemoryError::DBError)?;
        Ok(res.rows_affected() > 0)
    }

    /// Reconnect NULL parent endpoints of a revived source-backed
    /// thread. Returns the number of edges updated.
    async fn reconnect_parent_thread_id_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        parent_thread_canonical_key: &str,
        thread_id: i64,
        updated_at: i64,
    ) -> Result<u64> {
        let updated_at = fill_updated_at(updated_at);
        let res = sqlx::query::<Rdb>(RECONNECT_PARENT_SQL)
            .bind(thread_id)
            .bind(updated_at)
            .bind(parent_thread_canonical_key)
            .execute(tx)
            .await
            .map_err(LlmMemoryError::DBError)?;
        Ok(res.rows_affected())
    }

    /// Reconnect NULL child endpoints of a revived source-backed
    /// thread. Returns the number of edges updated.
    async fn reconnect_child_thread_id_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        child_thread_canonical_key: &str,
        thread_id: i64,
        updated_at: i64,
    ) -> Result<u64> {
        let updated_at = fill_updated_at(updated_at);
        let res = sqlx::query::<Rdb>(RECONNECT_CHILD_SQL)
            .bind(thread_id)
            .bind(updated_at)
            .bind(child_thread_canonical_key)
            .execute(tx)
            .await
            .map_err(LlmMemoryError::DBError)?;
        Ok(res.rows_affected())
    }

    /// Null the parent endpoint of every relation whose live parent
    /// thread is being deleted. The canonical key is retained for
    /// revival. Returns the number of edges updated.
    async fn detach_parent_thread_id_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        thread_id: i64,
        updated_at: i64,
    ) -> Result<u64> {
        let updated_at = fill_updated_at(updated_at);
        let res = sqlx::query::<Rdb>(DETACH_PARENT_SQL)
            .bind(updated_at)
            .bind(thread_id)
            .execute(tx)
            .await
            .map_err(LlmMemoryError::DBError)?;
        Ok(res.rows_affected())
    }

    /// Null the child endpoint of every relation whose live child thread
    /// is being deleted. The canonical key is retained for revival.
    async fn detach_child_thread_id_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        thread_id: i64,
        updated_at: i64,
    ) -> Result<u64> {
        let updated_at = fill_updated_at(updated_at);
        let res = sqlx::query::<Rdb>(DETACH_CHILD_SQL)
            .bind(updated_at)
            .bind(thread_id)
            .execute(tx)
            .await
            .map_err(LlmMemoryError::DBError)?;
        Ok(res.rows_affected())
    }

    /// Physical delete for inactive-history purge. Only non-active rows
    /// are expected to be passed here.
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

pub struct ThreadRelationRepositoryImpl {
    pool: &'static RdbPool,
    id_generator: IdGeneratorWrapper,
}

impl ThreadRelationRepositoryImpl {
    pub fn new(id_generator: IdGeneratorWrapper, pool: &'static RdbPool) -> Self {
        Self { pool, id_generator }
    }
}

impl UseRdbPool for ThreadRelationRepositoryImpl {
    fn db_pool(&self) -> &RdbPool {
        self.pool
    }
}

impl UseIdGenerator for ThreadRelationRepositoryImpl {
    fn id_generator(&self) -> &IdGeneratorWrapper {
        &self.id_generator
    }
}

impl ThreadRelationRepository for ThreadRelationRepositoryImpl {}

#[cfg(all(test, not(feature = "postgres")))]
mod tests {
    use super::*;
    use crate::infra::thread_group::rows::values;
    use crate::infra::thread_group::test_support::*;
    use anyhow::Context;
    use infra_utils::infra::test::TEST_RUNTIME;

    async fn _test_relation_lifecycle(pool: &'static RdbPool) -> Result<()> {
        let repo =
            ThreadRelationRepositoryImpl::new(crate::test_helper::shared_id_generator(), pool);

        // Self edge is rejected declaratively (parent key = child key).
        let mut self_edge = new_relation(7, 7);
        self_edge.parent_thread_canonical_key = self_edge.child_thread_canonical_key.clone();
        assert!(repo.insert_tx(pool, &self_edge).await.is_err());

        // Selection basis / confidence pairing is enforced by CHECK.
        let mut mismatched = new_relation(8, 9);
        mismatched.selection_basis = values::selection_basis::OPERATOR_CONFIRMATION.to_string();
        mismatched.selected_observation_id = None;
        assert!(repo.insert_tx(pool, &mismatched).await.is_err());

        // Canonical active edge parent(1) -> child(2).
        let e1 = repo.insert_tx(pool, &new_relation(1, 2)).await?;
        let row = repo.find_by_id(e1).await?.context("e1")?;
        assert_eq!(row.state, values::relation_state::ACTIVE);
        let active = repo
            .find_active_by_child_canonical_key(&key(2))
            .await?
            .context("active parent of child 2")?;
        assert_eq!(active.id, e1);

        // One active parent per child: a second active edge for the
        // same child key collides with the partial UNIQUE index.
        assert!(repo.insert_tx(pool, &new_relation(3, 2)).await.is_err());

        // Supersede, then the slot is free for the winning edge.
        let mut tx = pool.begin().await?;
        assert!(
            repo.set_state_tx(
                &mut *tx,
                e1,
                values::relation_state::ACTIVE,
                values::relation_state::SUPERSEDED,
                T0 + 1
            )
            .await?
        );
        // Re-running the transition detects the lost race.
        assert!(
            !repo
                .set_state_tx(
                    &mut *tx,
                    e1,
                    values::relation_state::ACTIVE,
                    values::relation_state::RETRACTED,
                    T0 + 2
                )
                .await?
        );
        let e2_params = new_relation(3, 2);
        let e2 = repo.insert_tx(&mut *tx, &e2_params).await?;
        tx.commit().await?;
        let active = repo
            .find_active_by_child_canonical_key(&key(2))
            .await?
            .context("new active parent")?;
        assert_eq!(active.id, e2);

        // History stays queryable; endpoint listings filter by state.
        let history = repo.list_by_child_canonical_key(&key(2), None).await?;
        assert_eq!(history.len(), 2);
        let active_only = repo
            .list_by_child_canonical_key(&key(2), Some(values::relation_state::ACTIVE))
            .await?;
        assert_eq!(active_only.len(), 1);
        assert_eq!(
            repo.list_by_parent_canonical_key(&key(3), Some(values::relation_state::ACTIVE))
                .await?
                .len(),
            1
        );

        // Endpoint thread ids resolve the connected component from both
        // sides.
        let edges = repo.list_active_by_endpoint_thread_ids(&[300]).await?;
        assert!(edges.iter().any(|e| e.id == e2));
        let edges = repo.list_active_by_endpoint_thread_ids(&[200]).await?;
        assert!(edges.iter().any(|e| e.id == e2));

        // Revival reconnects NULL endpoints only.
        let mut detached = new_relation(4, 5);
        detached.parent_thread_id = None;
        detached.child_thread_id = None;
        let e3 = repo.insert_tx(pool, &detached).await?;
        let mut tx = pool.begin().await?;
        assert_eq!(
            repo.reconnect_parent_thread_id_tx(&mut *tx, &key(4), 4400, T0 + 3)
                .await?,
            1
        );
        assert_eq!(
            repo.reconnect_child_thread_id_tx(&mut *tx, &key(5), 5500, T0 + 3)
                .await?,
            1
        );
        // Already-connected endpoints are untouched on a second pass.
        assert_eq!(
            repo.reconnect_parent_thread_id_tx(&mut *tx, &key(4), 4444, T0 + 4)
                .await?,
            0
        );
        tx.commit().await?;
        let row = repo.find_by_id(e3).await?.context("e3")?;
        assert_eq!(row.parent_thread_id, Some(4400));
        assert_eq!(row.child_thread_id, Some(5500));
        Ok(())
    }

    #[test]
    fn relation_lifecycle_sqlite() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let pool = setup_thread_group_pool().await;
            _test_relation_lifecycle(pool).await
        })
    }
}
