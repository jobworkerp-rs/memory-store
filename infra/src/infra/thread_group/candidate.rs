//! `thread_group_candidate_association` repository (design 5.5).
//!
//! Holds unresolved lineage candidates — pending identity, ambiguous
//! source identity, unresolved parent, heuristic / unsupported evidence
//! — until the reconciliation app layer promotes them to canonical
//! membership / relation. There is deliberately no UNIQUE constraint on
//! this table (the schema records plain indexes only), so idempotency is
//! the app's decision after inspecting `list_by_subject_identity*`; this
//! module exposes the listing primitives that make that check possible.

use super::rows::{
    CANDIDATE_ASSOCIATION_COLUMNS, NewThreadGroupCandidateAssociation,
    ThreadGroupCandidateAssociationRow,
};
use crate::error::LlmMemoryError;
use crate::infra::{IdGeneratorWrapper, UseIdGenerator, fill_timestamps, fill_updated_at};
use crate::sql::p;
use anyhow::Result;
use async_trait::async_trait;
use infra_utils::infra::rdb::{Rdb, RdbPool, UseRdbPool};
use sqlx::Executor;

const INSERT_SQL: &str = concat!(
    "INSERT INTO thread_group_candidate_association \
     (id, subject_thread_id, subject_source, subject_identity_scope_known, \
      subject_identity_scope_value, subject_owner_scope, subject_native_id, \
      candidate_group_id, candidate_parent_thread_id, state, selected_observation_id, \
      created_at, updated_at) \
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
    ")"
);

const FIND_BY_ID_SQL: &str = concat!(
    "SELECT ",
    CANDIDATE_ASSOCIATION_COLUMNS!(),
    " FROM thread_group_candidate_association WHERE id = ",
    p!(1)
);

const LIST_BY_STATE_SQL: &str = concat!(
    "SELECT ",
    CANDIDATE_ASSOCIATION_COLUMNS!(),
    " FROM thread_group_candidate_association WHERE state = ",
    p!(1),
    " ORDER BY updated_at, id"
);

const LIST_BY_SUBJECT_SQL: &str = concat!(
    "SELECT ",
    CANDIDATE_ASSOCIATION_COLUMNS!(),
    " FROM thread_group_candidate_association WHERE \
      subject_owner_scope = ",
    p!(1),
    " AND subject_source = ",
    p!(2),
    " AND subject_identity_scope_known = ",
    p!(3),
    " AND subject_identity_scope_value = ",
    p!(4),
    " AND subject_native_id = ",
    p!(5),
    " ORDER BY updated_at, id"
);

const LIST_BY_CANDIDATE_GROUP_SQL: &str = concat!(
    "SELECT ",
    CANDIDATE_ASSOCIATION_COLUMNS!(),
    " FROM thread_group_candidate_association WHERE candidate_group_id = ",
    p!(1),
    " ORDER BY updated_at, id"
);

const UPDATE_SELECTION_SQL: &str = concat!(
    "UPDATE thread_group_candidate_association \
     SET state = ",
    p!(1),
    ", selected_observation_id = ",
    p!(2),
    ", updated_at = ",
    p!(3),
    " WHERE id = ",
    p!(4),
    " AND state = ",
    p!(5)
);

const RECONNECT_SUBJECT_THREAD_SQL: &str = concat!(
    "UPDATE thread_group_candidate_association SET subject_thread_id = ",
    p!(1),
    ", updated_at = ",
    p!(2),
    " WHERE subject_owner_scope = ",
    p!(3),
    " AND subject_source = ",
    p!(4),
    " AND subject_identity_scope_value = ",
    p!(5),
    " AND subject_native_id = ",
    p!(6),
    " AND subject_thread_id IS NULL"
);

pub struct CandidateSubjectIdentity<'a> {
    pub owner_scope: &'a str,
    pub source: &'a str,
    pub identity_scope_value: &'a str,
    pub native_id: &'a str,
}

#[async_trait]
pub trait ThreadGroupCandidateAssociationRepository:
    UseRdbPool + UseIdGenerator + Send + Sync
{
    async fn insert_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        association: &NewThreadGroupCandidateAssociation,
    ) -> Result<i64> {
        let id = self.id_generator().generate_id()?;
        let (created_at, updated_at) =
            fill_timestamps(association.created_at, association.updated_at);
        sqlx::query::<Rdb>(INSERT_SQL)
            .bind(id)
            .bind(association.subject_thread_id)
            .bind(&association.subject_source)
            .bind(association.subject_identity_scope_known)
            .bind(&association.subject_identity_scope_value)
            .bind(&association.subject_owner_scope)
            .bind(&association.subject_native_id)
            .bind(association.candidate_group_id)
            .bind(association.candidate_parent_thread_id)
            .bind(&association.state)
            .bind(association.selected_observation_id)
            .bind(created_at)
            .bind(updated_at)
            .execute(tx)
            .await
            .map_err(LlmMemoryError::DBError)?;
        Ok(id)
    }

    async fn find_by_id(&self, id: i64) -> Result<Option<ThreadGroupCandidateAssociationRow>> {
        Ok(
            sqlx::query_as::<Rdb, ThreadGroupCandidateAssociationRow>(FIND_BY_ID_SQL)
                .bind(id)
                .fetch_optional(self.db_pool())
                .await
                .map_err(LlmMemoryError::DBError)?,
        )
    }

    async fn find_by_id_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        id: i64,
    ) -> Result<Option<ThreadGroupCandidateAssociationRow>> {
        Ok(
            sqlx::query_as::<Rdb, ThreadGroupCandidateAssociationRow>(FIND_BY_ID_SQL)
                .bind(id)
                .fetch_optional(tx)
                .await
                .map_err(LlmMemoryError::DBError)?,
        )
    }

    /// Unresolved-candidate work queue (`pending` / `ambiguous` /
    /// `conflict` / … scans), most recently touched first.
    async fn list_by_state(
        &self,
        state: &str,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> Result<Vec<ThreadGroupCandidateAssociationRow>> {
        let mut sql = String::from(LIST_BY_STATE_SQL);
        let mut next = 2usize;
        if limit.is_some() {
            sql.push_str(&format!(" LIMIT {}", crate::sql::dyn_placeholder(next)));
            next += 1;
        }
        if offset.is_some() {
            sql.push_str(&format!(" OFFSET {}", crate::sql::dyn_placeholder(next)));
        }
        let mut query =
            sqlx::query_as::<Rdb, ThreadGroupCandidateAssociationRow>(sqlx::AssertSqlSafe(sql))
                .bind(state);
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

    /// Candidate rows recorded for one owner-local subject identity —
    /// the app-layer idempotency inspection point (no storage-level
    /// uniqueness by design).
    async fn list_by_subject_identity_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        subject_owner_scope: &str,
        subject_source: &str,
        subject_identity_scope_known: bool,
        subject_identity_scope_value: &str,
        subject_native_id: &str,
    ) -> Result<Vec<ThreadGroupCandidateAssociationRow>> {
        Ok(
            sqlx::query_as::<Rdb, ThreadGroupCandidateAssociationRow>(LIST_BY_SUBJECT_SQL)
                .bind(subject_owner_scope)
                .bind(subject_source)
                .bind(subject_identity_scope_known)
                .bind(subject_identity_scope_value)
                .bind(subject_native_id)
                .fetch_all(tx)
                .await
                .map_err(LlmMemoryError::DBError)?,
        )
    }

    async fn list_by_subject_identity(
        &self,
        subject_owner_scope: &str,
        subject_source: &str,
        subject_identity_scope_known: bool,
        subject_identity_scope_value: &str,
        subject_native_id: &str,
    ) -> Result<Vec<ThreadGroupCandidateAssociationRow>> {
        Ok(
            sqlx::query_as::<Rdb, ThreadGroupCandidateAssociationRow>(LIST_BY_SUBJECT_SQL)
                .bind(subject_owner_scope)
                .bind(subject_source)
                .bind(subject_identity_scope_known)
                .bind(subject_identity_scope_value)
                .bind(subject_native_id)
                .fetch_all(self.db_pool())
                .await
                .map_err(LlmMemoryError::DBError)?,
        )
    }

    /// Candidates still pointing at a group id (e.g. unresolved-group
    /// re-evaluation after a merge / split).
    async fn list_by_candidate_group_id(
        &self,
        candidate_group_id: i64,
    ) -> Result<Vec<ThreadGroupCandidateAssociationRow>> {
        Ok(
            sqlx::query_as::<Rdb, ThreadGroupCandidateAssociationRow>(LIST_BY_CANDIDATE_GROUP_SQL)
                .bind(candidate_group_id)
                .fetch_all(self.db_pool())
                .await
                .map_err(LlmMemoryError::DBError)?,
        )
    }

    /// Resolve / re-select: set the new state and (optionally) the
    /// selected observation, pinned to the expected current state.
    async fn update_selection_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        id: i64,
        expected_state: &str,
        new_state: &str,
        selected_observation_id: Option<i64>,
        updated_at: i64,
    ) -> Result<bool> {
        let updated_at = fill_updated_at(updated_at);
        let res = sqlx::query::<Rdb>(UPDATE_SELECTION_SQL)
            .bind(new_state)
            .bind(selected_observation_id)
            .bind(updated_at)
            .bind(id)
            .bind(expected_state)
            .execute(tx)
            .await
            .map_err(LlmMemoryError::DBError)?;
        Ok(res.rows_affected() > 0)
    }

    /// Import / revival reconnection: attach a freshly created thread
    /// row to unresolved candidates for the same owner-local identity.
    /// Returns the number of rows reconnected.
    async fn reconnect_subject_thread_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        subject: CandidateSubjectIdentity<'_>,
        thread_id: i64,
        updated_at: i64,
    ) -> Result<u64> {
        let updated_at = fill_updated_at(updated_at);
        let res = sqlx::query::<Rdb>(RECONNECT_SUBJECT_THREAD_SQL)
            .bind(thread_id)
            .bind(updated_at)
            .bind(subject.owner_scope)
            .bind(subject.source)
            .bind(subject.identity_scope_value)
            .bind(subject.native_id)
            .execute(tx)
            .await
            .map_err(LlmMemoryError::DBError)?;
        Ok(res.rows_affected())
    }
}

pub struct ThreadGroupCandidateAssociationRepositoryImpl {
    pool: &'static RdbPool,
    id_generator: IdGeneratorWrapper,
}

impl ThreadGroupCandidateAssociationRepositoryImpl {
    pub fn new(id_generator: IdGeneratorWrapper, pool: &'static RdbPool) -> Self {
        Self { pool, id_generator }
    }
}

impl UseRdbPool for ThreadGroupCandidateAssociationRepositoryImpl {
    fn db_pool(&self) -> &RdbPool {
        self.pool
    }
}

impl UseIdGenerator for ThreadGroupCandidateAssociationRepositoryImpl {
    fn id_generator(&self) -> &IdGeneratorWrapper {
        &self.id_generator
    }
}

impl ThreadGroupCandidateAssociationRepository for ThreadGroupCandidateAssociationRepositoryImpl {}

#[cfg(all(test, not(feature = "postgres")))]
mod tests {
    use super::*;
    use crate::infra::thread_group::rows::values;
    use crate::infra::thread_group::test_support::*;
    use infra_utils::infra::test::TEST_RUNTIME;

    async fn _test_candidate_flow(pool: &'static RdbPool) -> Result<()> {
        let repo = ThreadGroupCandidateAssociationRepositoryImpl::new(
            crate::test_helper::shared_id_generator(),
            pool,
        );

        let assoc_id = repo.insert_tx(pool, &new_candidate(1)).await?;
        let row = repo.find_by_id(assoc_id).await?.expect("candidate row");
        assert_eq!(row.state, values::candidate_state::PENDING);
        assert!(row.subject_thread_id.is_none());

        // Subject listing is the idempotency inspection point (no
        // storage-level UNIQUE on this table by design).
        let listed = repo
            .list_by_subject_identity("user:1", "codex", true, "", "subject-1")
            .await?;
        assert_eq!(listed.len(), 1);

        // Promotion keeps the old state as a race guard.
        let mut tx = pool.begin().await?;
        assert!(
            !repo
                .update_selection_tx(
                    &mut *tx,
                    assoc_id,
                    values::candidate_state::CONFLICT,
                    values::candidate_state::CANDIDATE,
                    Some(555),
                    T0 + 1
                )
                .await?
        );
        assert!(
            repo.update_selection_tx(
                &mut *tx,
                assoc_id,
                values::candidate_state::PENDING,
                values::candidate_state::CANDIDATE,
                Some(555),
                T0 + 1
            )
            .await?
        );
        // Reconnect binds the resolved thread row to unresolved
        // candidates for the same owner-local identity.
        assert_eq!(
            repo.reconnect_subject_thread_tx(
                &mut *tx,
                CandidateSubjectIdentity {
                    owner_scope: "user:1",
                    source: "codex",
                    identity_scope_value: "",
                    native_id: "subject-1",
                },
                1234,
                T0 + 2
            )
            .await?,
            1
        );
        tx.commit().await?;

        let row = repo.find_by_id_tx(pool, assoc_id).await?.expect("updated");
        assert_eq!(row.state, values::candidate_state::CANDIDATE);
        assert_eq!(row.selected_observation_id, Some(555));
        assert_eq!(row.subject_thread_id, Some(1234));

        // State queue scan sees the updated row only in its new state.
        assert!(
            repo.list_by_state(values::candidate_state::PENDING, Some(10), None)
                .await?
                .iter()
                .all(|r| r.id != assoc_id)
        );
        assert!(
            repo.list_by_state(values::candidate_state::CANDIDATE, Some(10), None)
                .await?
                .iter()
                .any(|r| r.id == assoc_id)
        );
        Ok(())
    }

    #[test]
    fn candidate_flow_sqlite() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let pool = setup_thread_group_pool().await;
            _test_candidate_flow(pool).await
        })
    }
}
