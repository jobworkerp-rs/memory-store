//! `thread_observation` repository (design 5.4).
//!
//! Evidence rows are idempotent by their full UNIQUE key: subject
//! identity (with the known / unknown identity-scope distinction),
//! candidate-parent identity + presence, evidence kind, and evidence
//! fingerprint. Mutable `state` is excluded from the key, so a
//! reconciliation pass re-records identical evidence without
//! multiplying rows — `find_by_identity` is the lookup that decides
//! "already recorded".
//!
//! Presence semantics live entirely in caller-supplied columns
//! (`*_identity_scope_known`, `candidate_parent_present` plus `''`
//! value defaults, per design 5.4); this module never derives one
//! representation from another.

use super::rows::{
    NewThreadObservation, ObservationIdentity, THREAD_OBSERVATION_COLUMNS, ThreadObservationRow,
};
use crate::error::LlmMemoryError;
use crate::infra::{IdGeneratorWrapper, UseIdGenerator, fill_timestamps, fill_updated_at};
use crate::sql::p;
use anyhow::Result;
use async_trait::async_trait;
use infra_utils::infra::rdb::{Rdb, RdbPool, UseRdbPool};
use sqlx::Executor;

const INSERT_SQL: &str = concat!(
    "INSERT INTO thread_observation \
     (id, subject_source, subject_identity_scope_known, subject_identity_scope_value, \
      subject_user_id, subject_owner_scope, subject_native_id, \
      candidate_parent_present, candidate_parent_source, \
      candidate_parent_identity_scope_known, candidate_parent_identity_scope_value, \
      candidate_parent_user_id, candidate_parent_owner_scope, candidate_parent_native_id, \
      relation_kind, origin, evidence_kind, polarity, source_confidence, \
      evidence_fingerprint, source_record_ref, state, import_run_id, \
      observed_at, created_at, updated_at) \
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
    ",",
    p!(24),
    ",",
    p!(25),
    ",",
    p!(26),
    ")"
);

// The full UNIQUE key (identity + presence states + kind + fingerprint),
// 13 bind positions matching `ObservationIdentity` exactly.
const FIND_BY_IDENTITY_SQL: &str = concat!(
    "SELECT ",
    THREAD_OBSERVATION_COLUMNS!(),
    " FROM thread_observation WHERE \
      subject_source = ",
    p!(1),
    " AND subject_identity_scope_known = ",
    p!(2),
    " AND subject_identity_scope_value = ",
    p!(3),
    " AND subject_user_id = ",
    p!(4),
    " AND subject_native_id = ",
    p!(5),
    " AND candidate_parent_present = ",
    p!(6),
    " AND candidate_parent_source = ",
    p!(7),
    " AND candidate_parent_identity_scope_known = ",
    p!(8),
    " AND candidate_parent_identity_scope_value = ",
    p!(9),
    " AND candidate_parent_user_id IS NOT DISTINCT FROM ",
    p!(10),
    " AND candidate_parent_native_id = ",
    p!(11),
    " AND evidence_kind = ",
    p!(12),
    " AND evidence_fingerprint = ",
    p!(13)
);

const FIND_BY_ID_SQL: &str = concat!(
    "SELECT ",
    THREAD_OBSERVATION_COLUMNS!(),
    " FROM thread_observation WHERE id = ",
    p!(1)
);

const LIST_BY_STATE_SQL: &str = concat!(
    "SELECT ",
    THREAD_OBSERVATION_COLUMNS!(),
    " FROM thread_observation WHERE state = ",
    p!(1),
    " ORDER BY observed_at, id"
);

// Subject-side listing: owner-local by construction (subject_user_id
// is part of every bind), state filter optional.
const LIST_BY_SUBJECT_SQL: &str = concat!(
    "SELECT ",
    THREAD_OBSERVATION_COLUMNS!(),
    " FROM thread_observation WHERE \
      subject_user_id = ",
    p!(1),
    " AND subject_source = ",
    p!(2),
    " AND subject_identity_scope_known = ",
    p!(3),
    " AND subject_identity_scope_value = ",
    p!(4),
    " AND subject_native_id = ",
    p!(5),
    " ORDER BY observed_at, id"
);

const LIST_PENDING_BY_CANDIDATE_PARENT_SQL: &str = concat!(
    "SELECT ",
    THREAD_OBSERVATION_COLUMNS!(),
    " FROM thread_observation WHERE candidate_parent_present = TRUE \
      AND candidate_parent_identity_scope_known = TRUE \
      AND candidate_parent_user_id = ",
    p!(1),
    " AND candidate_parent_source = ",
    p!(2),
    " AND candidate_parent_identity_scope_value = ",
    p!(3),
    " AND candidate_parent_native_id = ",
    p!(4),
    " AND EXISTS (SELECT 1 FROM thread_group_candidate_association association \
                  WHERE association.selected_observation_id = thread_observation.id \
                    AND association.state = 'pending') \
      ORDER BY observed_at, id"
);

const SET_STATE_SQL: &str = concat!(
    "UPDATE thread_observation SET state = ",
    p!(1),
    ", updated_at = ",
    p!(2),
    " WHERE id = ",
    p!(3),
    " AND state = ",
    p!(4)
);

const DELETE_BY_ID_SQL: &str = concat!("DELETE FROM thread_observation WHERE id = ", p!(1));

#[async_trait]
pub trait ThreadObservationRepository: UseRdbPool + UseIdGenerator + Send + Sync {
    /// Record one evidence row with a generated id. A UNIQUE collision
    /// on the identity / evidence key surfaces as a DB error (PostgreSQL
    /// aborts the transaction); inside a write transaction callers
    /// pre-check with `find_by_identity_tx` and treat a row-count race as
    /// a serializable-retry signal (design 5.3).
    async fn insert_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        observation: &NewThreadObservation,
    ) -> Result<i64> {
        let id = self.id_generator().generate_id()?;
        let (created_at, updated_at) =
            fill_timestamps(observation.created_at, observation.updated_at);
        sqlx::query::<Rdb>(INSERT_SQL)
            .bind(id)
            .bind(&observation.subject_source)
            .bind(observation.subject_identity_scope_known)
            .bind(&observation.subject_identity_scope_value)
            .bind(observation.subject_user_id)
            .bind(common::thread_group_key::legacy_owner_scope(
                observation.subject_user_id,
            ))
            .bind(&observation.subject_native_id)
            .bind(observation.candidate_parent_present)
            .bind(&observation.candidate_parent_source)
            .bind(observation.candidate_parent_identity_scope_known)
            .bind(&observation.candidate_parent_identity_scope_value)
            .bind(observation.candidate_parent_user_id)
            .bind(
                observation
                    .candidate_parent_user_id
                    .map_or_else(String::new, common::thread_group_key::legacy_owner_scope),
            )
            .bind(&observation.candidate_parent_native_id)
            .bind(&observation.relation_kind)
            .bind(&observation.origin)
            .bind(&observation.evidence_kind)
            .bind(&observation.polarity)
            .bind(&observation.source_confidence)
            .bind(&observation.evidence_fingerprint)
            .bind(&observation.source_record_ref)
            .bind(&observation.state)
            .bind(&observation.import_run_id)
            .bind(observation.observed_at)
            .bind(created_at)
            .bind(updated_at)
            .execute(tx)
            .await
            .map_err(LlmMemoryError::DBError)?;
        Ok(id)
    }

    async fn find_by_id(&self, id: i64) -> Result<Option<ThreadObservationRow>> {
        Ok(sqlx::query_as::<Rdb, ThreadObservationRow>(FIND_BY_ID_SQL)
            .bind(id)
            .fetch_optional(self.db_pool())
            .await
            .map_err(LlmMemoryError::DBError)?)
    }

    /// Idempotent lookup over the full evidence UNIQUE key.
    async fn find_by_identity(
        &self,
        identity: &ObservationIdentity<'_>,
    ) -> Result<Option<ThreadObservationRow>> {
        Ok(
            sqlx::query_as::<Rdb, ThreadObservationRow>(FIND_BY_IDENTITY_SQL)
                .bind(identity.subject_source)
                .bind(identity.subject_identity_scope_known)
                .bind(identity.subject_identity_scope_value)
                .bind(identity.subject_user_id)
                .bind(identity.subject_native_id)
                .bind(identity.candidate_parent_present)
                .bind(identity.candidate_parent_source)
                .bind(identity.candidate_parent_identity_scope_known)
                .bind(identity.candidate_parent_identity_scope_value)
                .bind(identity.candidate_parent_user_id)
                .bind(identity.candidate_parent_native_id)
                .bind(identity.evidence_kind)
                .bind(identity.evidence_fingerprint)
                .fetch_optional(self.db_pool())
                .await
                .map_err(LlmMemoryError::DBError)?,
        )
    }

    async fn find_by_identity_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        identity: &ObservationIdentity<'_>,
    ) -> Result<Option<ThreadObservationRow>> {
        Ok(
            sqlx::query_as::<Rdb, ThreadObservationRow>(FIND_BY_IDENTITY_SQL)
                .bind(identity.subject_source)
                .bind(identity.subject_identity_scope_known)
                .bind(identity.subject_identity_scope_value)
                .bind(identity.subject_user_id)
                .bind(identity.subject_native_id)
                .bind(identity.candidate_parent_present)
                .bind(identity.candidate_parent_source)
                .bind(identity.candidate_parent_identity_scope_known)
                .bind(identity.candidate_parent_identity_scope_value)
                .bind(identity.candidate_parent_user_id)
                .bind(identity.candidate_parent_native_id)
                .bind(identity.evidence_kind)
                .bind(identity.evidence_fingerprint)
                .fetch_optional(tx)
                .await
                .map_err(LlmMemoryError::DBError)?,
        )
    }

    /// Pool-level retrieve-or-create on the evidence UNIQUE key: the
    /// importer's idempotency entry point for callers that do not run a
    /// transaction of their own. Returns `(row, newly_created)`.
    ///
    /// The collision fallback runs in a *fresh* read because a failed
    /// INSERT aborts the surrounding transaction on PostgreSQL — never
    /// call this from inside a write transaction; use
    /// `find_by_identity_tx` + `insert_tx` there.
    async fn insert_or_find(
        &self,
        observation: &NewThreadObservation,
    ) -> Result<(ThreadObservationRow, bool)> {
        let mut tx = self
            .db_pool()
            .begin()
            .await
            .map_err(LlmMemoryError::DBError)?;
        let insert_result = self.insert_tx(&mut *tx, observation).await;
        match insert_result {
            Ok(id) => {
                tx.commit().await.map_err(LlmMemoryError::DBError)?;
                let row = self.find_by_id(id).await?.ok_or_else(|| {
                    LlmMemoryError::RuntimeError(format!(
                        "observation {id} vanished right after insert"
                    ))
                })?;
                Ok((row, true))
            }
            Err(insert_err) => {
                drop(tx);
                if !is_unique_violation(&insert_err) {
                    return Err(insert_err);
                }
                let identity = ObservationIdentity {
                    subject_source: &observation.subject_source,
                    subject_identity_scope_known: observation.subject_identity_scope_known,
                    subject_identity_scope_value: &observation.subject_identity_scope_value,
                    subject_user_id: observation.subject_user_id,
                    subject_native_id: &observation.subject_native_id,
                    candidate_parent_present: observation.candidate_parent_present,
                    candidate_parent_source: &observation.candidate_parent_source,
                    candidate_parent_identity_scope_known: observation
                        .candidate_parent_identity_scope_known,
                    candidate_parent_identity_scope_value: &observation
                        .candidate_parent_identity_scope_value,
                    candidate_parent_user_id: observation.candidate_parent_user_id,
                    candidate_parent_native_id: &observation.candidate_parent_native_id,
                    evidence_kind: &observation.evidence_kind,
                    evidence_fingerprint: &observation.evidence_fingerprint,
                };
                match self.find_by_identity(&identity).await? {
                    Some(row) => Ok((row, false)),
                    // The winner deleted / changed the row between our
                    // failed insert and the fallback read: surface the
                    // original collision so the caller can retry.
                    None => Err(insert_err),
                }
            }
        }
    }

    /// Reconciliation work-queue scan, oldest evidence first.
    async fn list_by_state(
        &self,
        state: &str,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> Result<Vec<ThreadObservationRow>> {
        list_by_state_query(self.db_pool(), state, limit, offset).await
    }

    /// All observations recorded for one owner-local subject identity
    /// (any candidate parent / evidence kind). `identity_scope_known` +
    /// `identity_scope_value` distinguish `known("")` from `unknown`
    /// exactly as in the UNIQUE key.
    async fn list_by_subject(
        &self,
        subject_user_id: i64,
        subject_source: &str,
        subject_identity_scope_known: bool,
        subject_identity_scope_value: &str,
        subject_native_id: &str,
    ) -> Result<Vec<ThreadObservationRow>> {
        Ok(
            sqlx::query_as::<Rdb, ThreadObservationRow>(LIST_BY_SUBJECT_SQL)
                .bind(subject_user_id)
                .bind(subject_source)
                .bind(subject_identity_scope_known)
                .bind(subject_identity_scope_value)
                .bind(subject_native_id)
                .fetch_all(self.db_pool())
                .await
                .map_err(LlmMemoryError::DBError)?,
        )
    }

    async fn list_by_subject_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        subject_user_id: i64,
        subject_source: &str,
        subject_identity_scope_known: bool,
        subject_identity_scope_value: &str,
        subject_native_id: &str,
    ) -> Result<Vec<ThreadObservationRow>> {
        Ok(
            sqlx::query_as::<Rdb, ThreadObservationRow>(LIST_BY_SUBJECT_SQL)
                .bind(subject_user_id)
                .bind(subject_source)
                .bind(subject_identity_scope_known)
                .bind(subject_identity_scope_value)
                .bind(subject_native_id)
                .fetch_all(tx)
                .await
                .map_err(LlmMemoryError::DBError)?,
        )
    }

    /// Pending associations use the observation's candidate-parent identity
    /// as their lookup key, so discovery only reads evidence for this exact
    /// owner-local parent instead of loading the global pending queue.
    async fn list_pending_by_candidate_parent_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        parent_user_id: i64,
        parent_source: &str,
        parent_identity_scope_value: &str,
        parent_native_id: &str,
    ) -> Result<Vec<ThreadObservationRow>> {
        Ok(
            sqlx::query_as::<Rdb, ThreadObservationRow>(LIST_PENDING_BY_CANDIDATE_PARENT_SQL)
                .bind(parent_user_id)
                .bind(parent_source)
                .bind(parent_identity_scope_value)
                .bind(parent_native_id)
                .fetch_all(tx)
                .await
                .map_err(LlmMemoryError::DBError)?,
        )
    }

    /// State transition pinned to the expected current state
    /// (`pending → candidate → selected / conflict / superseded /
    /// unsupported`). Returns false on state mismatch so racing
    /// reconciliations observe the loss instead of clobbering.
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

    /// Physical delete for inactive-history purge. Only observations no
    /// active relation selects should be passed here.
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

/// True when the (anyhow-wrapped) error is a UNIQUE violation surfaced
/// by the DB driver. Errors produced by this crate's repositories reach
/// the caller as `anyhow::Error` around `LlmMemoryError::DBError`, so
/// the detection has to peel both layers.
pub(crate) fn is_unique_violation(err: &anyhow::Error) -> bool {
    match err.downcast_ref::<LlmMemoryError>() {
        Some(LlmMemoryError::DBError(sqlx::Error::Database(db))) => {
            db.kind() == sqlx::error::ErrorKind::UniqueViolation
        }
        _ => false,
    }
}

async fn list_by_state_query(
    pool: &RdbPool,
    state: &str,
    limit: Option<i64>,
    offset: Option<i64>,
) -> Result<Vec<ThreadObservationRow>> {
    use crate::sql::dyn_placeholder;
    let mut sql = String::from(LIST_BY_STATE_SQL);
    let mut next = 2usize;
    if limit.is_some() {
        sql.push_str(&format!(" LIMIT {}", dyn_placeholder(next)));
        next += 1;
    }
    if offset.is_some() {
        sql.push_str(&format!(" OFFSET {}", dyn_placeholder(next)));
    }
    let mut query =
        sqlx::query_as::<Rdb, ThreadObservationRow>(sqlx::AssertSqlSafe(sql)).bind(state);
    if let Some(limit) = limit {
        query = query.bind(limit);
    }
    if let Some(offset) = offset {
        query = query.bind(offset);
    }
    Ok(query
        .fetch_all(pool)
        .await
        .map_err(LlmMemoryError::DBError)?)
}

pub struct ThreadObservationRepositoryImpl {
    pool: &'static RdbPool,
    id_generator: IdGeneratorWrapper,
}

impl ThreadObservationRepositoryImpl {
    pub fn new(id_generator: IdGeneratorWrapper, pool: &'static RdbPool) -> Self {
        Self { pool, id_generator }
    }
}

impl UseRdbPool for ThreadObservationRepositoryImpl {
    fn db_pool(&self) -> &RdbPool {
        self.pool
    }
}

impl UseIdGenerator for ThreadObservationRepositoryImpl {
    fn id_generator(&self) -> &IdGeneratorWrapper {
        &self.id_generator
    }
}

impl ThreadObservationRepository for ThreadObservationRepositoryImpl {}

#[cfg(all(test, not(feature = "postgres")))]
mod tests {
    use super::*;
    use crate::infra::thread_group::rows::values;
    use crate::infra::thread_group::test_support::*;
    use anyhow::Context;
    use infra_utils::infra::test::TEST_RUNTIME;

    async fn _test_observation_idempotency(pool: &'static RdbPool) -> Result<()> {
        let repo =
            ThreadObservationRepositoryImpl::new(crate::test_helper::shared_id_generator(), pool);

        let params = new_observation(1);
        let id = repo.insert_tx(pool, &params).await?;
        let found = repo
            .find_by_identity(&observation_identity(&params))
            .await?
            .context("identity lookup")?;
        assert_eq!(found.id, id);

        // Same identity + fingerprint is deduplicated by the composite
        // UNIQUE key: retrieve-or-create returns the original row.
        let (again, created) = repo.insert_or_find(&params).await?;
        assert!(!created);
        assert_eq!(again.id, id);

        // Different mutable state is NOT part of the key; different
        // evidence kind, fingerprint, or identity-scope known-state IS.
        let mut other_kind = params.clone();
        other_kind.evidence_kind = values::evidence_kind::SOURCE_EVENT.to_string();
        let (row, created) = repo.insert_or_find(&other_kind).await?;
        assert!(created);
        assert_ne!(row.id, id);

        let mut other_scope_state = params.clone();
        other_scope_state.subject_identity_scope_known = false;
        let (_, created) = repo.insert_or_find(&other_scope_state).await?;
        assert!(created, "known('') and unknown are distinct identities");

        let mut absent_parent = params.clone();
        absent_parent.candidate_parent_present = false;
        absent_parent.candidate_parent_source = String::new();
        absent_parent.candidate_parent_identity_scope_known = false;
        absent_parent.candidate_parent_identity_scope_value = String::new();
        absent_parent.candidate_parent_user_id = None;
        absent_parent.candidate_parent_native_id = String::new();
        let (_, created) = repo.insert_or_find(&absent_parent).await?;
        assert!(created, "no-parent is a presence state, not a value");

        // Adapter-origin rows must carry source evidence (origin CHECK).
        let mut derived = new_observation(2);
        derived.origin = values::observation_origin::RECONCILER.to_string();
        assert!(repo.insert_tx(pool, &derived).await.is_err());
        derived.evidence_kind = values::evidence_kind::DERIVED_REJECTION.to_string();
        derived.source_confidence = None;
        let derived_id = repo.insert_tx(pool, &derived).await?;

        // State transitions pinned to the expected current state.
        let mut tx = pool.begin().await?;
        assert!(
            repo.set_state_tx(
                &mut *tx,
                derived_id,
                values::observation_state::PENDING,
                values::observation_state::CANDIDATE,
                T0 + 1
            )
            .await?
        );
        assert!(
            !repo
                .set_state_tx(
                    &mut *tx,
                    derived_id,
                    values::observation_state::PENDING,
                    values::observation_state::SELECTED,
                    T0 + 2
                )
                .await?
        );
        tx.commit().await?;

        // Subject-side listing and the state work-queue scan.
        let subject_rows = repo
            .list_by_subject(1, "codex", true, "", "subject-1")
            .await?;
        assert!(subject_rows.iter().any(|r| r.id == id));
        assert!(subject_rows.iter().any(|r| r.id == again.id));
        let candidates = repo
            .list_by_state(values::observation_state::CANDIDATE, Some(10), Some(0))
            .await?;
        assert!(candidates.iter().any(|r| r.id == derived_id));
        Ok(())
    }

    #[test]
    fn observation_idempotency_sqlite() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let pool = setup_thread_group_pool().await;
            _test_observation_idempotency(pool).await
        })
    }
}
