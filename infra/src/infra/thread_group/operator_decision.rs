//! `operator_decision` repository (design 5.8).
//!
//! Append-only operator audit: decisions are never updated or deleted
//! here, and each row pins the association it resolved, the actor, the
//! input evidence fingerprint, and the policy version. Selection
//! evaluation order (source_exact → operator confirmation → policy-
//! allowed source_strong) is app-layer logic; the relation's
//! `selected_operator_decision_id` provides the reference side.

use super::rows::{NewOperatorDecision, OPERATOR_DECISION_COLUMNS, OperatorDecisionRow};
use crate::error::LlmMemoryError;
use crate::infra::{IdGeneratorWrapper, UseIdGenerator, fill_timestamps};
use crate::sql::p;
use anyhow::Result;
use async_trait::async_trait;
use infra_utils::infra::rdb::{Rdb, RdbPool, UseRdbPool};
use sqlx::Executor;

const INSERT_SQL: &str = concat!(
    "INSERT INTO operator_decision \
     (id, user_id, owner_scope, candidate_association_id, actor_id, decision, reason, \
      input_evidence_fingerprint, policy_version, created_at) \
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
    ")"
);

const LIST_BY_ASSOCIATION_SQL: &str = concat!(
    "SELECT ",
    OPERATOR_DECISION_COLUMNS!(),
    " FROM operator_decision WHERE candidate_association_id = ",
    p!(1),
    " ORDER BY created_at, id"
);

// `ORDER BY created_at DESC, id DESC LIMIT 1` gives the latest decision
// deterministically even when two rows share a millisecond timestamp.
const FIND_LATEST_BY_ASSOCIATION_SQL: &str = concat!(
    "SELECT ",
    OPERATOR_DECISION_COLUMNS!(),
    " FROM operator_decision WHERE candidate_association_id = ",
    p!(1),
    " ORDER BY created_at DESC, id DESC"
);

#[async_trait]
pub trait OperatorDecisionRepository: UseRdbPool + UseIdGenerator + Send + Sync {
    /// Append one decision row (within the transaction that applies its
    /// effect: relation selection / retraction / supersession).
    async fn insert_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        decision: &NewOperatorDecision,
    ) -> Result<i64> {
        let id = self.id_generator().generate_id()?;
        let (created_at, _) = fill_timestamps(decision.created_at, decision.created_at);
        sqlx::query::<Rdb>(INSERT_SQL)
            .bind(id)
            .bind(decision.user_id)
            .bind(common::thread_group_key::legacy_owner_scope(
                decision.user_id,
            ))
            .bind(decision.candidate_association_id)
            .bind(&decision.actor_id)
            .bind(&decision.decision)
            .bind(&decision.reason)
            .bind(&decision.input_evidence_fingerprint)
            .bind(&decision.policy_version)
            .bind(created_at)
            .execute(tx)
            .await
            .map_err(LlmMemoryError::DBError)?;
        Ok(id)
    }

    /// Full decision history of one association, oldest first.
    async fn list_by_candidate_association_id(
        &self,
        candidate_association_id: i64,
    ) -> Result<Vec<OperatorDecisionRow>> {
        Ok(
            sqlx::query_as::<Rdb, OperatorDecisionRow>(LIST_BY_ASSOCIATION_SQL)
                .bind(candidate_association_id)
                .fetch_all(self.db_pool())
                .await
                .map_err(LlmMemoryError::DBError)?,
        )
    }

    /// Latest decision of one association (`confirm` / `reject` /
    /// `retract` provenance for the current state).
    async fn find_latest_by_candidate_association_id(
        &self,
        candidate_association_id: i64,
    ) -> Result<Option<OperatorDecisionRow>> {
        Ok(
            sqlx::query_as::<Rdb, OperatorDecisionRow>(FIND_LATEST_BY_ASSOCIATION_SQL)
                .bind(candidate_association_id)
                .fetch_optional(self.db_pool())
                .await
                .map_err(LlmMemoryError::DBError)?,
        )
    }
}

pub struct OperatorDecisionRepositoryImpl {
    pool: &'static RdbPool,
    id_generator: IdGeneratorWrapper,
}

impl OperatorDecisionRepositoryImpl {
    pub fn new(id_generator: IdGeneratorWrapper, pool: &'static RdbPool) -> Self {
        Self { pool, id_generator }
    }
}

impl UseRdbPool for OperatorDecisionRepositoryImpl {
    fn db_pool(&self) -> &RdbPool {
        self.pool
    }
}

impl UseIdGenerator for OperatorDecisionRepositoryImpl {
    fn id_generator(&self) -> &IdGeneratorWrapper {
        &self.id_generator
    }
}

impl OperatorDecisionRepository for OperatorDecisionRepositoryImpl {}

#[cfg(all(test, not(feature = "postgres")))]
mod tests {
    use super::*;
    use crate::infra::thread_group::test_support::*;
    use anyhow::Context;
    use infra_utils::infra::test::TEST_RUNTIME;

    async fn _test_decision_history(pool: &'static RdbPool) -> Result<()> {
        let repo =
            OperatorDecisionRepositoryImpl::new(crate::test_helper::shared_id_generator(), pool);
        let association_id = 777_001;

        let mut first = new_operator_decision(association_id);
        first.created_at = T0;
        let d1 = repo.insert_tx(pool, &first).await?;
        let mut second = new_operator_decision(association_id);
        second.created_at = T0 + 5;
        second.decision = "retract".to_string();
        let d2 = repo.insert_tx(pool, &second).await?;

        let history = repo
            .list_by_candidate_association_id(association_id)
            .await?;
        assert_eq!(
            history.iter().map(|r| r.id).collect::<Vec<_>>(),
            vec![d1, d2]
        );
        let latest = repo
            .find_latest_by_candidate_association_id(association_id)
            .await?
            .context("latest decision")?;
        assert_eq!(latest.id, d2);
        assert_eq!(latest.decision, "retract");
        Ok(())
    }

    #[test]
    fn decision_history_sqlite() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let pool = setup_thread_group_pool().await;
            _test_decision_history(pool).await
        })
    }
}
