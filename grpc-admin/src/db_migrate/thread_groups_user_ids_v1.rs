//! `thread-groups-user-ids-v1@1` post-schema data migration.
//!
//! Backfills typed user IDs from the retained canonical `user:<id>` owner
//! scopes. The preflight is application-side so malformed or contradictory
//! values fail before any typed owner column is changed.

use super::{
    DataMigrationTask,
    catalog::TaskCatalogEntry,
    state::{self, TaskLease, TaskStateKind},
};
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use common::thread_group_key::parse_legacy_owner_scope;
use infra_utils::infra::rdb::{RdbPool, RdbTransaction};
use serde::Serialize;
use std::collections::BTreeMap;

const DEFAULT_LEASE_MS: i64 = 120_000;
const TASK_IDENTITY: &str = "thread-groups-user-ids-v1@1";
const TASK_IDENTITY_V2: &str = "thread-groups-user-ids-v1@2";
const AMBIGUOUS_MARKER_KEY_SQL: &str = r#"
    SELECT COUNT(*) FROM (
        SELECT user_id, source, identity_scope, native_id FROM (
            SELECT marker.user_id, marker.source, marker.identity_scope, marker.native_id,
                   member.thread_canonical_key AS resolved_key
            FROM thread_deletion_marker marker
            JOIN thread_group_member member ON member.user_id = marker.user_id
                AND member.source = marker.source AND member.identity_scope = marker.identity_scope
                AND member.native_id = marker.native_id
            WHERE marker.thread_canonical_key IS NULL
            UNION ALL
            SELECT marker.user_id, marker.source, marker.identity_scope, marker.native_id,
                   canonical.key AS resolved_key
            FROM thread_deletion_marker marker
            JOIN source_thread_identity source_identity ON source_identity.user_id = marker.user_id
                AND source_identity.source = marker.source AND source_identity.identity_scope = marker.identity_scope
                AND source_identity.native_id = marker.native_id
            JOIN thread_canonical_key canonical ON canonical.thread_id = source_identity.thread_id
            WHERE marker.thread_canonical_key IS NULL
        ) resolved
        GROUP BY user_id, source, identity_scope, native_id
        HAVING COUNT(DISTINCT resolved_key) > 1
    ) ambiguous
"#;
const CANONICAL_MEMBER_KEY_MISMATCH_SQL: &str = "SELECT COUNT(*) FROM thread_canonical_key canonical \
    JOIN thread_group_member member ON member.thread_id = canonical.thread_id \
        AND member.state = 'active' \
    WHERE member.thread_canonical_key <> canonical.key";

pub(super) const BACKFILL_SQL: &[&str] = &[
    "UPDATE thread_group_member SET user_id = CAST(SUBSTR(owner_scope, 6) AS BIGINT) WHERE user_id IS NULL",
    "UPDATE thread_relation SET parent_user_id = CAST(SUBSTR(parent_owner_scope, 6) AS BIGINT) WHERE parent_user_id IS NULL",
    "UPDATE thread_relation SET child_user_id = CAST(SUBSTR(child_owner_scope, 6) AS BIGINT) WHERE child_user_id IS NULL",
    "UPDATE thread_observation SET subject_user_id = CAST(SUBSTR(subject_owner_scope, 6) AS BIGINT) WHERE subject_user_id IS NULL",
    "UPDATE thread_observation SET candidate_parent_user_id = CAST(SUBSTR(candidate_parent_owner_scope, 6) AS BIGINT) WHERE candidate_parent_present = TRUE AND candidate_parent_user_id IS NULL",
    "UPDATE thread_group_candidate_association SET subject_user_id = CAST(SUBSTR(subject_owner_scope, 6) AS BIGINT) WHERE subject_user_id IS NULL",
    "UPDATE source_thread_identity SET user_id = CAST(SUBSTR(owner_scope, 6) AS BIGINT) WHERE user_id IS NULL",
    "UPDATE thread_canonical_key SET user_id = CAST(SUBSTR(owner_scope, 6) AS BIGINT) WHERE user_id IS NULL",
    "UPDATE thread_deletion_marker SET user_id = CAST(SUBSTR(owner_scope, 6) AS BIGINT) WHERE user_id IS NULL",
    "UPDATE operator_decision SET user_id = CAST(SUBSTR(owner_scope, 6) AS BIGINT) WHERE user_id IS NULL",
    "UPDATE manual_collection SET user_id = CAST(SUBSTR(owner_scope, 6) AS BIGINT) WHERE user_id IS NULL",
    "UPDATE manual_collection_member SET user_id = CAST(SUBSTR(owner_scope, 6) AS BIGINT) WHERE user_id IS NULL",
    "UPDATE thread_group_event_outbox SET user_id = CAST(SUBSTR(owner_scope, 6) AS BIGINT) WHERE owner_scope IS NOT NULL AND user_id IS NULL",
    "UPDATE thread_deletion_marker SET thread_canonical_key = (SELECT MIN(k.key) FROM source_thread_identity s JOIN thread_canonical_key k ON k.thread_id = s.thread_id WHERE s.user_id = thread_deletion_marker.user_id AND s.source = thread_deletion_marker.source AND s.identity_scope = thread_deletion_marker.identity_scope AND s.native_id = thread_deletion_marker.native_id) WHERE thread_canonical_key IS NULL AND (SELECT COUNT(DISTINCT k.key) FROM source_thread_identity s JOIN thread_canonical_key k ON k.thread_id = s.thread_id WHERE s.user_id = thread_deletion_marker.user_id AND s.source = thread_deletion_marker.source AND s.identity_scope = thread_deletion_marker.identity_scope AND s.native_id = thread_deletion_marker.native_id) = 1",
    "UPDATE thread_deletion_marker SET thread_canonical_key = (SELECT MIN(m.thread_canonical_key) FROM thread_group_member m WHERE m.user_id = thread_deletion_marker.user_id AND m.source = thread_deletion_marker.source AND m.identity_scope = thread_deletion_marker.identity_scope AND m.native_id = thread_deletion_marker.native_id) WHERE thread_canonical_key IS NULL AND (SELECT COUNT(DISTINCT m.thread_canonical_key) FROM thread_group_member m WHERE m.user_id = thread_deletion_marker.user_id AND m.source = thread_deletion_marker.source AND m.identity_scope = thread_deletion_marker.identity_scope AND m.native_id = thread_deletion_marker.native_id) = 1",
];
pub(super) const TYPED_ID_BACKFILL_COUNT: usize = 13;

#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub struct InspectResult {
    /// Number of rows whose typed ID is missing, keyed by table and column.
    pub pending_fields: BTreeMap<String, u64>,
    pub task_state: Option<String>,
}

impl InspectResult {
    fn pending_count(&self) -> u64 {
        self.pending_fields.values().sum()
    }
}

pub struct ThreadGroupsUserIdsV1Task {
    pool: RdbPool,
    catalog: TaskCatalogEntry,
    lease_duration_ms: i64,
}

impl ThreadGroupsUserIdsV1Task {
    pub fn new(pool: RdbPool, catalog: TaskCatalogEntry) -> Result<Self> {
        catalog.validate()?;
        if catalog.identity() != TASK_IDENTITY && catalog.identity() != TASK_IDENTITY_V2 {
            bail!("unexpected task catalog identity: {}", catalog.identity());
        }
        Ok(Self {
            pool,
            catalog,
            lease_duration_ms: DEFAULT_LEASE_MS,
        })
    }

    pub async fn inspect(&self) -> Result<InspectResult> {
        let mut tx = self
            .pool
            .begin()
            .await
            .context("beginning owner ID inspection")?;
        let mut result = inspect_tx(&mut tx, self.uses_thread_owner_preflight(), true).await?;
        tx.rollback().await.context("ending owner ID inspection")?;
        result.task_state = state::load(&self.pool, &self.catalog.identity())
            .await?
            .map(|row| row.state);
        Ok(result)
    }

    pub async fn dry_run(&self) -> Result<InspectResult> {
        self.inspect().await
    }

    pub async fn apply(&self, execution_id: &str, holder_id: &str) -> Result<InspectResult> {
        if let Some(existing) = state::load(&self.pool, &self.catalog.identity()).await?
            && existing.kind()? == TaskStateKind::Completed
        {
            self.verify().await?;
            let mut result = self.inspect().await?;
            result.task_state = Some("completed".to_string());
            return Ok(result);
        }

        let now = command_utils::util::datetime::now_millis();
        let lease = state::claim(
            &self.pool,
            &self.catalog.identity(),
            &self.catalog.canonical_definition_digest,
            execution_id,
            holder_id,
            now,
            self.lease_duration_ms,
        )
        .await?;
        let run = self.apply_with_lease(&lease).await;
        match run {
            Ok(mut result) => {
                state::complete(
                    &self.pool,
                    &lease,
                    command_utils::util::datetime::now_millis(),
                )
                .await?;
                result.task_state = Some("completed".to_string());
                Ok(result)
            }
            Err(error) => {
                let _ = state::fail(
                    &self.pool,
                    &lease,
                    "owner_id_backfill_failed",
                    command_utils::util::datetime::now_millis(),
                )
                .await;
                Err(error)
            }
        }
    }

    async fn apply_with_lease(&self, lease: &TaskLease) -> Result<InspectResult> {
        let mut tx = self
            .pool
            .begin()
            .await
            .context("beginning owner ID backfill")?;
        state::renew_lease_tx(
            &mut tx,
            lease,
            command_utils::util::datetime::now_millis(),
            self.lease_duration_ms,
        )
        .await?;

        // Validate the complete legacy ledger before issuing any update.
        inspect_tx(&mut tx, self.uses_thread_owner_preflight(), true).await?;
        for statement in BACKFILL_SQL {
            sqlx::query(sqlx::AssertSqlSafe(*statement))
                .execute(&mut *tx)
                .await
                .with_context(|| format!("backfilling typed IDs with {statement}"))?;
        }
        let result = inspect_tx(&mut tx, self.uses_thread_owner_preflight(), true).await?;
        if result.pending_count() != 0 {
            bail!(
                "typed owner ID backfill left pending fields: {:?}",
                result.pending_fields
            );
        }
        tx.commit()
            .await
            .context("committing typed owner ID backfill")?;
        Ok(result)
    }

    pub async fn verify(&self) -> Result<()> {
        let result = self.inspect().await?;
        if result.pending_count() != 0 {
            bail!(
                "typed owner ID verification found pending fields: {:?}",
                result.pending_fields
            );
        }
        Ok(())
    }

    fn uses_thread_owner_preflight(&self) -> bool {
        self.catalog.identity() == TASK_IDENTITY_V2
    }
}

async fn inspect_tx(
    tx: &mut RdbTransaction<'_>,
    validate_thread_owners: bool,
    validate_canonical_membership_keys: bool,
) -> Result<InspectResult> {
    let mut result = InspectResult::default();
    validate_owner_pairs(
        tx,
        "SELECT owner_scope, user_id FROM thread_group_member",
        "thread_group_member.user_id",
        &mut result,
    )
    .await?;
    validate_relation_owners(tx, &mut result).await?;
    validate_observation_owners(tx, &mut result).await?;
    validate_owner_pairs(
        tx,
        "SELECT subject_owner_scope, subject_user_id FROM thread_group_candidate_association",
        "thread_group_candidate_association.subject_user_id",
        &mut result,
    )
    .await?;
    validate_owner_pairs(
        tx,
        "SELECT owner_scope, user_id FROM source_thread_identity",
        "source_thread_identity.user_id",
        &mut result,
    )
    .await?;
    validate_owner_pairs(
        tx,
        "SELECT owner_scope, user_id FROM thread_canonical_key",
        "thread_canonical_key.user_id",
        &mut result,
    )
    .await?;
    validate_owner_pairs(
        tx,
        "SELECT owner_scope, user_id FROM thread_deletion_marker",
        "thread_deletion_marker.user_id",
        &mut result,
    )
    .await?;
    validate_owner_pairs(
        tx,
        "SELECT owner_scope, user_id FROM operator_decision",
        "operator_decision.user_id",
        &mut result,
    )
    .await?;
    validate_owner_pairs(
        tx,
        "SELECT owner_scope, user_id FROM manual_collection",
        "manual_collection.user_id",
        &mut result,
    )
    .await?;
    validate_owner_pairs(
        tx,
        "SELECT owner_scope, user_id FROM manual_collection_member",
        "manual_collection_member.user_id",
        &mut result,
    )
    .await?;
    validate_optional_owner_pairs(
        tx,
        "SELECT owner_scope, user_id FROM thread_group_event_outbox",
        "thread_group_event_outbox.user_id",
        &mut result,
    )
    .await?;
    if validate_thread_owners {
        validate_referenced_thread_owners(tx, &mut result).await?;
    }
    validate_group_owners(tx).await?;
    if validate_canonical_membership_keys {
        validate_existing_canonical_keys(tx).await?;
    }
    validate_marker_key_assignments(tx).await?;
    Ok(result)
}

pub(super) async fn inspect_for_v3_tx(
    tx: &mut RdbTransaction<'_>,
) -> Result<BTreeMap<String, u64>> {
    Ok(inspect_tx(tx, true, false).await?.pending_fields)
}

pub(super) async fn backfill_typed_ids_v3_tx(
    tx: &mut RdbTransaction<'_>,
) -> Result<BTreeMap<String, u64>> {
    // @3 may fill owner IDs, but must not infer or rewrite a deletion marker's canonical key.
    for statement in BACKFILL_SQL.iter().take(TYPED_ID_BACKFILL_COUNT) {
        sqlx::query(sqlx::AssertSqlSafe(*statement))
            .execute(&mut **tx)
            .await
            .with_context(|| format!("backfilling typed IDs with {statement}"))?;
    }
    let result = inspect_for_v3_tx(tx).await?;
    if result.values().sum::<u64>() != 0 {
        bail!("typed owner ID backfill left pending fields: {:?}", result);
    }
    Ok(result)
}

async fn validate_referenced_thread_owners(
    tx: &mut RdbTransaction<'_>,
    result: &mut InspectResult,
) -> Result<()> {
    for (sql, field) in [
        (
            "SELECT member.owner_scope, member.user_id, thread.user_id \
             FROM thread_group_member member JOIN thread ON thread.id = member.thread_id",
            "thread_group_member.user_id",
        ),
        (
            "SELECT canonical.owner_scope, canonical.user_id, thread.user_id \
             FROM thread_canonical_key canonical JOIN thread ON thread.id = canonical.thread_id",
            "thread_canonical_key.user_id",
        ),
        (
            "SELECT identity.owner_scope, identity.user_id, thread.user_id \
             FROM source_thread_identity identity JOIN thread ON thread.id = identity.thread_id",
            "source_thread_identity.user_id",
        ),
        (
            "SELECT relation.parent_owner_scope, relation.parent_user_id, thread.user_id \
             FROM thread_relation relation JOIN thread ON thread.id = relation.parent_thread_id",
            "thread_relation.parent_user_id",
        ),
        (
            "SELECT relation.child_owner_scope, relation.child_user_id, thread.user_id \
             FROM thread_relation relation JOIN thread ON thread.id = relation.child_thread_id",
            "thread_relation.child_user_id",
        ),
        (
            "SELECT candidate.subject_owner_scope, candidate.subject_user_id, thread.user_id \
             FROM thread_group_candidate_association candidate \
             JOIN thread ON thread.id = candidate.subject_thread_id",
            "thread_group_candidate_association.subject_user_id",
        ),
        (
            "SELECT observation.subject_owner_scope, observation.subject_user_id, thread.user_id \
             FROM thread_observation observation \
             JOIN source_thread_identity identity \
               ON identity.owner_scope = observation.subject_owner_scope \
              AND identity.source = observation.subject_source \
              AND identity.identity_scope = observation.subject_identity_scope_value \
              AND identity.native_id = observation.subject_native_id \
             JOIN thread ON thread.id = identity.thread_id \
             WHERE observation.subject_identity_scope_known = TRUE",
            "thread_observation.subject_user_id",
        ),
        (
            "SELECT observation.candidate_parent_owner_scope, observation.candidate_parent_user_id, thread.user_id \
             FROM thread_observation observation \
             JOIN source_thread_identity identity \
               ON identity.owner_scope = observation.candidate_parent_owner_scope \
              AND identity.source = observation.candidate_parent_source \
              AND identity.identity_scope = observation.candidate_parent_identity_scope_value \
              AND identity.native_id = observation.candidate_parent_native_id \
             JOIN thread ON thread.id = identity.thread_id \
             WHERE observation.candidate_parent_present = TRUE \
               AND observation.candidate_parent_identity_scope_known = TRUE",
            "thread_observation.candidate_parent_user_id",
        ),
    ] {
        let rows: Vec<(String, Option<i64>, i64)> = sqlx::query_as(sqlx::AssertSqlSafe(sql))
            .fetch_all(&mut **tx)
            .await
            .with_context(|| format!("checking referenced Thread owners for {field}"))?;
        for (owner_scope, user_id, thread_user_id) in rows {
            let expected = parse_owner(&owner_scope, field)?;
            if expected != thread_user_id {
                bail!(
                    "{field} thread owner mismatch: legacy owner {expected} does not match referenced Thread owner {thread_user_id}"
                );
            }
            validate_typed_id(user_id, expected, field, result)?;
        }
    }
    Ok(())
}

async fn validate_existing_canonical_keys(tx: &mut RdbTransaction<'_>) -> Result<()> {
    let mismatched: i64 =
        sqlx::query_scalar(sqlx::AssertSqlSafe(CANONICAL_MEMBER_KEY_MISMATCH_SQL))
            .fetch_one(&mut **tx)
            .await
            .context("checking immutable canonical keys against live memberships")?;
    if mismatched > 0 {
        bail!("{mismatched} live memberships disagree with their thread canonical key");
    }
    Ok(())
}

pub(super) async fn validate_marker_key_assignments(tx: &mut RdbTransaction<'_>) -> Result<()> {
    let ambiguous: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(AMBIGUOUS_MARKER_KEY_SQL))
        .fetch_one(&mut **tx)
        .await
        .context("checking legacy deletion marker canonical-key aliases")?;
    if ambiguous > 0 {
        bail!("{ambiguous} deletion markers resolve to multiple canonical keys");
    }
    Ok(())
}

async fn validate_owner_pairs(
    tx: &mut RdbTransaction<'_>,
    sql: &'static str,
    field: &str,
    result: &mut InspectResult,
) -> Result<()> {
    let rows: Vec<(String, Option<i64>)> = sqlx::query_as(sqlx::AssertSqlSafe(sql))
        .fetch_all(&mut **tx)
        .await
        .with_context(|| format!("reading legacy owners for {field}"))?;
    for (owner_scope, user_id) in rows {
        let expected = parse_owner(&owner_scope, field)?;
        validate_typed_id(user_id, expected, field, result)?;
    }
    Ok(())
}

async fn validate_optional_owner_pairs(
    tx: &mut RdbTransaction<'_>,
    sql: &'static str,
    field: &str,
    result: &mut InspectResult,
) -> Result<()> {
    let rows: Vec<(Option<String>, Option<i64>)> = sqlx::query_as(sqlx::AssertSqlSafe(sql))
        .fetch_all(&mut **tx)
        .await
        .with_context(|| format!("reading optional legacy owners for {field}"))?;
    for (owner_scope, user_id) in rows {
        if let Some(owner_scope) = owner_scope {
            let expected = parse_owner(&owner_scope, field)?;
            validate_typed_id(user_id, expected, field, result)?;
        } else if user_id.is_some() {
            bail!("{field} is set although the legacy event owner is absent");
        }
    }
    Ok(())
}

async fn validate_relation_owners(
    tx: &mut RdbTransaction<'_>,
    result: &mut InspectResult,
) -> Result<()> {
    let rows: Vec<(String, Option<i64>, String, Option<i64>)> = sqlx::query_as(
        "SELECT parent_owner_scope, parent_user_id, child_owner_scope, child_user_id FROM thread_relation",
    )
    .fetch_all(&mut **tx)
    .await
    .context("reading relation endpoint owners")?;
    for (parent_scope, parent_id, child_scope, child_id) in rows {
        validate_typed_id(
            parent_id,
            parse_owner(&parent_scope, "thread_relation.parent_user_id")?,
            "thread_relation.parent_user_id",
            result,
        )?;
        validate_typed_id(
            child_id,
            parse_owner(&child_scope, "thread_relation.child_user_id")?,
            "thread_relation.child_user_id",
            result,
        )?;
    }
    Ok(())
}

async fn validate_observation_owners(
    tx: &mut RdbTransaction<'_>,
    result: &mut InspectResult,
) -> Result<()> {
    type ObservationOwnerRow = (String, Option<i64>, bool, String, Option<i64>);
    let rows: Vec<ObservationOwnerRow> = sqlx::query_as(
        "SELECT subject_owner_scope, subject_user_id, candidate_parent_present, \
         candidate_parent_owner_scope, candidate_parent_user_id FROM thread_observation",
    )
    .fetch_all(&mut **tx)
    .await
    .context("reading observation endpoint owners")?;
    for (subject_scope, subject_id, parent_present, parent_scope, parent_id) in rows {
        validate_typed_id(
            subject_id,
            parse_owner(&subject_scope, "thread_observation.subject_user_id")?,
            "thread_observation.subject_user_id",
            result,
        )?;
        if parent_present {
            validate_typed_id(
                parent_id,
                parse_owner(&parent_scope, "thread_observation.candidate_parent_user_id")?,
                "thread_observation.candidate_parent_user_id",
                result,
            )?;
        } else if parent_id.is_some() {
            bail!("absent observation parent must not have candidate_parent_user_id");
        }
    }
    Ok(())
}

pub(super) async fn validate_group_owners(tx: &mut RdbTransaction<'_>) -> Result<()> {
    let invalid: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM thread_group WHERE user_id <= 0")
        .fetch_one(&mut **tx)
        .await
        .context("validating ThreadGroup owner IDs")?;
    if invalid > 0 {
        bail!("{invalid} ThreadGroup rows have a non-positive user_id");
    }
    Ok(())
}

fn parse_owner(owner_scope: &str, field: &str) -> Result<i64> {
    parse_legacy_owner_scope(owner_scope)
        .filter(|user_id| *user_id > 0)
        .with_context(|| format!("{field} has invalid legacy owner scope {owner_scope:?}"))
}

fn validate_typed_id(
    user_id: Option<i64>,
    expected: i64,
    field: &str,
    result: &mut InspectResult,
) -> Result<()> {
    match user_id {
        Some(actual) if actual != expected => {
            bail!("{field} user_id {actual} disagrees with legacy owner {expected}");
        }
        Some(_) => {}
        None => *result.pending_fields.entry(field.to_string()).or_default() += 1,
    }
    Ok(())
}

#[async_trait]
impl DataMigrationTask for ThreadGroupsUserIdsV1Task {
    fn task_identity(&self) -> String {
        self.catalog.identity()
    }

    async fn inspect(&self) -> Result<serde_json::Value> {
        Ok(serde_json::to_value(
            ThreadGroupsUserIdsV1Task::inspect(self).await?,
        )?)
    }

    async fn dry_run(&self) -> Result<serde_json::Value> {
        Ok(serde_json::to_value(
            ThreadGroupsUserIdsV1Task::dry_run(self).await?,
        )?)
    }

    async fn apply(&self, execution_id: &str, holder_id: &str) -> Result<serde_json::Value> {
        Ok(serde_json::to_value(
            ThreadGroupsUserIdsV1Task::apply(self, execution_id, holder_id).await?,
        )?)
    }

    async fn verify(&self) -> Result<()> {
        ThreadGroupsUserIdsV1Task::verify(self).await
    }
}

/// Typed-owner backfill replacement with preflight validation against the
/// owner of every referenced live Thread.
pub struct ThreadGroupsUserIdsV2Task {
    inner: ThreadGroupsUserIdsV1Task,
}

impl ThreadGroupsUserIdsV2Task {
    pub fn new(pool: RdbPool, catalog: TaskCatalogEntry) -> Result<Self> {
        if catalog.identity() != TASK_IDENTITY_V2 {
            bail!(
                "unexpected replacement task catalog identity: {}",
                catalog.identity()
            );
        }
        Ok(Self {
            inner: ThreadGroupsUserIdsV1Task::new(pool, catalog)?,
        })
    }
}

#[async_trait]
impl DataMigrationTask for ThreadGroupsUserIdsV2Task {
    fn task_identity(&self) -> String {
        self.inner.task_identity()
    }

    async fn inspect(&self) -> Result<serde_json::Value> {
        Ok(serde_json::to_value(
            ThreadGroupsUserIdsV1Task::inspect(&self.inner).await?,
        )?)
    }

    async fn dry_run(&self) -> Result<serde_json::Value> {
        Ok(serde_json::to_value(
            ThreadGroupsUserIdsV1Task::dry_run(&self.inner).await?,
        )?)
    }

    async fn apply(&self, execution_id: &str, holder_id: &str) -> Result<serde_json::Value> {
        Ok(serde_json::to_value(
            ThreadGroupsUserIdsV1Task::apply(&self.inner, execution_id, holder_id).await?,
        )?)
    }

    async fn verify(&self) -> Result<()> {
        self.inner.verify().await
    }
}

#[cfg(all(test, not(feature = "postgres")))]
mod tests {
    use super::*;
    use crate::db_migrate::catalog;
    use infra::infra::thread_group::canonical_key::{
        ThreadCanonicalKeyRepository, ThreadCanonicalKeyRepositoryImpl,
    };
    use infra::infra::thread_group::deletion_marker::{
        ThreadDeletionMarkerRepository, ThreadDeletionMarkerRepositoryImpl,
    };
    use infra::infra::thread_group::group::{ThreadGroupRepository, ThreadGroupRepositoryImpl};
    use infra::infra::thread_group::member::{
        ThreadGroupMemberRepository, ThreadGroupMemberRepositoryImpl,
    };
    use infra::infra::thread_group::observation::{
        ThreadObservationRepository, ThreadObservationRepositoryImpl,
    };
    use infra::infra::thread_group::rows::{NewThreadDeletionMarker, SourceIdentityKey};
    use infra::infra::thread_group::source_identity::{
        SourceThreadIdentityRepository, SourceThreadIdentityRepositoryImpl,
    };
    use infra::infra::thread_group::test_support::{
        insert_thread, key, new_group, new_member, new_observation, setup_thread_group_pool,
    };
    use infra_utils::infra::rdb::RdbPool;
    use infra_utils::infra::test::TEST_RUNTIME;

    const TASK_IDENTITY: &str = "thread-groups-user-ids-v1@1";

    async fn prepare_task_state(pool: &RdbPool) {
        sqlx::raw_sql(
            "CREATE TABLE IF NOT EXISTS memories_data_migration_task_state (\
               task_identity TEXT PRIMARY KEY, canonical_definition_digest TEXT NOT NULL, state TEXT NOT NULL,\
               execution_id TEXT, holder_id TEXT, fencing_token BIGINT NOT NULL, heartbeat_at BIGINT,\
               lease_expires_at BIGINT, attempt_count BIGINT NOT NULL, checkpoint TEXT,\
               failure_classification TEXT, started_at BIGINT, updated_at BIGINT NOT NULL, completed_at BIGINT\
             )",
        )
        .execute(pool)
        .await
        .unwrap();
        sqlx::query(
            "DELETE FROM memories_data_migration_task_state \
             WHERE task_identity IN (?, ?)",
        )
        .bind(TASK_IDENTITY)
        .bind(TASK_IDENTITY_V2)
        .execute(pool)
        .await
        .unwrap();
    }

    fn task(pool: &RdbPool) -> ThreadGroupsUserIdsV1Task {
        ThreadGroupsUserIdsV1Task::new(pool.clone(), catalog::thread_groups_user_ids_v1().unwrap())
            .unwrap()
    }

    fn replacement_task(pool: &RdbPool) -> ThreadGroupsUserIdsV2Task {
        ThreadGroupsUserIdsV2Task::new(pool.clone(), catalog::thread_groups_user_ids_v2().unwrap())
            .unwrap()
    }

    async fn insert_group_member(pool: &'static RdbPool, n: u64) -> (i64, String, i64) {
        let groups =
            ThreadGroupRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool);
        let group = groups.create_tx(pool, &new_group(n)).await.unwrap();
        let thread_id = 960_000 + n as i64;
        insert_thread(pool, thread_id, None).await;
        let member_key = key(n);
        ThreadGroupMemberRepositoryImpl::new(pool)
            .insert_tx(
                pool,
                &new_member(group, Some(thread_id), member_key.clone()),
            )
            .await
            .unwrap();
        (group, member_key, thread_id)
    }

    #[test]
    fn backfills_owner_ids_and_keeps_absent_observation_parent_null() {
        TEST_RUNTIME.block_on(async {
            let pool = setup_thread_group_pool().await;
            prepare_task_state(pool).await;

            let (group_id, member_key, thread_id) = insert_group_member(pool, 96_101).await;
            sqlx::query(
                "UPDATE thread_group_member SET user_id = NULL WHERE group_id = ? AND thread_canonical_key = ?",
            )
            .bind(group_id)
            .bind(&member_key)
            .execute(pool)
            .await
            .unwrap();

            let member_identity = SourceIdentityKey {
                user_id: 1,
                source: "codex",
                identity_scope: "",
                native_id: "session-1",
            };
            ThreadDeletionMarkerRepositoryImpl::new(pool)
                .put_tx(
                    pool,
                    &NewThreadDeletionMarker {
                        identity: member_identity,
                        forbid_reimport: true,
                        recursive: false,
                        actor_id: "migration-test".to_string(),
                        reason: None,
                        deleted_at: 1,
                        thread_canonical_key: None,
                    },
                )
                .await
                .unwrap();
            sqlx::query(
                "UPDATE thread_deletion_marker SET user_id = NULL WHERE native_id = ?",
            )
            .bind("session-1")
            .execute(pool)
            .await
            .unwrap();

            let identity = SourceIdentityKey {
                user_id: 1,
                source: "codex",
                identity_scope: "",
                native_id: "backfill-source-identity",
            };
            SourceThreadIdentityRepositoryImpl::new(pool)
                .upsert_resolved_tx(pool, &identity, thread_id, 1)
                .await
                .unwrap();
            sqlx::query(
                "UPDATE source_thread_identity SET user_id = NULL WHERE source = ? AND identity_scope = ? AND native_id = ?",
            )
            .bind(identity.source)
            .bind(identity.identity_scope)
            .bind(identity.native_id)
            .execute(pool)
            .await
            .unwrap();

            let source_alias = SourceIdentityKey {
                user_id: 1,
                source: "codex",
                identity_scope: "",
                native_id: "backfill-source-alias",
            };
            SourceThreadIdentityRepositoryImpl::new(pool)
                .upsert_resolved_tx(pool, &source_alias, thread_id, 1)
                .await
                .unwrap();
            sqlx::query(
                "UPDATE source_thread_identity SET user_id = NULL WHERE source = ? AND identity_scope = ? AND native_id = ?",
            )
            .bind(source_alias.source)
            .bind(source_alias.identity_scope)
            .bind(source_alias.native_id)
            .execute(pool)
            .await
            .unwrap();
            let canonical_key = key(96_101);
            ThreadCanonicalKeyRepositoryImpl::new(pool)
                .assign_tx(
                    pool,
                    thread_id,
                    1,
                    &canonical_key,
                    infra::infra::thread_group::rows::values::canonical_key_origin::SOURCE_IDENTITY,
                    1,
                )
                .await
                .unwrap();
            sqlx::query("UPDATE thread_canonical_key SET user_id = NULL WHERE thread_id = ?")
                .bind(thread_id)
                .execute(pool)
                .await
                .unwrap();
            let alias_marker = SourceIdentityKey {
                user_id: 1,
                source: "codex",
                identity_scope: "",
                native_id: "backfill-source-alias",
            };
            ThreadDeletionMarkerRepositoryImpl::new(pool)
                .put_tx(
                    pool,
                    &NewThreadDeletionMarker {
                        identity: alias_marker,
                        forbid_reimport: true,
                        recursive: false,
                        actor_id: "migration-test".to_string(),
                        reason: None,
                        deleted_at: 1,
                        thread_canonical_key: None,
                    },
                )
                .await
                .unwrap();
            sqlx::query(
                "UPDATE thread_deletion_marker SET user_id = NULL WHERE native_id = ?",
            )
            .bind("backfill-source-alias")
            .execute(pool)
            .await
            .unwrap();

            let observations = ThreadObservationRepositoryImpl::new(
                infra::test_helper::shared_id_generator(),
                pool,
            );
            let mut present_parent = new_observation(96_101);
            present_parent.subject_native_id = "backfill-subject-present".to_string();
            let present_id = observations
                .insert_tx(pool, &present_parent)
                .await
                .unwrap();
            let mut absent_parent = new_observation(96_102);
            absent_parent.subject_native_id = "backfill-subject-absent".to_string();
            absent_parent.candidate_parent_present = false;
            absent_parent.candidate_parent_source.clear();
            absent_parent.candidate_parent_identity_scope_known = false;
            absent_parent.candidate_parent_identity_scope_value.clear();
            absent_parent.candidate_parent_user_id = None;
            absent_parent.candidate_parent_native_id.clear();
            let absent_id = observations
                .insert_tx(pool, &absent_parent)
                .await
                .unwrap();
            sqlx::query(
                "UPDATE thread_observation SET subject_user_id = NULL, candidate_parent_user_id = NULL WHERE id = ?",
            )
            .bind(present_id)
            .execute(pool)
            .await
            .unwrap();
            sqlx::query("UPDATE thread_observation SET subject_user_id = NULL WHERE id = ?")
                .bind(absent_id)
                .execute(pool)
                .await
                .unwrap();

            let result = task(pool).apply("owner-ids-exec-1", "test").await.unwrap();
            assert_eq!(result.pending_count(), 0);
            let member_user_id: i64 = sqlx::query_scalar(
                "SELECT user_id FROM thread_group_member WHERE group_id = ? AND thread_canonical_key = ?",
            )
            .bind(group_id)
            .bind(&member_key)
            .fetch_one(pool)
            .await
            .unwrap();
            assert_eq!(member_user_id, 1);
            let legacy_marker_key: Option<String> = sqlx::query_scalar(
                "SELECT thread_canonical_key FROM thread_deletion_marker WHERE native_id = ?",
            )
            .bind("session-1")
            .fetch_one(pool)
            .await
            .unwrap();
            assert_eq!(legacy_marker_key.as_deref(), Some(member_key.as_str()));
            let alias_marker_key: Option<String> = sqlx::query_scalar(
                "SELECT thread_canonical_key FROM thread_deletion_marker WHERE native_id = ?",
            )
            .bind("backfill-source-alias")
            .fetch_one(pool)
            .await
            .unwrap();
            assert_eq!(alias_marker_key.as_deref(), Some(canonical_key.as_str()));
            let assigned_key: String = sqlx::query_scalar(
                "SELECT key FROM thread_canonical_key WHERE thread_id = ?",
            )
            .bind(thread_id)
            .fetch_one(pool)
            .await
            .unwrap();
            assert_eq!(assigned_key, canonical_key);
            let source_user_id: i64 = sqlx::query_scalar(
                "SELECT user_id FROM source_thread_identity WHERE native_id = ?",
            )
            .bind(identity.native_id)
            .fetch_one(pool)
            .await
            .unwrap();
            assert_eq!(source_user_id, 1);
            let present_parent_user_id: Option<i64> = sqlx::query_scalar(
                "SELECT candidate_parent_user_id FROM thread_observation WHERE id = ?",
            )
            .bind(present_id)
            .fetch_one(pool)
            .await
            .unwrap();
            assert_eq!(present_parent_user_id, Some(1));
            let absent_parent_user_id: Option<i64> = sqlx::query_scalar(
                "SELECT candidate_parent_user_id FROM thread_observation WHERE id = ?",
            )
            .bind(absent_id)
            .fetch_one(pool)
            .await
            .unwrap();
            assert_eq!(absent_parent_user_id, None);
            task(pool).verify().await.unwrap();
            let rerun = task(pool)
                .apply("owner-ids-exec-rerun", "test")
                .await
                .unwrap();
            assert_eq!(rerun.pending_count(), 0);
        });
    }

    #[test]
    fn malformed_legacy_owner_scope_fails_before_backfilling_rows() {
        TEST_RUNTIME.block_on(async {
            let pool = setup_thread_group_pool().await;
            prepare_task_state(pool).await;
            let (group_id, member_key, _) = insert_group_member(pool, 96_201).await;
            sqlx::query(
                "UPDATE thread_group_member SET owner_scope = 'user:01', user_id = NULL WHERE group_id = ? AND thread_canonical_key = ?",
            )
            .bind(group_id)
            .bind(&member_key)
            .execute(pool)
            .await
            .unwrap();

            let error = task(pool)
                .apply("owner-ids-exec-invalid", "test")
                .await
                .unwrap_err();
            assert!(error.to_string().contains("invalid legacy owner scope"));
            let user_id: Option<i64> = sqlx::query_scalar(
                "SELECT user_id FROM thread_group_member WHERE group_id = ? AND thread_canonical_key = ?",
            )
            .bind(group_id)
            .bind(&member_key)
            .fetch_one(pool)
            .await
            .unwrap();
            assert_eq!(user_id, None);

            prepare_task_state(pool).await;
            sqlx::query(
                "UPDATE thread_group_member SET owner_scope = 'user:1', user_id = 2 WHERE group_id = ? AND thread_canonical_key = ?",
            )
            .bind(group_id)
            .bind(&member_key)
            .execute(pool)
            .await
            .unwrap();
            let mismatch = task(pool)
                .apply("owner-ids-exec-mismatch", "test")
                .await
                .unwrap_err();
            assert!(mismatch.to_string().contains("disagrees with legacy owner"));
            let unchanged_user_id: Option<i64> = sqlx::query_scalar(
                "SELECT user_id FROM thread_group_member WHERE group_id = ? AND thread_canonical_key = ?",
            )
            .bind(group_id)
            .bind(&member_key)
            .fetch_one(pool)
            .await
            .unwrap();
            assert_eq!(unchanged_user_id, Some(2));
            sqlx::query(
                "UPDATE thread_group_member SET user_id = 1 \
                 WHERE group_id = ? AND thread_canonical_key = ?",
            )
            .bind(group_id)
            .bind(&member_key)
            .execute(pool)
            .await
            .unwrap();
        });
    }

    #[test]
    fn owner_mismatch_with_referenced_thread_fails_before_backfill() {
        TEST_RUNTIME.block_on(async {
            let pool = setup_thread_group_pool().await;
            prepare_task_state(pool).await;
            let (group_id, member_key, thread_id) = insert_group_member(pool, 96_205).await;

            sqlx::query(
                "UPDATE thread_group_member SET owner_scope = 'user:2', user_id = NULL \
                 WHERE group_id = ? AND thread_canonical_key = ?",
            )
            .bind(group_id)
            .bind(&member_key)
            .execute(pool)
            .await
            .unwrap();
            let identity = SourceIdentityKey {
                user_id: 2,
                source: "codex",
                identity_scope: "",
                native_id: "owner-mismatch",
            };
            SourceThreadIdentityRepositoryImpl::new(pool)
                .upsert_resolved_tx(pool, &identity, thread_id, 1)
                .await
                .unwrap();
            sqlx::query(
                "UPDATE source_thread_identity SET user_id = NULL \
                 WHERE source = 'codex' AND native_id = 'owner-mismatch'",
            )
            .execute(pool)
            .await
            .unwrap();

            let error = replacement_task(pool)
                .apply("owner-ids-exec-thread-mismatch", "test")
                .await
                .expect_err("typed owners must match the referenced Thread owner");
            assert!(
                format!("{error:#}").contains("thread owner mismatch"),
                "unexpected preflight failure: {error:#}"
            );
            let member_owner: Option<i64> = sqlx::query_scalar(
                "SELECT user_id FROM thread_group_member WHERE group_id = ? AND thread_canonical_key = ?",
            )
            .bind(group_id)
            .bind(&member_key)
            .fetch_one(pool)
            .await
            .unwrap();
            let identity_owner: Option<i64> = sqlx::query_scalar(
                "SELECT user_id FROM source_thread_identity WHERE native_id = 'owner-mismatch'",
            )
            .fetch_one(pool)
            .await
            .unwrap();
            assert_eq!(member_owner, None);
            assert_eq!(identity_owner, None);
        });
    }
}
