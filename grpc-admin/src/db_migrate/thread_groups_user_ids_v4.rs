//! `thread-groups-user-ids-v1@4`: the `@3` canonical membership repair with the
//! typed `user_id` columns as the owner of record.
//!
//! Released generations keep their original contract, so this owner policy is
//! a new generation rather than a change to `@3`. The policy itself lives in
//! `typed_owner_backfill`.

use super::{
    DataMigrationTask,
    catalog::{THREAD_GROUPS_USER_IDS_V4_IDENTITY, TaskCatalogEntry},
    thread_groups_user_ids_v3::{OwnerBasis, ThreadGroupsUserIdsV3Task},
};
use anyhow::{Result, bail};
use async_trait::async_trait;
use infra_utils::infra::rdb::RdbPool;

pub struct ThreadGroupsUserIdsV4Task {
    inner: ThreadGroupsUserIdsV3Task,
}

impl ThreadGroupsUserIdsV4Task {
    pub fn new(pool: RdbPool, catalog: TaskCatalogEntry) -> Result<Self> {
        if catalog.identity() != THREAD_GROUPS_USER_IDS_V4_IDENTITY {
            bail!(
                "unexpected replacement task catalog identity: {}",
                catalog.identity()
            );
        }
        Ok(Self {
            inner: ThreadGroupsUserIdsV3Task::with_owner_basis(
                pool,
                catalog,
                OwnerBasis::TypedUserId,
            )?,
        })
    }
}

#[async_trait]
impl DataMigrationTask for ThreadGroupsUserIdsV4Task {
    fn task_identity(&self) -> String {
        self.inner.task_identity()
    }

    async fn inspect(&self) -> Result<serde_json::Value> {
        Ok(serde_json::to_value(self.inner.inspect().await?)?)
    }

    async fn dry_run(&self) -> Result<serde_json::Value> {
        Ok(serde_json::to_value(self.inner.dry_run().await?)?)
    }

    async fn apply(&self, execution_id: &str, holder_id: &str) -> Result<serde_json::Value> {
        Ok(serde_json::to_value(
            self.inner.apply(execution_id, holder_id).await?,
        )?)
    }

    async fn verify(&self) -> Result<()> {
        self.inner.verify().await
    }
}

#[cfg(all(test, not(feature = "postgres")))]
mod tests {
    use super::ThreadGroupsUserIdsV4Task;
    use crate::db_migrate::{
        DataMigrationTask,
        catalog::{self, THREAD_GROUPS_USER_IDS_V4_IDENTITY},
        state::{self, TaskStateKind},
        thread_groups_user_ids_v3::{
            ThreadGroupsUserIdsV3Task,
            tests::{business_table_snapshot, prepare_task_rows, seed_membership, test_pool},
        },
    };
    use infra_utils::infra::rdb::RdbPool;
    use infra_utils::infra::test::TEST_RUNTIME;
    use serde_json::Value;

    fn task(pool: &RdbPool) -> ThreadGroupsUserIdsV4Task {
        ThreadGroupsUserIdsV4Task::new(pool.clone(), catalog::thread_groups_user_ids_v4().unwrap())
            .unwrap()
    }

    async fn execute_all(pool: &RdbPool, statements: &[&'static str]) {
        for statement in statements {
            sqlx::query(*statement).execute(pool).await.unwrap();
        }
    }

    async fn insert_outbox_event(
        pool: &RdbPool,
        event_id: &str,
        owner_scope: Option<&str>,
        user_id: Option<i64>,
        thread_id: Option<i64>,
    ) {
        sqlx::query(
            "INSERT INTO thread_group_event_outbox \
             (event_id, event_type, operation_id, policy_version, owner_scope, user_id, thread_id, payload, created_at) \
             VALUES (?, 'thread_group_observation_recorded', 'fixture-operation', 'fixture-policy', ?, ?, ?, '{}', 60)",
        )
        .bind(event_id)
        .bind(owner_scope)
        .bind(user_id)
        .bind(thread_id)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn owner_by_thread(pool: &RdbPool, table: &str, thread_id: i64) -> Option<i64> {
        let sql = format!("SELECT user_id FROM {table} WHERE thread_id = ?");
        sqlx::query_scalar(sqlx::AssertSqlSafe(sql))
            .bind(thread_id)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    async fn outbox_owner(pool: &RdbPool, event_id: &str) -> Option<i64> {
        sqlx::query_scalar("SELECT user_id FROM thread_group_event_outbox WHERE event_id = ?")
            .bind(event_id)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    async fn task_state_kind(pool: &RdbPool) -> Option<TaskStateKind> {
        state::load(pool, THREAD_GROUPS_USER_IDS_V4_IDENTITY)
            .await
            .unwrap()
            .map(|row| row.kind().unwrap())
    }

    fn is_empty_map(report: &Value, field: &str) -> bool {
        report[field].as_object().is_some_and(|map| map.is_empty())
    }

    #[test]
    fn typed_owners_pass_regardless_of_legacy_scope_values() {
        TEST_RUNTIME.block_on(async {
            let pool = test_pool().await;
            prepare_task_rows(pool).await;
            seed_membership(pool, false).await;
            execute_all(
                pool,
                &[
                    "UPDATE thread_group_member SET owner_scope = 'broken'",
                    "UPDATE thread_canonical_key SET owner_scope = 'user:9'",
                    "UPDATE source_thread_identity SET owner_scope = 'user:01'",
                ],
            )
            .await;
            // Written after the typed-owner migration: no legacy owner at all.
            insert_outbox_event(pool, "typed-only", None, Some(1), None).await;
            insert_outbox_event(pool, "malformed-scope", Some("user:01"), Some(1), None).await;
            insert_outbox_event(pool, "contradicting-scope", Some("user:2"), Some(1), None).await;

            // The released @3 contract refuses this database; @4 exists for it.
            let legacy = ThreadGroupsUserIdsV3Task::new(
                pool.clone(),
                catalog::thread_groups_user_ids_v3().unwrap(),
            )
            .unwrap()
            .inspect()
            .await
            .unwrap();
            assert_eq!(
                legacy.stop_reasons.get("typed_owner_or_marker_preflight"),
                Some(&1)
            );

            let report = task(pool).inspect().await.unwrap();
            assert!(is_empty_map(&report, "stop_reasons"), "{report}");
            assert!(is_empty_map(&report, "pending_typed_fields"), "{report}");

            task(pool).apply("typed-owners", "test").await.unwrap();
            task(pool).verify().await.unwrap();
            assert_eq!(task_state_kind(pool).await, Some(TaskStateKind::Completed));
        });
    }

    #[test]
    fn alias_repair_proves_identity_by_typed_owner_when_legacy_scopes_are_unusable() {
        TEST_RUNTIME.block_on(async {
            let pool = test_pool().await;
            prepare_task_rows(pool).await;
            let seed = seed_membership(pool, true).await;
            execute_all(
                pool,
                &[
                    "UPDATE thread_group_member SET owner_scope = 'broken'",
                    "UPDATE thread_canonical_key SET owner_scope = 'broken'",
                    "UPDATE source_thread_identity SET owner_scope = 'broken'",
                ],
            )
            .await;

            let report = task(pool).apply("alias-repair", "test").await.unwrap();
            assert_eq!(report["repaired_memberships"], 1, "{report}");
            let key: String = sqlx::query_scalar(
                "SELECT thread_canonical_key FROM thread_group_member WHERE group_id = ? AND thread_id = ?",
            )
            .bind(seed.group_id)
            .bind(seed.thread_id)
            .fetch_one(pool)
            .await
            .unwrap();
            assert_eq!(key, seed.saved_key);
            task(pool).verify().await.unwrap();
        });
    }

    #[test]
    fn missing_typed_owner_is_filled_from_legacy_scope_then_referenced_thread_except_events() {
        TEST_RUNTIME.block_on(async {
            let pool = test_pool().await;
            prepare_task_rows(pool).await;
            let seed = seed_membership(pool, false).await;
            for statement in [
                "UPDATE thread_group_member SET user_id = NULL, owner_scope = 'user:1' WHERE thread_id = ?",
                "UPDATE thread_canonical_key SET user_id = NULL, owner_scope = 'broken' WHERE thread_id = ?",
            ] {
                sqlx::query(statement)
                    .bind(seed.thread_id)
                    .execute(pool)
                    .await
                    .unwrap();
            }
            insert_outbox_event(pool, "thread-owned", None, None, Some(seed.thread_id)).await;
            insert_outbox_event(pool, "ownerless", None, None, None).await;
            insert_outbox_event(pool, "legacy-owned", Some("user:1"), None, None).await;

            let report = task(pool).apply("fill-owners", "test").await.unwrap();
            assert_eq!(
                report["typed_id_fields_backfilled"]["thread_canonical_key.user_id"],
                1,
                "{report}"
            );
            assert_eq!(
                owner_by_thread(pool, "thread_group_member", seed.thread_id).await,
                Some(1)
            );
            assert_eq!(
                owner_by_thread(pool, "thread_canonical_key", seed.thread_id).await,
                Some(1)
            );
            // Without a legacy owner an event is ownerless history; its Thread
            // reference must not be turned into an owner.
            assert_eq!(outbox_owner(pool, "thread-owned").await, None);
            assert_eq!(outbox_owner(pool, "ownerless").await, None);
            assert_eq!(outbox_owner(pool, "legacy-owned").await, Some(1));
            task(pool).verify().await.unwrap();
        });
    }

    #[test]
    fn owner_required_row_without_any_owner_source_blocks_without_writes() {
        TEST_RUNTIME.block_on(async {
            let pool = test_pool().await;
            prepare_task_rows(pool).await;
            seed_membership(pool, false).await;
            execute_all(
                pool,
                &["INSERT INTO manual_collection (id, user_id, owner_scope, title, created_at, updated_at) \
                   VALUES (91, NULL, 'broken', 'unowned', 1, 1)"],
            )
            .await;
            let before = business_table_snapshot(pool).await;

            let report = task(pool).inspect().await.unwrap();
            assert_eq!(
                report["pending_typed_fields"]["manual_collection.user_id"],
                1
            );
            assert_eq!(report["stop_reasons"]["typed_owner_unresolvable"], 1);
            assert!(task(pool).apply("unowned", "test").await.is_err());
            assert_eq!(business_table_snapshot(pool).await, before);
            assert_ne!(task_state_kind(pool).await, Some(TaskStateKind::Completed));
        });
    }

    #[test]
    fn typed_owner_contradicting_referenced_thread_blocks() {
        TEST_RUNTIME.block_on(async {
            for contradiction in [
                "UPDATE thread_group_member SET user_id = 2 WHERE role = 'member'",
                "UPDATE thread_group_member SET user_id = NULL, owner_scope = 'user:2' WHERE role = 'member'",
            ] {
                let pool = test_pool().await;
                prepare_task_rows(pool).await;
                seed_membership(pool, false).await;
                execute_all(pool, &[contradiction]).await;
                let before = business_table_snapshot(pool).await;

                let report = task(pool).inspect().await.unwrap();
                assert_eq!(
                    report["stop_reasons"]["typed_owner_or_marker_preflight"],
                    1,
                    "{contradiction}: {report}"
                );
                assert!(task(pool).apply("contradiction", "test").await.is_err());
                assert_eq!(business_table_snapshot(pool).await, before);
            }
        });
    }

    #[test]
    fn database_completed_by_v3_records_v4_completion_without_business_changes() {
        TEST_RUNTIME.block_on(async {
            let pool = test_pool().await;
            prepare_task_rows(pool).await;
            seed_membership(pool, true).await;
            ThreadGroupsUserIdsV3Task::new(
                pool.clone(),
                catalog::thread_groups_user_ids_v3().unwrap(),
            )
            .unwrap()
            .apply("v3", "test")
            .await
            .unwrap();
            let before = business_table_snapshot(pool).await;

            let report = task(pool).apply("v4", "test").await.unwrap();
            assert_eq!(report["status"], "no_op");
            assert_eq!(business_table_snapshot(pool).await, before);
            assert_eq!(task_state_kind(pool).await, Some(TaskStateKind::Completed));
        });
    }
}
