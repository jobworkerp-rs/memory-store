//! `thread_group_member` repository (design 5.2).
//!
//! The partial UNIQUE index (`thread_canonical_key WHERE state IN
//! ('active','deleted')`) is the storage-level invariant behind "current
//! membership of one canonical key is at most one row". Every write here
//! is a single statement so a transaction never observes a two-current
//! window; multi-row moves (merge / split) compose
//! `redirect_current_tx` + `insert_tx` inside one app-layer transaction,
//! in that order, because inserting the destination current row first
//! would collide with the still-current source row.
//!
//! `thread_id` / `deleted_at` / state transitions mirror the DDL CHECK
//! shapes exactly (deleted ⇒ thread_id NULL + deleted_at set; active ⇒
//! thread_id set). The repository does not enforce the state machine —
//! the `expected_state`-style WHERE guards just make each transition
//! single-shot and race-visible via the affected-row count.

use super::rows::{NewThreadGroupMember, THREAD_GROUP_MEMBER_COLUMNS, ThreadGroupMemberRow};
use crate::error::LlmMemoryError;
use crate::infra::{fill_timestamps, fill_updated_at};
use crate::sql::{IN_LIST_CHUNK_SIZE, build_in_placeholders, p};
use anyhow::Result;
use async_trait::async_trait;
use infra_utils::infra::rdb::{Rdb, RdbPool, UseRdbPool};
use sqlx::Executor;

const INSERT_SQL: &str = concat!(
    "INSERT INTO thread_group_member \
     (group_id, thread_id, thread_canonical_key, owner_scope, source, identity_scope, native_id, \
      role, state, provenance, deleted_at, created_at, updated_at) \
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

// Current membership of one canonical key: the partial UNIQUE index
// guarantees at most one row.
const FIND_CURRENT_SQL: &str = concat!(
    "SELECT ",
    THREAD_GROUP_MEMBER_COLUMNS!(),
    " FROM thread_group_member WHERE thread_canonical_key = ",
    p!(1),
    " AND state IN ('active', 'deleted')"
);

const FIND_CURRENT_BY_KEYS_SQL_PREFIX: &str = concat!(
    "SELECT ",
    THREAD_GROUP_MEMBER_COLUMNS!(),
    " FROM thread_group_member WHERE thread_canonical_key IN ("
);

const LIST_CURRENT_BY_GROUP_SQL: &str = concat!(
    "SELECT ",
    THREAD_GROUP_MEMBER_COLUMNS!(),
    " FROM thread_group_member WHERE group_id = ",
    p!(1),
    " AND state IN ('active', 'deleted') ORDER BY thread_canonical_key"
);

const EXISTS_SOURCE_IDENTITY_OUTSIDE_GROUP_SQL: &str = concat!(
    "SELECT 1 FROM thread_group_member WHERE owner_scope = ",
    p!(1),
    " AND source = ",
    p!(2),
    " AND identity_scope = ",
    p!(3),
    " AND native_id = ",
    p!(4),
    " AND group_id <> ",
    p!(5),
    " LIMIT 1"
);

const LIST_ALL_BY_GROUP_SQL: &str = concat!(
    "SELECT ",
    THREAD_GROUP_MEMBER_COLUMNS!(),
    " FROM thread_group_member WHERE group_id = ",
    p!(1),
    " ORDER BY thread_canonical_key, state"
);

const COUNT_CURRENT_BY_GROUP_SQL: &str = concat!(
    "SELECT COUNT(*) FROM thread_group_member WHERE group_id = ",
    p!(1),
    " AND state IN ('active', 'deleted')"
);

// Thread row was deleted: active -> deleted placeholder (the CHECK
// requires thread_id NULL and deleted_at non-null together).
const MARK_CURRENT_DELETED_SQL: &str = concat!(
    "UPDATE thread_group_member SET state = 'deleted', thread_id = NULL, deleted_at = ",
    p!(1),
    ", updated_at = ",
    p!(2),
    " WHERE thread_canonical_key = ",
    p!(3),
    " AND state = 'active'"
);

// Revival updates only the current deleted row (design 5.2) and
// reconnects the fresh thread row.
const REVIVE_CURRENT_SQL: &str = concat!(
    "UPDATE thread_group_member SET state = 'active', thread_id = ",
    p!(1),
    ", deleted_at = NULL, updated_at = ",
    p!(2),
    " WHERE thread_canonical_key = ",
    p!(3),
    " AND state = 'deleted'"
);

// Move step 1 (merge / split): demote the current row of one group to
// redirected history. `deleted_at` / state carried into the destination
// row is read from the returned row *before* this update, so callers
// compose find_current_tx -> redirect_current_tx -> insert_tx.
const REDIRECT_CURRENT_SQL: &str = concat!(
    "UPDATE thread_group_member SET state = 'redirected', updated_at = ",
    p!(1),
    " WHERE thread_canonical_key = ",
    p!(2),
    " AND group_id = ",
    p!(3),
    " AND state IN ('active', 'deleted')"
);

// Membership pinning / root designation: role + provenance on the
// current row only.
const SET_ROLE_PROVENANCE_SQL: &str = concat!(
    "UPDATE thread_group_member SET role = ",
    p!(1),
    ", provenance = ",
    p!(2),
    ", updated_at = ",
    p!(3),
    " WHERE thread_canonical_key = ",
    p!(4),
    " AND state IN ('active', 'deleted')"
);

// Purge support: drop every membership row (including redirected
// history) that points at a group being removed.
const DELETE_BY_GROUP_SQL: &str =
    concat!("DELETE FROM thread_group_member WHERE group_id = ", p!(1));

// Read-model helper (design 5.1): latest_activity_at is derived, never
// stored — max conversation time over current *active* members whose
// thread row still exists. Deleted placeholders contribute nothing by
// construction (thread_id is NULL, so the join drops them).
const FIND_LATEST_ACTIVITY_SQL: &str = concat!(
    "SELECT MAX(t.last_message_at) FROM thread_group_member m \
     JOIN thread t ON t.id = m.thread_id \
     WHERE m.group_id = ",
    p!(1),
    " AND m.state = 'active'"
);

#[async_trait]
pub trait ThreadGroupMemberRepository: UseRdbPool + Send + Sync {
    /// Insert one membership row (any state). Inserting a second
    /// *current* row for the same canonical key fails on the partial
    /// UNIQUE index.
    async fn insert_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        member: &NewThreadGroupMember,
    ) -> Result<()> {
        let (created_at, updated_at) = fill_timestamps(member.created_at, member.updated_at);
        sqlx::query::<Rdb>(INSERT_SQL)
            .bind(member.group_id)
            .bind(member.thread_id)
            .bind(&member.thread_canonical_key)
            .bind(&member.owner_scope)
            .bind(&member.source)
            .bind(&member.identity_scope)
            .bind(&member.native_id)
            .bind(&member.role)
            .bind(&member.state)
            .bind(&member.provenance)
            .bind(member.deleted_at)
            .bind(created_at)
            .bind(updated_at)
            .execute(tx)
            .await
            .map_err(LlmMemoryError::DBError)?;
        Ok(())
    }

    /// Idempotent lookup of the current membership ('active' or
    /// 'deleted') of a thread canonical key.
    async fn find_current_by_thread_canonical_key(
        &self,
        thread_canonical_key: &str,
    ) -> Result<Option<ThreadGroupMemberRow>> {
        Ok(
            sqlx::query_as::<Rdb, ThreadGroupMemberRow>(FIND_CURRENT_SQL)
                .bind(thread_canonical_key)
                .fetch_optional(self.db_pool())
                .await
                .map_err(LlmMemoryError::DBError)?,
        )
    }

    async fn find_current_by_thread_canonical_key_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        thread_canonical_key: &str,
    ) -> Result<Option<ThreadGroupMemberRow>> {
        Ok(
            sqlx::query_as::<Rdb, ThreadGroupMemberRow>(FIND_CURRENT_SQL)
                .bind(thread_canonical_key)
                .fetch_optional(tx)
                .await
                .map_err(LlmMemoryError::DBError)?,
        )
    }

    /// Batch lookup used to resolve both endpoints of cross-group relations
    /// without issuing one query per canonical key.
    async fn find_current_by_thread_canonical_keys(
        &self,
        thread_canonical_keys: &[String],
    ) -> Result<Vec<ThreadGroupMemberRow>> {
        if thread_canonical_keys.is_empty() {
            return Ok(Vec::new());
        }
        let mut rows = Vec::new();
        for chunk in thread_canonical_keys.chunks(IN_LIST_CHUNK_SIZE) {
            let sql = format!(
                "{}{}) AND state IN ('active', 'deleted')",
                FIND_CURRENT_BY_KEYS_SQL_PREFIX,
                build_in_placeholders(chunk.len(), 1)
            );
            let mut query = sqlx::query_as::<Rdb, ThreadGroupMemberRow>(sqlx::AssertSqlSafe(sql));
            for key in chunk {
                query = query.bind(key);
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

    async fn list_current_by_group_id(&self, group_id: i64) -> Result<Vec<ThreadGroupMemberRow>> {
        Ok(
            sqlx::query_as::<Rdb, ThreadGroupMemberRow>(LIST_CURRENT_BY_GROUP_SQL)
                .bind(group_id)
                .fetch_all(self.db_pool())
                .await
                .map_err(LlmMemoryError::DBError)?,
        )
    }

    /// True when any membership row outside `exclude_group_id` still
    /// references the owner-local source identity. Used by the purge to
    /// retain a deletion marker that is shared with another group's
    /// history.
    async fn exists_source_identity_outside_group(
        &self,
        owner_scope: &str,
        source: &str,
        identity_scope: &str,
        native_id: &str,
        exclude_group_id: i64,
    ) -> Result<bool> {
        let found = sqlx::query_scalar::<_, i64>(EXISTS_SOURCE_IDENTITY_OUTSIDE_GROUP_SQL)
            .bind(owner_scope)
            .bind(source)
            .bind(identity_scope)
            .bind(native_id)
            .bind(exclude_group_id)
            .fetch_optional(self.db_pool())
            .await
            .map_err(LlmMemoryError::DBError)?;
        Ok(found.is_some())
    }

    /// In-transaction variant of
    /// `exists_source_identity_outside_group` for the purge marker check.
    async fn exists_source_identity_outside_group_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        owner_scope: &str,
        source: &str,
        identity_scope: &str,
        native_id: &str,
        exclude_group_id: i64,
    ) -> Result<bool> {
        let found = sqlx::query_scalar::<_, i64>(EXISTS_SOURCE_IDENTITY_OUTSIDE_GROUP_SQL)
            .bind(owner_scope)
            .bind(source)
            .bind(identity_scope)
            .bind(native_id)
            .bind(exclude_group_id)
            .fetch_optional(tx)
            .await
            .map_err(LlmMemoryError::DBError)?;
        Ok(found.is_some())
    }

    /// Every membership row of a group, including redirected history.
    /// Used by the inactive-history purge preview.
    async fn list_all_by_group_id(&self, group_id: i64) -> Result<Vec<ThreadGroupMemberRow>> {
        Ok(
            sqlx::query_as::<Rdb, ThreadGroupMemberRow>(LIST_ALL_BY_GROUP_SQL)
                .bind(group_id)
                .fetch_all(self.db_pool())
                .await
                .map_err(LlmMemoryError::DBError)?,
        )
    }

    async fn list_current_by_group_id_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        group_id: i64,
    ) -> Result<Vec<ThreadGroupMemberRow>> {
        Ok(
            sqlx::query_as::<Rdb, ThreadGroupMemberRow>(LIST_CURRENT_BY_GROUP_SQL)
                .bind(group_id)
                .fetch_all(tx)
                .await
                .map_err(LlmMemoryError::DBError)?,
        )
    }

    /// Empty-group detection counts current membership only
    /// ('active' + 'deleted'; design 5.2).
    async fn count_current_by_group_id_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        group_id: i64,
    ) -> Result<i64> {
        Ok(sqlx::query_scalar::<Rdb, i64>(COUNT_CURRENT_BY_GROUP_SQL)
            .bind(group_id)
            .fetch_one(tx)
            .await
            .map_err(LlmMemoryError::DBError)?)
    }

    /// Bulk current-membership lookup by live thread ids (read model /
    /// group expansion). Chunked for the SQLite bind cap.
    async fn list_current_by_thread_ids(
        &self,
        thread_ids: &[i64],
    ) -> Result<Vec<ThreadGroupMemberRow>> {
        let mut rows = Vec::new();
        for chunk in thread_ids.chunks(IN_LIST_CHUNK_SIZE) {
            if chunk.is_empty() {
                continue;
            }
            let placeholders = build_in_placeholders(chunk.len(), 1);
            let cols = THREAD_GROUP_MEMBER_COLUMNS!();
            let sql = format!(
                "SELECT {cols} FROM thread_group_member \
                 WHERE thread_id IN ({placeholders}) AND state IN ('active', 'deleted') \
                 ORDER BY thread_canonical_key"
            );
            let mut query = sqlx::query_as::<Rdb, ThreadGroupMemberRow>(sqlx::AssertSqlSafe(sql));
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

    /// `active -> deleted` placeholder (thread row removal keeps the
    /// node in the lineage tree). Returns false when the current
    /// membership is not active.
    async fn mark_current_deleted_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        thread_canonical_key: &str,
        deleted_at: i64,
        updated_at: i64,
    ) -> Result<bool> {
        let updated_at = fill_updated_at(updated_at);
        let res = sqlx::query::<Rdb>(MARK_CURRENT_DELETED_SQL)
            .bind(deleted_at)
            .bind(updated_at)
            .bind(thread_canonical_key)
            .execute(tx)
            .await
            .map_err(LlmMemoryError::DBError)?;
        Ok(res.rows_affected() > 0)
    }

    /// `deleted -> active` revival attaching the freshly created thread
    /// row. Only the current deleted row is touched; redirected history
    /// rows never revive.
    async fn revive_current_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        thread_canonical_key: &str,
        thread_id: i64,
        updated_at: i64,
    ) -> Result<bool> {
        let updated_at = fill_updated_at(updated_at);
        let res = sqlx::query::<Rdb>(REVIVE_CURRENT_SQL)
            .bind(thread_id)
            .bind(updated_at)
            .bind(thread_canonical_key)
            .execute(tx)
            .await
            .map_err(LlmMemoryError::DBError)?;
        Ok(res.rows_affected() > 0)
    }

    /// Move step 1: demote the current membership of `group_id` to
    /// redirected history. Returns false when the key's current row is
    /// not in the expected group (another transaction moved it first —
    /// the app layer retries serializably).
    async fn redirect_current_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        thread_canonical_key: &str,
        expected_group_id: i64,
        updated_at: i64,
    ) -> Result<bool> {
        let updated_at = fill_updated_at(updated_at);
        let res = sqlx::query::<Rdb>(REDIRECT_CURRENT_SQL)
            .bind(updated_at)
            .bind(thread_canonical_key)
            .bind(expected_group_id)
            .execute(tx)
            .await
            .map_err(LlmMemoryError::DBError)?;
        Ok(res.rows_affected() > 0)
    }

    /// Designate root / pin membership provenance on the current row.
    async fn set_role_provenance_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        thread_canonical_key: &str,
        role: &str,
        provenance: &str,
        updated_at: i64,
    ) -> Result<bool> {
        let updated_at = fill_updated_at(updated_at);
        let res = sqlx::query::<Rdb>(SET_ROLE_PROVENANCE_SQL)
            .bind(role)
            .bind(provenance)
            .bind(updated_at)
            .bind(thread_canonical_key)
            .execute(tx)
            .await
            .map_err(LlmMemoryError::DBError)?;
        Ok(res.rows_affected() > 0)
    }

    /// Purge: delete every membership row (current + redirected
    /// history) of one group. Returns the affected-row count.
    async fn delete_by_group_id_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        group_id: i64,
    ) -> Result<u64> {
        let res = sqlx::query::<Rdb>(DELETE_BY_GROUP_SQL)
            .bind(group_id)
            .execute(tx)
            .await
            .map_err(LlmMemoryError::DBError)?;
        Ok(res.rows_affected())
    }

    /// Derived `latest_activity_at` for one group (design 5.1): NULL
    /// when no current active member has a live thread row with a
    /// conversation time. Sorting NULLs last is the read model's
    /// concern, not this query's.
    async fn find_latest_activity_at_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        group_id: i64,
    ) -> Result<Option<i64>> {
        Ok(
            sqlx::query_scalar::<Rdb, Option<i64>>(FIND_LATEST_ACTIVITY_SQL)
                .bind(group_id)
                .fetch_one(tx)
                .await
                .map_err(LlmMemoryError::DBError)?,
        )
    }
}

pub struct ThreadGroupMemberRepositoryImpl {
    pool: &'static RdbPool,
}

impl ThreadGroupMemberRepositoryImpl {
    pub fn new(pool: &'static RdbPool) -> Self {
        Self { pool }
    }
}

impl UseRdbPool for ThreadGroupMemberRepositoryImpl {
    fn db_pool(&self) -> &RdbPool {
        self.pool
    }
}

impl ThreadGroupMemberRepository for ThreadGroupMemberRepositoryImpl {}

#[cfg(all(test, not(feature = "postgres")))]
mod tests {
    use super::*;
    use crate::infra::thread_group::group::{ThreadGroupRepository, ThreadGroupRepositoryImpl};
    use crate::infra::thread_group::rows::values;
    use crate::infra::thread_group::test_support::*;
    use anyhow::Context;
    use infra_utils::infra::test::TEST_RUNTIME;

    async fn _test_membership_lifecycle(pool: &'static RdbPool) -> Result<()> {
        let groups =
            ThreadGroupRepositoryImpl::new(crate::test_helper::shared_id_generator(), pool);
        let members = ThreadGroupMemberRepositoryImpl::new(pool);
        let g_a = groups.create_tx(pool, &new_group(11)).await?;
        let g_b = groups.create_tx(pool, &new_group(12)).await?;
        insert_thread(pool, 1100, Some(T0 + 5)).await;
        let k = key(101);

        // Attach as active member of g_a.
        let mut tx = pool.begin().await?;
        members
            .insert_tx(&mut *tx, &new_member(g_a, Some(1100), k.clone()))
            .await?;
        // A second current row for the same canonical key (any group)
        // collides with the partial UNIQUE index.
        let dup = members
            .insert_tx(&mut *tx, &new_member(g_b, Some(1100), k.clone()))
            .await;
        assert!(dup.is_err());
        tx.rollback().await?;

        // Re-attach inside a clean transaction for the rest of the flow.
        let mut tx = pool.begin().await?;
        members
            .insert_tx(&mut *tx, &new_member(g_a, Some(1100), k.clone()))
            .await?;
        tx.commit().await?;

        let current = members
            .find_current_by_thread_canonical_key(&k)
            .await?
            .context("current membership")?;
        assert_eq!(current.group_id, g_a);
        assert_eq!(current.state, values::member_state::ACTIVE);

        // deleted placeholder: CHECK forces thread_id NULL + deleted_at set.
        let mut tx = pool.begin().await?;
        assert!(
            members
                .mark_current_deleted_tx(&mut *tx, &k, T0 + 1, T0 + 1)
                .await?
        );
        // 'deleted' still counts as current membership…
        assert_eq!(
            members.count_current_by_group_id_tx(&mut *tx, g_a).await?,
            1
        );
        // …and contributes nothing to the derived latest activity.
        assert_eq!(
            members.find_latest_activity_at_tx(&mut *tx, g_a).await?,
            None
        );
        tx.commit().await?;
        let current = members
            .find_current_by_thread_canonical_key(&k)
            .await?
            .context("deleted placeholder")?;
        assert!(current.thread_id.is_none());
        assert_eq!(current.deleted_at, Some(T0 + 1));

        // Invalid shape: 'deleted' with a live thread_id violates CHECK.
        let mut bad = new_member(g_b, Some(1100), key(102));
        bad.state = values::member_state::DELETED.to_string();
        assert!(members.insert_tx(pool, &bad).await.is_err());

        // Revival updates the current deleted row in place.
        let mut tx = pool.begin().await?;
        assert!(
            members
                .revive_current_tx(&mut *tx, &k, 1100, T0 + 2)
                .await?
        );
        // Active member of a live thread carries the conversation time.
        assert_eq!(
            members.find_latest_activity_at_tx(&mut *tx, g_a).await?,
            Some(T0 + 5)
        );
        tx.commit().await?;

        // Move (merge/split step): redirect then insert on destination.
        // The deleted placeholder state was just exercised above; the
        // move here carries the revived active state.
        let mut tx = pool.begin().await?;
        assert!(
            members
                .redirect_current_tx(&mut *tx, &k, g_b, T0 + 3)
                .await
                .is_ok_and(|v| !v),
            "wrong expected group must not redirect"
        );
        assert!(
            members
                .redirect_current_tx(&mut *tx, &k, g_a, T0 + 3)
                .await?
        );
        let mut moved = new_member(g_b, Some(1100), k.clone());
        moved.created_at = T0 + 3;
        moved.updated_at = T0 + 3;
        members.insert_tx(&mut *tx, &moved).await?;
        assert_eq!(
            members.count_current_by_group_id_tx(&mut *tx, g_a).await?,
            0
        );
        assert_eq!(
            members.count_current_by_group_id_tx(&mut *tx, g_b).await?,
            1
        );
        tx.commit().await?;
        let current = members
            .find_current_by_thread_canonical_key(&k)
            .await?
            .context("moved membership")?;
        assert_eq!(current.group_id, g_b);
        assert_eq!(current.state, values::member_state::ACTIVE);

        // Pinning: role / provenance only ever touch the current row.
        let mut tx = pool.begin().await?;
        assert!(
            members
                .set_role_provenance_tx(
                    &mut *tx,
                    &k,
                    values::member_role::ROOT,
                    values::grouping_authority::OPERATOR,
                    T0 + 4
                )
                .await?
        );
        tx.commit().await?;
        let current = members
            .find_current_by_thread_canonical_key(&k)
            .await?
            .context("pinned membership")?;
        assert_eq!(current.role, values::member_role::ROOT);
        assert_eq!(current.provenance, values::grouping_authority::OPERATOR);

        // Bulk lookup by live thread ids + group listing.
        let bulk = members.list_current_by_thread_ids(&[1100]).await?;
        assert!(bulk.iter().any(|m| m.group_id == g_b));
        let in_b = members.list_current_by_group_id(g_b).await?;
        assert_eq!(in_b.len(), 1);
        assert!(members.list_current_by_group_id(g_a).await?.is_empty());

        // Purge deletes the redirected history row of the old group.
        let mut tx = pool.begin().await?;
        assert_eq!(members.delete_by_group_id_tx(&mut *tx, g_a).await?, 1);
        tx.commit().await?;
        Ok(())
    }

    #[test]
    fn membership_lifecycle_sqlite() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let pool = setup_thread_group_pool().await;
            _test_membership_lifecycle(pool).await
        })
    }
}
