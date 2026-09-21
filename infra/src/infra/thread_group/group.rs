//! `thread_group` repository (design 5.1).
//!
//! Group identity (`group_canonical_key`) is derived in the app layer;
//! this repository stores it verbatim. The UNIQUE partial index makes
//! `find_active_by_canonical_key` the idempotent lookup for "does this
//! group already exist", and the transition primitives (`redirect_tx`,
//! `mark_split_tx`) keep the `status` / `redirect_to_group_id` pairing
//! CHECK satisfiable in a single statement each.

use super::rows::{NewThreadGroup, THREAD_GROUP_COLUMNS, ThreadGroupRow};
use crate::error::LlmMemoryError;
use crate::infra::{IdGeneratorWrapper, UseIdGenerator, fill_timestamps, fill_updated_at};
use crate::sql::{IN_LIST_CHUNK_SIZE, build_in_placeholders, dyn_placeholder, p};
use anyhow::Result;
use async_trait::async_trait;
use infra_utils::infra::rdb::{Rdb, RdbPool, UseRdbPool};
use sqlx::Executor;

const INSERT_SQL: &str = concat!(
    "INSERT INTO thread_group \
     (id, group_canonical_key, title, status, grouping_authority, redirect_to_group_id, created_at, updated_at) \
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
    ")"
);

const FIND_BY_ID_SQL: &str = concat!(
    "SELECT ",
    THREAD_GROUP_COLUMNS!(),
    " FROM thread_group WHERE id = ",
    p!(1)
);

const FIND_ACTIVE_BY_CANONICAL_KEY_SQL: &str = concat!(
    "SELECT ",
    THREAD_GROUP_COLUMNS!(),
    " FROM thread_group WHERE group_canonical_key = ",
    p!(1),
    " AND status = 'active'"
);

const UPDATE_TITLE_SQL: &str = concat!(
    "UPDATE thread_group SET title = ",
    p!(1),
    ", updated_at = ",
    p!(2),
    " WHERE id = ",
    p!(3)
);

const SET_GROUPING_AUTHORITY_SQL: &str = concat!(
    "UPDATE thread_group SET grouping_authority = ",
    p!(1),
    ", updated_at = ",
    p!(2),
    " WHERE id = ",
    p!(3)
);

// The WHERE guard pins the source status so a redirect never overwrites
// an already-redirected / split group (redirect targets must not chain
// from history; chain flattening is the app layer's job).
const REDIRECT_SQL: &str = concat!(
    "UPDATE thread_group SET status = 'redirected', redirect_to_group_id = ",
    p!(1),
    ", updated_at = ",
    p!(2),
    " WHERE id = ",
    p!(3),
    " AND status = 'active'"
);

// Flattening an existing redirect (already `redirected`) onto a new
// target when its current target is merged or purged.
const REPOINT_REDIRECT_SQL: &str = concat!(
    "UPDATE thread_group SET redirect_to_group_id = ",
    p!(1),
    ", updated_at = ",
    p!(2),
    " WHERE id = ",
    p!(3),
    " AND status = 'redirected'"
);

// A split group keeps `redirect_to_group_id` NULL; the pairing CHECK
// allows exactly that once the status leaves 'redirected'.
const MARK_SPLIT_SQL: &str = concat!(
    "UPDATE thread_group SET status = 'split', updated_at = ",
    p!(1),
    " WHERE id = ",
    p!(2),
    " AND status = 'active'"
);

const DELETE_SQL: &str = concat!("DELETE FROM thread_group WHERE id = ", p!(1));

#[async_trait]
pub trait ThreadGroupRepository: UseRdbPool + UseIdGenerator + Send + Sync {
    /// Insert a group row with a generated id. Returns the new id.
    /// A UNIQUE collision on `group_canonical_key` (active partial
    /// index) surfaces as a DB error; PostgreSQL aborts the enclosing
    /// transaction, so the caller must check
    /// `find_active_by_canonical_key_tx` first inside a write
    /// transaction and retry serializably on races.
    async fn create_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        group: &NewThreadGroup,
    ) -> Result<i64> {
        let id = self.id_generator().generate_id()?;
        let (created_at, updated_at) = fill_timestamps(group.created_at, group.updated_at);
        sqlx::query::<Rdb>(INSERT_SQL)
            .bind(id)
            .bind(&group.group_canonical_key)
            .bind(&group.title)
            .bind(&group.status)
            .bind(&group.grouping_authority)
            .bind(group.redirect_to_group_id)
            .bind(created_at)
            .bind(updated_at)
            .execute(tx)
            .await
            .map_err(LlmMemoryError::DBError)?;
        Ok(id)
    }

    /// Direct fetch by internal id — the resolution path for successor
    /// group ids recorded at split time (design 5.1: history is read by
    /// direct fetch, no chain walking).
    async fn find_by_id(&self, id: i64) -> Result<Option<ThreadGroupRow>> {
        Ok(sqlx::query_as::<Rdb, ThreadGroupRow>(FIND_BY_ID_SQL)
            .bind(id)
            .fetch_optional(self.db_pool())
            .await
            .map_err(LlmMemoryError::DBError)?)
    }

    /// Idempotent lookup over the active-key partial UNIQUE index.
    async fn find_active_by_canonical_key(
        &self,
        group_canonical_key: &str,
    ) -> Result<Option<ThreadGroupRow>> {
        Ok(
            sqlx::query_as::<Rdb, ThreadGroupRow>(FIND_ACTIVE_BY_CANONICAL_KEY_SQL)
                .bind(group_canonical_key)
                .fetch_optional(self.db_pool())
                .await
                .map_err(LlmMemoryError::DBError)?,
        )
    }

    /// In-transaction variant so an app write section observes its own
    /// uncommitted inserts (e.g. split successor created earlier in the
    /// same transaction).
    async fn find_active_by_canonical_key_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        group_canonical_key: &str,
    ) -> Result<Option<ThreadGroupRow>> {
        Ok(
            sqlx::query_as::<Rdb, ThreadGroupRow>(FIND_ACTIVE_BY_CANONICAL_KEY_SQL)
                .bind(group_canonical_key)
                .fetch_optional(tx)
                .await
                .map_err(LlmMemoryError::DBError)?,
        )
    }

    /// Bulk direct fetch (successor-id resolution). Chunked to stay
    /// below the SQLite bind-variable cap.
    async fn find_by_ids(&self, ids: &[i64]) -> Result<Vec<ThreadGroupRow>> {
        let mut rows = Vec::new();
        for chunk in ids.chunks(IN_LIST_CHUNK_SIZE) {
            if chunk.is_empty() {
                continue;
            }
            let placeholders = build_in_placeholders(chunk.len(), 1);
            let cols = THREAD_GROUP_COLUMNS!();
            let sql =
                format!("SELECT {cols} FROM thread_group WHERE id IN ({placeholders}) ORDER BY id");
            let mut query = sqlx::query_as::<Rdb, ThreadGroupRow>(sqlx::AssertSqlSafe(sql));
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

    /// Standard listing (active groups only per design 5.1; other
    /// statuses pass the token explicitly). `limit` / `offset` optional
    /// for paging, ordered by id for a stable cursor.
    async fn list_by_status(
        &self,
        status: &str,
        limit: Option<i64>,
        offset: Option<i64>,
    ) -> Result<Vec<ThreadGroupRow>> {
        let mut sql = String::from(concat!(
            "SELECT ",
            THREAD_GROUP_COLUMNS!(),
            " FROM thread_group WHERE status = "
        ));
        sql.push_str(&dyn_placeholder(1));
        sql.push_str(" ORDER BY id");
        let mut next = 2usize;
        if limit.is_some() {
            sql.push_str(&format!(" LIMIT {}", dyn_placeholder(next)));
            next += 1;
        }
        if offset.is_some() {
            sql.push_str(&format!(" OFFSET {}", dyn_placeholder(next)));
        }
        let mut query =
            sqlx::query_as::<Rdb, ThreadGroupRow>(sqlx::AssertSqlSafe(sql)).bind(status);
        if let Some(l) = limit {
            query = query.bind(l);
        }
        if let Some(o) = offset {
            query = query.bind(o);
        }
        Ok(query
            .fetch_all(self.db_pool())
            .await
            .map_err(LlmMemoryError::DBError)?)
    }

    async fn update_title_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        id: i64,
        title: Option<&str>,
        updated_at: i64,
    ) -> Result<bool> {
        let updated_at = fill_updated_at(updated_at);
        let res = sqlx::query::<Rdb>(UPDATE_TITLE_SQL)
            .bind(title)
            .bind(updated_at)
            .bind(id)
            .execute(tx)
            .await
            .map_err(LlmMemoryError::DBError)?;
        Ok(res.rows_affected() > 0)
    }

    /// Flip `grouping_authority` (e.g. operator claim on a reconciler
    /// group). The app layer owns the authority-change policy.
    async fn set_grouping_authority_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        id: i64,
        grouping_authority: &str,
        updated_at: i64,
    ) -> Result<bool> {
        let updated_at = fill_updated_at(updated_at);
        let res = sqlx::query::<Rdb>(SET_GROUPING_AUTHORITY_SQL)
            .bind(grouping_authority)
            .bind(updated_at)
            .bind(id)
            .execute(tx)
            .await
            .map_err(LlmMemoryError::DBError)?;
        Ok(res.rows_affected() > 0)
    }

    /// `active → redirected` pointing at the (active or split) target
    /// group. Returns false when the source is not active.
    async fn redirect_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        id: i64,
        redirect_to_group_id: i64,
        updated_at: i64,
    ) -> Result<bool> {
        let updated_at = fill_updated_at(updated_at);
        let res = sqlx::query::<Rdb>(REDIRECT_SQL)
            .bind(redirect_to_group_id)
            .bind(updated_at)
            .bind(id)
            .execute(tx)
            .await
            .map_err(LlmMemoryError::DBError)?;
        Ok(res.rows_affected() > 0)
    }

    /// Repoint an already-`redirected` group at a new target. Returns
    /// false when the row is not currently redirected.
    async fn repoint_redirect_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        id: i64,
        redirect_to_group_id: i64,
        updated_at: i64,
    ) -> Result<bool> {
        let updated_at = fill_updated_at(updated_at);
        let res = sqlx::query::<Rdb>(REPOINT_REDIRECT_SQL)
            .bind(redirect_to_group_id)
            .bind(updated_at)
            .bind(id)
            .execute(tx)
            .await
            .map_err(LlmMemoryError::DBError)?;
        Ok(res.rows_affected() > 0)
    }

    /// `active → split`. Returns false when the source is not active.
    async fn mark_split_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        id: i64,
        updated_at: i64,
    ) -> Result<bool> {
        let updated_at = fill_updated_at(updated_at);
        let res = sqlx::query::<Rdb>(MARK_SPLIT_SQL)
            .bind(updated_at)
            .bind(id)
            .execute(tx)
            .await
            .map_err(LlmMemoryError::DBError)?;
        Ok(res.rows_affected() > 0)
    }

    /// Purge a history group row (redirected / split). The app layer
    /// first removes member rows / audit references per its purge
    /// manifest; this primitive deletes exactly one row.
    async fn delete_tx<'c, E: Executor<'c, Database = Rdb>>(&self, tx: E, id: i64) -> Result<bool> {
        let res = sqlx::query::<Rdb>(DELETE_SQL)
            .bind(id)
            .execute(tx)
            .await
            .map_err(LlmMemoryError::DBError)?;
        Ok(res.rows_affected() > 0)
    }
}

pub struct ThreadGroupRepositoryImpl {
    pool: &'static RdbPool,
    id_generator: IdGeneratorWrapper,
}

impl ThreadGroupRepositoryImpl {
    pub fn new(id_generator: IdGeneratorWrapper, pool: &'static RdbPool) -> Self {
        Self { pool, id_generator }
    }
}

impl UseRdbPool for ThreadGroupRepositoryImpl {
    fn db_pool(&self) -> &RdbPool {
        self.pool
    }
}

impl UseIdGenerator for ThreadGroupRepositoryImpl {
    fn id_generator(&self) -> &IdGeneratorWrapper {
        &self.id_generator
    }
}

impl ThreadGroupRepository for ThreadGroupRepositoryImpl {}

#[cfg(all(test, not(feature = "postgres")))]
mod tests {
    use super::*;
    use crate::infra::thread_group::rows::values;
    use crate::infra::thread_group::test_support::*;
    use anyhow::Context;
    use infra_utils::infra::test::TEST_RUNTIME;

    async fn _test_group_lifecycle(pool: &'static RdbPool) -> Result<()> {
        let repo = ThreadGroupRepositoryImpl::new(crate::test_helper::shared_id_generator(), pool);

        // Create + direct fetch + idempotent active-key lookup.
        let g1 = repo
            .create_tx(pool, &new_group(1))
            .await
            .context("create g1")?;
        let row = repo.find_by_id(g1).await?.context("g1 by id")?;
        assert_eq!(row.group_canonical_key, key(1));
        assert_eq!(row.status, values::group_status::ACTIVE);
        assert!(repo.find_active_by_canonical_key(&key(1)).await?.is_some());

        // A second active group under the same canonical key collides
        // with the partial UNIQUE index.
        assert!(repo.create_tx(pool, &new_group(1)).await.is_err());

        // Declarative CHECK: 'redirected' requires a target.
        let mut broken = new_group(900);
        broken.status = values::group_status::REDIRECTED.to_string();
        assert!(repo.create_tx(pool, &broken).await.is_err());

        // Redirect g1 -> g2, then split g3 (a re-created active group
        // under the freed key).
        let g2 = repo.create_tx(pool, &new_group(2)).await?;
        assert!(repo.redirect_tx(pool, g1, g2, T0 + 1).await?);
        let row = repo.find_by_id(g1).await?.context("g1")?;
        assert_eq!(row.status, values::group_status::REDIRECTED);
        assert_eq!(row.redirect_to_group_id, Some(g2));
        // History keeps the key, but the active namespace is free again.
        assert!(repo.find_active_by_canonical_key(&key(1)).await?.is_none());
        let g3 = repo.create_tx(pool, &new_group(1)).await?;

        // Transitions are single-shot: neither redirect nor split can
        // fire from a non-active status.
        assert!(!repo.mark_split_tx(pool, g1, T0 + 2).await?);
        assert!(repo.mark_split_tx(pool, g3, T0 + 2).await?);
        assert!(!repo.redirect_tx(pool, g3, g2, T0 + 3).await?);
        let row = repo.find_by_id(g3).await?.context("g3")?;
        assert_eq!(row.status, values::group_status::SPLIT);
        assert_eq!(row.redirect_to_group_id, None);

        // Active listing excludes both history rows; bulk fetch (and
        // successor-id resolution) reads every status by id.
        let active = repo
            .list_by_status(values::group_status::ACTIVE, None, None)
            .await?;
        assert!(active.iter().any(|g| g.id == g2));
        assert!(!active.iter().any(|g| g.id == g1 || g.id == g3));
        let all = repo.find_by_ids(&[g1, g2, g3]).await?;
        assert_eq!(all.len(), 3);

        // Paging window is well-formed (page sizes are fixed so other
        // tests' rows cannot perturb the assertions).
        let g4 = repo.create_tx(pool, &new_group(4)).await?;
        let page = repo
            .list_by_status(values::group_status::ACTIVE, Some(1), Some(0))
            .await?;
        assert_eq!(page.len(), 1);
        let page2 = repo
            .list_by_status(values::group_status::ACTIVE, Some(1), Some(1))
            .await?;
        assert_eq!(page2.len(), 1);
        assert_ne!(page[0].id, page2[0].id);

        // Title / authority updates.
        assert!(
            repo.update_title_tx(pool, g4, Some("renamed"), T0 + 4)
                .await?
        );
        let row = repo.find_by_id(g4).await?.context("g4")?;
        assert_eq!(row.title.as_deref(), Some("renamed"));
        assert!(
            repo.set_grouping_authority_tx(pool, g4, values::grouping_authority::OPERATOR, T0 + 5)
                .await?
        );
        assert_eq!(
            repo.find_by_id(g4).await?.context("g4")?.grouping_authority,
            values::grouping_authority::OPERATOR
        );

        // Purge primitive deletes exactly the targeted history row.
        assert!(repo.delete_tx(pool, g1).await?);
        assert!(repo.find_by_id(g1).await?.is_none());
        Ok(())
    }

    #[test]
    fn group_lifecycle_sqlite() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let pool = setup_thread_group_pool().await;
            _test_group_lifecycle(pool).await
        })
    }
}
