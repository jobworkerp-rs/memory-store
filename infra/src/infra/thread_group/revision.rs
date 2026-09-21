//! Cheap read-model revision for the ThreadGroup search snapshot.
//!
//! The search cursor's snapshot token only needs to detect that the
//! underlying read model changed between pages. Materialising every
//! Memory / Group row to hash it scales with table size, so the token is
//! instead derived from backend-side aggregates (`COUNT(*)`,
//! `MAX(updated_at)`) that work identically on SQLite and PostgreSQL.

use anyhow::Result;
use async_trait::async_trait;
use infra_utils::infra::rdb::{Rdb, RdbPool, UseRdbPool};
use sqlx::Executor;

use crate::error::LlmMemoryError;

/// Backend aggregate for one table.
///
/// `updated_at_checksum` is the checksum of the table's mutation timestamp,
/// which changes
/// for any `updated_at` edit without the i64 overflow risk of a raw
/// `SUM(updated_at)` over a very large table, and works on both SQLite and
/// PostgreSQL.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadModelTableRevision {
    pub row_count: i64,
    pub max_updated_at: i64,
    pub updated_at_checksum: i64,
}

/// Aggregate revision of every table that can affect a ThreadGroup search
/// result: group / membership / relation / unresolved candidate
/// structure, plus the live Thread / Memory / ThreadLabel rows the member
/// queries read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ThreadGroupReadModelRevision {
    pub groups: ReadModelTableRevision,
    pub members: ReadModelTableRevision,
    pub relations: ReadModelTableRevision,
    pub candidate_associations: ReadModelTableRevision,
    pub threads: ReadModelTableRevision,
    pub memories: ReadModelTableRevision,
    pub thread_labels: ReadModelTableRevision,
}

#[async_trait]
pub trait ThreadGroupReadModelRevisionRepository: UseRdbPool + Send + Sync {
    async fn read_model_revision(&self) -> Result<ThreadGroupReadModelRevision> {
        Ok(ThreadGroupReadModelRevision {
            groups: table_revision(self.db_pool(), "thread_group", "updated_at").await?,
            members: table_revision(self.db_pool(), "thread_group_member", "updated_at").await?,
            relations: table_revision(self.db_pool(), "thread_relation", "updated_at").await?,
            candidate_associations: table_revision(
                self.db_pool(),
                "thread_group_candidate_association",
                "updated_at",
            )
            .await?,
            threads: table_revision(self.db_pool(), "thread", "updated_at").await?,
            memories: table_revision(self.db_pool(), "memory", "updated_at").await?,
            thread_labels: table_revision(self.db_pool(), "thread_label", "created_at").await?,
        })
    }
}

async fn table_revision<'e, E: Executor<'e, Database = Rdb>>(
    executor: E,
    table: &'static str,
    timestamp_column: &'static str,
) -> Result<ReadModelTableRevision> {
    // Table and timestamp names are compile-time constants from the impl
    // below, never caller input; identifiers cannot be bound parameters.
    let sql = format!(
        "SELECT COUNT(*), COALESCE(MAX({timestamp_column}), 0), \
         COALESCE(CAST(SUM({timestamp_column} % 2147483647) AS BIGINT), 0) FROM {table}"
    );
    let (row_count, max_updated_at, updated_at_checksum) =
        sqlx::query_as::<Rdb, (i64, i64, i64)>(sqlx::AssertSqlSafe(sql))
            .fetch_one(executor)
            .await
            .map_err(LlmMemoryError::DBError)?;
    Ok(ReadModelTableRevision {
        row_count,
        max_updated_at,
        updated_at_checksum,
    })
}

pub struct ThreadGroupReadModelRevisionRepositoryImpl {
    pool: &'static RdbPool,
}

impl ThreadGroupReadModelRevisionRepositoryImpl {
    pub fn new(pool: &'static RdbPool) -> Self {
        Self { pool }
    }
}

impl UseRdbPool for ThreadGroupReadModelRevisionRepositoryImpl {
    fn db_pool(&self) -> &RdbPool {
        self.pool
    }
}

impl ThreadGroupReadModelRevisionRepository for ThreadGroupReadModelRevisionRepositoryImpl {}

#[cfg(all(test, not(feature = "postgres")))]
mod tests {
    use super::*;

    #[tokio::test]
    async fn revision_tracks_counts_and_max_updated_at() {
        use crate::infra::thread_group::group::ThreadGroupRepository;
        use crate::infra::thread_group::group::ThreadGroupRepositoryImpl;
        use crate::infra::thread_group::member::ThreadGroupMemberRepository;
        use crate::infra::thread_group::member::ThreadGroupMemberRepositoryImpl;
        use crate::infra::thread_group::test_support::setup_thread_group_pool;
        use crate::infra::thread_group::test_support::{key, new_group, new_member};

        let pool = setup_thread_group_pool().await;
        let repo = ThreadGroupReadModelRevisionRepositoryImpl::new(pool);
        let before = repo.read_model_revision().await.expect("revision");

        let generator = crate::test_helper::shared_id_generator();
        let groups = ThreadGroupRepositoryImpl::new(generator, pool);
        let group_id = groups
            .create_tx(pool, &new_group(7_001))
            .await
            .expect("create group");
        let members = ThreadGroupMemberRepositoryImpl::new(pool);
        members
            .insert_tx(pool, &new_member(group_id, Some(30_001), key(7_001)))
            .await
            .expect("create member");

        let after = repo.read_model_revision().await.expect("revision");
        assert_eq!(after.groups.row_count, before.groups.row_count + 1);
        assert_eq!(after.members.row_count, before.members.row_count + 1);
        assert!(after.groups.max_updated_at >= before.groups.max_updated_at);

        use crate::infra::thread_label::rdb::{ThreadLabelRepository, ThreadLabelRepositoryImpl};
        let labels = ThreadLabelRepositoryImpl::new(pool);
        labels
            .add_labels(30_001, &["project:revision".to_string()], 9_999_999)
            .await
            .expect("create label");
        let labelled = repo.read_model_revision().await.expect("revision");
        assert_eq!(
            labelled.thread_labels.row_count,
            before.thread_labels.row_count + 1
        );

        // A sub-max `updated_at` edit still changes the checksum, so the
        // snapshot detects it even when MAX(updated_at) does not move.
        groups
            .update_title_tx(pool, group_id, Some("renamed"), 9_999_999)
            .await
            .expect("update title");
        let renamed = repo.read_model_revision().await.expect("revision");
        assert_ne!(
            renamed.groups.updated_at_checksum,
            after.groups.updated_at_checksum
        );
    }
}
