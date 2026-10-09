//! Read-only checks that must pass before a local migration changes anything.
//!
//! The released data tasks join ThreadGroup rows with INNER JOINs, so orphaned
//! references would silently escape them; these checks fail closed instead.

use super::output::{ErrorCode, Resolution, fail};
use anyhow::{Context, Result};
use infra_utils::infra::rdb::RdbPool;

const ORPHAN_REFERENCE_CHECKS: &[(&str, &str)] = &[
    (
        "ThreadGroup members whose group does not exist",
        "SELECT COUNT(*) FROM thread_group_member member \
         LEFT JOIN thread_group grp ON grp.id = member.group_id WHERE grp.id IS NULL",
    ),
    (
        "ThreadGroup members referencing a Thread that does not exist",
        "SELECT COUNT(*) FROM thread_group_member member \
         LEFT JOIN thread ON thread.id = member.thread_id \
         WHERE member.thread_id IS NOT NULL AND thread.id IS NULL",
    ),
];

/// Fail with `integrity_violation` when ThreadGroup rows reference rows that
/// do not exist. Databases created before ThreadGroups have nothing to check.
pub async fn check_thread_group_references(pool: &RdbPool) -> Result<()> {
    if !table_exists(pool, "thread_group").await? {
        return Ok(());
    }
    for table in ["thread_group_member", "thread"] {
        if !table_exists(pool, table).await? {
            return violation(format!("thread_group exists but {table} is missing"));
        }
    }
    for (description, sql) in ORPHAN_REFERENCE_CHECKS {
        let count: i64 = sqlx::query_scalar(*sql)
            .fetch_one(pool)
            .await
            .with_context(|| format!("checking {description}"))?;
        if count > 0 {
            return violation(format!("{count} {description}"));
        }
    }
    Ok(())
}

/// Reject databases that the adoption baseline cannot represent: those created
/// before `memory_kind`, whose migration needed operator-approved pruning and
/// online re-embedding and is therefore done only by the older releases that
/// shipped it, and those whose `memory` / `thread` layouts disagree.
pub async fn check_memory_kind_era(pool: &RdbPool) -> Result<()> {
    let memory = memory_kind_column(pool, "memory").await?;
    let thread = memory_kind_column(pool, "thread").await?;
    match (memory, thread) {
        (Column::TableMissing, Column::TableMissing) | (Column::Required, Column::Required) => {
            Ok(())
        }
        (Column::Missing | Column::Optional, Column::Missing | Column::Optional) => fail(
            ErrorCode::LegacySchemaUnsupported,
            Resolution::LegacyUpgradeRequired,
            "the database predates memory_kind; migrate it with the older releases first",
        ),
        _ => fail(
            ErrorCode::InvalidTarget,
            Resolution::ToolUpdateRequired,
            "memory and thread tables disagree on the memory_kind schema",
        ),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Column {
    TableMissing,
    Missing,
    /// Present but nullable: the expand stage of the old migration.
    Optional,
    Required,
}

async fn memory_kind_column(pool: &RdbPool, table: &str) -> Result<Column> {
    if !table_exists(pool, table).await? {
        return Ok(Column::TableMissing);
    }
    let columns: Vec<(String, bool)> =
        sqlx::query_as("SELECT name, \"notnull\" FROM pragma_table_info(?)")
            .bind(table)
            .fetch_all(pool)
            .await
            .with_context(|| format!("reading the columns of {table}"))?;
    Ok(
        match columns.iter().find(|(name, _)| name == "memory_kind") {
            None => Column::Missing,
            Some((_, false)) => Column::Optional,
            Some((_, true)) => Column::Required,
        },
    )
}

fn violation(message: String) -> Result<()> {
    // Retrying cannot change the data; a memories release must handle it.
    fail(
        ErrorCode::IntegrityViolation,
        Resolution::ToolUpdateRequired,
        message,
    )
}

async fn table_exists(pool: &RdbPool, table: &str) -> Result<bool> {
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?")
            .bind(table)
            .fetch_one(pool)
            .await
            .with_context(|| format!("checking whether {table} exists"))?;
    Ok(count > 0)
}

#[cfg(all(test, not(feature = "postgres")))]
mod tests {
    use super::*;
    use crate::db_migrate::local::output::{ErrorCode, LocalFailure, Resolution};
    use crate::db_migrate::thread_groups_user_ids_v3::tests::{seed_membership, test_pool};

    async fn violation(pool: &RdbPool) -> Option<ErrorCode> {
        check_thread_group_references(pool)
            .await
            .err()
            .and_then(|e| e.downcast_ref::<LocalFailure>().map(|f| f.error_code))
    }

    async fn legacy_outcome(ddl: &str) -> Result<(), Option<(ErrorCode, Resolution)>> {
        let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
        if !ddl.is_empty() {
            sqlx::raw_sql(sqlx::AssertSqlSafe(ddl.to_string()))
                .execute(&pool)
                .await
                .unwrap();
        }
        check_memory_kind_era(&pool).await.map_err(|error| {
            error
                .downcast_ref::<LocalFailure>()
                .map(|f| (f.error_code, f.resolution))
        })
    }

    #[test]
    fn databases_before_memory_kind_need_an_older_release_first() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            let legacy = Err(Some((
                ErrorCode::LegacySchemaUnsupported,
                Resolution::LegacyUpgradeRequired,
            )));
            // No memory_kind at all.
            assert_eq!(
                legacy_outcome("CREATE TABLE memory (id BIGINT); CREATE TABLE thread (id BIGINT);")
                    .await,
                legacy
            );
            // The interrupted expand stage of the old migration (nullable column).
            assert_eq!(
                legacy_outcome(
                    "CREATE TABLE memory (id BIGINT, memory_kind INTEGER); \
                     CREATE TABLE thread (id BIGINT, memory_kind INTEGER);"
                )
                .await,
                legacy
            );
        });
    }

    #[test]
    fn current_new_and_inconsistent_layouts_are_told_apart() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            assert_eq!(legacy_outcome("").await, Ok(()));
            assert_eq!(
                legacy_outcome(
                    "CREATE TABLE memory (id BIGINT, memory_kind INTEGER NOT NULL); \
                     CREATE TABLE thread (id BIGINT, memory_kind INTEGER NOT NULL);"
                )
                .await,
                Ok(())
            );
            let invalid = Err(Some((
                ErrorCode::InvalidTarget,
                Resolution::ToolUpdateRequired,
            )));
            assert_eq!(
                legacy_outcome("CREATE TABLE memory (id BIGINT, memory_kind INTEGER NOT NULL);")
                    .await,
                invalid
            );
            assert_eq!(
                legacy_outcome(
                    "CREATE TABLE memory (id BIGINT, memory_kind INTEGER NOT NULL); \
                     CREATE TABLE thread (id BIGINT);"
                )
                .await,
                invalid
            );
        });
    }

    #[test]
    fn consistent_thread_groups_pass() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            let pool = test_pool().await;
            seed_membership(pool, false).await;
            check_thread_group_references(pool).await.unwrap();
        });
    }

    #[test]
    fn database_before_thread_groups_passes() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
            check_thread_group_references(&pool).await.unwrap();
        });
    }

    #[test]
    fn member_without_group_is_a_violation() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            let pool = test_pool().await;
            let seed = seed_membership(pool, false).await;
            sqlx::query("DELETE FROM thread_group WHERE id = ?")
                .bind(seed.group_id)
                .execute(pool)
                .await
                .unwrap();
            assert_eq!(violation(pool).await, Some(ErrorCode::IntegrityViolation));
        });
    }

    #[test]
    fn member_referencing_a_missing_thread_is_a_violation() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            let pool = test_pool().await;
            let seed = seed_membership(pool, false).await;
            sqlx::query("DELETE FROM thread WHERE id = ?")
                .bind(seed.thread_id)
                .execute(pool)
                .await
                .unwrap();
            assert_eq!(violation(pool).await, Some(ErrorCode::IntegrityViolation));
        });
    }

    #[test]
    fn group_table_without_member_table_is_a_violation() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
            sqlx::query("CREATE TABLE thread_group (id BIGINT)")
                .execute(&pool)
                .await
                .unwrap();
            sqlx::query("CREATE TABLE thread (id BIGINT)")
                .execute(&pool)
                .await
                .unwrap();
            assert_eq!(violation(&pool).await, Some(ErrorCode::IntegrityViolation));
        });
    }
}
