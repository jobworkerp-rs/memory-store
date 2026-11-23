use super::*;
use crate::db_migrate::schema::history::table_exists;
use crate::db_migrate::schema::migrations::{HISTORY_TABLE, versions};
use crate::db_migrate::schema::test_support::{
    self, FULL_FIXTURE, LEGACY_FIXTURES, TestDatabase, inspect,
};
use std::borrow::Cow;

async fn history_rows(database: &TestDatabase) -> i64 {
    database
        .scalar_i64(&format!("SELECT COUNT(*) FROM {HISTORY_TABLE}"))
        .await
}

async fn has_table(database: &TestDatabase, table: &str) -> bool {
    let mut conn = database.conn().await;
    table_exists(&mut conn, table).await.unwrap()
}

#[tokio::test]
async fn an_empty_database_gets_the_whole_catalog() {
    let database = test_support::empty().await;
    let planned = plan(&database.pool, database.location).await.unwrap();
    assert_eq!(planned.inspection.state, SchemaState::Uninitialized);
    assert!(!planned.inspection.handover_required());
    assert_eq!(planned.pending.len(), versions().len());

    let report = apply(&database.pool, database.location).await.unwrap();
    assert!(!report.handed_over);
    assert_eq!(report.applied, versions());
    assert_eq!(inspect(&database).await.state, SchemaState::Managed);
    assert_eq!(history_rows(&database).await, versions().len() as i64);

    let again = apply(&database.pool, database.location).await.unwrap();
    assert_eq!(
        again,
        ApplyReport {
            handed_over: false,
            applied: Vec::new()
        }
    );
    database.close().await;
}

#[tokio::test]
async fn legacy_fixtures_are_handed_over_and_completed() {
    for &name in LEGACY_FIXTURES {
        let database = test_support::from_fixture(name).await;
        let before = inspect(&database).await;
        let pending = before.state.pending_count().unwrap();

        let report = apply(&database.pool, database.location).await.unwrap();
        assert!(report.handed_over, "{name}");
        assert_eq!(
            report.applied,
            versions()[versions().len() - pending..].to_vec(),
            "{name}"
        );
        let after = inspect(&database).await;
        assert_eq!(after.state, SchemaState::Managed, "{name}");
        assert_eq!(after.legacy_history, None, "{name}");
        database.close().await;
    }
}

#[tokio::test]
async fn a_plan_changes_nothing() {
    for &name in LEGACY_FIXTURES {
        let database = test_support::from_fixture(name).await;
        let before = inspect(&database).await;
        let planned = plan(&database.pool, database.location).await.unwrap();
        assert!(planned.inspection.handover_required(), "{name}");
        assert_eq!(
            planned.pending.len(),
            before.state.pending_count().unwrap(),
            "{name}"
        );
        assert_eq!(inspect(&database).await, before, "{name}");
        assert!(!has_table(&database, HISTORY_TABLE).await, "{name}");
        database.close().await;
    }

    let database = test_support::empty().await;
    plan(&database.pool, database.location).await.unwrap();
    assert!(!has_table(&database, HISTORY_TABLE).await);
    database.close().await;
}

#[tokio::test]
async fn an_unmanaged_database_is_refused_untouched() {
    let database = test_support::empty().await;
    database.execute("CREATE TABLE thread (id BIGINT)").await;
    assert_eq!(inspect(&database).await.state, SchemaState::Unmanaged);

    let error = apply(&database.pool, database.location).await.unwrap_err();
    assert!(error.to_string().starts_with("unmanaged"), "{error:#}");
    assert!(!has_table(&database, HISTORY_TABLE).await);
    database.close().await;
}

#[tokio::test]
async fn a_corrupt_history_is_refused_untouched() {
    let database = test_support::from_fixture(FULL_FIXTURE).await;
    database
        .execute(&format!(
            "UPDATE {} SET hash = 'tampered' WHERE version = '20260920000001'",
            database.location.qualified_table()
        ))
        .await;
    let error = apply(&database.pool, database.location).await.unwrap_err();
    assert!(error.to_string().starts_with("schema_corrupt"), "{error:#}");
    assert!(!has_table(&database, HISTORY_TABLE).await);
    database.close().await;
}

#[tokio::test]
async fn a_changed_applied_migration_is_corrupt() {
    let database = test_support::empty().await;
    apply(&database.pool, database.location).await.unwrap();
    database
        .execute(&format!(
            "UPDATE {HISTORY_TABLE} SET checksum = {} WHERE version = 20260920000001",
            if cfg!(feature = "postgres") {
                "'\\x00'::bytea"
            } else {
                "x'00'"
            }
        ))
        .await;
    assert_eq!(inspect(&database).await.state, SchemaState::SchemaCorrupt);
    let error = apply(&database.pool, database.location).await.unwrap_err();
    assert!(error.to_string().starts_with("schema_corrupt"), "{error:#}");
    database.close().await;
}

#[tokio::test]
async fn a_database_of_a_newer_release_is_refused() {
    let database = test_support::empty().await;
    apply(&database.pool, database.location).await.unwrap();
    let newer = 20991231000001_i64;
    database
        .execute(&format!(
            "INSERT INTO {HISTORY_TABLE} (version, description, success, checksum, execution_time) \
             VALUES ({newer}, 'future', TRUE, {}, 1); \
             UPDATE memories_schema_contract SET version = '{newer}'",
            if cfg!(feature = "postgres") {
                "'\\x01'::bytea"
            } else {
                "x'01'"
            }
        ))
        .await;
    assert_eq!(inspect(&database).await.state, SchemaState::NewerThanTool);
    let error = apply(&database.pool, database.location).await.unwrap_err();
    assert!(
        error.to_string().starts_with("newer_than_tool"),
        "{error:#}"
    );
    database.close().await;
}

#[tokio::test]
async fn a_failed_migration_rolls_back_and_is_not_recorded() {
    let database = test_support::empty().await;
    let broken_version = 20991231000001_i64;
    let mut catalog = migrations::migrations().to_vec();
    catalog.push(sqlx::migrate::Migration::new(
        broken_version,
        Cow::Borrowed("broken"),
        sqlx::migrate::MigrationType::Simple,
        sqlx::SqlSafeStr::into_sql_str(sqlx::AssertSqlSafe(
            "CREATE TABLE left_behind (id BIGINT); SELECT * FROM no_such_table;",
        )),
        false,
    ));
    let mut migrator = sqlx::migrate::Migrator::with_migrations(catalog);
    migrator.dangerous_set_table_name(HISTORY_TABLE);

    let error = apply_with(&database.pool, database.location, &migrator)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("20991231000001"), "{error:#}");
    assert!(!has_table(&database, "left_behind").await);
    assert_eq!(
        database
            .scalar_i64(&format!(
                "SELECT COUNT(*) FROM {HISTORY_TABLE} WHERE version = {broken_version}"
            ))
            .await,
        0
    );
    // Everything before the broken migration was applied and recorded.
    assert_eq!(inspect(&database).await.state, SchemaState::Managed);
    database.close().await;
}

#[tokio::test]
async fn concurrent_applies_apply_each_migration_once() {
    let database = test_support::empty().await;
    let (first, second) = tokio::join!(
        apply(&database.pool, database.location),
        apply(&database.pool, database.location)
    );
    // The loser may fail on the winner's tables, but nothing is applied twice.
    assert!(first.is_ok() || second.is_ok(), "{first:?} {second:?}");
    assert_eq!(inspect(&database).await.state, SchemaState::Managed);
    assert_eq!(history_rows(&database).await, versions().len() as i64);
    database.close().await;
}
