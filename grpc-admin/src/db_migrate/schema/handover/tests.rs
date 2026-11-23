use super::*;
use crate::db_migrate::schema::test_support::{
    self, FULL_FIXTURE, LEGACY_FIXTURES, TestDatabase, inspect,
};

fn history_count() -> String {
    format!("SELECT COUNT(*) FROM {HISTORY_TABLE}")
}

async fn history_table_exists(database: &TestDatabase) -> bool {
    let mut conn = database.conn().await;
    history::table_exists(&mut conn, HISTORY_TABLE)
        .await
        .unwrap()
}

async fn legacy_history_exists(database: &TestDatabase) -> bool {
    let mut conn = database.conn().await;
    history::legacy_history_exists(&mut conn, database.location)
        .await
        .unwrap()
}

#[tokio::test]
async fn every_legacy_fixture_is_handed_over_once() {
    for &name in LEGACY_FIXTURES {
        let database = test_support::from_fixture(name).await;
        let before = inspect(&database).await;
        assert!(before.handover_required(), "{name}: {before:?}");
        let expected_versions = before
            .state
            .pending_count()
            .map_or(0, |pending| migrations::migrations().len() - pending);

        let outcome = hand_over(&database.pool, database.location).await.unwrap();
        assert_eq!(
            outcome,
            HandoverOutcome::HandedOver {
                versions: expected_versions
            },
            "{name}"
        );
        let after = inspect(&database).await;
        assert_eq!(after.state, before.state, "{name}");
        assert_eq!(after.legacy_history, None, "{name}");
        assert!(!legacy_history_exists(&database).await, "{name}");
        assert_eq!(
            database.scalar_i64(&history_count()).await,
            expected_versions as i64,
            "{name}"
        );
        assert_eq!(
            hand_over(&database.pool, database.location).await.unwrap(),
            HandoverOutcome::NotRequired,
            "{name}: a second handover changes nothing"
        );
        assert_eq!(inspect(&database).await, after, "{name}");
        database.close().await;
    }
}

#[tokio::test]
async fn applied_revisions_keep_their_legacy_duration() {
    let database = test_support::from_fixture(FULL_FIXTURE).await;
    hand_over(&database.pool, database.location).await.unwrap();
    // A fresh database applied every migration itself, so none of them is
    // a baseline without a duration.
    assert_eq!(
        database
            .scalar_i64(&format!(
                "SELECT COUNT(*) FROM {HISTORY_TABLE} WHERE execution_time <= 0"
            ))
            .await,
        0
    );
    database.close().await;
}

#[tokio::test]
async fn a_failed_handover_leaves_the_legacy_history_in_place() {
    let database = test_support::from_fixture(FULL_FIXTURE).await;
    let before = inspect(&database).await;

    let error = hand_over_with(&database.pool, database.location, true)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("injected"), "{error:#}");
    assert_eq!(inspect(&database).await, before);
    assert!(legacy_history_exists(&database).await);
    assert!(!history_table_exists(&database).await);

    // The rolled-back attempt can simply be repeated.
    assert!(matches!(
        hand_over(&database.pool, database.location).await.unwrap(),
        HandoverOutcome::HandedOver { .. }
    ));
    database.close().await;
}

#[tokio::test]
async fn a_changed_legacy_digest_is_refused_without_change() {
    let database = test_support::from_fixture(FULL_FIXTURE).await;
    database
        .execute(&format!(
            "UPDATE {} SET hash = 'tampered' WHERE version = '20260920000001'",
            database.location.qualified_table()
        ))
        .await;
    assert_eq!(inspect(&database).await.state, SchemaState::SchemaCorrupt);

    let error = hand_over(&database.pool, database.location)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("schema_corrupt"), "{error:#}");
    assert!(legacy_history_exists(&database).await);
    assert!(!history_table_exists(&database).await);
    database.close().await;
}

#[tokio::test]
async fn a_failed_legacy_attempt_is_refused_without_change() {
    for failure in [
        "error = 'statement failed', applied = 1",
        "applied = total - 1",
    ] {
        let database = test_support::from_fixture(FULL_FIXTURE).await;
        database
            .execute(&format!(
                "UPDATE {} SET {failure} WHERE version = '20261009000001'",
                database.location.qualified_table()
            ))
            .await;
        assert_eq!(
            inspect(&database).await.state,
            SchemaState::SchemaCorrupt,
            "{failure}"
        );

        let error = hand_over(&database.pool, database.location)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("schema_corrupt"), "{error:#}");
        assert!(legacy_history_exists(&database).await);
        assert!(!history_table_exists(&database).await);
        database.close().await;
    }
}

#[tokio::test]
async fn both_histories_together_are_corrupt() {
    let database = test_support::from_fixture(FULL_FIXTURE).await;
    hand_over(&database.pool, database.location).await.unwrap();
    // A legacy history reappearing next to the migration history cannot be
    // told apart from damage.
    database
        .execute(&format!(
            "CREATE TABLE {} (version TEXT, type BIGINT, hash TEXT); \
             INSERT INTO {} VALUES ('20260803000001', 2, 'x')",
            database.location.qualified_table(),
            database.location.qualified_table()
        ))
        .await;
    assert_eq!(inspect(&database).await.state, SchemaState::SchemaCorrupt);
    database.close().await;
}

#[tokio::test]
async fn concurrent_handovers_record_each_version_once() {
    let database = test_support::from_fixture(FULL_FIXTURE).await;
    let (first, second) = tokio::join!(
        hand_over(&database.pool, database.location),
        hand_over(&database.pool, database.location)
    );
    let outcomes = [first.unwrap(), second.unwrap()];
    assert_eq!(
        outcomes
            .iter()
            .filter(|outcome| matches!(outcome, HandoverOutcome::HandedOver { .. }))
            .count(),
        1,
        "{outcomes:?}"
    );
    assert_eq!(
        database.scalar_i64(&history_count()).await,
        migrations::migrations().len() as i64
    );
    database.close().await;
}

#[tokio::test]
async fn a_database_without_legacy_history_needs_no_handover() {
    let database = test_support::empty().await;
    assert_eq!(
        hand_over(&database.pool, database.location).await.unwrap(),
        HandoverOutcome::NotRequired
    );
    assert!(!history_table_exists(&database).await);
    database.close().await;
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn the_dedicated_schema_goes_only_when_empty() {
    let database = test_support::from_fixture("dedicated-fresh-full").await;
    hand_over(&database.pool, database.location).await.unwrap();
    let schema_exists =
        "SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = 'atlas_schema_revisions')";
    assert!(!database.scalar_bool(schema_exists).await);
    database.close().await;

    let database = test_support::from_fixture("dedicated-fresh-full").await;
    database
        .execute("CREATE TABLE atlas_schema_revisions.kept (id BIGINT)")
        .await;
    hand_over(&database.pool, database.location).await.unwrap();
    assert!(database.scalar_bool(schema_exists).await);
    assert!(
        database
            .scalar_bool("SELECT to_regclass('atlas_schema_revisions.kept') IS NOT NULL")
            .await
    );
    database.close().await;

    // Any object in the schema keeps it, not only relations.
    let database = test_support::from_fixture("dedicated-fresh-full").await;
    database
        .execute("CREATE COLLATION atlas_schema_revisions.kept FROM \"C\"")
        .await;
    hand_over(&database.pool, database.location).await.unwrap();
    assert!(database.scalar_bool(schema_exists).await);
    database.close().await;
}
