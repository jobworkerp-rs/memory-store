use super::*;
use crate::db_migrate::schema::engine;
use crate::db_migrate::schema::migrations::{latest_version, versions};
use crate::db_migrate::schema::test_support::{self, LEGACY_FIXTURES, TestDatabase};
use sqlparser::dialect::{PostgreSqlDialect, SQLiteDialect};

async fn migrated_database() -> TestDatabase {
    let database = test_support::empty().await;
    engine::apply(&database.pool, database.location)
        .await
        .unwrap();
    database
}

fn objects(differences: &[Difference]) -> Vec<&str> {
    differences
        .iter()
        .map(|difference| difference.object.as_str())
        .collect()
}

#[cfg(feature = "postgres")]
#[test]
fn only_whole_schema_qualifiers_are_removed() {
    use super::postgres::without_schema_qualifiers;
    let schemas = ["memories", "memories_verify_1_2"];
    assert_eq!(
        without_schema_qualifiers(
            "CREATE INDEX i ON memories.thread USING btree (user_id)",
            &schemas
        ),
        without_schema_qualifiers(
            "CREATE INDEX i ON \"memories_verify_1_2\".thread USING btree (user_id)",
            &schemas
        )
    );
    assert_eq!(
        without_schema_qualifiers(
            "REFERENCES thread_memories.x CHECK (kind = 'memories.a')",
            &schemas
        ),
        normalize_sql(
            &PostgreSqlDialect {},
            "REFERENCES thread_memories.x CHECK (kind = 'memories.a')"
        ),
        "identifiers containing a schema name and string literals stay"
    );
}

#[test]
fn quoting_case_and_whitespace_are_not_differences() {
    let sqlite = SQLiteDialect {};
    assert_eq!(
        normalize_sql(&sqlite, "CREATE INDEX `a_b` ON `t` (`x`, `y` DESC)"),
        normalize_sql(&sqlite, "create index a_b on t(x,\n  y desc)")
    );
    assert_ne!(
        normalize_sql(&sqlite, "CHECK (kind = 'A')"),
        normalize_sql(&sqlite, "CHECK (kind = 'a')"),
        "string literals keep their case"
    );
    let postgres = PostgreSqlDialect {};
    assert_eq!(
        normalize_sql(&postgres, "CHECK ((\"Kind\" = 'x'::text))"),
        normalize_sql(&postgres, "check ((kind = 'x'::text))")
    );
}

#[cfg(not(feature = "postgres"))]
#[test]
fn checks_are_found_at_column_and_table_level() {
    let sqlite = SQLiteDialect {};
    let checks = super::sqlite::check_constraints(
        &sqlite,
        "CREATE TABLE `t` (`k` TEXT PRIMARY KEY CHECK (`k` = 'one'), \
         `n` INTEGER NOT NULL, CONSTRAINT c CHECK (n IN (1, (2))))",
    )
    .unwrap();
    assert_eq!(checks, vec!["k = 'one'", "n in ( 1 , ( 2 ) )"]);
    assert!(
        super::sqlite::check_constraints(&sqlite, "CREATE TABLE t (check_count INTEGER)")
            .unwrap()
            .is_empty()
    );
}

#[cfg(not(feature = "postgres"))]
#[test]
fn uninterpretable_checks_are_not_skipped() {
    // An unterminated literal cannot be tokenized; skipping it would hide
    // every CHECK of the table from the comparison.
    assert!(
        super::sqlite::check_constraints(
            &SQLiteDialect {},
            "CREATE TABLE t (k TEXT CHECK (k = 'one))"
        )
        .is_err()
    );
}

#[test]
fn unverifiable_targets_and_differences_are_drift() {
    let message = |checked| verdict(checked).unwrap_err().to_string();
    assert!(verdict(Ok(Vec::new())).is_ok());

    let difference = Difference {
        object: "table t".to_string(),
        expected: Some(String::new()),
        actual: None,
    };
    assert_eq!(
        message(Ok(vec![difference])),
        "drift_detected: 1 difference(s): missing table t"
    );

    let unbuilt = unverifiable(anyhow::anyhow!("replay failed"))
        .context("while building the verification database");
    assert!(
        message(Err(unbuilt)).starts_with("drift_detected: "),
        "a verification database that cannot be built is drift"
    );

    let operational = anyhow::anyhow!("connecting to the target");
    assert_eq!(message(Err(operational)), "connecting to the target");
}

#[test]
fn differences_name_the_object() {
    let expected = Snapshot::from([
        ("table t".to_string(), String::new()),
        ("column t.a".to_string(), "integer".to_string()),
        ("column t.b".to_string(), "text".to_string()),
    ]);
    let actual = Snapshot::from([
        ("table t".to_string(), String::new()),
        ("column t.a".to_string(), "bigint".to_string()),
        ("column t.c".to_string(), "text".to_string()),
    ]);
    let differences = diff(&expected, &actual);
    assert_eq!(
        objects(&differences),
        ["column t.a", "column t.b", "column t.c"]
    );
    assert_eq!(
        differences
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
        [
            "changed column t.a: expected `integer`, found `bigint`",
            "missing column t.b",
            "unexpected column t.c"
        ]
    );
}

#[tokio::test]
async fn a_migrated_database_has_no_drift() {
    let database = migrated_database().await;
    let differences = check(&database.pool, &latest_version()).await.unwrap();
    assert!(differences.is_empty(), "{differences:?}");
    database.close().await;
}

#[tokio::test]
async fn databases_migrated_by_the_previous_engine_have_no_drift() {
    for &name in LEGACY_FIXTURES {
        let database = test_support::from_fixture(name).await;
        engine::apply(&database.pool, database.location)
            .await
            .unwrap();
        let differences = check(&database.pool, &latest_version()).await.unwrap();
        assert!(differences.is_empty(), "{name}: {differences:#?}");
        database.close().await;
    }
}

#[tokio::test]
async fn a_partially_migrated_database_matches_its_own_version() {
    let partial = LEGACY_FIXTURES
        .iter()
        .find(|name| name.ends_with("fresh-partial"))
        .unwrap();
    let database = test_support::from_fixture(partial).await;
    let applied = engine::inspect(&database.pool, database.location)
        .await
        .unwrap()
        .state;
    let applied_count = versions().len() - applied.pending_count().unwrap();
    let version = &versions()[applied_count - 1];
    let differences = check(&database.pool, version).await.unwrap();
    assert!(differences.is_empty(), "{differences:#?}");
    assert!(
        !check(&database.pool, &latest_version())
            .await
            .unwrap()
            .is_empty()
    );
    database.close().await;
}

#[tokio::test]
async fn an_added_column_is_drift() {
    let database = migrated_database().await;
    database
        .execute("ALTER TABLE thread ADD COLUMN drifted BIGINT")
        .await;
    let differences = check(&database.pool, &latest_version()).await.unwrap();
    assert_eq!(objects(&differences), ["column thread.drifted"]);
    database.close().await;
}

#[tokio::test]
async fn an_added_generated_column_is_drift() {
    let database = migrated_database().await;
    let storage = if cfg!(feature = "postgres") {
        "STORED"
    } else {
        "VIRTUAL"
    };
    database
        .execute(&format!(
            "ALTER TABLE thread ADD COLUMN drifted BIGINT GENERATED ALWAYS AS (user_id) {storage}"
        ))
        .await;
    let differences = check(&database.pool, &latest_version()).await.unwrap();
    assert_eq!(objects(&differences), ["column thread.drifted"]);
    database.close().await;
}

/// CNPG installs `pg_stat_statements` (views) into the application schema.
/// Neither an extension's views nor its tables, with their columns,
/// constraints, and indexes, are part of the catalog's schema.
#[cfg(feature = "postgres")]
#[tokio::test]
async fn objects_of_an_extension_are_not_drift() {
    let database = migrated_database().await;
    database
        .execute(
            "CREATE EXTENSION pg_stat_statements WITH SCHEMA public; \
             CREATE TABLE extension_state (id BIGINT PRIMARY KEY, kind TEXT NOT NULL CHECK (kind <> '')); \
             CREATE INDEX extension_state_kind ON extension_state (kind); \
             ALTER EXTENSION pg_stat_statements ADD TABLE extension_state",
        )
        .await;
    let differences = check(&database.pool, &latest_version()).await.unwrap();
    assert!(differences.is_empty(), "{differences:#?}");
    database.close().await;
}

/// Extensions may also live in a `search_path`-scoped application schema.
#[cfg(feature = "postgres")]
#[tokio::test]
async fn objects_of_an_extension_in_a_scoped_schema_are_not_drift() {
    let database = test_support::from_fixture(test_support::FULL_FIXTURE).await;
    engine::apply(&database.pool, database.location)
        .await
        .unwrap();
    database
        .execute("CREATE EXTENSION pg_stat_statements WITH SCHEMA memories")
        .await;
    let differences = check(&database.pool, &latest_version()).await.unwrap();
    assert!(differences.is_empty(), "{differences:#?}");
    database.close().await;
}

/// The extension filter must not hide what the application side created.
#[cfg(feature = "postgres")]
#[tokio::test]
async fn a_view_outside_any_extension_is_drift() {
    let database = migrated_database().await;
    database
        .execute(
            "CREATE EXTENSION pg_stat_statements WITH SCHEMA public; \
             CREATE VIEW drifted_view AS SELECT 1 AS one",
        )
        .await;
    let differences = check(&database.pool, &latest_version()).await.unwrap();
    assert_eq!(objects(&differences), ["view drifted_view"]);
    database.close().await;
}

#[tokio::test]
async fn a_dropped_index_is_drift() {
    let database = migrated_database().await;
    database.execute("DROP INDEX thread_user_id").await;
    let differences = check(&database.pool, &latest_version()).await.unwrap();
    assert_eq!(objects(&differences), ["index thread_user_id"]);
    assert_eq!(differences[0].actual, None);
    database.close().await;
}

#[tokio::test]
async fn a_changed_check_constraint_is_drift() {
    let database = migrated_database().await;
    #[cfg(not(feature = "postgres"))]
    {
        let sql: String = {
            let mut conn = database.conn().await;
            sqlx::query_scalar(
                "SELECT sql FROM sqlite_master WHERE name = 'thread_time_migration_state'",
            )
            .fetch_one(&mut *conn)
            .await
            .unwrap()
        };
        let changed = sql.replace("thread-time-fields-v1", "something-else");
        assert_ne!(changed, sql);
        database
            .execute(&format!(
                "DROP TABLE thread_time_migration_state; {changed}"
            ))
            .await;
    }
    #[cfg(feature = "postgres")]
    {
        let name: String = {
            let mut conn = database.conn().await;
            sqlx::query_scalar(
                "SELECT conname::text FROM pg_constraint WHERE conrelid = 'thread_time_migration_state'::regclass \
                 AND contype = 'c' AND pg_get_constraintdef(oid) LIKE '%thread-time-fields-v1%'",
            )
            .fetch_one(&mut *conn)
            .await
            .unwrap()
        };
        database
            .execute(&format!(
                "ALTER TABLE thread_time_migration_state DROP CONSTRAINT {name}; \
                 ALTER TABLE thread_time_migration_state ADD CONSTRAINT {name} \
                 CHECK (migration_key = 'something-else')"
            ))
            .await;
    }
    let differences = check(&database.pool, &latest_version()).await.unwrap();
    assert_eq!(differences.len(), 2, "{differences:#?}");
    assert!(
        differences
            .iter()
            .all(|difference| difference.object.contains("thread_time_migration_state")),
        "{differences:#?}"
    );
    database.close().await;
}

#[tokio::test]
async fn a_changed_column_type_is_drift() {
    let database = migrated_database().await;
    #[cfg(not(feature = "postgres"))]
    {
        // SQLite cannot alter a column type; rebuild the table instead.
        let sql: String = {
            let mut conn = database.conn().await;
            sqlx::query_scalar(
                "SELECT sql FROM sqlite_master WHERE name = 'thread_time_migration_state'",
            )
            .fetch_one(&mut *conn)
            .await
            .unwrap()
        };
        let changed = sql.replace("rdb_completed_at BIGINT", "rdb_completed_at TEXT");
        assert_ne!(changed, sql);
        database
            .execute(&format!(
                "DROP TABLE thread_time_migration_state; {changed}"
            ))
            .await;
    }
    #[cfg(feature = "postgres")]
    database
        .execute("ALTER TABLE thread_time_migration_state ALTER COLUMN rdb_completed_at TYPE text")
        .await;
    let differences = check(&database.pool, &latest_version()).await.unwrap();
    assert_eq!(
        objects(&differences),
        ["column thread_time_migration_state.rdb_completed_at"]
    );
    database.close().await;
}

/// Schema shared by the comparison cases below; the catalog itself has no
/// foreign key or table-level unique constraint to change.
const COMPARED_SCHEMA: &str = "\
    CREATE TABLE parent (id BIGINT PRIMARY KEY); \
    CREATE TABLE child ( \
        id BIGINT PRIMARY KEY, \
        parent_id BIGINT NOT NULL REFERENCES parent (id) ON DELETE CASCADE, \
        code TEXT NOT NULL DEFAULT 'a', \
        state TEXT NOT NULL, \
        UNIQUE (code) \
    ); \
    CREATE INDEX child_active ON child (parent_id) WHERE state = 'active';";

/// Snapshot of a fresh database holding only `sql`.
async fn snapshot_of(sql: &str) -> Snapshot {
    let database = test_support::empty().await;
    database.execute(sql).await;
    let snapshot = {
        let mut conn = database.conn().await;
        #[cfg(not(feature = "postgres"))]
        let snapshot = super::sqlite::snapshot(&mut conn).await;
        #[cfg(feature = "postgres")]
        let snapshot = super::postgres::snapshot(&mut conn, "public", &["public"]).await;
        snapshot.unwrap()
    };
    database.close().await;
    snapshot
}

/// Whether some difference names an object containing every one of
/// `words`, present only on the `expected` side (or only on the target).
fn has_one_sided(differences: &[Difference], words: &[&str], expected_side: bool) -> bool {
    differences.iter().any(|difference| {
        words.iter().all(|word| difference.object.contains(word))
            && difference.expected.is_some() == expected_side
            && difference.actual.is_some() != expected_side
    })
}

/// [`COMPARED_SCHEMA`] with `from` replaced by `to`.
fn changed_schema(from: &str, to: &str) -> String {
    let changed = COMPARED_SCHEMA.replace(from, to);
    assert_ne!(changed, COMPARED_SCHEMA, "`{from}` is not in the schema");
    changed
}

#[tokio::test]
async fn changed_definitions_are_drift() {
    let expected = snapshot_of(COMPARED_SCHEMA).await;

    // Changes that keep the object, so only its definition differs.
    for (from, to, object) in [
        ("state TEXT NOT NULL", "state TEXT", "column child.state"),
        ("DEFAULT 'a'", "DEFAULT 'b'", "column child.code"),
        (
            "WHERE state = 'active'",
            "WHERE state = 'done'",
            "index child_active",
        ),
    ] {
        let differences = diff(&expected, &snapshot_of(&changed_schema(from, to)).await);
        assert_eq!(objects(&differences), [object], "{from} -> {to}");
    }

    // Constraints are named by their definition, so a change replaces one.
    for (from, to, removed, added) in [
        (
            "ON DELETE CASCADE",
            "ON DELETE SET NULL",
            ["child", "on delete cascade"],
            ["child", "on delete set null"],
        ),
        (
            "UNIQUE (code)",
            "UNIQUE (state)",
            ["unique", "code"],
            ["unique", "state"],
        ),
    ] {
        let differences = diff(&expected, &snapshot_of(&changed_schema(from, to)).await);
        assert!(
            has_one_sided(&differences, &removed, true),
            "{from} -> {to}: {differences:#?}"
        );
        assert!(
            has_one_sided(&differences, &added, false),
            "{from} -> {to}: {differences:#?}"
        );
    }
}

#[tokio::test]
async fn tables_are_compared_and_identifier_case_is_not() {
    let compared = snapshot_of(COMPARED_SCHEMA).await;
    let with_extra = snapshot_of(&format!(
        "{COMPARED_SCHEMA} CREATE TABLE extra (id BIGINT);"
    ))
    .await;
    let missing = diff(&with_extra, &compared);
    assert!(
        has_one_sided(&missing, &["table extra"], true),
        "{missing:#?}"
    );
    let unexpected = diff(&compared, &with_extra);
    assert!(
        has_one_sided(&unexpected, &["table extra"], false),
        "{unexpected:#?}"
    );

    let shouted = COMPARED_SCHEMA
        .replace("TABLE child", "TABLE Child")
        .replace("REFERENCES parent (id)", "REFERENCES PARENT (ID)")
        .replace("INDEX child_active ON child", "INDEX Child_Active ON CHILD");
    assert_ne!(shouted, COMPARED_SCHEMA);
    let differences = diff(&compared, &snapshot_of(&shouted).await);
    assert!(differences.is_empty(), "{differences:#?}");
}
