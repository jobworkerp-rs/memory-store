use sqlx::Row;

const SQLITE_SCHEMA: &str = include_str!("../../sql/sqlite/001_schema.sql");
const POSTGRES_SCHEMA: &str = include_str!("../../sql/postgres/001_init_postgres.sql");
const MEMORY_KIND_INDEXES: [&str; 2] = [
    "thread_user_memory_kind_updated_at",
    "memory_user_memory_kind_updated_at",
];

#[tokio::test]
async fn sqlite_fresh_schema_keeps_the_memory_kind_contract() {
    let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
    sqlx::raw_sql(sqlx::AssertSqlSafe(SQLITE_SCHEMA))
        .execute(&pool)
        .await
        .unwrap();

    for table in ["thread", "memory"] {
        let rows = sqlx::query(sqlx::AssertSqlSafe(format!("PRAGMA table_info(`{table}`)")))
            .fetch_all(&pool)
            .await
            .unwrap();
        let kind = rows
            .iter()
            .find(|row| row.get::<String, _>("name") == "memory_kind")
            .unwrap();
        assert_eq!(kind.get::<i64, _>("notnull"), 1);
        assert!(
            kind.try_get::<Option<String>, _>("dflt_value")
                .unwrap()
                .is_none()
        );
    }

    let index_names = sqlx::query_scalar::<_, String>(
        "SELECT name FROM sqlite_master WHERE type = 'index' ORDER BY name",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    for index in MEMORY_KIND_INDEXES {
        assert!(index_names.iter().any(|name| name == index));
    }
    assert!(!SQLITE_SCHEMA.contains("CHECK (`memory_kind` BETWEEN 1 AND 7)"));
}

#[test]
fn postgres_fresh_schema_keeps_the_memory_kind_contract() {
    for table in ["thread", "memory"] {
        let start = POSTGRES_SCHEMA
            .find(&format!("CREATE TABLE IF NOT EXISTS {table} ("))
            .unwrap();
        let remainder = &POSTGRES_SCHEMA[start..];
        let table_schema = &remainder[..=remainder.find(");").unwrap()];

        assert!(table_schema.contains("memory_kind INT NOT NULL"));
        assert!(!table_schema.contains("memory_kind INT NOT NULL DEFAULT"));
        assert!(!table_schema.contains("CHECK (memory_kind BETWEEN 1 AND 7)"));
    }

    for index in MEMORY_KIND_INDEXES {
        assert!(POSTGRES_SCHEMA.contains(&format!("CREATE INDEX IF NOT EXISTS {index}")));
    }
}
