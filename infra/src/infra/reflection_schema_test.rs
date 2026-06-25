//! Smoke tests for the Phase A migration `003_reflection_schema.sql`.
//!
//! Phase B will introduce dedicated reflection repositories with full
//! CRUD coverage; until then these tests guard the bare minimum:
//!   - the migration applies cleanly on top of 001 / 002
//!   - every newly introduced table is reachable by name
//!   - the seed data (16 dictionary modes + 8 indicator norms) lands.

#[cfg(test)]
#[allow(clippy::module_inception)]
mod tests {
    use anyhow::Result;
    use infra_utils::infra::rdb::Rdb;

    use crate::sql::p;

    /// Tables added by `003_reflection_schema.sql`.
    const REFLECTION_TABLES: &[&str] = &[
        "thread_reflection_index",
        "reflection_failure_mode",
        "reflection_tool",
        "reflection_tool_outcome",
        "reflection_fact",
        "reflection_applied_target",
        "reflection_few_shot_usage",
        "tool_outcome_stats",
        "tool_contribution_stats",
        "failure_mode_dictionary",
        "failure_signature_indicator_norm",
        "thread_aggregate_key",
    ];

    use crate::infra::reflection::test_support::setup_pool;

    async fn assert_reflection_tables_exist(pool: &infra_utils::infra::rdb::RdbPool) -> Result<()> {
        for &table in REFLECTION_TABLES {
            // SELECT 1 with `LIMIT 0` resolves the table identifier
            // without scanning rows; a missing table surfaces as a
            // sqlx error so the assertion is precise.
            let sql = format!("SELECT 1 FROM {table} LIMIT 0");
            sqlx::query::<Rdb>(sqlx::AssertSqlSafe(sql))
                .execute(pool)
                .await
                .map_err(|e| anyhow::anyhow!("table {table} not reachable: {e}"))?;
        }
        Ok(())
    }

    async fn assert_dictionary_seeded(pool: &infra_utils::infra::rdb::RdbPool) -> Result<()> {
        // 16 reflection dictionary entries from spec §3.4.2.
        let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM failure_mode_dictionary")
            .fetch_one(pool)
            .await?;
        assert!(
            row.0 >= 16,
            "expected at least 16 failure_mode_dictionary entries, got {}",
            row.0
        );
        // 8 initial indicator norm rows.
        let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM failure_signature_indicator_norm")
            .fetch_one(pool)
            .await?;
        assert!(
            row.0 >= 8,
            "expected at least 8 failure_signature_indicator_norm entries, got {}",
            row.0
        );
        Ok(())
    }

    // Hard-coded `?` placeholders break under postgres; route every
    // bind through `p!` so the same statement compiles for both
    // backends.
    const DELETE_AGGREGATE_KEY_SQL: &str = concat!(
        "DELETE FROM thread_aggregate_key WHERE user_id = ",
        p!(1),
        " AND labels_hash = ",
        p!(2),
    );
    const INSERT_AGGREGATE_KEY_SQL: &str = concat!(
        "INSERT INTO thread_aggregate_key (user_id, labels_hash, thread_id, created_at) VALUES (",
        p!(1),
        ", ",
        p!(2),
        ", ",
        p!(3),
        ", ",
        p!(4),
        ")"
    );

    async fn assert_thread_aggregate_key_unique_constraint(
        pool: &infra_utils::infra::rdb::RdbPool,
    ) -> Result<()> {
        // First insert succeeds, second insert with same (user_id,
        // labels_hash) but different thread_id collides on the PK.
        let user_id: i64 = 999_001;
        let labels_hash = "a".repeat(64);
        let _ = sqlx::query::<Rdb>(DELETE_AGGREGATE_KEY_SQL)
            .bind(user_id)
            .bind(&labels_hash)
            .execute(pool)
            .await;

        let now: i64 = 1_700_000_000_000;
        sqlx::query::<Rdb>(INSERT_AGGREGATE_KEY_SQL)
            .bind(user_id)
            .bind(&labels_hash)
            .bind(1_i64)
            .bind(now)
            .execute(pool)
            .await?;

        let collision = sqlx::query::<Rdb>(INSERT_AGGREGATE_KEY_SQL)
            .bind(user_id)
            .bind(&labels_hash)
            .bind(2_i64)
            .bind(now)
            .execute(pool)
            .await;
        assert!(
            collision.is_err(),
            "second insert with the same (user_id, labels_hash) must violate PRIMARY KEY"
        );

        // Cleanup
        let _ = sqlx::query::<Rdb>(DELETE_AGGREGATE_KEY_SQL)
            .bind(user_id)
            .bind(&labels_hash)
            .execute(pool)
            .await;
        Ok(())
    }

    #[test]
    fn run_reflection_schema_smoke_test() -> Result<()> {
        use infra_utils::infra::test::TEST_RUNTIME;
        TEST_RUNTIME.block_on(async {
            let pool = setup_pool().await;
            assert_reflection_tables_exist(pool).await?;
            assert_dictionary_seeded(pool).await?;
            assert_thread_aggregate_key_unique_constraint(pool).await?;
            Ok(())
        })
    }
}
