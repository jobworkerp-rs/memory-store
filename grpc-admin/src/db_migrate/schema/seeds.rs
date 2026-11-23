//! Static seed expectations: reference rows that every environment must
//! hold once the schema reaches the expectation's version.

use anyhow::{Context, Result, bail};
use infra_utils::infra::rdb::RdbPool;
use serde::Deserialize;

#[cfg(feature = "postgres")]
const SEED_EXPECTATIONS_JSON: &str =
    include_str!("../../../../infra/db-migrate/postgres/seed-expectations.json");
#[cfg(not(feature = "postgres"))]
const SEED_EXPECTATIONS_JSON: &str =
    include_str!("../../../../infra/db-migrate/sqlite/seed-expectations.json");

/// Path of the compiled seed expectations below the release catalog root.
pub const SEED_EXPECTATIONS_FILE: &str = if cfg!(feature = "postgres") {
    "postgres/seed-expectations.json"
} else {
    "sqlite/seed-expectations.json"
};

#[derive(Debug, Deserialize)]
pub struct SeedExpectations {
    pub version: String,
    pub tables: Vec<SeedExpectation>,
}

#[derive(Debug, Deserialize)]
pub struct SeedExpectation {
    pub table: String,
    pub key_column: String,
    pub keys: Vec<String>,
}

/// The compiled expectations as shipped (for bundle verification).
pub fn compiled_seed_expectations() -> &'static str {
    SEED_EXPECTATIONS_JSON
}

pub fn load() -> Result<SeedExpectations> {
    parse(SEED_EXPECTATIONS_JSON)
}

fn parse(json: &str) -> Result<SeedExpectations> {
    let expectations: SeedExpectations =
        serde_json::from_str(json).context("parsing seed expectations")?;
    if super::migrations::parse_version(&expectations.version).is_none() {
        bail!("seed expectations version must be a 14-digit ASCII timestamp");
    }
    if expectations.tables.is_empty() {
        bail!("seed expectations must contain at least one table");
    }
    for expectation in &expectations.tables {
        if !is_safe_sql_identifier(&expectation.table)
            || !is_safe_sql_identifier(&expectation.key_column)
            || expectation.keys.is_empty()
            || expectation.keys.iter().any(|key| key.is_empty())
            || expectation.keys.windows(2).any(|pair| pair[0] >= pair[1])
        {
            bail!("seed expectations contain an invalid table, key column, or key ordering");
        }
    }
    Ok(expectations)
}

fn is_safe_sql_identifier(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte == b'_')
}

/// Check the seed rows of a database at `target_version`.
pub async fn verify(pool: &RdbPool, target_version: &str) -> Result<()> {
    let expectations = load()?;
    if expectations.version.as_str() > target_version {
        return Ok(());
    }
    for expectation in expectations.tables {
        let sql = format!(
            "SELECT {} FROM {} ORDER BY {}",
            expectation.key_column, expectation.table, expectation.key_column
        );
        let mut actual: Vec<String> = sqlx::query_scalar(sqlx::AssertSqlSafe(sql))
            .fetch_all(pool)
            .await
            .with_context(|| format!("reading seed table {}", expectation.table))?;
        // Database collation is deployment-specific; compare canonical key order.
        actual.sort_unstable();
        if actual != expectation.keys {
            bail!(
                "seed_mismatch: table={} key_column={} expected={:?} actual={:?}",
                expectation.table,
                expectation.key_column,
                expectation.keys,
                actual
            );
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checked_in_seed_expectations_are_nonempty_and_backend_equivalent() {
        let sqlite = parse(include_str!(
            "../../../../infra/db-migrate/sqlite/seed-expectations.json"
        ))
        .unwrap();
        let postgres = parse(include_str!(
            "../../../../infra/db-migrate/postgres/seed-expectations.json"
        ))
        .unwrap();
        assert_eq!(sqlite.version, postgres.version);
        let shape = |expectations: &SeedExpectations| {
            expectations
                .tables
                .iter()
                .map(|table| {
                    (
                        table.table.clone(),
                        table.key_column.clone(),
                        table.keys.clone(),
                    )
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(shape(&sqlite), shape(&postgres));
        assert!(load().is_ok());
    }

    #[test]
    fn invalid_expectations_are_rejected() {
        assert!(parse(r#"{"version":"2026","tables":[]}"#).is_err());
        assert!(parse(r#"{"version":"20260803000001","tables":[]}"#).is_err());
        assert!(
            parse(
                r#"{"version":"20260803000001","tables":[{"table":"t","key_column":"k","keys":["b","a"]}]}"#
            )
            .is_err(),
            "keys must be in ascending order"
        );
        assert!(
            parse(
                r#"{"version":"20260803000001","tables":[{"table":"t; DROP","key_column":"k","keys":["a"]}]}"#
            )
            .is_err()
        );
    }

    #[tokio::test]
    async fn missing_static_rows_are_detected() {
        let database = crate::db_migrate::schema::test_support::empty().await;
        crate::db_migrate::schema::engine::apply(&database.pool, database.location)
            .await
            .unwrap();
        let latest = crate::db_migrate::schema::migrations::latest_version();
        verify(&database.pool, &latest).await.unwrap();

        database
            .execute("DELETE FROM failure_mode_dictionary WHERE mode = 'OTHER'")
            .await;
        let error = verify(&database.pool, &latest).await.unwrap_err();
        assert!(error.to_string().starts_with("seed_mismatch"), "{error:#}");
        database.close().await;
    }
}
