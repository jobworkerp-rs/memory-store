//! PostgreSQL schema snapshots. The scratch database is a temporary schema
//! in the target database, created and dropped by the check.

use super::{Snapshot, is_history_table, join_tokens, normalize_sql, replay, unverifiable};
use anyhow::{Context, Result};
use infra_utils::infra::rdb::RdbPool;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::tokenizer::{Token, Tokenizer};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgConnection, Row};
use std::sync::atomic::{AtomicU32, Ordering};

pub(super) async fn snapshots(pool: &RdbPool, version: &str) -> Result<(Snapshot, Snapshot)> {
    let target_schema: String = sqlx::query_scalar("SELECT current_schema()")
        .fetch_one(pool)
        .await
        .context("reading the application schema")?;
    // The counter keeps checks started in the same millisecond apart.
    static NEXT_SCRATCH: AtomicU32 = AtomicU32::new(0);
    let scratch_schema = format!(
        "memories_verify_{}_{}_{}",
        std::process::id(),
        command_utils::util::datetime::now_millis(),
        NEXT_SCRATCH.fetch_add(1, Ordering::Relaxed)
    );
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE SCHEMA {scratch_schema}"
    )))
    .execute(pool)
    .await
    .context("creating the temporary verification schema")
    .map_err(unverifiable)?;

    let compared = compare(pool, version, &target_schema, &scratch_schema).await;
    let dropped = sqlx::query(sqlx::AssertSqlSafe(format!(
        "DROP SCHEMA {scratch_schema} CASCADE"
    )))
    .execute(pool)
    .await
    .context("removing the temporary verification schema");
    let snapshots = compared?;
    dropped?;
    Ok(snapshots)
}

async fn compare(
    pool: &RdbPool,
    version: &str,
    target_schema: &str,
    scratch_schema: &str,
) -> Result<(Snapshot, Snapshot)> {
    let options = pool
        .connect_options()
        .as_ref()
        .clone()
        .options([("search_path", scratch_schema)]);
    let scratch = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .context("connecting to the temporary verification schema")
        .map_err(unverifiable)?;
    let built = replay(&scratch, version).await;
    scratch.close().await;
    built.map_err(unverifiable)?;

    let schemas = [target_schema, scratch_schema];
    let mut conn = pool.acquire().await.context("connecting to the target")?;
    let expected = snapshot(&mut conn, scratch_schema, &schemas).await?;
    let actual = snapshot(&mut conn, target_schema, &schemas).await?;
    Ok((expected, actual))
}

/// Normalized `sql` with qualifiers naming any of `schemas` removed. Only a
/// whole identifier followed by a period counts, so names that merely
/// contain a schema name and string literals are kept.
pub(super) fn without_schema_qualifiers(sql: &str, schemas: &[&str]) -> String {
    let dialect = PostgreSqlDialect {};
    let Ok(tokens) = Tokenizer::new(&dialect, sql).tokenize() else {
        return normalize_sql(&dialect, sql);
    };
    let is_qualifier = |index: usize| {
        matches!(&tokens[index], Token::Word(word) if schemas.contains(&word.value.as_str()))
            && matches!(tokens.get(index + 1), Some(Token::Period))
    };
    join_tokens(
        tokens
            .iter()
            .enumerate()
            .filter(|&(index, _)| !is_qualifier(index) && !(index > 0 && is_qualifier(index - 1)))
            .map(|(_, token)| token),
    )
}

/// Condition on the relation `c`: it does not belong to an extension.
/// Extensions may install relations into the application schema (CNPG puts
/// `pg_stat_statements` there); they are the extension's, not the catalog's.
macro_rules! not_extension_member {
    () => {
        "NOT EXISTS (SELECT 1 FROM pg_depend e WHERE e.classid = 'pg_class'::regclass \
         AND e.objid = c.oid AND e.deptype = 'e')"
    };
}

/// Snapshot of `schema`; names qualified by any of `schemas` lose the
/// qualifier, since both sides live in different schemas.
pub(crate) async fn snapshot(
    conn: &mut PgConnection,
    schema: &str,
    schemas: &[&str],
) -> Result<Snapshot> {
    let clean = |text: &str| without_schema_qualifiers(text, schemas);
    let mut snapshot = Snapshot::new();

    let tables: Vec<String> = sqlx::query_scalar(concat!(
        "SELECT c.relname::text FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = $1 AND c.relkind IN ('r', 'p') AND ",
        not_extension_member!()
    ))
    .bind(schema)
    .fetch_all(&mut *conn)
    .await
    .context("reading the tables")?;
    for table in tables.iter().filter(|table| !is_history_table(table)) {
        snapshot.insert(format!("table {table}"), String::new());
    }

    let columns = sqlx::query(concat!(
        "SELECT c.relname::text AS table_name, a.attname::text AS column_name, \
         format_type(a.atttypid, a.atttypmod) AS data_type, a.attnotnull AS not_null, \
         pg_get_expr(d.adbin, d.adrelid) AS default_value, \
         a.attidentity::text AS identity, a.attgenerated::text AS generated \
         FROM pg_attribute a \
         JOIN pg_class c ON c.oid = a.attrelid \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum \
         WHERE n.nspname = $1 AND c.relkind IN ('r', 'p') AND a.attnum > 0 AND NOT a.attisdropped AND ",
        not_extension_member!()
    ))
    .bind(schema)
    .fetch_all(&mut *conn)
    .await
    .context("reading the columns")?;
    for column in columns {
        let table: String = column.get("table_name");
        if is_history_table(&table) {
            continue;
        }
        let name: String = column.get("column_name");
        let data_type: String = column.get("data_type");
        let not_null: bool = column.get("not_null");
        let default: Option<String> = column.get("default_value");
        let identity: String = column.get("identity");
        let generated: String = column.get("generated");
        snapshot.insert(
            format!("column {table}.{name}"),
            format!(
                "{} not_null={not_null} default={} identity={identity} generated={generated}",
                clean(&data_type),
                default.map_or_else(|| "none".to_string(), |d| clean(&d))
            ),
        );
    }

    let constraints = sqlx::query(concat!(
        "SELECT c.relname::text AS table_name, con.contype::text AS kind, \
         pg_get_constraintdef(con.oid) AS definition \
         FROM pg_constraint con \
         JOIN pg_class c ON c.oid = con.conrelid \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = $1 AND ",
        not_extension_member!()
    ))
    .bind(schema)
    .fetch_all(&mut *conn)
    .await
    .context("reading the constraints")?;
    for constraint in constraints {
        let table: String = constraint.get("table_name");
        if is_history_table(&table) {
            continue;
        }
        let kind: String = constraint.get("kind");
        let definition: String = constraint.get("definition");
        snapshot.insert(
            format!("constraint {table} {kind}: {}", clean(&definition)),
            String::new(),
        );
    }

    let indexes = sqlx::query(concat!(
        "SELECT c.relname::text AS table_name, i.relname::text AS index_name, \
         pg_get_indexdef(i.oid) AS definition \
         FROM pg_index x \
         JOIN pg_class i ON i.oid = x.indexrelid \
         JOIN pg_class c ON c.oid = x.indrelid \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = $1 AND ",
        not_extension_member!()
    ))
    .bind(schema)
    .fetch_all(&mut *conn)
    .await
    .context("reading the indexes")?;
    for index in indexes {
        let table: String = index.get("table_name");
        if is_history_table(&table) {
            continue;
        }
        let name: String = index.get("index_name");
        let definition: String = index.get("definition");
        snapshot.insert(format!("index {name}"), clean(&definition));
    }

    let views = sqlx::query(concat!(
        "SELECT c.relname::text AS view_name, pg_get_viewdef(c.oid) AS definition \
         FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = $1 AND c.relkind = 'v' AND ",
        not_extension_member!()
    ))
    .bind(schema)
    .fetch_all(&mut *conn)
    .await
    .context("reading the views")?;
    for view in views {
        let name: String = view.get("view_name");
        let definition: String = view.get("definition");
        snapshot.insert(format!("view {name}"), clean(&definition));
    }
    Ok(snapshot)
}
