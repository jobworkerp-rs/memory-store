//! SQLite schema snapshots. The scratch database is a private in-memory
//! database; the target is only read.

use super::{Snapshot, is_history_table, join_tokens, normalize_sql, replay, unverifiable};
use anyhow::{Context, Result};
use infra_utils::infra::rdb::RdbPool;
use sqlparser::dialect::{Dialect, SQLiteDialect};
use sqlparser::tokenizer::{Token, Tokenizer};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Row, SqliteConnection};

pub(super) async fn snapshots(pool: &RdbPool, version: &str) -> Result<(Snapshot, Snapshot)> {
    let reference = SqlitePoolOptions::new()
        // A single connection keeps the private in-memory database alive.
        .max_connections(1)
        .connect_with(
            SqliteConnectOptions::new()
                .in_memory(true)
                .foreign_keys(false),
        )
        .await
        .context("opening the in-memory verification database")
        .map_err(unverifiable)?;
    let expected = async {
        replay(&reference, version).await.map_err(unverifiable)?;
        let mut conn = reference.acquire().await?;
        snapshot(&mut conn).await
    }
    .await;
    reference.close().await;
    let mut conn = pool.acquire().await.context("connecting to the target")?;
    Ok((expected?, snapshot(&mut conn).await?))
}

pub(crate) async fn snapshot(conn: &mut SqliteConnection) -> Result<Snapshot> {
    let dialect = SQLiteDialect {};
    let mut snapshot = Snapshot::new();
    let objects = sqlx::query(
        "SELECT type, name, tbl_name, sql FROM sqlite_master \
         WHERE name NOT LIKE 'sqlite_%' AND sql IS NOT NULL ORDER BY name",
    )
    .fetch_all(&mut *conn)
    .await
    .context("reading the schema")?;
    for object in objects {
        let kind: String = object.get("type");
        let name: String = object.get("name");
        let table: String = object.get("tbl_name");
        let sql: String = object.get("sql");
        if is_history_table(&table.to_lowercase()) {
            continue;
        }
        // Indexes listed here are explicit; automatic ones belong to the
        // table's constraints.
        if kind == "table" {
            table_entries(conn, &dialect, &name, &sql, &mut snapshot).await?;
        } else {
            snapshot.insert(
                format!("{kind} {}", name.to_lowercase()),
                normalize_sql(&dialect, &sql),
            );
        }
    }
    Ok(snapshot)
}

/// Entries of one table. `stored_name` is queried as stored; entries use
/// the lowercase form, as SQLite identifiers are case-insensitive.
async fn table_entries(
    conn: &mut SqliteConnection,
    dialect: &SQLiteDialect,
    stored_name: &str,
    sql: &str,
    snapshot: &mut Snapshot,
) -> Result<()> {
    let table = stored_name.to_lowercase();
    snapshot.insert(format!("table {table}"), String::new());

    // hidden = 1 marks virtual-table internals; generated columns (2, 3)
    // are part of the schema.
    let columns = sqlx::query(
        "SELECT name, type, \"notnull\" AS not_null, dflt_value, pk FROM pragma_table_xinfo(?) WHERE hidden <> 1",
    )
    .bind(stored_name)
    .fetch_all(&mut *conn)
    .await
    .with_context(|| format!("reading the columns of {table}"))?;
    let mut primary_key = Vec::new();
    for column in columns {
        let name: String = column.get("name");
        let declared: String = column.get("type");
        let not_null: bool = column.get("not_null");
        let default: Option<String> = column.get("dflt_value");
        let pk: i64 = column.get("pk");
        if pk > 0 {
            primary_key.push((pk, name.to_lowercase()));
        }
        snapshot.insert(
            format!("column {table}.{}", name.to_lowercase()),
            format!(
                "{} not_null={not_null} default={}",
                normalize_sql(dialect, &declared),
                default.map_or_else(|| "none".to_string(), |d| normalize_sql(dialect, &d))
            ),
        );
    }
    if !primary_key.is_empty() {
        primary_key.sort();
        let columns = primary_key
            .into_iter()
            .map(|(_, name)| name)
            .collect::<Vec<_>>();
        snapshot.insert(
            format!("primary key {table}"),
            format!("({})", columns.join(", ")),
        );
    }

    let unique_indexes =
        sqlx::query("SELECT name FROM pragma_index_list(?) WHERE origin = 'u' ORDER BY name")
            .bind(stored_name)
            .fetch_all(&mut *conn)
            .await
            .with_context(|| format!("reading the unique constraints of {table}"))?;
    for index in unique_indexes {
        let index: String = index.get("name");
        let columns: Vec<String> =
            sqlx::query_scalar("SELECT lower(name) FROM pragma_index_info(?) ORDER BY seqno")
                .bind(&index)
                .fetch_all(&mut *conn)
                .await?;
        snapshot.insert(
            format!("unique {table} ({})", columns.join(", ")),
            String::new(),
        );
    }

    let foreign_keys = sqlx::query(
        "SELECT id, \"table\" AS target, \"from\" AS source_column, \"to\" AS target_column, \
         on_update, on_delete, \"match\" AS match_kind FROM pragma_foreign_key_list(?) ORDER BY id, seq",
    )
    .bind(stored_name)
    .fetch_all(&mut *conn)
    .await
    .with_context(|| format!("reading the foreign keys of {table}"))?;
    let mut grouped: std::collections::BTreeMap<i64, (Vec<String>, Vec<String>, String)> =
        Default::default();
    for key in foreign_keys {
        let id: i64 = key.get("id");
        let target: String = key.get("target");
        let source: String = key.get("source_column");
        let referenced: Option<String> = key.get("target_column");
        let on_update: String = key.get("on_update");
        let on_delete: String = key.get("on_delete");
        let match_kind: String = key.get("match_kind");
        let entry = grouped.entry(id).or_insert_with(|| {
            (
                Vec::new(),
                Vec::new(),
                format!(
                    "{} on update {} on delete {} match {}",
                    target.to_lowercase(),
                    on_update.to_lowercase(),
                    on_delete.to_lowercase(),
                    match_kind.to_lowercase()
                ),
            )
        });
        entry.0.push(source.to_lowercase());
        entry.1.push(referenced.unwrap_or_default().to_lowercase());
    }
    for (sources, targets, rest) in grouped.into_values() {
        snapshot.insert(
            format!(
                "foreign key {table} ({}) -> ({}) {rest}",
                sources.join(", "),
                targets.join(", ")
            ),
            String::new(),
        );
    }

    for check in check_constraints(dialect, sql)
        .with_context(|| format!("reading the CHECK constraints of {table}"))
        .map_err(unverifiable)?
    {
        snapshot.insert(format!("check {table}: {check}"), String::new());
    }
    Ok(())
}

/// The normalized expressions of every `CHECK ( ... )` in a CREATE TABLE
/// statement, column-level or table-level alike.
pub(super) fn check_constraints(dialect: &dyn Dialect, create_table: &str) -> Result<Vec<String>> {
    let tokens = Tokenizer::new(dialect, create_table).tokenize()?;
    let tokens = tokens
        .into_iter()
        .filter(|token| !matches!(token, Token::Whitespace(_)))
        .collect::<Vec<_>>();
    let mut checks = Vec::new();
    let mut index = 0;
    while index < tokens.len() {
        let is_check = matches!(&tokens[index], Token::Word(word) if word.quote_style.is_none() && word.value.eq_ignore_ascii_case("check"));
        if is_check && matches!(tokens.get(index + 1), Some(Token::LParen)) {
            let start = index + 2;
            let mut depth = 1;
            let mut end = start;
            while end < tokens.len() && depth > 0 {
                match tokens[end] {
                    Token::LParen => depth += 1,
                    Token::RParen => depth -= 1,
                    _ => {}
                }
                end += 1;
            }
            checks.push(join_tokens(tokens[start..end.saturating_sub(1)].iter()));
            index = end;
        } else {
            index += 1;
        }
    }
    checks.sort();
    Ok(checks)
}
