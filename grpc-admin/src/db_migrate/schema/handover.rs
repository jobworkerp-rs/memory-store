//! One-time handover of the legacy (Atlas) history to the migration history.

use super::history::{self, Connection, Inspection, LegacyHistoryLocation, SchemaState};
use super::migrations::{self, HISTORY_TABLE, LEGACY_HISTORY_SCHEMA, version_string};
use anyhow::{Context, Result, bail};
use infra_utils::infra::rdb::{Rdb, RdbPool};
use sqlx::migrate::Migrate;

/// Serializes concurrent handovers of one PostgreSQL database.
#[cfg(feature = "postgres")]
const HANDOVER_LOCK_ID: i64 = 0x6d65_6d5f_6876_0001;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandoverOutcome {
    /// The target has no legacy history (already handed over, or never had one).
    NotRequired,
    /// This many versions were recorded in the migration history.
    HandedOver { versions: usize },
}

/// Hand the legacy history over in one transaction: verify it, record its
/// versions in the migration history, and remove it. Any failure rolls the
/// whole handover back.
pub async fn hand_over(pool: &RdbPool, location: LegacyHistoryLocation) -> Result<HandoverOutcome> {
    hand_over_with(pool, location, false).await
}

async fn hand_over_with(
    pool: &RdbPool,
    location: LegacyHistoryLocation,
    fail_before_commit: bool,
) -> Result<HandoverOutcome> {
    let mut conn = pool
        .acquire()
        .await
        .context("connecting for the history handover")?;
    let mut tx = begin_exclusive(&mut conn).await?;
    let before = history::inspect(&mut tx, location).await?;
    if before.legacy_history.is_none() {
        tx.rollback().await?;
        return Ok(HandoverOutcome::NotRequired);
    }
    let applied_count = applied_count(before)?;

    tx.ensure_migrations_table(HISTORY_TABLE)
        .await
        .context("creating the migration history")?;
    for migration in &migrations::migrations()[..applied_count] {
        record(&mut tx, location, migration).await?;
    }
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "DROP TABLE {}",
        location.qualified_table()
    )))
    .execute(&mut *tx)
    .await
    .context("removing the legacy migration history")?;
    if location == LegacyHistoryLocation::DedicatedSchema {
        drop_legacy_schema_if_empty(&mut tx).await?;
    }

    let after = history::inspect(&mut tx, location).await?;
    if after.state != before.state || after.legacy_history.is_some() {
        bail!(
            "the history handover changed the schema state from {} to {}",
            before.state.as_str(),
            after.state.as_str()
        );
    }
    if fail_before_commit {
        bail!("injected handover failure");
    }
    tx.commit()
        .await
        .context("committing the history handover")?;
    Ok(HandoverOutcome::HandedOver {
        versions: applied_count,
    })
}

/// Versions the legacy history marks as applied; any state other than
/// `pending` / `managed` is refused before anything changes.
fn applied_count(inspection: Inspection) -> Result<usize> {
    match inspection.state {
        SchemaState::Pending { applied_count } => Ok(applied_count),
        SchemaState::Managed => Ok(migrations::migrations().len()),
        SchemaState::NewerThanTool => bail!(history::NEWER_THAN_TOOL_MESSAGE),
        SchemaState::SchemaCorrupt | SchemaState::Uninitialized | SchemaState::Unmanaged => {
            bail!(history::SCHEMA_CORRUPT_MESSAGE)
        }
    }
}

#[cfg(not(feature = "postgres"))]
async fn begin_exclusive(
    conn: &mut sqlx::pool::PoolConnection<Rdb>,
) -> Result<sqlx::Transaction<'_, Rdb>> {
    // IMMEDIATE takes the write lock up front, so a concurrent handover
    // waits and then finds the legacy history gone.
    sqlx::Connection::begin_with(&mut **conn, "BEGIN IMMEDIATE")
        .await
        .context("starting the history handover")
}

#[cfg(feature = "postgres")]
async fn begin_exclusive(
    conn: &mut sqlx::pool::PoolConnection<Rdb>,
) -> Result<sqlx::Transaction<'_, Rdb>> {
    let mut tx = sqlx::Connection::begin(&mut **conn)
        .await
        .context("starting the history handover")?;
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(HANDOVER_LOCK_ID)
        .execute(&mut *tx)
        .await
        .context("locking the history handover")?;
    Ok(tx)
}

/// Record one applied version, keeping the legacy apply time and duration
/// when the legacy history has them (a baseline revision has no duration,
/// and versions before a baseline have no record at all).
async fn record(
    conn: &mut Connection,
    location: LegacyHistoryLocation,
    migration: &sqlx::migrate::Migration,
) -> Result<()> {
    let legacy = location.qualified_table();
    let sql = format!(
        "INSERT INTO {HISTORY_TABLE} (version, description, installed_on, success, checksum, execution_time) \
         SELECT {p1}, {p2}, \
         COALESCE((SELECT executed_at FROM {legacy} WHERE version = {p3}), CURRENT_TIMESTAMP), \
         TRUE, {p4}, \
         COALESCE((SELECT execution_time FROM {legacy} WHERE version = {p3} AND type = 2), 0)",
        p1 = param(1),
        p2 = param(2),
        p3 = param(3),
        p4 = param(4),
    );
    sqlx::query(sqlx::AssertSqlSafe(sql))
        .bind(migration.version)
        .bind(migration.description.as_ref())
        .bind(version_string(migration.version))
        .bind(migration.checksum.as_ref())
        .execute(&mut *conn)
        .await
        .with_context(|| {
            format!(
                "recording version {} in the migration history",
                version_string(migration.version)
            )
        })?;
    Ok(())
}

/// A numbered bind parameter, reusable within one statement.
fn param(index: usize) -> String {
    if cfg!(feature = "postgres") {
        format!("${index}")
    } else {
        format!("?{index}")
    }
}

/// Remove Atlas's dedicated schema once its history table is gone, unless
/// something else lives there.
async fn drop_legacy_schema_if_empty(conn: &mut Connection) -> Result<()> {
    // Every object in a schema depends on it, whatever catalog holds it.
    let empty: bool = sqlx::query_scalar(
        "SELECT NOT EXISTS (SELECT 1 FROM pg_depend d JOIN pg_namespace n ON n.oid = d.refobjid \
         WHERE d.refclassid = 'pg_namespace'::regclass AND n.nspname = $1)",
    )
    .bind(LEGACY_HISTORY_SCHEMA)
    .fetch_one(&mut *conn)
    .await
    .context("checking the dedicated legacy history schema")?;
    if empty {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "DROP SCHEMA {LEGACY_HISTORY_SCHEMA}"
        )))
        .execute(&mut *conn)
        .await
        .context("removing the dedicated legacy history schema")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
