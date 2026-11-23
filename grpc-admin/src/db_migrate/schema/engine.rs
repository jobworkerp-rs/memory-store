//! Planning and applying the compiled migration catalog.
//!
//! On SQLite the pool must be opened with foreign keys disabled, as the
//! previous engine applied migrations that way (see
//! [`super::target::connect_for_migration`]).

use super::handover::{self, HandoverOutcome};
use super::history::{
    self, Inspection, LegacyHistoryLocation, SCHEMA_CORRUPT_MESSAGE, SchemaState,
};
use super::migrations::{self, version_string};
use anyhow::{Context, Result, anyhow, bail};
use infra_utils::infra::rdb::RdbPool;
use sqlx::migrate::{MigrateError, Migration, Migrator};

/// What `schema apply` would do; computing it changes nothing.
#[derive(Debug)]
pub struct ApplyPlan {
    pub inspection: Inspection,
    /// Migrations not yet applied, in application order.
    pub pending: &'static [Migration],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyReport {
    /// The legacy history was handed over first.
    pub handed_over: bool,
    /// Versions applied by this run.
    pub applied: Vec<String>,
}

pub async fn inspect(pool: &RdbPool, location: LegacyHistoryLocation) -> Result<Inspection> {
    let mut conn = pool
        .acquire()
        .await
        .context("connecting to the migration target")?;
    history::inspect(&mut conn, location).await
}

/// What `schema apply` would do. Unmanaged, corrupt, and newer databases
/// are refused, as `apply` refuses them.
pub async fn plan(pool: &RdbPool, location: LegacyHistoryLocation) -> Result<ApplyPlan> {
    let inspection = inspect(pool, location).await?;
    Ok(ApplyPlan {
        inspection,
        pending: pending_migrations(inspection.state)?,
    })
}

/// Hand the legacy history over when needed, then apply every pending
/// migration. Unmanaged, corrupt, and newer databases are refused before
/// anything changes.
pub async fn apply(pool: &RdbPool, location: LegacyHistoryLocation) -> Result<ApplyReport> {
    apply_with(pool, location, &migrations::migrator()).await
}

async fn apply_with(
    pool: &RdbPool,
    location: LegacyHistoryLocation,
    migrator: &Migrator,
) -> Result<ApplyReport> {
    let before = inspect(pool, location).await?;
    let pending = pending_migrations(before.state)?;
    // A legacy history never reappears once handed over, so a target
    // without one skips the handover's write lock altogether.
    let handed_over = before.legacy_history.is_some()
        && matches!(
            handover::hand_over(pool, location).await?,
            HandoverOutcome::HandedOver { .. }
        );
    migrator.run(pool).await.map_err(describe_migrate_error)?;
    let after = inspect(pool, location).await?;
    if after.state != SchemaState::Managed {
        bail!(
            "schema apply ended in state {} instead of managed",
            after.state.as_str()
        );
    }
    Ok(ApplyReport {
        handed_over,
        applied: pending
            .iter()
            .map(|migration| version_string(migration.version))
            .collect(),
    })
}

/// The migrations `schema apply` would apply from `state`; states it may
/// not proceed from are refused.
fn pending_migrations(state: SchemaState) -> Result<&'static [Migration]> {
    state.refuse_unmanageable()?;
    state
        .pending_migrations()
        .context("the applied prefix is longer than the compiled catalog")
}

/// Map SQLx's view of a damaged history to the coordinator's vocabulary.
fn describe_migrate_error(error: MigrateError) -> anyhow::Error {
    match error {
        MigrateError::VersionMismatch(version)
        | MigrateError::VersionMissing(version)
        | MigrateError::Dirty(version) => anyhow!(
            "{SCHEMA_CORRUPT_MESSAGE} (version {})",
            version_string(version)
        ),
        MigrateError::ExecuteMigration(source, version) => anyhow::Error::new(source).context(
            format!("applying migration {} failed", version_string(version)),
        ),
        other => anyhow::Error::new(other).context("applying the schema migrations failed"),
    }
}

#[cfg(test)]
mod tests;
