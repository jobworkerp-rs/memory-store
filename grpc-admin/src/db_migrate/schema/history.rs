//! Classification of a database by its migration history, schema contract,
//! and common task state.

use super::migrations::{
    self, HISTORY_TABLE, LEGACY_HISTORY_SCHEMA, LEGACY_HISTORY_TABLE, version_string,
};
use anyhow::{Context, Result};
use infra_utils::infra::rdb::Rdb;
use sqlx::migrate::Migration;

pub type Connection = <Rdb as sqlx::Database>::Connection;

pub const SCHEMA_CORRUPT_MESSAGE: &str = "schema_corrupt: migration history, schema contract, and task state must be introduced together";
pub const NEWER_THAN_TOOL_MESSAGE: &str = "newer_than_tool: the database was migrated by a newer release; use that release or a newer one";
pub const UNMANAGED_MESSAGE: &str =
    "unmanaged: the database predates the migration tool; migrate it with an earlier release first";

/// The version whose migration introduces the schema contract and the
/// common task state.
const SCHEMA_CONTRACT_VERSION: i64 = 20260803000003;
/// Adoption candidates the previous engine could record as a baseline.
const ADOPTION_BASELINE_VERSIONS: [i64; 2] = [20260803000001, 20260803000002];

const LEGACY_BASELINE_REVISION: i64 = 1;
const LEGACY_APPLIED_REVISION: i64 = 2;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SchemaState {
    Uninitialized,
    /// Application tables without any history, contract, or task state.
    Unmanaged,
    Pending {
        applied_count: usize,
    },
    Managed,
    /// A newer release migrated the database past this tool's catalog.
    NewerThanTool,
    SchemaCorrupt,
}

impl SchemaState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Uninitialized => "uninitialized",
            Self::Unmanaged => "unmanaged",
            Self::Pending { .. } => "pending",
            Self::Managed => "managed",
            Self::NewerThanTool => "newer_than_tool",
            Self::SchemaCorrupt => "schema_corrupt",
        }
    }

    /// Migrations of the compiled catalog already applied, when the state
    /// tells.
    pub fn applied_count(self) -> Option<usize> {
        match self {
            Self::Uninitialized => Some(0),
            Self::Pending { applied_count } => Some(applied_count),
            Self::Managed => Some(migrations::migrations().len()),
            Self::Unmanaged | Self::NewerThanTool | Self::SchemaCorrupt => None,
        }
    }

    /// Migrations still to apply, when the state tells.
    pub fn pending_count(self) -> Option<usize> {
        self.applied_count()
            .and_then(|applied| migrations::migrations().len().checked_sub(applied))
    }

    /// The migrations still to apply, when the state tells.
    pub fn pending_migrations(self) -> Option<&'static [sqlx::migrate::Migration]> {
        self.applied_count()
            .and_then(|applied| migrations::migrations().get(applied..))
    }

    /// Refuse a database this tool may not migrate or verify, naming the
    /// reason.
    pub fn refuse_unmanageable(self) -> Result<()> {
        match self {
            Self::Unmanaged => anyhow::bail!(UNMANAGED_MESSAGE),
            Self::NewerThanTool => anyhow::bail!(NEWER_THAN_TOOL_MESSAGE),
            Self::SchemaCorrupt => anyhow::bail!(SCHEMA_CORRUPT_MESSAGE),
            Self::Uninitialized | Self::Pending { .. } | Self::Managed => Ok(()),
        }
    }
}

/// Where the legacy history of a target lives.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LegacyHistoryLocation {
    /// Next to the application tables (SQLite, or PostgreSQL with an
    /// explicit `search_path`).
    ApplicationSchema,
    /// In Atlas's dedicated schema (PostgreSQL without `search_path`).
    DedicatedSchema,
}

impl LegacyHistoryLocation {
    /// The location the previous engine used for a target URL.
    pub fn for_url(url: &str) -> Self {
        if url.starts_with("sqlite:") || super::target::names_search_path(url) {
            Self::ApplicationSchema
        } else {
            Self::DedicatedSchema
        }
    }

    pub(crate) fn qualified_table(self) -> String {
        match self {
            Self::ApplicationSchema => LEGACY_HISTORY_TABLE.to_string(),
            Self::DedicatedSchema => format!("{LEGACY_HISTORY_SCHEMA}.{LEGACY_HISTORY_TABLE}"),
        }
    }
}

/// The state of a target and, when it still has one, its legacy history.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Inspection {
    pub state: SchemaState,
    /// Set while the legacy history has not been handed over.
    pub legacy_history: Option<LegacyHistoryLocation>,
}

impl Inspection {
    /// `schema apply` must hand the legacy history over before applying.
    pub fn handover_required(&self) -> bool {
        self.legacy_history.is_some()
            && matches!(
                self.state,
                SchemaState::Pending { .. } | SchemaState::Managed
            )
    }
}

/// Classify the target. Reads only.
pub async fn inspect(conn: &mut Connection, location: LegacyHistoryLocation) -> Result<Inspection> {
    let current = read_current_history(conn).await?;
    let legacy_exists = legacy_history_exists(conn, location).await?;
    let has_application_table = table_exists(conn, "thread").await?;
    let has_contract = table_exists(conn, "memories_schema_contract").await?;
    let has_task_state = table_exists(conn, "memories_data_migration_task_state").await?;
    let legacy_history = legacy_exists.then_some(location);
    let inspection = |state| Inspection {
        state,
        legacy_history,
    };

    if !current.is_empty() && legacy_exists {
        return Ok(inspection(SchemaState::SchemaCorrupt));
    }
    if current.is_empty() && !legacy_exists {
        return Ok(inspection(
            match (has_application_table, has_contract, has_task_state) {
                (false, false, false) => SchemaState::Uninitialized,
                (true, false, false) => SchemaState::Unmanaged,
                _ => SchemaState::SchemaCorrupt,
            },
        ));
    }
    if !has_application_table {
        return Ok(inspection(SchemaState::SchemaCorrupt));
    }
    let catalog = migrations::migrations();
    let shape = if legacy_exists {
        legacy_history_shape(&read_legacy_history(conn, location).await?, catalog)
    } else {
        current_history_shape(&current, catalog)
    };
    let state = match shape {
        HistoryShape::Prefix(applied_count) => {
            if !control_tables_match_prefix(conn, applied_count, has_contract, has_task_state)
                .await?
            {
                SchemaState::SchemaCorrupt
            } else if applied_count == migrations::migrations().len() {
                SchemaState::Managed
            } else {
                SchemaState::Pending { applied_count }
            }
        }
        HistoryShape::NewerThanTool { latest } => {
            if has_contract && has_task_state && contract_is(conn, &latest).await? {
                SchemaState::NewerThanTool
            } else {
                SchemaState::SchemaCorrupt
            }
        }
        HistoryShape::Invalid => SchemaState::SchemaCorrupt,
    };
    Ok(inspection(state))
}

#[derive(Debug, PartialEq, Eq)]
enum HistoryShape {
    /// A non-empty prefix of the compiled catalog with this many versions.
    Prefix(usize),
    /// The whole catalog followed only by later versions this tool does not
    /// know: a newer release migrated the database.
    NewerThanTool {
        latest: String,
    },
    Invalid,
}

#[derive(Debug)]
struct CurrentRecord {
    version: i64,
    success: bool,
    checksum: Vec<u8>,
}

/// Shape of a current history against `catalog`, the compiled one outside
/// tests.
fn current_history_shape(records: &[CurrentRecord], catalog: &[Migration]) -> HistoryShape {
    let (known, later) = records.split_at(records.len().min(catalog.len()));
    for (record, migration) in known.iter().zip(catalog) {
        if record.version != migration.version
            || !record.success
            || record.checksum != migration.checksum.as_ref()
        {
            return HistoryShape::Invalid;
        }
    }
    later_versions_shape(
        known.len(),
        later
            .iter()
            .map(|record| (record.version, record.success))
            .collect(),
        catalog,
    )
}

#[derive(Debug)]
struct LegacyRecord {
    version: String,
    revision_type: i64,
    hash: String,
    /// See [`revision_completed`].
    completed: bool,
}

/// Shape of a legacy history against `catalog`, the compiled one outside
/// tests.
fn legacy_history_shape(records: &[LegacyRecord], catalog: &[Migration]) -> HistoryShape {
    let expected = migrations::legacy_history_digests(catalog);
    let Some(first) = records.first() else {
        return HistoryShape::Invalid;
    };
    // A failed attempt never counts as applied, wherever it sits.
    if records.iter().any(|record| !record.completed) {
        return HistoryShape::Invalid;
    }
    let Some(start) = expected
        .iter()
        .position(|(version, _)| *version == first.version)
    else {
        return HistoryShape::Invalid;
    };
    // Atlas records the baseline of an adopted database as its only
    // baseline revision; a fresh database starts with an applied revision.
    let first_is_valid = match first.revision_type {
        LEGACY_APPLIED_REVISION => start == 0,
        LEGACY_BASELINE_REVISION => {
            start == 0
                || ADOPTION_BASELINE_VERSIONS
                    .iter()
                    .any(|version| version_string(*version) == first.version)
        }
        _ => false,
    };
    if !first_is_valid {
        return HistoryShape::Invalid;
    }
    let known_count = expected.len() - start;
    let (known, later) = records.split_at(records.len().min(known_count));
    for (offset, record) in known.iter().enumerate() {
        let (version, digest) = &expected[start + offset];
        let digest_matches = if record.revision_type == LEGACY_BASELINE_REVISION {
            offset == 0 && record.hash.is_empty()
        } else {
            record.revision_type == LEGACY_APPLIED_REVISION && record.hash == *digest
        };
        if record.version != *version || !digest_matches {
            return HistoryShape::Invalid;
        }
    }
    let later = later
        .iter()
        .map(|record| {
            (
                // An unparseable version fails the "later than the catalog" check.
                migrations::parse_version(&record.version).unwrap_or(-1),
                record.revision_type == LEGACY_APPLIED_REVISION,
            )
        })
        .collect();
    later_versions_shape(start + known.len(), later, catalog)
}

/// Shape of a history whose first `known_count` catalog versions matched,
/// followed by `later` (version, succeeded) records.
fn later_versions_shape(
    known_count: usize,
    later: Vec<(i64, bool)>,
    catalog: &[Migration],
) -> HistoryShape {
    let Some(&(latest, _)) = later.last() else {
        return HistoryShape::Prefix(known_count);
    };
    let catalog_latest = catalog.last().map_or(0, |migration| migration.version);
    let later_is_valid = known_count == catalog.len()
        && later
            .iter()
            .all(|&(version, succeeded)| version > catalog_latest && succeeded)
        && later.windows(2).all(|pair| pair[0].0 < pair[1].0);
    if later_is_valid {
        HistoryShape::NewerThanTool {
            latest: version_string(latest),
        }
    } else {
        HistoryShape::Invalid
    }
}

/// The contract and common task state are introduced by the schema-contract
/// migration. Before that boundary neither table may exist; from it onward
/// the single contract row must name the applied prefix tip.
async fn control_tables_match_prefix(
    conn: &mut Connection,
    applied_count: usize,
    has_contract: bool,
    has_task_state: bool,
) -> Result<bool> {
    let catalog = migrations::migrations();
    let contract_index = catalog
        .iter()
        .position(|migration| migration.version == SCHEMA_CONTRACT_VERSION)
        .context("the compiled catalog lacks the schema-contract migration")?;
    if applied_count <= contract_index {
        return Ok(!has_contract && !has_task_state);
    }
    if !has_contract || !has_task_state {
        return Ok(false);
    }
    contract_is(conn, &version_string(catalog[applied_count - 1].version)).await
}

/// Whether the contract table holds exactly the `rdb_schema` row for `version`.
pub async fn contract_is(conn: &mut Connection, version: &str) -> Result<bool> {
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT contract_key, version FROM memories_schema_contract ORDER BY contract_key ASC",
    )
    .fetch_all(&mut *conn)
    .await
    .context("reading the schema contract")?;
    Ok(rows == vec![("rdb_schema".to_string(), version.to_string())])
}

/// Whether `table_name` exists in the connection's application schema.
pub async fn table_exists(conn: &mut Connection, table_name: &str) -> Result<bool> {
    #[cfg(feature = "postgres")]
    let sql = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = $1)";
    #[cfg(not(feature = "postgres"))]
    let sql = "SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?)";
    sqlx::query_scalar(sql)
        .bind(table_name)
        .fetch_one(&mut *conn)
        .await
        .with_context(|| format!("checking whether table {table_name} exists"))
}

pub(crate) async fn legacy_history_exists(
    conn: &mut Connection,
    location: LegacyHistoryLocation,
) -> Result<bool> {
    match location {
        LegacyHistoryLocation::ApplicationSchema => table_exists(conn, LEGACY_HISTORY_TABLE).await,
        LegacyHistoryLocation::DedicatedSchema => sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2)",
        )
        .bind(LEGACY_HISTORY_SCHEMA)
        .bind(LEGACY_HISTORY_TABLE)
        .fetch_one(&mut *conn)
        .await
        .context("checking the dedicated legacy history schema"),
    }
}

async fn read_current_history(conn: &mut Connection) -> Result<Vec<CurrentRecord>> {
    if !table_exists(conn, HISTORY_TABLE).await? {
        return Ok(Vec::new());
    }
    let rows: Vec<(i64, bool, Vec<u8>)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT version, success, checksum FROM {HISTORY_TABLE} ORDER BY version ASC"
    )))
    .fetch_all(&mut *conn)
    .await
    .context("reading the migration history")?;
    Ok(rows
        .into_iter()
        .map(|(version, success, checksum)| CurrentRecord {
            version,
            success,
            checksum,
        })
        .collect())
}

async fn read_legacy_history(
    conn: &mut Connection,
    location: LegacyHistoryLocation,
) -> Result<Vec<LegacyRecord>> {
    type Row = (String, i64, String, i64, i64, Option<String>);
    let rows: Vec<Row> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT version, type, hash, applied, total, error FROM {} ORDER BY version ASC",
        location.qualified_table()
    )))
    .fetch_all(&mut *conn)
    .await
    .context("reading the legacy migration history")?;
    Ok(rows
        .into_iter()
        .map(
            |(version, revision_type, hash, applied, total, error)| LegacyRecord {
                version,
                revision_type,
                hash,
                completed: revision_completed(applied, total, error.as_deref()),
            },
        )
        .collect())
}

/// Atlas also records failed and partially applied attempts: those carry an
/// error, or ran fewer statements than the file holds.
fn revision_completed(applied: i64, total: i64, error: Option<&str>) -> bool {
    applied == total && error.is_none_or(str::is_empty)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Shapes against the compiled catalog.
    fn compiled_current_shape(records: &[CurrentRecord]) -> HistoryShape {
        current_history_shape(records, migrations::migrations())
    }

    fn compiled_legacy_shape(records: &[LegacyRecord]) -> HistoryShape {
        legacy_history_shape(records, migrations::migrations())
    }

    fn current(count: usize) -> Vec<CurrentRecord> {
        migrations::migrations()
            .iter()
            .take(count)
            .map(|migration| CurrentRecord {
                version: migration.version,
                success: true,
                checksum: migration.checksum.to_vec(),
            })
            .collect()
    }

    fn applied(version: String, hash: String) -> LegacyRecord {
        LegacyRecord {
            version,
            revision_type: LEGACY_APPLIED_REVISION,
            hash,
            completed: true,
        }
    }

    fn baseline(version: String) -> LegacyRecord {
        LegacyRecord {
            version,
            revision_type: LEGACY_BASELINE_REVISION,
            hash: String::new(),
            completed: true,
        }
    }

    fn legacy_applied(count: usize) -> Vec<LegacyRecord> {
        migrations::legacy_history_digests(migrations::migrations())
            .into_iter()
            .take(count)
            .map(|(version, hash)| applied(version, hash))
            .collect()
    }

    /// Atlas history of a database adopted at `baseline`, applied up to the
    /// whole catalog.
    fn legacy_adopted(baseline: i64) -> Vec<LegacyRecord> {
        let digests = migrations::legacy_history_digests(migrations::migrations());
        let start = digests
            .iter()
            .position(|(version, _)| *version == version_string(baseline))
            .unwrap();
        let mut records = vec![self::baseline(version_string(baseline))];
        records.extend(
            digests
                .into_iter()
                .skip(start + 1)
                .map(|(version, hash)| applied(version, hash)),
        );
        records
    }

    fn catalog_len() -> usize {
        migrations::migrations().len()
    }

    #[test]
    fn a_current_history_prefix_is_accepted() {
        assert_eq!(compiled_current_shape(&current(1)), HistoryShape::Prefix(1));
        assert_eq!(
            compiled_current_shape(&current(catalog_len())),
            HistoryShape::Prefix(catalog_len())
        );
    }

    #[test]
    fn a_current_history_must_match_the_catalog() {
        let mut changed = current(2);
        changed[1].checksum[0] ^= 0xff;
        assert_eq!(compiled_current_shape(&changed), HistoryShape::Invalid);

        let mut failed = current(2);
        failed[1].success = false;
        assert_eq!(compiled_current_shape(&failed), HistoryShape::Invalid);

        let mut gap = current(3);
        gap.remove(1);
        assert_eq!(compiled_current_shape(&gap), HistoryShape::Invalid);
    }

    #[test]
    fn later_current_versions_mean_a_newer_release() {
        let mut newer = current(catalog_len());
        newer.push(CurrentRecord {
            version: 20991231000001,
            success: true,
            checksum: vec![1],
        });
        assert_eq!(
            compiled_current_shape(&newer),
            HistoryShape::NewerThanTool {
                latest: "20991231000001".to_string()
            }
        );

        let mut failed_newer = newer;
        failed_newer.last_mut().unwrap().success = false;
        assert_eq!(compiled_current_shape(&failed_newer), HistoryShape::Invalid);

        // An unknown version before the end of the catalog is not "newer".
        let mut early_unknown = current(2);
        early_unknown.push(CurrentRecord {
            version: 20991231000001,
            success: true,
            checksum: vec![1],
        });
        assert_eq!(
            compiled_current_shape(&early_unknown),
            HistoryShape::Invalid
        );
    }

    #[test]
    fn a_legacy_history_prefix_is_accepted() {
        assert_eq!(
            compiled_legacy_shape(&legacy_applied(4)),
            HistoryShape::Prefix(4)
        );
        assert_eq!(
            compiled_legacy_shape(&legacy_applied(catalog_len())),
            HistoryShape::Prefix(catalog_len())
        );
    }

    #[test]
    fn adopted_legacy_histories_count_the_baseline_as_applied() {
        for baseline in ADOPTION_BASELINE_VERSIONS {
            assert_eq!(
                compiled_legacy_shape(&legacy_adopted(baseline)),
                HistoryShape::Prefix(catalog_len()),
                "baseline {baseline}"
            );
        }
    }

    #[test]
    fn a_legacy_history_must_carry_the_compiled_digests() {
        let mut changed = legacy_applied(3);
        changed[2].hash = "AAAA".to_string();
        assert_eq!(compiled_legacy_shape(&changed), HistoryShape::Invalid);

        let mut baseline_with_hash = legacy_adopted(ADOPTION_BASELINE_VERSIONS[0]);
        baseline_with_hash[0].hash = "AAAA".to_string();
        assert_eq!(
            compiled_legacy_shape(&baseline_with_hash),
            HistoryShape::Invalid
        );
    }

    #[test]
    fn a_failed_legacy_revision_is_not_counted_as_applied() {
        // Atlas keeps a failed attempt as a row with the expected digest;
        // the migration did not take effect, so neither the failed row nor
        // anything after it may be handed over.
        let mut failed_last = legacy_applied(4);
        failed_last[3].completed = false;
        assert_eq!(compiled_legacy_shape(&failed_last), HistoryShape::Invalid);

        let mut failed_newer = legacy_applied(catalog_len());
        let mut failed = applied("20991231000001".to_string(), "unknown".to_string());
        failed.completed = false;
        failed_newer.push(failed);
        assert_eq!(compiled_legacy_shape(&failed_newer), HistoryShape::Invalid);
    }

    #[test]
    fn a_revision_completes_only_without_error_and_with_every_statement() {
        assert!(revision_completed(3, 3, None));
        assert!(revision_completed(3, 3, Some("")));
        assert!(revision_completed(0, 0, Some("")), "a baseline record");
        assert!(!revision_completed(3, 3, Some("statement failed")));
        assert!(!revision_completed(2, 3, None));
        assert!(!revision_completed(2, 3, Some("")));
    }

    #[test]
    fn a_legacy_history_must_start_at_a_known_boundary() {
        assert_eq!(compiled_legacy_shape(&[]), HistoryShape::Invalid);
        // An applied revision may start only at the first migration.
        let mut late_start = legacy_applied(3);
        late_start.remove(0);
        assert_eq!(compiled_legacy_shape(&late_start), HistoryShape::Invalid);
        // A baseline may start only at an adoption candidate.
        let mut late_baseline = legacy_adopted(ADOPTION_BASELINE_VERSIONS[1]);
        late_baseline.remove(0);
        late_baseline[0].revision_type = LEGACY_BASELINE_REVISION;
        late_baseline[0].hash = String::new();
        assert_eq!(compiled_legacy_shape(&late_baseline), HistoryShape::Invalid);
        // Only the first record may be a baseline.
        let mut second_baseline = legacy_applied(3);
        second_baseline[1].revision_type = LEGACY_BASELINE_REVISION;
        second_baseline[1].hash = String::new();
        assert_eq!(
            compiled_legacy_shape(&second_baseline),
            HistoryShape::Invalid
        );
    }

    #[test]
    fn later_legacy_versions_mean_a_newer_release() {
        let mut newer = legacy_applied(catalog_len());
        newer.push(applied("20991231000001".to_string(), "unknown".to_string()));
        assert_eq!(
            compiled_legacy_shape(&newer),
            HistoryShape::NewerThanTool {
                latest: "20991231000001".to_string()
            }
        );
    }

    #[test]
    fn a_single_migration_catalog_is_classified_at_its_boundaries() {
        let catalog = &migrations::migrations()[..1];
        let only = version_string(catalog[0].version);
        let next = version_string(migrations::migrations()[1].version);

        assert_eq!(
            current_history_shape(&current(1), catalog),
            HistoryShape::Prefix(1)
        );
        // The real second migration is unknown to this catalog: it is a
        // newer release's, not damage.
        assert_eq!(
            current_history_shape(&current(2), catalog),
            HistoryShape::NewerThanTool {
                latest: next.clone()
            }
        );
        let mut failed = current(1);
        failed[0].success = false;
        assert_eq!(
            current_history_shape(&failed, catalog),
            HistoryShape::Invalid
        );

        assert_eq!(
            legacy_history_shape(&legacy_applied(1), catalog),
            HistoryShape::Prefix(1)
        );
        assert_eq!(
            legacy_history_shape(&[baseline(only)], catalog),
            HistoryShape::Prefix(1)
        );
        assert_eq!(
            legacy_history_shape(&legacy_applied(2), catalog),
            HistoryShape::NewerThanTool { latest: next }
        );
        assert_eq!(legacy_history_shape(&[], catalog), HistoryShape::Invalid);
    }

    #[test]
    fn the_location_of_the_legacy_history_follows_the_url() {
        assert_eq!(
            LegacyHistoryLocation::for_url("sqlite:///tmp/a.db?mode=rwc"),
            LegacyHistoryLocation::ApplicationSchema
        );
        assert_eq!(
            LegacyHistoryLocation::for_url("postgres://u:p@h/db?sslmode=disable"),
            LegacyHistoryLocation::DedicatedSchema
        );
        assert_eq!(
            LegacyHistoryLocation::for_url("postgres://u:p@h/db?search_path=memories"),
            LegacyHistoryLocation::ApplicationSchema
        );
        assert_eq!(
            LegacyHistoryLocation::for_url("postgresql://u:p@h/db?options%5Bsearch_path%5D=m"),
            LegacyHistoryLocation::ApplicationSchema
        );
    }

    #[test]
    fn pending_counts_follow_the_state() {
        let total = catalog_len();
        assert_eq!(SchemaState::Uninitialized.pending_count(), Some(total));
        assert_eq!(
            SchemaState::Pending { applied_count: 2 }.pending_count(),
            Some(total - 2)
        );
        assert_eq!(SchemaState::Managed.pending_count(), Some(0));
        assert_eq!(SchemaState::Unmanaged.pending_count(), None);
        assert_eq!(SchemaState::NewerThanTool.pending_count(), None);
        assert_eq!(SchemaState::SchemaCorrupt.pending_count(), None);
    }
}
