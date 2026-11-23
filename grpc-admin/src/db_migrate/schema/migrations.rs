//! Schema migrations compiled into this binary, for its RDB backend.

use base64::{Engine as _, engine::general_purpose::STANDARD};
use sha2::{Digest, Sha256};
use sqlx::migrate::{Migration, Migrator};

/// The migration history table (SQLx's history format).
pub const HISTORY_TABLE: &str = "memories_schema_migrations";
/// The history table written by the previous migration engine (Atlas).
pub const LEGACY_HISTORY_TABLE: &str = "atlas_schema_revisions";
/// The schema in which Atlas kept its history when the target URL named no
/// `search_path`.
pub const LEGACY_HISTORY_SCHEMA: &str = "atlas_schema_revisions";

#[cfg(feature = "postgres")]
static EMBEDDED: Migrator = sqlx::migrate!("../infra/db-migrate/postgres/migrations");
#[cfg(not(feature = "postgres"))]
static EMBEDDED: Migrator = sqlx::migrate!("../infra/db-migrate/sqlite/migrations");

/// The backend whose migrations this binary carries.
pub const BACKEND: &str = if cfg!(feature = "postgres") {
    "postgres"
} else {
    "sqlite"
};

/// A migrator for the compiled catalog that records into [`HISTORY_TABLE`].
pub fn migrator() -> Migrator {
    let mut migrator = Migrator::with_migrations(migrations().to_vec());
    migrator.dangerous_set_table_name(HISTORY_TABLE);
    migrator
}

/// The compiled migrations in ascending version order.
pub fn migrations() -> &'static [Migration] {
    &EMBEDDED.migrations
}

/// The 14-digit `YYYYMMDDHHMMSS` version of a migration.
pub fn version_string(version: i64) -> String {
    format!("{version:014}")
}

/// The numeric value of a 14-digit `YYYYMMDDHHMMSS` version string, or
/// `None` for any other text.
pub fn parse_version(version: &str) -> Option<i64> {
    (version.len() == 14 && version.bytes().all(|byte| byte.is_ascii_digit()))
        .then(|| version.parse().ok())
        .flatten()
}

/// Versions of the compiled catalog, oldest first.
pub fn versions() -> Vec<String> {
    migrations()
        .iter()
        .map(|migration| version_string(migration.version))
        .collect()
}

/// The newest version of the compiled catalog.
pub fn latest_version() -> String {
    let latest = migrations()
        .last()
        .expect("the compiled migration catalog is never empty");
    version_string(latest.version)
}

/// The source file name of a migration. SQLx keeps only the description,
/// with underscores replaced by spaces.
pub fn file_name(migration: &Migration) -> String {
    format!(
        "{}_{}.sql",
        version_string(migration.version),
        migration.description.replace(' ', "_")
    )
}

/// The content digest the previous engine recorded in its history for each
/// migration of `catalog` (`atlas.sum` form without the `h1:` prefix): a SHA-256
/// over every file name and body up to and including that migration.
pub fn legacy_history_digests(catalog: &[Migration]) -> Vec<(String, String)> {
    let mut cumulative = Sha256::new();
    catalog
        .iter()
        .map(|migration| {
            cumulative.update(file_name(migration).as_bytes());
            cumulative.update(migration.sql.as_str().as_bytes());
            (
                version_string(migration.version),
                STANDARD.encode(cumulative.clone().finalize()),
            )
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source_directory() -> std::path::PathBuf {
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../infra/db-migrate")
            .join(BACKEND)
            .join("migrations")
    }

    #[test]
    fn versions_are_unique_ascending_timestamps() {
        let versions = versions();
        assert!(!versions.is_empty());
        assert!(
            versions
                .iter()
                .all(|version| parse_version(version).is_some())
        );
        assert!(versions.windows(2).all(|pair| pair[0] < pair[1]));
        assert_eq!(latest_version(), *versions.last().unwrap());
    }

    #[test]
    fn only_fourteen_digit_versions_parse() {
        assert_eq!(parse_version("20260803000001"), Some(20260803000001));
        assert_eq!(parse_version("2026080300000"), None);
        assert_eq!(parse_version("202608030000011"), None);
        assert_eq!(parse_version("2026080300000a"), None);
        assert_eq!(parse_version("+2026080300001"), None);
        assert_eq!(parse_version(""), None);
    }

    #[test]
    fn every_migration_runs_in_a_transaction() {
        // The history row is written in the same transaction as the body,
        // which is what keeps a concurrent apply from applying twice.
        assert!(migrations().iter().all(|migration| !migration.no_tx));
    }

    #[test]
    fn file_names_are_restored_exactly() {
        let mut on_disk = std::fs::read_dir(source_directory())
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into_string().unwrap())
            .filter(|name| name.ends_with(".sql"))
            .collect::<Vec<_>>();
        on_disk.sort();
        let restored = migrations().iter().map(file_name).collect::<Vec<_>>();
        assert_eq!(restored, on_disk);
    }

    #[test]
    fn the_migrator_records_into_the_memories_history_table() {
        let migrator = migrator();
        assert_eq!(migrator.table_name, HISTORY_TABLE);
        assert_eq!(migrator.migrations.len(), migrations().len());
        assert!(migrator.locking);
    }

    #[test]
    fn legacy_digests_are_cumulative() {
        let digests = legacy_history_digests(migrations());
        assert_eq!(digests.len(), migrations().len());
        // Each digest covers the earlier files too, so all of them differ
        // even when two migration bodies are identical.
        let unique = digests
            .iter()
            .map(|(_, digest)| digest)
            .collect::<std::collections::BTreeSet<_>>();
        assert_eq!(unique.len(), digests.len());
    }

    #[test]
    fn both_backends_carry_the_same_versions_and_descriptions() {
        let list = |backend: &str| {
            let directory = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../infra/db-migrate")
                .join(backend)
                .join("migrations");
            let mut names = std::fs::read_dir(directory)
                .unwrap()
                .map(|entry| entry.unwrap().file_name().into_string().unwrap())
                .filter(|name| name.ends_with(".sql"))
                .collect::<Vec<_>>();
            names.sort();
            names
        };
        assert_eq!(list("sqlite"), list("postgres"));
    }
}
