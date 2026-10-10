//! Pre-migration backups of every resource a local migration may change.
//!
//! A backup directory is complete only once its manifest exists: the manifest
//! is written last, after every copied file is on disk, so an interrupted
//! backup can never be mistaken for a restorable one.

pub use super::files::FileEntry;
use super::files::{
    available_space, barrier, copy_directory, flush, hash_file, remove_path, sync_directory,
    total_size, write_atomically,
};
use super::output::{ErrorCode, LocalFailure, Resolution, classify, fail};
use super::target::SqliteTarget;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

const MANIFEST_FORMAT: &str = "memories-local-backup-v1";
const MANIFEST_FILE: &str = "manifest.json";
pub(crate) const BACKUP_PREFIX: &str = "memories-backup-";
const DELETING_PREFIX: &str = ".deleting-";
const SQLITE_DIRECTORY: &str = "sqlite";
const RESOURCE_DIRECTORY: &str = "resources";
const MIN_SPACE_MARGIN: u64 = 64 * 1024 * 1024;

/// Facts about the database at backup time, needed to judge a later restore.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackupInfo {
    pub schema_status: String,
    pub schema_version: Option<String>,
    pub bundle_digest: Option<String>,
    /// Embedding space of the vector tables when the backup holds any.
    pub embedding_space_id: Option<String>,
}

/// A directory, other than the SQLite database, that a selected task changes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackupResource {
    pub name: String,
    pub source: PathBuf,
}

impl BackupResource {
    pub fn directory(name: &str, source: PathBuf) -> Self {
        Self {
            name: name.to_string(),
            source,
        }
    }

    /// Only local directories can be copied consistently with the database.
    pub fn from_uri(name: &str, uri: &str) -> Result<Self> {
        if uri.starts_with("file:")
            && let Some(path) = url::Url::parse(uri)
                .ok()
                .and_then(|parsed| parsed.to_file_path().ok())
        {
            return Ok(Self::directory(name, path));
        }
        if uri.contains("://") {
            return fail(
                ErrorCode::UnsupportedResource,
                Resolution::ToolUpdateRequired,
                format!("{name} at {uri} is not a local directory and cannot be backed up"),
            );
        }
        Ok(Self::directory(name, PathBuf::from(uri)))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SqliteEntry {
    /// Location inside the backup directory.
    pub path: PathBuf,
    pub size: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResourceEntry {
    pub name: String,
    pub source: PathBuf,
    /// Whether the resource existed; an absent one is restored as absent.
    pub present: bool,
    /// Location inside the backup directory.
    pub path: PathBuf,
    pub files: Vec<FileEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackupManifest {
    pub format: String,
    pub backup_id: String,
    pub created_at: i64,
    pub schema_status: String,
    pub schema_version: Option<String>,
    pub bundle_digest: Option<String>,
    pub sqlite: SqliteEntry,
    pub resources: Vec<ResourceEntry>,
    /// Embedding space of the backed-up vector tables (absent in backups
    /// of earlier releases).
    #[serde(default)]
    pub embedding_space_id: Option<String>,
}

impl BackupManifest {
    /// Load the manifest of a complete backup created by this tool.
    pub fn load(backup: &Path) -> Result<Self> {
        let path = backup.join(MANIFEST_FILE);
        let bytes = std::fs::read(&path).map_err(|error| {
            LocalFailure::new(
                ErrorCode::BackupIncomplete,
                Resolution::Retry,
                format!("{} has no readable manifest: {error}", backup.display()),
            )
        })?;
        let manifest: Self = serde_json::from_slice(&bytes).map_err(|error| {
            LocalFailure::new(
                ErrorCode::BackupIncomplete,
                Resolution::Retry,
                format!("{} has an invalid manifest: {error}", backup.display()),
            )
        })?;
        if manifest.format != MANIFEST_FORMAT {
            return fail(
                ErrorCode::BackupIncomplete,
                Resolution::Retry,
                format!("{} was not created by this tool", backup.display()),
            );
        }
        Ok(manifest)
    }

    /// Check that every recorded file exists with its recorded size.
    pub fn verify_presence(&self, backup: &Path) -> Result<()> {
        let mut expected = vec![(backup.join(&self.sqlite.path), self.sqlite.size)];
        for resource in self.resources.iter().filter(|resource| resource.present) {
            let root = backup.join(&resource.path);
            expected.extend(
                resource
                    .files
                    .iter()
                    .map(|file| (root.join(&file.path), file.size)),
            );
        }
        for (path, size) in expected {
            let actual = std::fs::metadata(&path)
                .ok()
                .filter(|metadata| metadata.is_file())
                .map(|metadata| metadata.len());
            if actual != Some(size) {
                return fail(
                    ErrorCode::BackupIncomplete,
                    Resolution::Retry,
                    format!(
                        "{} is missing or has another size than recorded",
                        path.display()
                    ),
                );
            }
        }
        Ok(())
    }

    /// Check that every recorded file is present with its recorded content.
    pub fn verify_contents(&self, backup: &Path) -> Result<()> {
        verify_file(
            &backup.join(&self.sqlite.path),
            self.sqlite.size,
            &self.sqlite.sha256,
        )?;
        for resource in self.resources.iter().filter(|resource| resource.present) {
            let root = backup.join(&resource.path);
            for file in &resource.files {
                verify_file(&root.join(&file.path), file.size, &file.sha256)?;
            }
        }
        Ok(())
    }
}

fn verify_file(path: &Path, size: u64, sha256: &str) -> Result<()> {
    let actual = std::fs::metadata(path)
        .ok()
        .filter(|metadata| metadata.is_file())
        .map(|metadata| metadata.len());
    if actual != Some(size) || hash_file(path).ok().as_deref() != Some(sha256) {
        return fail(
            ErrorCode::BackupIncomplete,
            Resolution::Retry,
            format!("{} is missing or differs from the manifest", path.display()),
        );
    }
    Ok(())
}

/// Space needed for a backup of `total` bytes: the copy plus a safety margin.
pub fn required_space(total: u64) -> u64 {
    total + (total / 10).max(MIN_SPACE_MARGIN)
}

/// Create a complete backup under `parent` and return its directory.
pub async fn create_backup(
    parent: &Path,
    target: &SqliteTarget,
    resources: &[BackupResource],
    info: &BackupInfo,
) -> Result<PathBuf> {
    let available = available_space(parent)?;
    create_backup_with_space(parent, target, resources, info, available).await
}

pub(crate) async fn create_backup_with_space(
    parent: &Path,
    target: &SqliteTarget,
    resources: &[BackupResource],
    info: &BackupInfo,
    available: u64,
) -> Result<PathBuf> {
    let mut total = total_size(target.database())? + total_size(&target.wal())?;
    for resource in resources {
        total += total_size(&resource.source)?;
    }
    let required = required_space(total);
    if available < required {
        return fail(
            ErrorCode::InsufficientSpace,
            Resolution::Retry,
            format!(
                "{} has {available} bytes free but the backup needs {required}",
                parent.display()
            ),
        );
    }
    std::fs::create_dir_all(parent)
        .with_context(|| format!("creating backup directory {}", parent.display()))?;
    let created_at = command_utils::util::datetime::now_millis();
    let backup_id = format!("{BACKUP_PREFIX}{created_at}-{}", std::process::id());
    let backup = parent.join(&backup_id);
    std::fs::create_dir(&backup)
        .with_context(|| format!("creating backup {}", backup.display()))?;
    let written = write_backup(&backup, &backup_id, created_at, target, resources, info).await;
    if let Err(error) = written {
        let _ = remove_path(&backup);
        return Err(classify(error, ErrorCode::BackupFailed, Resolution::Retry));
    }
    sync_directory(parent)?;
    Ok(backup)
}

async fn write_backup(
    backup: &Path,
    backup_id: &str,
    created_at: i64,
    target: &SqliteTarget,
    resources: &[BackupResource],
    info: &BackupInfo,
) -> Result<()> {
    let sqlite_directory = backup.join(SQLITE_DIRECTORY);
    std::fs::create_dir(&sqlite_directory)?;
    let file_name = target
        .database()
        .file_name()
        .context("database path has no file name")?;
    let sqlite_path = PathBuf::from(SQLITE_DIRECTORY).join(file_name);
    snapshot_sqlite(target, &backup.join(&sqlite_path)).await?;
    let snapshot = backup.join(&sqlite_path);
    flush(&std::fs::File::open(&snapshot)?)?;
    sync_directory(&sqlite_directory)?;
    let sqlite = SqliteEntry {
        size: std::fs::metadata(&snapshot)?.len(),
        sha256: hash_file(&snapshot)?,
        path: sqlite_path,
    };

    let mut resource_entries = Vec::with_capacity(resources.len());
    for resource in resources {
        let path = PathBuf::from(RESOURCE_DIRECTORY).join(&resource.name);
        let present = resource.source.exists();
        let files = if present {
            copy_directory(&resource.source, &backup.join(&path))?
        } else {
            Vec::new()
        };
        resource_entries.push(ResourceEntry {
            name: resource.name.clone(),
            source: resource.source.clone(),
            present,
            path,
            files,
        });
    }
    if backup.join(RESOURCE_DIRECTORY).exists() {
        sync_directory(&backup.join(RESOURCE_DIRECTORY))?;
    }
    // Every copied file must be durable before the manifest publishes them.
    barrier(backup)?;

    let manifest = BackupManifest {
        format: MANIFEST_FORMAT.to_string(),
        backup_id: backup_id.to_string(),
        created_at,
        schema_status: info.schema_status.clone(),
        schema_version: info.schema_version.clone(),
        bundle_digest: info.bundle_digest.clone(),
        sqlite,
        resources: resource_entries,
        embedding_space_id: info.embedding_space_id.clone(),
    };
    let bytes = serde_json::to_vec_pretty(&manifest).context("serializing backup manifest")?;
    write_atomically(&backup.join(MANIFEST_FILE), &bytes)
}

/// `VACUUM INTO` copies a transactionally consistent image that includes
/// committed pages still held in the WAL.
async fn snapshot_sqlite(target: &SqliteTarget, destination: &Path) -> Result<()> {
    use sqlx::{ConnectOptions, Connection, sqlite::SqliteConnectOptions};

    let destination = destination
        .to_str()
        .context("backup path must be valid UTF-8 for SQLite")?;
    let mut connection = SqliteConnectOptions::new()
        .filename(target.database())
        .read_only(true)
        .connect()
        .await
        .context("opening the database for backup")?;
    let statement = format!("VACUUM INTO '{}'", destination.replace('\'', "''"));
    sqlx::query(sqlx::AssertSqlSafe(statement))
        .execute(&mut connection)
        .await
        .context("snapshotting the database")?;
    connection.close().await?;
    Ok(())
}

/// Complete backups created by this tool under `parent`, newest first.
pub fn list_backups(parent: &Path) -> Result<Vec<(PathBuf, BackupManifest)>> {
    let mut backups = Vec::new();
    let entries = match std::fs::read_dir(parent) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(backups),
        Err(error) => {
            return Err(anyhow::Error::new(error).context(format!("listing {}", parent.display())));
        }
    };
    for entry in entries {
        let path = entry?.path();
        let is_backup = path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with(BACKUP_PREFIX));
        if is_backup && let Ok(manifest) = BackupManifest::load(&path) {
            backups.push((path, manifest));
        }
    }
    backups
        .sort_by(|left, right| (right.1.created_at, &right.0).cmp(&(left.1.created_at, &left.0)));
    Ok(backups)
}

/// Delete complete backups beyond the newest `keep`, never touching
/// unfinished backups or anything not created here.
pub fn apply_retention(parent: &Path, keep: usize) -> Result<Vec<PathBuf>> {
    if !parent.is_dir() {
        return Ok(Vec::new());
    }
    remove_interrupted_deletions(parent)?;
    let mut removed = Vec::new();
    for (path, _) in list_backups(parent)?.into_iter().skip(keep) {
        // Renaming first means a crash leaves a name that the next run
        // recognizes as a deletion in progress, not an unfinished backup.
        let name = path.file_name().context("backup path has no name")?;
        let mut deleting_name = std::ffi::OsString::from(DELETING_PREFIX);
        deleting_name.push(name);
        let deleting = parent.join(deleting_name);
        std::fs::rename(&path, &deleting)
            .with_context(|| format!("retiring backup {}", path.display()))?;
        sync_directory(parent)?;
        remove_path(&deleting)?;
        removed.push(path);
    }
    sync_directory(parent)?;
    Ok(removed)
}

fn remove_interrupted_deletions(parent: &Path) -> Result<()> {
    let Ok(entries) = std::fs::read_dir(parent) else {
        return Ok(());
    };
    for entry in entries {
        let path = entry?.path();
        let interrupted = path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.starts_with(&format!("{DELETING_PREFIX}{BACKUP_PREFIX}")));
        if interrupted {
            remove_path(&path)?;
        }
    }
    Ok(())
}

#[cfg(all(test, not(feature = "postgres")))]
mod tests {
    use super::*;
    use crate::db_migrate::local::output::{ErrorCode, LocalFailure};
    use sqlx::{ConnectOptions, Connection, sqlite::SqliteConnectOptions};

    const MIB: u64 = 1024 * 1024;

    fn info() -> BackupInfo {
        BackupInfo {
            schema_status: "managed".to_string(),
            schema_version: Some("20260930000001".to_string()),
            bundle_digest: Some("bundle".to_string()),
            embedding_space_id: None,
        }
    }

    async fn wal_database(target: &SqliteTarget) -> sqlx::SqliteConnection {
        let mut connection = SqliteConnectOptions::new()
            .filename(target.database())
            .create_if_missing(true)
            .connect()
            .await
            .unwrap();
        for statement in [
            "PRAGMA journal_mode = WAL",
            "PRAGMA wal_autocheckpoint = 0",
            "CREATE TABLE t (value TEXT)",
            "INSERT INTO t VALUES ('committed-in-wal')",
        ] {
            sqlx::query(statement)
                .execute(&mut connection)
                .await
                .unwrap();
        }
        connection
    }

    fn lancedb_fixture(root: &Path) -> PathBuf {
        let table = root.join("threads.lance").join("data");
        std::fs::create_dir_all(&table).unwrap();
        std::fs::write(table.join("part-0.lance"), b"vector bytes").unwrap();
        std::fs::write(root.join("threads.lance").join("_versions"), b"v1").unwrap();
        root.to_path_buf()
    }

    #[test]
    fn backup_contains_wal_content_resources_and_a_manifest() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            let workspace = tempfile::tempdir().unwrap();
            let target = SqliteTarget::at(workspace.path().join("default.sqlite3"));
            // The open connection keeps the committed row only in the WAL.
            let connection = wal_database(&target).await;
            let vectors = lancedb_fixture(&workspace.path().join("lancedb"));
            let parent = workspace.path().join("backups");

            let resources = vec![
                BackupResource::directory("thread_lancedb", vectors.clone()),
                BackupResource::directory("absent_vectors", workspace.path().join("missing")),
            ];
            let backup = create_backup(&parent, &target, &resources, &info())
                .await
                .unwrap();
            connection.close().await.unwrap();

            let manifest = BackupManifest::load(&backup).unwrap();
            assert_eq!(manifest.schema_version.as_deref(), Some("20260930000001"));
            let mut snapshot = SqliteConnectOptions::new()
                .filename(backup.join(&manifest.sqlite.path))
                .connect()
                .await
                .unwrap();
            let value: String = sqlx::query_scalar("SELECT value FROM t")
                .fetch_one(&mut snapshot)
                .await
                .unwrap();
            assert_eq!(value, "committed-in-wal");

            let copied = backup.join(&manifest.resources[0].path);
            assert_eq!(
                std::fs::read(copied.join("threads.lance/data/part-0.lance")).unwrap(),
                b"vector bytes"
            );
            assert_eq!(manifest.resources[0].files.len(), 2);
            assert!(manifest.resources[0].present);
            assert!(!manifest.resources[1].present);
            manifest.verify_contents(&backup).unwrap();
        });
    }

    #[test]
    fn insufficient_space_fails_before_creating_anything() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            let workspace = tempfile::tempdir().unwrap();
            let target = SqliteTarget::at(workspace.path().join("db"));
            wal_database(&target).await.close().await.unwrap();
            let parent = workspace.path().join("backups");

            let error = create_backup_with_space(&parent, &target, &[], &info(), 64 * MIB - 1)
                .await
                .unwrap_err();
            assert_eq!(
                error.downcast_ref::<LocalFailure>().map(|f| f.error_code),
                Some(ErrorCode::InsufficientSpace)
            );
            assert!(!parent.exists());
        });
    }

    #[test]
    fn required_space_keeps_a_margin_of_ten_percent_or_64_mib() {
        assert_eq!(required_space(0), 64 * MIB);
        assert_eq!(required_space(100 * MIB), 164 * MIB);
        assert_eq!(required_space(1000 * MIB), 1100 * MIB);
    }

    #[test]
    fn remote_resource_uris_are_not_backed_up() {
        let error = BackupResource::from_uri("thread_lancedb", "s3://bucket/vectors").unwrap_err();
        assert_eq!(
            error.downcast_ref::<LocalFailure>().map(|f| f.error_code),
            Some(ErrorCode::UnsupportedResource)
        );
        let local =
            BackupResource::from_uri("thread_lancedb", "file:///var/lib/lance%20db").unwrap();
        assert_eq!(local.source, PathBuf::from("/var/lib/lance db"));
        let plain = BackupResource::from_uri("thread_lancedb", "/var/lib/lancedb").unwrap();
        assert_eq!(plain.source, PathBuf::from("/var/lib/lancedb"));
    }

    #[test]
    fn retention_keeps_the_newest_complete_backups_only() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            let workspace = tempfile::tempdir().unwrap();
            let target = SqliteTarget::at(workspace.path().join("db"));
            wal_database(&target).await.close().await.unwrap();
            let parent = workspace.path().join("backups");
            let mut created = Vec::new();
            for _ in 0..3 {
                created.push(create_backup(&parent, &target, &[], &info()).await.unwrap());
            }
            let foreign = parent.join("photos");
            std::fs::create_dir(&foreign).unwrap();
            let unfinished = parent.join("memories-backup-unfinished");
            std::fs::create_dir(&unfinished).unwrap();

            let removed = apply_retention(&parent, 1).unwrap();
            assert_eq!(removed, vec![created[1].clone(), created[0].clone()]);
            assert!(created[2].exists(), "newest backup is kept");
            assert!(foreign.exists() && unfinished.exists());

            assert_eq!(
                apply_retention(&parent, 0).unwrap(),
                vec![created[2].clone()]
            );
            assert!(foreign.exists() && unfinished.exists());
        });
    }

    #[test]
    fn retention_without_any_backup_directory_is_a_no_op() {
        let workspace = tempfile::tempdir().unwrap();
        let parent = workspace.path().join("never-created");
        assert!(apply_retention(&parent, 1).unwrap().is_empty());
        assert!(!parent.exists());
    }

    #[test]
    fn tampered_backup_fails_content_verification() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            let workspace = tempfile::tempdir().unwrap();
            let target = SqliteTarget::at(workspace.path().join("db"));
            wal_database(&target).await.close().await.unwrap();
            let vectors = lancedb_fixture(&workspace.path().join("lancedb"));
            let backup = create_backup(
                &workspace.path().join("backups"),
                &target,
                &[BackupResource::directory("thread_lancedb", vectors)],
                &info(),
            )
            .await
            .unwrap();
            let manifest = BackupManifest::load(&backup).unwrap();
            std::fs::write(
                backup
                    .join(&manifest.resources[0].path)
                    .join("threads.lance/_versions"),
                b"changed",
            )
            .unwrap();
            let error = manifest.verify_contents(&backup).unwrap_err();
            assert_eq!(
                error.downcast_ref::<LocalFailure>().map(|f| f.error_code),
                Some(ErrorCode::BackupIncomplete)
            );
        });
    }
}
