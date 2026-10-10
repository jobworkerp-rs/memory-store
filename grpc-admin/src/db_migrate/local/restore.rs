//! Restoring every resource of a backup as one unit.
//!
//! Replacement is journaled: originals are moved aside (never deleted) until
//! every restored item is in place, so a failure puts the originals back and a
//! crash is undone by the next run before it restores again.

use super::backup::BackupManifest;
use super::files::{
    FileEntry, available_space, barrier, copy_directory, copy_file, remove_path, sync_directory,
    with_suffix, write_atomically,
};
use super::output::{ErrorCode, Resolution, classify, fail};
use super::target::SqliteTarget;
use super::writer::ensure_no_other_connection;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

const JOURNAL_FORMAT: &str = "memories-local-restore-v1";
const JOURNAL_SUFFIX: &str = ".memories-restore.json";
const STAGED_SUFFIX: &str = ".memories-restore-staged";
const STASH_PREFIX: &str = ".memories-restore-stash-";

/// Restored content waiting next to its destination, with the digest the
/// backup recorded for it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Staged {
    path: PathBuf,
    from: PathBuf,
    content: StagedContent,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum StagedContent {
    File { size: u64, sha256: String },
    Directory { files: Vec<FileEntry> },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct JournalItem {
    destination: PathBuf,
    original_present: bool,
    /// Where the original is kept until the restore completes.
    stash: PathBuf,
    staged: Option<Staged>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct RestoreJournal {
    format: String,
    backup: PathBuf,
    items: Vec<JournalItem>,
}

impl RestoreJournal {
    fn path(target: &SqliteTarget) -> PathBuf {
        with_suffix(target.database(), JOURNAL_SUFFIX)
    }

    fn load(target: &SqliteTarget) -> Result<Option<Self>> {
        let path = Self::path(target);
        match std::fs::read(&path) {
            Ok(bytes) => {
                let journal: Self = serde_json::from_slice(&bytes)
                    .with_context(|| format!("reading restore journal {}", path.display()))?;
                if journal.format != JOURNAL_FORMAT {
                    anyhow::bail!("{} is not a restore journal of this tool", path.display());
                }
                Ok(Some(journal))
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => {
                Err(anyhow::Error::new(error).context(format!("reading {}", path.display())))
            }
        }
    }

    fn store(&self, target: &SqliteTarget) -> Result<()> {
        let bytes = serde_json::to_vec_pretty(self).context("serializing restore journal")?;
        write_atomically(&Self::path(target), &bytes)
    }

    fn remove(target: &SqliteTarget) -> Result<()> {
        let path = Self::path(target);
        remove_path(&path)?;
        sync_parent(&path)
    }

    /// Return every destination to its state before the restore started.
    fn roll_back(&self) -> Result<()> {
        for item in self.items.iter().rev() {
            if let Some(staged) = &item.staged {
                remove_path(&staged.path)?;
            }
            if item.original_present {
                if item.stash.symlink_metadata().is_ok() {
                    remove_path(&item.destination)?;
                    std::fs::rename(&item.stash, &item.destination)
                        .with_context(|| format!("putting back {}", item.destination.display()))?;
                }
            } else {
                remove_path(&item.destination)?;
            }
            sync_parent(&item.destination)?;
        }
        self.remove_stashes()
    }

    fn remove_stashes(&self) -> Result<()> {
        for item in &self.items {
            remove_path(&item.stash)?;
            if let Some(parent) = item.stash.parent()
                && parent
                    .file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with(STASH_PREFIX))
            {
                remove_path(parent)?;
            }
        }
        Ok(())
    }
}

/// A restore of the target that was interrupted and must be run again.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InterruptedRestore {
    /// The backup it was restoring, when the journal is readable.
    pub backup: Option<PathBuf>,
}

pub fn interrupted_restore(target: &SqliteTarget) -> Option<InterruptedRestore> {
    RestoreJournal::path(target)
        .exists()
        .then(|| InterruptedRestore {
            backup: RestoreJournal::load(target)
                .ok()
                .flatten()
                .map(|journal| journal.backup),
        })
}

/// Restore `backup` over the current database and task resources.
pub async fn restore_backup(
    backup: &Path,
    target: &SqliteTarget,
    latest_schema_version: &str,
) -> Result<()> {
    restore_with(backup, target, latest_schema_version, &mut |_| Ok(())).await
}

/// `before_swap` runs before each item is replaced; tests inject failures.
async fn restore_with(
    backup: &Path,
    target: &SqliteTarget,
    latest_schema_version: &str,
    before_swap: &mut dyn FnMut(usize) -> Result<()>,
) -> Result<()> {
    if let Some(journal) = RestoreJournal::load(target)? {
        journal.roll_back().map_err(restore_failed)?;
        RestoreJournal::remove(target)?;
    }
    let manifest = validate_backup(backup, latest_schema_version)?;
    ensure_no_other_connection(target)?;
    ensure_space(target, &manifest)?;

    let journal = plan(backup, target, &manifest);
    journal.store(target)?;
    let replaced = match stage(&journal).await {
        Ok(()) => swap(&journal, before_swap),
        Err(error) => Err(error),
    };
    match replaced {
        Ok(()) => {
            journal.remove_stashes()?;
            RestoreJournal::remove(target)
        }
        Err(error) => {
            journal.roll_back().map_err(restore_failed)?;
            RestoreJournal::remove(target)?;
            Err(restore_failed(error))
        }
    }
}

/// Cheap checks before anything is copied; content digests are compared
/// while staging so that the backup is read only once.
fn validate_backup(backup: &Path, latest_schema_version: &str) -> Result<BackupManifest> {
    let manifest = BackupManifest::load(backup)?;
    manifest.verify_presence(backup)?;
    if let Some(version) = &manifest.schema_version
        && version.as_str() > latest_schema_version
    {
        return fail(
            ErrorCode::BackupNewerThanTool,
            Resolution::ToolUpdateRequired,
            format!("backup schema {version} is newer than this tool ({latest_schema_version})"),
        );
    }
    Ok(manifest)
}

fn ensure_space(target: &SqliteTarget, manifest: &BackupManifest) -> Result<()> {
    let mut needs = vec![(target.database().to_path_buf(), manifest.sqlite.size)];
    for resource in manifest
        .resources
        .iter()
        .filter(|resource| resource.present)
    {
        let size = resource.files.iter().map(|file| file.size).sum();
        needs.push((resource.source.clone(), size));
    }
    for (destination, size) in needs {
        let directory = destination.parent().unwrap_or(&destination);
        let available = available_space(directory)?;
        let required = super::backup::required_space(size);
        if available < required {
            return fail(
                ErrorCode::InsufficientSpace,
                Resolution::Retry,
                format!(
                    "{} has {available} bytes free but restoring needs {required}",
                    directory.display()
                ),
            );
        }
    }
    Ok(())
}

fn plan(backup: &Path, target: &SqliteTarget, manifest: &BackupManifest) -> RestoreJournal {
    let restore_id = format!(
        "{}-{}",
        command_utils::util::datetime::now_millis(),
        std::process::id()
    );
    let database_stash = target
        .database()
        .parent()
        .unwrap_or(Path::new("/"))
        .join(format!("{STASH_PREFIX}{restore_id}"));
    let database = Staged {
        path: with_suffix(target.database(), STAGED_SUFFIX),
        from: backup.join(&manifest.sqlite.path),
        content: StagedContent::File {
            size: manifest.sqlite.size,
            sha256: manifest.sqlite.sha256.clone(),
        },
    };
    let mut items = Vec::new();
    // The restored database must not be combined with the current WAL/SHM.
    for (destination, staged) in [
        (target.database().to_path_buf(), Some(database)),
        (target.wal(), None),
        (target.shm(), None),
    ] {
        let name = destination
            .file_name()
            .map(PathBuf::from)
            .unwrap_or_default();
        items.push(JournalItem {
            original_present: destination.symlink_metadata().is_ok(),
            stash: database_stash.join(name),
            staged,
            destination,
        });
    }
    for resource in &manifest.resources {
        let parent = resource.source.parent().unwrap_or(Path::new("/"));
        items.push(JournalItem {
            original_present: resource.source.symlink_metadata().is_ok(),
            stash: parent.join(format!("{STASH_PREFIX}{restore_id}-{}", resource.name)),
            staged: resource.present.then(|| Staged {
                path: with_suffix(&resource.source, STAGED_SUFFIX),
                from: backup.join(&resource.path),
                content: StagedContent::Directory {
                    files: resource.files.clone(),
                },
            }),
            destination: resource.source.clone(),
        });
    }
    RestoreJournal {
        format: JOURNAL_FORMAT.to_string(),
        backup: backup.to_path_buf(),
        items,
    }
}

/// Copy the backup next to each destination so that placing it is a rename,
/// checking every byte against the manifest on the way.
async fn stage(journal: &RestoreJournal) -> Result<()> {
    for staged in journal.items.iter().filter_map(|item| item.staged.as_ref()) {
        let matches = match &staged.content {
            StagedContent::File { size, sha256 } => {
                let copied = copy_file(&staged.from, &staged.path)?;
                copied.size == *size && copied.sha256 == *sha256
            }
            StagedContent::Directory { files } => {
                copy_directory(&staged.from, &staged.path)? == *files
            }
        };
        if !matches {
            return fail(
                ErrorCode::BackupIncomplete,
                Resolution::Retry,
                format!("{} differs from the backup manifest", staged.from.display()),
            );
        }
        if let Some(parent) = staged.path.parent() {
            barrier(parent)?;
        }
    }
    let database = journal.items[0]
        .staged
        .as_ref()
        .context("the database is always staged")?;
    check_sqlite_integrity(&database.path).await
}

fn swap(journal: &RestoreJournal, before_swap: &mut dyn FnMut(usize) -> Result<()>) -> Result<()> {
    for (index, item) in journal.items.iter().enumerate() {
        before_swap(index)?;
        if item.original_present {
            if let Some(parent) = item.stash.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::rename(&item.destination, &item.stash)
                .with_context(|| format!("moving aside {}", item.destination.display()))?;
        }
        if let Some(staged) = &item.staged {
            std::fs::rename(&staged.path, &item.destination)
                .with_context(|| format!("placing {}", item.destination.display()))?;
        }
        sync_parent(&item.destination)?;
    }
    Ok(())
}

async fn check_sqlite_integrity(path: &Path) -> Result<()> {
    use sqlx::{ConnectOptions, Connection, sqlite::SqliteConnectOptions};

    let checked = async {
        let mut connection = SqliteConnectOptions::new()
            .filename(path)
            .read_only(true)
            .connect()
            .await?;
        let result: String = sqlx::query_scalar("PRAGMA integrity_check")
            .fetch_one(&mut connection)
            .await?;
        connection.close().await?;
        anyhow::Ok(result)
    }
    .await;
    match checked {
        Ok(result) if result == "ok" => Ok(()),
        Ok(result) => fail(
            ErrorCode::BackupIncomplete,
            Resolution::Retry,
            format!("backup database failed the integrity check: {result}"),
        ),
        Err(error) => fail(
            ErrorCode::BackupIncomplete,
            Resolution::Retry,
            format!("backup database cannot be opened: {error:#}"),
        ),
    }
}

fn restore_failed(error: anyhow::Error) -> anyhow::Error {
    classify(error, ErrorCode::RestoreFailed, Resolution::Retry)
}

fn sync_parent(path: &Path) -> Result<()> {
    match path.parent() {
        Some(parent) if parent.exists() => sync_directory(parent),
        _ => Ok(()),
    }
}

/// A local apply attempt waits for `local restore` (interrupted restore,
/// or a failure that requires one). Embedding commands refuse to run
/// meanwhile (`apply_restore_required`).
pub fn restore_pending(database_url: &str) -> bool {
    let Ok(target) = SqliteTarget::from_url(database_url) else {
        return false;
    };
    interrupted_restore(&target).is_some()
        || super::attempt::AttemptRecord::load(&target)
            .ok()
            .flatten()
            .is_some_and(|record| {
                record.status
                    == super::attempt::AttemptStatus::Failed(
                        super::output::Resolution::RestoreRequired,
                    )
            })
}

#[cfg(all(test, not(feature = "postgres")))]
mod tests {
    use super::*;
    use crate::db_migrate::local::backup::{BackupInfo, BackupResource, create_backup};
    use crate::db_migrate::local::output::{ErrorCode, LocalFailure};
    use sqlx::{ConnectOptions, Connection, sqlite::SqliteConnectOptions};

    struct Fixture {
        _workspace: tempfile::TempDir,
        target: SqliteTarget,
        vectors: PathBuf,
        backup: PathBuf,
    }

    async fn write_value(target: &SqliteTarget, value: &str) {
        let mut connection = SqliteConnectOptions::new()
            .filename(target.database())
            .create_if_missing(true)
            .connect()
            .await
            .unwrap();
        sqlx::query("CREATE TABLE IF NOT EXISTS t (value TEXT)")
            .execute(&mut connection)
            .await
            .unwrap();
        sqlx::query("DELETE FROM t")
            .execute(&mut connection)
            .await
            .unwrap();
        sqlx::query("INSERT INTO t VALUES (?)")
            .bind(value)
            .execute(&mut connection)
            .await
            .unwrap();
        connection.close().await.unwrap();
    }

    async fn read_value(target: &SqliteTarget) -> String {
        let mut connection = SqliteConnectOptions::new()
            .filename(target.database())
            .connect()
            .await
            .unwrap();
        let value = sqlx::query_scalar("SELECT value FROM t")
            .fetch_one(&mut connection)
            .await
            .unwrap();
        connection.close().await.unwrap();
        value
    }

    /// A backup holding `before`, then the live data changed to `after`.
    async fn fixture(schema_version: &str) -> Fixture {
        let workspace = tempfile::tempdir().unwrap();
        let target = SqliteTarget::at(workspace.path().join("default.sqlite3"));
        write_value(&target, "before").await;
        let vectors = workspace.path().join("lancedb");
        std::fs::create_dir_all(vectors.join("threads.lance")).unwrap();
        std::fs::write(vectors.join("threads.lance/v"), b"before").unwrap();
        let backup = create_backup(
            &workspace.path().join("backups"),
            &target,
            &[BackupResource::directory("thread_lancedb", vectors.clone())],
            &BackupInfo {
                schema_status: "managed".to_string(),
                schema_version: Some(schema_version.to_string()),
                bundle_digest: None,
                embedding_space_id: None,
            },
        )
        .await
        .unwrap();
        write_value(&target, "after").await;
        std::fs::write(vectors.join("threads.lance/v"), b"after").unwrap();
        std::fs::write(vectors.join("threads.lance/extra"), b"after").unwrap();
        Fixture {
            _workspace: workspace,
            target,
            vectors,
            backup,
        }
    }

    fn error_code(error: &anyhow::Error) -> Option<ErrorCode> {
        error.downcast_ref::<LocalFailure>().map(|f| f.error_code)
    }

    async fn assert_untouched(fixture: &Fixture) {
        assert_eq!(read_value(&fixture.target).await, "after");
        assert_eq!(
            std::fs::read(fixture.vectors.join("threads.lance/v")).unwrap(),
            b"after"
        );
    }

    async fn assert_restored(fixture: &Fixture) {
        assert_eq!(read_value(&fixture.target).await, "before");
        assert_eq!(
            std::fs::read(fixture.vectors.join("threads.lance/v")).unwrap(),
            b"before"
        );
        assert!(!fixture.vectors.join("threads.lance/extra").exists());
        let directory = fixture.target.database().parent().unwrap();
        let leftovers = std::fs::read_dir(directory)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| name.contains("restore"))
            .collect::<Vec<_>>();
        assert!(leftovers.is_empty(), "{leftovers:?}");
    }

    #[test]
    fn complete_backup_restores_database_and_resources_together() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            let fixture = fixture("20260930000001").await;
            restore_backup(&fixture.backup, &fixture.target, "20260930000001")
                .await
                .unwrap();
            assert_restored(&fixture).await;
        });
    }

    #[test]
    fn incomplete_backup_is_rejected_without_touching_current_data() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            let fixture = fixture("20260930000001").await;
            let manifest = BackupManifest::load(&fixture.backup).unwrap();
            std::fs::remove_file(fixture.backup.join(&manifest.sqlite.path)).unwrap();
            let error = restore_backup(&fixture.backup, &fixture.target, "20260930000001")
                .await
                .unwrap_err();
            assert_eq!(error_code(&error), Some(ErrorCode::BackupIncomplete));
            assert_untouched(&fixture).await;
        });
    }

    #[test]
    fn tampered_backup_content_is_detected_while_staging_without_touching_current_data() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            let fixture = fixture("20260930000001").await;
            let manifest = BackupManifest::load(&fixture.backup).unwrap();
            // Same size, different bytes: only the digest can tell.
            std::fs::write(
                fixture
                    .backup
                    .join(&manifest.resources[0].path)
                    .join("threads.lance/v"),
                b"BEFORE",
            )
            .unwrap();
            let error = restore_backup(&fixture.backup, &fixture.target, "20260930000001")
                .await
                .unwrap_err();
            assert_eq!(error_code(&error), Some(ErrorCode::BackupIncomplete));
            assert_untouched(&fixture).await;
            assert!(interrupted_restore(&fixture.target).is_none());
        });
    }

    #[test]
    fn backup_newer_than_the_tool_is_rejected() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            let fixture = fixture("20991231000000").await;
            let error = restore_backup(&fixture.backup, &fixture.target, "20260930000001")
                .await
                .unwrap_err();
            assert_eq!(error_code(&error), Some(ErrorCode::BackupNewerThanTool));
            assert_untouched(&fixture).await;
        });
    }

    #[test]
    fn failure_while_replacing_puts_the_original_data_back() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            let fixture = fixture("20260930000001").await;
            let error = restore_with(
                &fixture.backup,
                &fixture.target,
                "20260930000001",
                &mut |index| {
                    if index == 1 {
                        anyhow::bail!("injected failure while replacing")
                    }
                    Ok(())
                },
            )
            .await
            .unwrap_err();
            assert_eq!(error_code(&error), Some(ErrorCode::RestoreFailed));
            assert_untouched(&fixture).await;
            assert!(interrupted_restore(&fixture.target).is_none());
        });
    }

    #[test]
    fn interrupted_restore_completes_when_run_again() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            let fixture = fixture("20260930000001").await;
            // A crash leaves the journal and a half-swapped state behind.
            let manifest = BackupManifest::load(&fixture.backup).unwrap();
            let journal = plan(&fixture.backup, &fixture.target, &manifest);
            journal.store(&fixture.target).unwrap();
            stage(&journal).await.unwrap();
            swap(&journal, &mut |index| {
                if index == 1 {
                    anyhow::bail!("crash")
                }
                Ok(())
            })
            .unwrap_err();
            assert!(interrupted_restore(&fixture.target).is_some());

            restore_backup(&fixture.backup, &fixture.target, "20260930000001")
                .await
                .unwrap();
            assert_restored(&fixture).await;
        });
    }

    #[test]
    fn resource_absent_at_backup_time_is_absent_after_restore() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            let workspace = tempfile::tempdir().unwrap();
            let target = SqliteTarget::at(workspace.path().join("db"));
            write_value(&target, "before").await;
            let vectors = workspace.path().join("lancedb");
            let backup = create_backup(
                &workspace.path().join("backups"),
                &target,
                &[BackupResource::directory("thread_lancedb", vectors.clone())],
                &BackupInfo {
                    schema_status: "pending".to_string(),
                    schema_version: Some("20260803000003".to_string()),
                    bundle_digest: None,
                    embedding_space_id: None,
                },
            )
            .await
            .unwrap();
            std::fs::create_dir_all(&vectors).unwrap();
            std::fs::write(vectors.join("created-later"), b"x").unwrap();

            restore_backup(&backup, &target, "20260930000001")
                .await
                .unwrap();
            assert!(!vectors.exists());
            assert_eq!(read_value(&target).await, "before");
        });
    }
}
