//! Detecting other processes that still have the target SQLite database open.
//!
//! SQLite's unix VFS coordinates connections with advisory `fcntl` locks, so
//! asking the kernel about those locks finds a forgotten writer even while it
//! is idle, which a busy-timeout probe cannot.

use super::output::{ErrorCode, Resolution, fail};
use super::target::SqliteTarget;
use anyhow::Result;
use std::os::fd::AsRawFd;
use std::path::Path;

// Lock layout of SQLite's unix VFS (os_unix.c): every connection to a WAL
// database holds a shared lock on the WAL-index "dead man switch" byte while
// it is open, and rollback-journal transactions lock the PENDING / RESERVED /
// SHARED bytes of the database file.
const SHM_DEAD_MAN_SWITCH_OFFSET: i64 = 128;
const DATABASE_LOCK_OFFSET: i64 = 0x4000_0000;
const DATABASE_LOCK_LENGTH: i64 = 2 + 510;

// Open-file-description queries also report locks of this process, so tests
// can open a connection in-process; POSIX queries only see other processes.
#[cfg(target_os = "linux")]
const GET_LOCK: libc::c_int = libc::F_OFD_GETLK;
#[cfg(not(target_os = "linux"))]
const GET_LOCK: libc::c_int = libc::F_GETLK;

/// Fail with `writer_active` while any other connection has the database open.
///
/// Opening and closing a descriptor drops this process's POSIX locks on the
/// file, so callers must hold no SQLite connection to it during the check.
pub fn ensure_no_other_connection(target: &SqliteTarget) -> Result<()> {
    let probes = [
        (target.shm(), SHM_DEAD_MAN_SWITCH_OFFSET, 1),
        (
            target.database().to_path_buf(),
            DATABASE_LOCK_OFFSET,
            DATABASE_LOCK_LENGTH,
        ),
    ];
    for (path, offset, length) in probes {
        if !path.exists() {
            continue;
        }
        match lock_held_elsewhere(&path, offset, length) {
            Ok(false) => {}
            Ok(true) => {
                return fail(
                    ErrorCode::WriterActive,
                    Resolution::Retry,
                    format!(
                        "{} is open by another process; stop Memories and every process using it",
                        target.database().display()
                    ),
                );
            }
            Err(error) => {
                return fail(
                    ErrorCode::WriterActive,
                    Resolution::Retry,
                    format!(
                        "could not confirm that no other process uses {}: {error}",
                        path.display()
                    ),
                );
            }
        }
    }
    Ok(())
}

fn lock_held_elsewhere(path: &Path, offset: i64, length: i64) -> std::io::Result<bool> {
    let file = std::fs::File::open(path)?;
    // SAFETY: `flock` is a plain C struct; all-zero is a valid initial value
    // (OFD queries additionally require `l_pid == 0`).
    let mut lock: libc::flock = unsafe { std::mem::zeroed() };
    lock.l_type = libc::F_WRLCK as _;
    lock.l_whence = libc::SEEK_SET as _;
    lock.l_start = offset as _;
    lock.l_len = length as _;
    // SAFETY: the descriptor stays open for the call and `lock` is a valid,
    // exclusively borrowed `flock`.
    let result = unsafe { libc::fcntl(file.as_raw_fd(), GET_LOCK, &mut lock) };
    if result == -1 {
        return Err(std::io::Error::last_os_error());
    }
    Ok(i32::from(lock.l_type) != libc::F_UNLCK)
}

#[cfg(all(test, target_os = "linux", not(feature = "postgres")))]
mod tests {
    use super::*;
    use crate::db_migrate::local::output::{ErrorCode, LocalFailure};
    use crate::db_migrate::local::target::SqliteTarget;
    use sqlx::{ConnectOptions, Connection, sqlite::SqliteConnectOptions};
    use std::str::FromStr;

    fn error_code(result: anyhow::Result<()>) -> Option<ErrorCode> {
        result
            .err()
            .and_then(|e| e.downcast_ref::<LocalFailure>().map(|f| f.error_code))
    }

    async fn connect(target: &SqliteTarget, journal: &str) -> sqlx::SqliteConnection {
        let mut connection =
            SqliteConnectOptions::from_str(&format!("sqlite://{}", target.database().display()))
                .unwrap()
                .create_if_missing(true)
                .connect()
                .await
                .unwrap();
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "PRAGMA journal_mode = {journal}"
        )))
        .execute(&mut connection)
        .await
        .unwrap();
        sqlx::query("CREATE TABLE IF NOT EXISTS t (id INTEGER)")
            .execute(&mut connection)
            .await
            .unwrap();
        connection
    }

    #[test]
    fn database_without_connections_has_no_writer() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            let directory = tempfile::tempdir().unwrap();
            let target = SqliteTarget::at(directory.path().join("db"));
            connect(&target, "WAL").await.close().await.unwrap();
            ensure_no_other_connection(&target).unwrap();
            // A database that does not exist yet has no connections either.
            ensure_no_other_connection(&SqliteTarget::at(directory.path().join("new"))).unwrap();
        });
    }

    #[test]
    fn idle_wal_connection_is_detected_until_it_closes() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            let directory = tempfile::tempdir().unwrap();
            let target = SqliteTarget::at(directory.path().join("db"));
            let mut connection = connect(&target, "WAL").await;
            sqlx::query("SELECT COUNT(*) FROM t")
                .execute(&mut connection)
                .await
                .unwrap();
            assert_eq!(
                error_code(ensure_no_other_connection(&target)),
                Some(ErrorCode::WriterActive)
            );
            connection.close().await.unwrap();
            ensure_no_other_connection(&target).unwrap();
        });
    }

    #[test]
    fn rollback_journal_transaction_is_detected() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            let directory = tempfile::tempdir().unwrap();
            let target = SqliteTarget::at(directory.path().join("db"));
            let mut connection = connect(&target, "DELETE").await;
            let mut transaction = connection.begin().await.unwrap();
            sqlx::query("INSERT INTO t VALUES (1)")
                .execute(&mut *transaction)
                .await
                .unwrap();
            assert_eq!(
                error_code(ensure_no_other_connection(&target)),
                Some(ErrorCode::WriterActive)
            );
            transaction.rollback().await.unwrap();
        });
    }
}
