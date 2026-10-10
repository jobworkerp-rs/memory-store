//! Exclusion of embedding migration commands from each other and from
//! running writers (spec §3.3 "対応構成と排他").
//!
//! SQLite: an advisory file lock in the embedding state directory
//! serializes commands; writers are found through the database file's
//! own locks. PostgreSQL: advisory locks on a dedicated connection held
//! for the whole command; writers hold the writer key shared, so taking
//! it exclusively both detects them and keeps new ones waiting.

use super::output::ErrorCode;
#[cfg(not(feature = "postgres"))]
use anyhow::Result;
use infra_utils::infra::rdb::RdbPool;
use std::path::Path;

/// Held for the duration of a changing command; dropping it releases
/// the locks.
pub struct Exclusion {
    #[cfg(feature = "postgres")]
    _connection: sqlx::PgConnection,
    /// The writer key was taken exclusively (no writer runs). Reported by
    /// the ordinary checks rather than at acquisition, so the stage tables
    /// answer first even while a writer runs (spec §3.3 decision order).
    #[cfg(feature = "postgres")]
    writers_absent: bool,
    #[cfg(not(feature = "postgres"))]
    _file: std::fs::File,
}

impl Exclusion {
    /// Whether a writer held the writer key when the exclusion was taken
    /// (PostgreSQL; SQLite writers are found through the database file).
    pub fn writer_present(&self) -> bool {
        #[cfg(feature = "postgres")]
        {
            !self.writers_absent
        }
        #[cfg(not(feature = "postgres"))]
        {
            false
        }
    }
}

/// Why exclusion could not be obtained.
#[derive(Debug)]
pub enum LockError {
    Refused(ErrorCode),
    Other(anyhow::Error),
}

impl From<anyhow::Error> for LockError {
    fn from(e: anyhow::Error) -> Self {
        Self::Other(e)
    }
}

impl From<sqlx::Error> for LockError {
    fn from(e: sqlx::Error) -> Self {
        Self::Other(e.into())
    }
}

/// Take the operation lock (`operation_in_progress` when held elsewhere).
/// On PostgreSQL this also tries the writer key; whether a writer runs is
/// left to [`Exclusion::writer_present`].
#[cfg(feature = "postgres")]
pub async fn acquire(
    pool: &RdbPool,
    _state_dir: &Path,
) -> std::result::Result<Exclusion, LockError> {
    use infra::infra::embedding_space::writer_lock::{OPERATION_KEY, WRITER_KEY};
    let mut conn = pool.acquire().await?.detach();
    let operation: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
        .bind(OPERATION_KEY)
        .fetch_one(&mut conn)
        .await?;
    if !operation {
        return Err(LockError::Refused(ErrorCode::OperationInProgress));
    }
    let writers_absent: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
        .bind(WRITER_KEY)
        .fetch_one(&mut conn)
        .await?;
    Ok(Exclusion {
        _connection: conn,
        writers_absent,
    })
}

#[cfg(not(feature = "postgres"))]
pub async fn acquire(
    _pool: &RdbPool,
    state_dir: &Path,
) -> std::result::Result<Exclusion, LockError> {
    acquire_file(state_dir)
}

/// The SQLite operation lock alone (it needs no database connection).
#[cfg(not(feature = "postgres"))]
pub fn acquire_file(state_dir: &Path) -> std::result::Result<Exclusion, LockError> {
    use anyhow::Context as _;
    use std::os::fd::AsRawFd;
    std::fs::create_dir_all(state_dir)
        .with_context(|| format!("creating {}", state_dir.display()))?;
    let path = state_dir.join("operation.lock");
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&path)
        .with_context(|| format!("opening {}", path.display()))?;
    // SAFETY: plain syscall on a descriptor this function owns.
    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if rc != 0 {
        let err = std::io::Error::last_os_error();
        return if err.kind() == std::io::ErrorKind::WouldBlock {
            Err(LockError::Refused(ErrorCode::OperationInProgress))
        } else {
            Err(LockError::Other(
                anyhow::Error::new(err).context("locking the state directory"),
            ))
        };
    }
    Ok(Exclusion { _file: file })
}

/// Whether any writer has the SQLite database open. The caller must hold
/// no connection of its own to the database during the check.
#[cfg(not(feature = "postgres"))]
pub fn sqlite_writer_present(database_url: &str) -> Result<bool> {
    use crate::db_migrate::local::target::SqliteTarget;
    let target = SqliteTarget::from_url(database_url)?;
    Ok(crate::db_migrate::local::writer::ensure_no_other_connection(&target).is_err())
}

/// Whether another command holds the operation lock (for `inspect`'s
/// `wait`), without taking it.
#[cfg(not(feature = "postgres"))]
pub fn operation_locked(state_dir: &Path) -> bool {
    use std::os::fd::AsRawFd;
    let Ok(file) = std::fs::File::open(state_dir.join("operation.lock")) else {
        return false;
    };
    // SAFETY: plain syscalls on a descriptor this function owns.
    unsafe {
        if libc::flock(file.as_raw_fd(), libc::LOCK_SH | libc::LOCK_NB) != 0 {
            return true;
        }
        libc::flock(file.as_raw_fd(), libc::LOCK_UN);
    }
    false
}

#[cfg(feature = "postgres")]
pub async fn operation_locked_pg(pool: &RdbPool) -> bool {
    use infra::infra::embedding_space::writer_lock::OPERATION_KEY;
    let Ok(mut conn) = pool.acquire().await else {
        return false;
    };
    let free: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock_shared($1)")
        .bind(OPERATION_KEY)
        .fetch_one(&mut *conn)
        .await
        .unwrap_or(true);
    if free {
        let _ = sqlx::query("SELECT pg_advisory_unlock_shared($1)")
            .bind(OPERATION_KEY)
            .execute(&mut *conn)
            .await;
    }
    !free
}

#[cfg(all(test, not(feature = "postgres")))]
mod tests {
    use super::*;

    #[test]
    fn second_command_is_refused_while_the_first_holds_the_lock() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            let dir = tempfile::tempdir().unwrap();
            let pool = infra_utils::infra::test::setup_test_rdb_from("../infra/sql/sqlite").await;
            assert!(!operation_locked(dir.path()));
            let held = acquire(pool, dir.path()).await.ok().unwrap();
            // flock locks belong to the open file description, so a second
            // open in the same process contends like another process.
            assert!(matches!(
                acquire(pool, dir.path()).await,
                Err(LockError::Refused(ErrorCode::OperationInProgress))
            ));
            assert!(operation_locked(dir.path()));
            drop(held);
            assert!(acquire(pool, dir.path()).await.is_ok());
        });
    }
}

#[cfg(all(test, feature = "postgres"))]
mod tests {
    use super::*;

    #[test]
    fn writers_and_other_commands_are_detected() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            let pool = infra_utils::infra::test::setup_test_rdb_from("../infra/sql/postgres").await;
            let dir = tempfile::tempdir().unwrap();
            let held = acquire(pool, dir.path()).await.ok().unwrap();
            assert!(operation_locked_pg(pool).await);
            assert!(matches!(
                acquire(pool, dir.path()).await,
                Err(LockError::Refused(ErrorCode::OperationInProgress))
            ));
            drop(held);
            assert!(!operation_locked_pg(pool).await);

            // Two writers share the writer key; a command is then refused.
            use infra::infra::embedding_space::writer_lock::WRITER_KEY;
            let mut w1 = pool.acquire().await.unwrap().detach();
            let mut w2 = pool.acquire().await.unwrap().detach();
            for w in [&mut w1, &mut w2] {
                let ok: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock_shared($1)")
                    .bind(WRITER_KEY)
                    .fetch_one(w)
                    .await
                    .unwrap();
                assert!(ok, "writers share the key");
            }
            let seen = acquire(pool, dir.path()).await.ok().unwrap();
            assert!(seen.writer_present());
            drop(seen);
            drop(w1);
            drop(w2);
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            assert!(
                !acquire(pool, dir.path())
                    .await
                    .ok()
                    .unwrap()
                    .writer_present()
            );
        });
    }
}
