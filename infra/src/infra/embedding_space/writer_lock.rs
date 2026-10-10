//! Writer presence on PostgreSQL. Every writer process (server and
//! maintenance binaries) holds a shared advisory lock on a dedicated
//! connection while it runs; embedding migration commands take the same
//! key exclusively, so they see running writers and writers starting
//! during a command wait until it ends.

/// Advisory lock key held shared by writers, exclusively by commands.
pub const WRITER_KEY: i64 = 0x6d65_6d6f_7269_6573;
/// Advisory lock key serializing embedding migration commands.
pub const OPERATION_KEY: i64 = WRITER_KEY + 1;

#[cfg(feature = "postgres")]
static HELD: tokio::sync::OnceCell<sqlx::PgConnection> = tokio::sync::OnceCell::const_new();

/// Take the shared writer lock for the life of the process. Waits while
/// an embedding migration command holds it exclusively.
#[cfg(feature = "postgres")]
pub async fn hold_shared(pool: &infra_utils::infra::rdb::RdbPool) -> anyhow::Result<()> {
    use anyhow::Context as _;
    HELD.get_or_try_init(|| async {
        let mut conn = pool
            .acquire()
            .await
            .context("acquiring the writer lock connection")?
            .detach();
        let free: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock_shared($1)")
            .bind(WRITER_KEY)
            .fetch_one(&mut conn)
            .await?;
        if !free {
            tracing::warn!(
                "an embedding migration command is running; waiting for it to finish before writing"
            );
            sqlx::query("SELECT pg_advisory_lock_shared($1)")
                .bind(WRITER_KEY)
                .execute(&mut conn)
                .await?;
        }
        anyhow::Ok(conn)
    })
    .await?;
    Ok(())
}

#[cfg(not(feature = "postgres"))]
pub async fn hold_shared(_pool: &infra_utils::infra::rdb::RdbPool) -> anyhow::Result<()> {
    // SQLite writers are found through the database file's own locks.
    Ok(())
}
