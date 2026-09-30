//! Cross-backend exclusive-section helpers (design 5.2 / 5.3).
//!
//! The ThreadGroup flows (deletion vs. re-import, connected-component
//! reconciliation) need "locking or equivalent serialization per
//! identity" that must not depend on the thread row existing. The
//! design isolates the backend difference in the repository adapter:
//!
//! - PostgreSQL: transaction-scoped advisory locks keyed by a stable
//!   hash of the identity / canonical-key tuple
//!   (`pg_advisory_xact_lock` + `hashtextextended`, PG 11+). Two
//!   transactions locking the same tuple serialize; the lock releases
//!   automatically at commit / abort, matching the "same exclusive
//!   section covers marker check, write, and marker consumption"
//!   contract.
//! - SQLite: the single-writer database lock plus the deferred
//!   transaction already serializes writers — the primitives here are
//!   no-ops by design (bounded retry on SQLITE_BUSY stays in the app /
//!   pool layer).
//!
//! Deadlock avoidance is a caller contract: lock multiple keys in
//! stable (sorted) order, as the recursive-delete rule in design 5.2
//! requires.

use super::rows::SourceIdentityKey;
#[cfg(feature = "postgres")]
use crate::error::LlmMemoryError;
#[cfg(feature = "postgres")]
use crate::sql::p;
use anyhow::Result;
use async_trait::async_trait;
use infra_utils::infra::rdb::{Rdb, RdbPool, UseRdbPool};
use sqlx::Executor;

/// Namespace prefixes keep identity locks and key locks in disjoint
/// advisory-lock key spaces even if a raw string ever collided.
const IDENTITY_LOCK_NAMESPACE: &str = "thread_group_identity";
const THREAD_KEY_LOCK_NAMESPACE: &str = "thread_group_canonical_key";
const MEMBERSHIP_LOCK_NAMESPACE: &str = "thread_group_membership_mutation";

#[cfg(feature = "postgres")]
const LOCK_SQL: &str = concat!(
    "SELECT pg_advisory_xact_lock(hashtextextended(",
    p!(1),
    ", 0))"
);

fn encode_lock_subject(namespace: &str, parts: &[&str]) -> String {
    // Length-prefixed join so no boundary shuffle can alias two
    // different tuples onto the same lock key.
    let mut out = String::from(namespace);
    for part in parts {
        out.push('\u{1f}');
        out.push_str(&part.len().to_string());
        out.push(':');
        out.push_str(part);
    }
    out
}

#[async_trait]
pub trait ThreadGroupLockRepository: UseRdbPool + Send + Sync {
    /// Keep membership placement, empty-group redirects, and operator group
    /// mutations in one transaction-scoped section across source identities.
    async fn lock_group_membership_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
    ) -> Result<()> {
        acquire_lock(tx, encode_lock_subject(MEMBERSHIP_LOCK_NAMESPACE, &[])).await
    }

    /// Serialize the owner-local source-identity exclusive section
    /// (delete / marker purge / override re-import share it, design
    /// 5.2). Must be called inside the transaction that performs the
    /// protected writes.
    async fn lock_source_identity_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        identity: &SourceIdentityKey<'_>,
    ) -> Result<()> {
        let user_id = identity.user_id.to_string();
        let subject = encode_lock_subject(
            IDENTITY_LOCK_NAMESPACE,
            &[
                &user_id,
                identity.source,
                identity.identity_scope,
                identity.native_id,
            ],
        );
        acquire_lock(tx, subject).await
    }

    /// Serialize one thread canonical key's membership / relation
    /// section (connected-component locking: the app locks every
    /// member key in sorted order, design 5.3).
    async fn lock_thread_canonical_key_tx<'c, E: Executor<'c, Database = Rdb>>(
        &self,
        tx: E,
        thread_canonical_key: &str,
    ) -> Result<()> {
        let subject = encode_lock_subject(THREAD_KEY_LOCK_NAMESPACE, &[thread_canonical_key]);
        acquire_lock(tx, subject).await
    }
}

// Backend split lives here: PostgreSQL takes a transaction-scoped
// advisory lock keyed by the hashed subject; SQLite's single-writer
// transaction is already the exclusive section, so the helper is an
// explicit no-op rather than a statement that would differ in error
// behavior across drivers.
#[cfg(feature = "postgres")]
async fn acquire_lock<'c, E: Executor<'c, Database = Rdb>>(tx: E, subject: String) -> Result<()> {
    sqlx::query::<Rdb>(LOCK_SQL)
        .bind(subject)
        .fetch_optional(tx)
        .await
        .map_err(LlmMemoryError::DBError)?;
    Ok(())
}

#[cfg(not(feature = "postgres"))]
async fn acquire_lock<'c, E: Executor<'c, Database = Rdb>>(tx: E, subject: String) -> Result<()> {
    let _ = (tx, subject);
    Ok(())
}

pub struct ThreadGroupLockRepositoryImpl {
    pool: &'static RdbPool,
}

impl ThreadGroupLockRepositoryImpl {
    pub fn new(pool: &'static RdbPool) -> Self {
        Self { pool }
    }
}

impl UseRdbPool for ThreadGroupLockRepositoryImpl {
    fn db_pool(&self) -> &RdbPool {
        self.pool
    }
}

impl ThreadGroupLockRepository for ThreadGroupLockRepositoryImpl {}

#[cfg(all(test, not(feature = "postgres")))]
mod tests {
    use super::*;
    use crate::infra::thread_group::test_support::*;
    use infra_utils::infra::test::TEST_RUNTIME;

    async fn _test_lock_primitives_run(pool: &'static RdbPool) -> Result<()> {
        let repo = ThreadGroupLockRepositoryImpl::new(pool);
        // On SQLite the section is the writer transaction itself; the
        // primitives must at least execute cleanly inside one and hold
        // no open statement cursor.
        let mut tx = pool.begin().await?;
        repo.lock_source_identity_tx(&mut *tx, &identity(1)).await?;
        repo.lock_thread_canonical_key_tx(&mut *tx, &key(1)).await?;
        repo.lock_thread_canonical_key_tx(&mut *tx, &key(2)).await?;
        tx.commit().await?;
        Ok(())
    }

    #[test]
    fn lock_primitives_run_sqlite() -> Result<()> {
        TEST_RUNTIME.block_on(async {
            let pool = setup_thread_group_pool().await;
            _test_lock_primitives_run(pool).await
        })
    }
}
