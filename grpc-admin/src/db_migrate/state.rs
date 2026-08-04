//! Durable, fenced state for post-schema data migration tasks.

use anyhow::{Context, Result, bail};
use infra_utils::infra::rdb::{RdbPool, RdbTransaction};
use serde::{Deserialize, Serialize};

#[cfg(feature = "postgres")]
const INSERT_IF_ABSENT_SQL: &str = "INSERT INTO memories_data_migration_task_state \
    (task_identity, canonical_definition_digest, state, fencing_token, attempt_count, updated_at) \
    VALUES ($1, $2, 'pending', 0, 0, $3) ON CONFLICT (task_identity) DO NOTHING";
#[cfg(not(feature = "postgres"))]
const INSERT_IF_ABSENT_SQL: &str = "INSERT OR IGNORE INTO memories_data_migration_task_state \
    (task_identity, canonical_definition_digest, state, fencing_token, attempt_count, updated_at) \
    VALUES (?, ?, 'pending', 0, 0, ?)";

#[cfg(feature = "postgres")]
const LOAD_FOR_UPDATE_SQL: &str = "SELECT task_identity, canonical_definition_digest, state, execution_id, \
    holder_id, fencing_token, heartbeat_at, lease_expires_at, attempt_count, checkpoint, \
    failure_classification, started_at, updated_at, completed_at \
    FROM memories_data_migration_task_state WHERE task_identity = $1 FOR UPDATE";
#[cfg(not(feature = "postgres"))]
const LOAD_FOR_UPDATE_SQL: &str = "SELECT task_identity, canonical_definition_digest, state, execution_id, \
    holder_id, fencing_token, heartbeat_at, lease_expires_at, attempt_count, checkpoint, \
    failure_classification, started_at, updated_at, completed_at \
    FROM memories_data_migration_task_state WHERE task_identity = ?";

#[cfg(feature = "postgres")]
const LOAD_SQL: &str = "SELECT task_identity, canonical_definition_digest, state, execution_id, \
    holder_id, fencing_token, heartbeat_at, lease_expires_at, attempt_count, checkpoint, \
    failure_classification, started_at, updated_at, completed_at \
    FROM memories_data_migration_task_state WHERE task_identity = $1";
#[cfg(not(feature = "postgres"))]
const LOAD_SQL: &str = "SELECT task_identity, canonical_definition_digest, state, execution_id, \
    holder_id, fencing_token, heartbeat_at, lease_expires_at, attempt_count, checkpoint, \
    failure_classification, started_at, updated_at, completed_at \
    FROM memories_data_migration_task_state WHERE task_identity = ?";

#[cfg(feature = "postgres")]
const CLAIM_SQL: &str = "UPDATE memories_data_migration_task_state SET state = 'running', execution_id = $1, \
    holder_id = $2, fencing_token = $3, heartbeat_at = $4, lease_expires_at = $5, \
    attempt_count = attempt_count + 1, failure_classification = NULL, started_at = $4, updated_at = $4 \
    WHERE task_identity = $6 AND canonical_definition_digest = $7 AND fencing_token = $8";
#[cfg(not(feature = "postgres"))]
const CLAIM_SQL: &str = "UPDATE memories_data_migration_task_state SET state = 'running', execution_id = ?, \
    holder_id = ?, fencing_token = ?, heartbeat_at = ?, lease_expires_at = ?, \
    attempt_count = attempt_count + 1, failure_classification = NULL, started_at = ?, updated_at = ? \
    WHERE task_identity = ? AND canonical_definition_digest = ? AND fencing_token = ?";

#[cfg(feature = "postgres")]
const UPDATE_CHECKPOINT_SQL: &str = "UPDATE memories_data_migration_task_state SET checkpoint = $1, heartbeat_at = $2, \
    lease_expires_at = $3, updated_at = $2 WHERE task_identity = $4 AND canonical_definition_digest = $5 \
    AND state = 'running' AND fencing_token = $6";
#[cfg(not(feature = "postgres"))]
const UPDATE_CHECKPOINT_SQL: &str = "UPDATE memories_data_migration_task_state SET checkpoint = ?, heartbeat_at = ?, \
    lease_expires_at = ?, updated_at = ? WHERE task_identity = ? AND canonical_definition_digest = ? \
    AND state = 'running' AND fencing_token = ?";

#[cfg(feature = "postgres")]
const RENEW_LEASE_SQL: &str = "UPDATE memories_data_migration_task_state SET heartbeat_at = $1, \
    lease_expires_at = $2, updated_at = $1 WHERE task_identity = $3 AND canonical_definition_digest = $4 \
    AND state = 'running' AND fencing_token = $5";
#[cfg(not(feature = "postgres"))]
const RENEW_LEASE_SQL: &str = "UPDATE memories_data_migration_task_state SET heartbeat_at = ?, \
    lease_expires_at = ?, updated_at = ? WHERE task_identity = ? AND canonical_definition_digest = ? \
    AND state = 'running' AND fencing_token = ?";

#[cfg(feature = "postgres")]
const COMPLETE_SQL: &str = "UPDATE memories_data_migration_task_state SET state = 'completed', heartbeat_at = NULL, \
    lease_expires_at = NULL, completed_at = $1, updated_at = $1 WHERE task_identity = $2 \
    AND canonical_definition_digest = $3 AND state = 'running' AND fencing_token = $4";
#[cfg(not(feature = "postgres"))]
const COMPLETE_SQL: &str = "UPDATE memories_data_migration_task_state SET state = 'completed', heartbeat_at = NULL, \
    lease_expires_at = NULL, completed_at = ?, updated_at = ? WHERE task_identity = ? \
    AND canonical_definition_digest = ? AND state = 'running' AND fencing_token = ?";

#[cfg(feature = "postgres")]
const FAIL_SQL: &str = "UPDATE memories_data_migration_task_state SET state = 'failed', heartbeat_at = NULL, \
    lease_expires_at = NULL, failure_classification = $1, updated_at = $2 WHERE task_identity = $3 \
    AND canonical_definition_digest = $4 AND state = 'running' AND fencing_token = $5";
#[cfg(not(feature = "postgres"))]
const FAIL_SQL: &str = "UPDATE memories_data_migration_task_state SET state = 'failed', heartbeat_at = NULL, \
    lease_expires_at = NULL, failure_classification = ?, updated_at = ? WHERE task_identity = ? \
    AND canonical_definition_digest = ? AND state = 'running' AND fencing_token = ?";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStateKind {
    Pending,
    Running,
    Completed,
    Failed,
}

impl TaskStateKind {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "pending" => Ok(Self::Pending),
            "running" => Ok(Self::Running),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            _ => bail!("unknown data migration task state: {value}"),
        }
    }
}

#[derive(Debug, Clone, sqlx::FromRow)]
pub struct TaskStateRow {
    pub task_identity: String,
    pub canonical_definition_digest: String,
    pub state: String,
    pub execution_id: Option<String>,
    pub holder_id: Option<String>,
    pub fencing_token: i64,
    pub heartbeat_at: Option<i64>,
    pub lease_expires_at: Option<i64>,
    pub attempt_count: i64,
    pub checkpoint: Option<String>,
    pub failure_classification: Option<String>,
    pub started_at: Option<i64>,
    pub updated_at: i64,
    pub completed_at: Option<i64>,
}

impl TaskStateRow {
    pub fn kind(&self) -> Result<TaskStateKind> {
        TaskStateKind::parse(&self.state)
    }

    pub fn is_active_lease(&self, now: i64) -> Result<bool> {
        Ok(self.kind()? == TaskStateKind::Running
            && self.lease_expires_at.is_some_and(|expiry| expiry > now))
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskLease {
    pub task_identity: String,
    pub canonical_definition_digest: String,
    pub execution_id: String,
    pub holder_id: String,
    pub fencing_token: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct TaskCheckpointEnvelope<T> {
    pub format: String,
    pub task_identity: String,
    pub canonical_definition_digest: String,
    pub payload: T,
}

pub async fn load(pool: &RdbPool, task_identity: &str) -> Result<Option<TaskStateRow>> {
    sqlx::query_as(LOAD_SQL)
        .bind(task_identity)
        .fetch_optional(pool)
        .await
        .context("loading data migration task state")
}

pub async fn claim(
    pool: &RdbPool,
    task_identity: &str,
    digest: &str,
    execution_id: &str,
    holder_id: &str,
    now: i64,
    lease_duration_ms: i64,
) -> Result<TaskLease> {
    if lease_duration_ms <= 0 {
        bail!("task lease duration must be positive");
    }
    let mut tx = pool.begin().await.context("begin task lease transaction")?;
    sqlx::query(INSERT_IF_ABSENT_SQL)
        .bind(task_identity)
        .bind(digest)
        .bind(now)
        .execute(&mut *tx)
        .await
        .context("creating data migration task state")?;
    let current: TaskStateRow = sqlx::query_as(LOAD_FOR_UPDATE_SQL)
        .bind(task_identity)
        .fetch_one(&mut *tx)
        .await
        .context("locking data migration task state")?;
    if current.canonical_definition_digest != digest {
        bail!("task state definition digest does not match the fixed registry");
    }
    if current.kind()? == TaskStateKind::Completed {
        bail!("task is already completed");
    }
    if current.is_active_lease(now)? {
        bail!("task is owned by an active executor");
    }
    let token = current
        .fencing_token
        .checked_add(1)
        .context("task fencing token overflow")?;
    let expires_at = now
        .checked_add(lease_duration_ms)
        .context("task lease expiry overflow")?;
    let changed = {
        #[cfg(feature = "postgres")]
        let query = sqlx::query(CLAIM_SQL)
            .bind(execution_id)
            .bind(holder_id)
            .bind(token)
            .bind(now)
            .bind(expires_at)
            .bind(task_identity)
            .bind(digest)
            .bind(current.fencing_token);
        #[cfg(not(feature = "postgres"))]
        let query = sqlx::query(CLAIM_SQL)
            .bind(execution_id)
            .bind(holder_id)
            .bind(token)
            .bind(now)
            .bind(expires_at)
            .bind(now)
            .bind(now)
            .bind(task_identity)
            .bind(digest)
            .bind(current.fencing_token);
        query.execute(&mut *tx).await?.rows_affected()
    };
    if changed != 1 {
        bail!("task lease was lost before it could be claimed");
    }
    tx.commit().await.context("commit task lease")?;
    Ok(TaskLease {
        task_identity: task_identity.to_owned(),
        canonical_definition_digest: digest.to_owned(),
        execution_id: execution_id.to_owned(),
        holder_id: holder_id.to_owned(),
        fencing_token: token,
    })
}

pub async fn save_checkpoint_tx<T: Serialize>(
    tx: &mut RdbTransaction<'_>,
    lease: &TaskLease,
    checkpoint: &TaskCheckpointEnvelope<T>,
    now: i64,
    lease_duration_ms: i64,
) -> Result<()> {
    if checkpoint.task_identity != lease.task_identity
        || checkpoint.canonical_definition_digest != lease.canonical_definition_digest
    {
        bail!("checkpoint identity does not match task lease");
    }
    let json = serde_json::to_string(checkpoint).context("serializing task checkpoint")?;
    let expires_at = now
        .checked_add(lease_duration_ms)
        .context("task lease expiry overflow")?;
    #[cfg(feature = "postgres")]
    let query = sqlx::query(UPDATE_CHECKPOINT_SQL)
        .bind(json)
        .bind(now)
        .bind(expires_at)
        .bind(&lease.task_identity)
        .bind(&lease.canonical_definition_digest)
        .bind(lease.fencing_token);
    #[cfg(not(feature = "postgres"))]
    let query = sqlx::query(UPDATE_CHECKPOINT_SQL)
        .bind(json)
        .bind(now)
        .bind(expires_at)
        .bind(now)
        .bind(&lease.task_identity)
        .bind(&lease.canonical_definition_digest)
        .bind(lease.fencing_token);
    if query.execute(&mut **tx).await?.rows_affected() != 1 {
        bail!("task checkpoint write lost its lease");
    }
    Ok(())
}

/// Extend an active lease without changing its checkpoint. Long-running
/// non-transactional work uses this between bounded units so a stale holder
/// cannot continue its LanceDB side effects after ownership was fenced off.
pub async fn renew_lease(
    pool: &RdbPool,
    lease: &TaskLease,
    now: i64,
    lease_duration_ms: i64,
) -> Result<()> {
    let expires_at = now
        .checked_add(lease_duration_ms)
        .context("task lease expiry overflow")?;
    #[cfg(feature = "postgres")]
    let query = sqlx::query(RENEW_LEASE_SQL)
        .bind(now)
        .bind(expires_at)
        .bind(&lease.task_identity)
        .bind(&lease.canonical_definition_digest)
        .bind(lease.fencing_token);
    #[cfg(not(feature = "postgres"))]
    let query = sqlx::query(RENEW_LEASE_SQL)
        .bind(now)
        .bind(expires_at)
        .bind(now)
        .bind(&lease.task_identity)
        .bind(&lease.canonical_definition_digest)
        .bind(lease.fencing_token);
    if query.execute(pool).await?.rows_affected() != 1 {
        bail!("task lease heartbeat lost its lease");
    }
    Ok(())
}

/// Fence a non-transactional side effect while retaining the task-state row
/// lock in the caller's transaction. The caller must commit only after its
/// side effect and checkpoint transition have both succeeded.
pub async fn renew_lease_tx(
    tx: &mut RdbTransaction<'_>,
    lease: &TaskLease,
    now: i64,
    lease_duration_ms: i64,
) -> Result<()> {
    let expires_at = now
        .checked_add(lease_duration_ms)
        .context("task lease expiry overflow")?;
    #[cfg(feature = "postgres")]
    let query = sqlx::query(RENEW_LEASE_SQL)
        .bind(now)
        .bind(expires_at)
        .bind(&lease.task_identity)
        .bind(&lease.canonical_definition_digest)
        .bind(lease.fencing_token);
    #[cfg(not(feature = "postgres"))]
    let query = sqlx::query(RENEW_LEASE_SQL)
        .bind(now)
        .bind(expires_at)
        .bind(now)
        .bind(&lease.task_identity)
        .bind(&lease.canonical_definition_digest)
        .bind(lease.fencing_token);
    if query.execute(&mut **tx).await?.rows_affected() != 1 {
        bail!("task lease heartbeat lost its lease");
    }
    Ok(())
}

pub async fn complete(pool: &RdbPool, lease: &TaskLease, now: i64) -> Result<()> {
    #[cfg(feature = "postgres")]
    let query = sqlx::query(COMPLETE_SQL)
        .bind(now)
        .bind(&lease.task_identity)
        .bind(&lease.canonical_definition_digest)
        .bind(lease.fencing_token);
    #[cfg(not(feature = "postgres"))]
    let query = sqlx::query(COMPLETE_SQL)
        .bind(now)
        .bind(now)
        .bind(&lease.task_identity)
        .bind(&lease.canonical_definition_digest)
        .bind(lease.fencing_token);
    if query.execute(pool).await?.rows_affected() != 1 {
        bail!("task completion lost its lease");
    }
    Ok(())
}

pub async fn fail(pool: &RdbPool, lease: &TaskLease, classification: &str, now: i64) -> Result<()> {
    #[cfg(feature = "postgres")]
    let query = sqlx::query(FAIL_SQL)
        .bind(classification)
        .bind(now)
        .bind(&lease.task_identity)
        .bind(&lease.canonical_definition_digest)
        .bind(lease.fencing_token);
    #[cfg(not(feature = "postgres"))]
    let query = sqlx::query(FAIL_SQL)
        .bind(classification)
        .bind(now)
        .bind(&lease.task_identity)
        .bind(&lease.canonical_definition_digest)
        .bind(lease.fencing_token);
    if query.execute(pool).await?.rows_affected() != 1 {
        bail!("task failure write lost its lease");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{TaskCheckpointEnvelope, TaskStateKind, TaskStateRow};
    #[cfg(not(feature = "postgres"))]
    use super::{claim, complete, load, renew_lease, renew_lease_tx};

    #[cfg(not(feature = "postgres"))]
    async fn test_pool() -> infra_utils::infra::rdb::RdbPool {
        use sqlx::sqlite::SqlitePoolOptions;

        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::raw_sql(
            "CREATE TABLE memories_data_migration_task_state (\
               task_identity TEXT PRIMARY KEY, canonical_definition_digest TEXT NOT NULL, state TEXT NOT NULL,\
               execution_id TEXT, holder_id TEXT, fencing_token BIGINT NOT NULL, heartbeat_at BIGINT,\
               lease_expires_at BIGINT, attempt_count BIGINT NOT NULL, checkpoint TEXT,\
               failure_classification TEXT, started_at BIGINT, updated_at BIGINT NOT NULL, completed_at BIGINT\
             );",
        )
        .execute(&pool)
        .await
        .unwrap();
        pool
    }

    #[test]
    fn expired_running_state_is_not_an_active_lease() {
        let row = TaskStateRow {
            task_identity: "example@1".into(),
            canonical_definition_digest: "digest".into(),
            state: "running".into(),
            execution_id: None,
            holder_id: None,
            fencing_token: 1,
            heartbeat_at: Some(10),
            lease_expires_at: Some(20),
            attempt_count: 1,
            checkpoint: None,
            failure_classification: None,
            started_at: Some(10),
            updated_at: 10,
            completed_at: None,
        };
        assert_eq!(row.kind().unwrap(), TaskStateKind::Running);
        assert!(row.is_active_lease(19).unwrap());
        assert!(!row.is_active_lease(20).unwrap());
    }

    #[test]
    fn checkpoint_envelope_round_trips_its_identity() {
        let envelope = TaskCheckpointEnvelope {
            format: "example@1/checkpoint-v1".into(),
            task_identity: "example@1".into(),
            canonical_definition_digest: "digest".into(),
            payload: vec![1_u8, 2, 3],
        };
        let encoded = serde_json::to_string(&envelope).unwrap();
        let decoded: TaskCheckpointEnvelope<Vec<u8>> = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, envelope);
    }

    #[cfg(not(feature = "postgres"))]
    #[test]
    fn claim_fences_previous_executor_and_completed_task_cannot_be_reclaimed() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            let pool = test_pool().await;
            let first = claim(
                &pool,
                "example@1",
                "digest",
                "execution-1",
                "holder-1",
                10,
                10,
            )
            .await
            .unwrap();
            assert_eq!(first.fencing_token, 1);
            assert!(
                claim(
                    &pool,
                    "example@1",
                    "digest",
                    "execution-2",
                    "holder-2",
                    11,
                    10
                )
                .await
                .is_err()
            );
            let second = claim(
                &pool,
                "example@1",
                "digest",
                "execution-2",
                "holder-2",
                20,
                10,
            )
            .await
            .unwrap();
            assert_eq!(second.fencing_token, 2);
            complete(&pool, &second, 21).await.unwrap();
            assert_eq!(
                load(&pool, "example@1")
                    .await
                    .unwrap()
                    .unwrap()
                    .kind()
                    .unwrap(),
                TaskStateKind::Completed
            );
            assert!(
                claim(
                    &pool,
                    "example@1",
                    "digest",
                    "execution-3",
                    "holder-3",
                    30,
                    10
                )
                .await
                .is_err()
            );
        });
    }

    #[cfg(not(feature = "postgres"))]
    #[test]
    fn lease_heartbeat_extends_only_the_current_fenced_owner() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            let pool = test_pool().await;
            let first = claim(
                &pool,
                "example@1",
                "digest",
                "execution-1",
                "holder-1",
                10,
                10,
            )
            .await
            .unwrap();
            renew_lease(&pool, &first, 15, 10).await.unwrap();
            let saved = load(&pool, "example@1").await.unwrap().unwrap();
            assert_eq!(saved.heartbeat_at, Some(15));
            assert_eq!(saved.lease_expires_at, Some(25));

            let second = claim(
                &pool,
                "example@1",
                "digest",
                "execution-2",
                "holder-2",
                25,
                10,
            )
            .await
            .unwrap();
            assert!(renew_lease(&pool, &first, 26, 10).await.is_err());
            renew_lease(&pool, &second, 26, 10).await.unwrap();
        });
    }

    #[cfg(not(feature = "postgres"))]
    #[test]
    fn transactional_heartbeat_rejects_a_stale_fencing_token() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            let pool = test_pool().await;
            let first = claim(&pool, "example@1", "digest", "one", "holder", 10, 10)
                .await
                .unwrap();
            let second = claim(&pool, "example@1", "digest", "two", "holder", 20, 10)
                .await
                .unwrap();
            let mut tx = pool.begin().await.unwrap();
            assert!(renew_lease_tx(&mut tx, &first, 21, 10).await.is_err());
            renew_lease_tx(&mut tx, &second, 21, 10).await.unwrap();
            tx.commit().await.unwrap();
        });
    }
}
