//! `thread-groups-canonical-keys-v1@1` post-schema data migration.
//!
//! Assigns an immutable canonical key to every existing thread without
//! touching thread rows themselves (design 5.7 / implementation plan §4):
//!
//! - source-backed threads (resolved rows in `source_thread_identity`)
//!   derive their key deterministically from the canonical serialization
//!   v2 of the smallest resolved `(owner_scope, source, identity_scope,
//!   native_id)` tuple, with origin `source_identity`;
//! - manual / non-source threads get a random key assigned exactly once,
//!   stored in `thread_canonical_key` (the persistent correspondence
//!   table) with origin `backfill_mapping`, and reuse it on re-runs.
//!
//! Internal DB ids, `created_at`, and import arrival order are never key
//! inputs; ids are only used as keyset cursors, and tuple ordering is a
//! deterministic choice among mappings of the *same* thread, never a key
//! input that differs across threads.

use super::{
    DataMigrationTask,
    catalog::TaskCatalogEntry,
    state::{self, TaskCheckpointEnvelope, TaskLease, TaskStateKind},
};
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use common::thread_group_key::{
    IdentityScope, SourceIdentity, parse_legacy_owner_scope, source_thread_canonical_key,
};
use infra_utils::infra::rdb::{RdbPool, RdbTransaction};
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

const CHECKPOINT_FORMAT_V1: &str = "thread-groups-canonical-keys-v1@1/checkpoint-v1";
const CHECKPOINT_FORMAT_V2: &str = "thread-groups-canonical-keys-v1@2/checkpoint-v2";
const DEFAULT_BATCH_SIZE: i64 = 500;
const DEFAULT_LEASE_MS: i64 = 120_000;
const KEY_ORIGIN_SOURCE_IDENTITY: &str = "source_identity";
const KEY_ORIGIN_BACKFILL_MAPPING: &str = "backfill_mapping";
/// Frozen grouping policy version the key assignment contract belongs to
/// (`thread-group-policy-v1`, spec 5.5).
const POLICY_VERSION: &str = "thread-group-policy-v1";
/// Adapter contract version: the canonical serialization v2 key derivation
/// contract (design 4.2.1 / 5.7) implemented by this task. A checkpoint
/// recorded under a different contract must not be resumed, because the
/// derived keys would no longer be reproducible.
const ADAPTER_VERSION: &str = "canonical-serialization-v2";

#[cfg(feature = "postgres")]
const INSERT_KEY_SQL: &str = "INSERT INTO thread_canonical_key \
    (thread_id, owner_scope, key, origin, assigned_at) VALUES ($1, $2, $3, $4, $5)";
#[cfg(not(feature = "postgres"))]
const INSERT_KEY_SQL: &str = "INSERT INTO thread_canonical_key \
    (thread_id, owner_scope, key, origin, assigned_at) VALUES (?, ?, ?, ?, ?)";

#[cfg(feature = "postgres")]
const THREAD_HAS_KEY_SQL: &str = "SELECT key FROM thread_canonical_key WHERE thread_id = $1";
#[cfg(not(feature = "postgres"))]
const THREAD_HAS_KEY_SQL: &str = "SELECT key FROM thread_canonical_key WHERE thread_id = ?";

#[cfg(feature = "postgres")]
const THREAD_USER_ID_SQL: &str = "SELECT user_id FROM thread WHERE id = $1";
#[cfg(not(feature = "postgres"))]
const THREAD_USER_ID_SQL: &str = "SELECT user_id FROM thread WHERE id = ?";

const COUNT_THREADS_SQL: &str = "SELECT COUNT(*) FROM thread";
const COUNT_KEY_ROWS_SQL: &str = "SELECT COUNT(*) FROM thread_canonical_key";
const PENDING_SOURCE_BACKED_SQL: &str = "SELECT COUNT(*) FROM thread t WHERE NOT EXISTS \
    (SELECT 1 FROM thread_canonical_key k WHERE k.thread_id = t.id) \
    AND EXISTS (SELECT 1 FROM source_thread_identity s WHERE s.thread_id = t.id)";
const PENDING_MANUAL_SQL: &str = "SELECT COUNT(*) FROM thread t WHERE NOT EXISTS \
    (SELECT 1 FROM thread_canonical_key k WHERE k.thread_id = t.id) \
    AND NOT EXISTS (SELECT 1 FROM source_thread_identity s WHERE s.thread_id = t.id)";
const ORPHAN_KEY_ROWS_SQL: &str = "SELECT COUNT(*) FROM thread_canonical_key k \
    LEFT JOIN thread t ON t.id = k.thread_id WHERE t.id IS NULL";
const KEY_ROWS_SQL: &str = "SELECT key, origin FROM thread_canonical_key";
const DUPLICATE_KEYS_SQL: &str = "SELECT COUNT(*) FROM \
    (SELECT key FROM thread_canonical_key GROUP BY key HAVING COUNT(*) > 1) duplicates";

const SOURCE_BACKED_IDS_BASE_SQL: &str = "SELECT t.id FROM thread t WHERE NOT EXISTS \
    (SELECT 1 FROM thread_canonical_key k WHERE k.thread_id = t.id) \
    AND EXISTS (SELECT 1 FROM source_thread_identity s WHERE s.thread_id = t.id)";
const MANUAL_IDS_BASE_SQL: &str = "SELECT t.id FROM thread t WHERE NOT EXISTS \
    (SELECT 1 FROM thread_canonical_key k WHERE k.thread_id = t.id) \
    AND NOT EXISTS (SELECT 1 FROM source_thread_identity s WHERE s.thread_id = t.id)";

fn placeholder(index: usize) -> String {
    #[cfg(feature = "postgres")]
    {
        format!("${index}")
    }
    #[cfg(not(feature = "postgres"))]
    {
        let _ = index;
        "?".to_string()
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum KeyPhase {
    Pending,
    SourceBacked,
    Manual,
    Verified,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct Checkpoint {
    pub phase: KeyPhase,
    pub source_backed_last_thread_id: Option<i64>,
    pub manual_last_thread_id: Option<i64>,
    pub migration_version: String,
    pub adapter_version: String,
    pub policy_version: String,
}

impl Default for Checkpoint {
    fn default() -> Self {
        Self {
            phase: KeyPhase::Pending,
            source_backed_last_thread_id: None,
            manual_last_thread_id: None,
            migration_version: String::new(),
            adapter_version: ADAPTER_VERSION.to_owned(),
            policy_version: POLICY_VERSION.to_owned(),
        }
    }
}

impl Checkpoint {
    fn validate(&self) -> Result<()> {
        if self.migration_version.is_empty()
            || self.adapter_version.is_empty()
            || self.policy_version.is_empty()
        {
            bail!("checkpoint must record migration, adapter, and policy versions");
        }
        let (source_cursor, manual_cursor) = (
            self.source_backed_last_thread_id,
            self.manual_last_thread_id,
        );
        match self.phase {
            KeyPhase::Pending => {
                if source_cursor.is_some() || manual_cursor.is_some() {
                    bail!("pending checkpoint must not carry a processing cursor");
                }
            }
            KeyPhase::SourceBacked => {
                if manual_cursor.is_some() {
                    bail!("source-backed phase must not carry a manual cursor");
                }
            }
            KeyPhase::Manual => {
                if source_cursor.is_some() {
                    bail!("manual phase must not carry a source-backed cursor");
                }
            }
            KeyPhase::Verified => {
                if source_cursor.is_some() || manual_cursor.is_some() {
                    bail!("verified checkpoint must not carry a processing cursor");
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub struct InspectResult {
    pub threads: i64,
    pub threads_with_canonical_key: i64,
    pub pending_source_backed: i64,
    pub pending_manual: i64,
    pub state: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub struct RunResult {
    pub source_backed_assigned: u64,
    pub manual_assigned: u64,
    pub outcome: String,
}

pub struct ThreadGroupsCanonicalKeysV1Task {
    pool: RdbPool,
    catalog: TaskCatalogEntry,
    batch_size: i64,
    lease_duration_ms: i64,
    #[cfg(all(test, not(feature = "postgres")))]
    interrupt_after_batches: Option<u32>,
    #[cfg(all(test, not(feature = "postgres")))]
    batches_committed: std::sync::atomic::AtomicU32,
}

impl ThreadGroupsCanonicalKeysV1Task {
    pub fn new(pool: RdbPool, catalog: TaskCatalogEntry) -> Result<Self> {
        catalog.validate()?;
        Ok(Self {
            pool,
            catalog,
            batch_size: DEFAULT_BATCH_SIZE,
            lease_duration_ms: DEFAULT_LEASE_MS,
            #[cfg(all(test, not(feature = "postgres")))]
            interrupt_after_batches: None,
            #[cfg(all(test, not(feature = "postgres")))]
            batches_committed: std::sync::atomic::AtomicU32::new(0),
        })
    }

    fn is_replacement_generation(&self) -> bool {
        self.catalog.identity() == "thread-groups-canonical-keys-v1@2"
    }

    fn checkpoint_format(&self) -> &'static str {
        if self.is_replacement_generation() {
            CHECKPOINT_FORMAT_V2
        } else {
            CHECKPOINT_FORMAT_V1
        }
    }

    #[cfg(all(test, not(feature = "postgres")))]
    fn with_batch_size(mut self, batch_size: i64) -> Self {
        self.batch_size = batch_size;
        self
    }

    #[cfg(all(test, not(feature = "postgres")))]
    fn interrupt_after_batches(mut self, batches: u32) -> Self {
        self.interrupt_after_batches = Some(batches);
        self
    }

    #[cfg(all(test, not(feature = "postgres")))]
    fn interrupt_if_requested(&self) -> Result<()> {
        use std::sync::atomic::Ordering;
        let committed = self.batches_committed.load(Ordering::SeqCst);
        if self.interrupt_after_batches == Some(committed) {
            bail!("injected interruption after {committed} committed batches");
        }
        Ok(())
    }

    #[cfg(all(test, not(feature = "postgres")))]
    fn note_batch_committed(&self) {
        use std::sync::atomic::Ordering;
        self.batches_committed.fetch_add(1, Ordering::SeqCst);
    }

    #[cfg(any(not(test), feature = "postgres"))]
    fn interrupt_if_requested(&self) -> Result<()> {
        Ok(())
    }

    #[cfg(any(not(test), feature = "postgres"))]
    fn note_batch_committed(&self) {}

    pub async fn inspect(&self) -> Result<InspectResult> {
        self.preflight().await?;
        let threads = sqlx::query_scalar::<_, i64>(COUNT_THREADS_SQL)
            .fetch_one(&self.pool)
            .await
            .context("counting threads for data migration")?;
        let threads_with_canonical_key = sqlx::query_scalar::<_, i64>(COUNT_KEY_ROWS_SQL)
            .fetch_one(&self.pool)
            .await
            .context("counting assigned canonical keys")?;
        let pending_source_backed = sqlx::query_scalar::<_, i64>(PENDING_SOURCE_BACKED_SQL)
            .fetch_one(&self.pool)
            .await
            .context("counting source-backed threads pending key assignment")?;
        let pending_manual = sqlx::query_scalar::<_, i64>(PENDING_MANUAL_SQL)
            .fetch_one(&self.pool)
            .await
            .context("counting manual threads pending key assignment")?;
        let state = state::load(&self.pool, &self.catalog.identity())
            .await?
            .map(|row| row.state);
        Ok(InspectResult {
            threads,
            threads_with_canonical_key,
            pending_source_backed,
            pending_manual,
            state,
        })
    }

    pub async fn dry_run(&self) -> Result<InspectResult> {
        self.inspect().await
    }

    pub async fn apply(&self, execution_id: &str, holder_id: &str) -> Result<RunResult> {
        self.preflight().await?;
        if let Some(existing) = state::load(&self.pool, &self.catalog.identity()).await?
            && existing.kind()? == TaskStateKind::Completed
        {
            self.verify().await?;
            return Ok(RunResult {
                outcome: "already_completed".to_string(),
                ..RunResult::default()
            });
        }
        let now = command_utils::util::datetime::now_millis();
        let lease = state::claim(
            &self.pool,
            &self.catalog.identity(),
            &self.catalog.canonical_definition_digest,
            execution_id,
            holder_id,
            now,
            self.lease_duration_ms,
        )
        .await?;
        let run = async {
            let result = if self.is_replacement_generation() {
                self.apply_replacement_with_lease(&lease).await?
            } else {
                self.apply_with_lease(&lease).await?
            };
            self.verify_with_lease(&lease).await?;
            Ok(result)
        }
        .await;
        match run {
            Ok(result) => {
                state::complete(
                    &self.pool,
                    &lease,
                    command_utils::util::datetime::now_millis(),
                )
                .await?;
                Ok(result)
            }
            Err(error) => {
                let _ = state::fail(
                    &self.pool,
                    &lease,
                    "task_execution_failed",
                    command_utils::util::datetime::now_millis(),
                )
                .await;
                Err(error)
            }
        }
    }

    pub async fn verify(&self) -> Result<()> {
        self.verify_inner(None).await
    }

    async fn verify_with_lease(&self, lease: &TaskLease) -> Result<()> {
        self.verify_inner(Some(lease)).await
    }

    async fn verify_inner(&self, lease: Option<&TaskLease>) -> Result<()> {
        self.preflight().await?;
        // The UNIQUE index makes duplicate keys unreachable; verification
        // re-asserts the invariant instead of trusting the index alone.
        let duplicated = sqlx::query_scalar::<_, i64>(DUPLICATE_KEYS_SQL)
            .fetch_one(&self.pool)
            .await
            .context("checking duplicate canonical keys")?;
        if duplicated != 0 {
            bail!("{duplicated} canonical keys are assigned to more than one live thread");
        }
        let mut after: Option<i64> = None;
        loop {
            if let Some(lease) = lease {
                self.renew_lease(lease).await?;
            }
            let (sql, binds): (String, Vec<i64>) = match after {
                Some(after) => (
                    format!(
                        "SELECT t.id, k.key, k.origin FROM thread t \
                         LEFT JOIN thread_canonical_key k ON k.thread_id = t.id \
                         WHERE t.id > {} ORDER BY t.id ASC LIMIT {}",
                        placeholder(1),
                        placeholder(2)
                    ),
                    vec![after],
                ),
                None => (
                    format!(
                        "SELECT t.id, k.key, k.origin FROM thread t \
                         LEFT JOIN thread_canonical_key k ON k.thread_id = t.id \
                         ORDER BY t.id ASC LIMIT {}",
                        placeholder(1)
                    ),
                    Vec::new(),
                ),
            };
            let mut query = sqlx::query_as::<_, (i64, Option<String>, Option<String>)>(
                sqlx::AssertSqlSafe(sql),
            );
            for bind in binds {
                query = query.bind(bind);
            }
            let rows = query
                .bind(self.batch_size)
                .fetch_all(&self.pool)
                .await
                .context("fetching thread canonical key verification page")?;
            if rows.is_empty() {
                break;
            }
            let ids: Vec<i64> = rows.iter().map(|row| row.0).collect();
            let mappings = self.resolved_mappings_for(&ids).await?;
            for (thread_id, key, origin) in &rows {
                let Some(key) = key else {
                    bail!("thread {thread_id} has no canonical key");
                };
                let Some(origin) = origin else {
                    bail!("thread {thread_id} key row has no origin");
                };
                if origin == KEY_ORIGIN_SOURCE_IDENTITY {
                    // A persisted key is the immutable correspondence record. The
                    // replacement generation must not invalidate it when source
                    // mappings are removed or rebound after assignment.
                    let matches = if self.is_replacement_generation() {
                        true
                    } else {
                        let thread_mappings = mappings.get(thread_id).with_context(|| {
                            format!(
                                "thread {thread_id} has a source_identity key but no resolved source identity mapping"
                            )
                        })?;
                        let derived =
                            derive_source_backed_key(&thread_mappings[0]).with_context(|| {
                                format!("deriving the canonical key of thread {thread_id}")
                            })?;
                        derived == *key
                    };
                    if !matches {
                        bail!(
                            "thread {thread_id} saved canonical key does not match any resolved source identity mapping"
                        );
                    }
                }
            }
            after = rows.last().map(|row| row.0);
        }
        Ok(())
    }

    async fn apply_with_lease(&self, lease: &TaskLease) -> Result<RunResult> {
        let mut checkpoint = self.load_checkpoint().await?;
        let source_backed_assigned = self
            .assign_source_backed_keys(lease, &mut checkpoint)
            .await?;
        let manual_assigned = self.assign_manual_keys(lease, &mut checkpoint).await?;
        Ok(RunResult {
            source_backed_assigned,
            manual_assigned,
            outcome: "assigned".to_string(),
        })
    }

    async fn apply_replacement_with_lease(&self, lease: &TaskLease) -> Result<RunResult> {
        let mut checkpoint = self.load_checkpoint().await?;
        let source_backed_assigned = self
            .assign_source_backed_keys_replacement(lease, &mut checkpoint)
            .await?;
        let manual_assigned = self.assign_manual_keys(lease, &mut checkpoint).await?;
        Ok(RunResult {
            source_backed_assigned,
            manual_assigned,
            outcome: "assigned".to_string(),
        })
    }

    /// Read-only fail-closed checks that must pass before any lease,
    /// checkpoint, or key row is written.
    async fn preflight(&self) -> Result<()> {
        let orphan_key_rows = sqlx::query_scalar::<_, i64>(ORPHAN_KEY_ROWS_SQL)
            .fetch_one(&self.pool)
            .await
            .context("checking canonical key rows without a thread row")?;
        if orphan_key_rows != 0 {
            bail!(
                "canonical key preflight failed: {orphan_key_rows} key rows reference no thread row"
            );
        }
        let key_rows: Vec<(String, String)> = sqlx::query_as(KEY_ROWS_SQL)
            .fetch_all(&self.pool)
            .await
            .context("scanning canonical key rows for preflight")?;
        for (key, origin) in &key_rows {
            if !is_valid_key(key) || !is_valid_origin(origin) {
                bail!("canonical key preflight failed: malformed key row (origin={origin})");
            }
        }
        if self.is_replacement_generation() {
            self.validate_referenced_thread_owners().await?;
        }
        Ok(())
    }

    async fn validate_referenced_thread_owners(&self) -> Result<()> {
        for (sql, field) in [
            (
                "SELECT identity.owner_scope, thread.user_id \
                 FROM source_thread_identity identity JOIN thread ON thread.id = identity.thread_id",
                "source identity",
            ),
            (
                "SELECT canonical.owner_scope, thread.user_id \
                 FROM thread_canonical_key canonical JOIN thread ON thread.id = canonical.thread_id",
                "canonical key",
            ),
        ] {
            let rows: Vec<(String, i64)> = sqlx::query_as(sqlx::AssertSqlSafe(sql))
                .fetch_all(&self.pool)
                .await
                .with_context(|| format!("checking {field} owner against Thread owner"))?;
            for (owner_scope, thread_user_id) in rows {
                let parsed = parse_legacy_owner_scope(&owner_scope)
                    .with_context(|| format!("{field} has invalid owner scope"))?;
                if parsed != thread_user_id {
                    bail!(
                        "{field} owner mismatch: owner {parsed} does not match referenced Thread owner {thread_user_id}"
                    );
                }
            }
        }
        Ok(())
    }

    async fn load_checkpoint(&self) -> Result<Checkpoint> {
        let checkpoint = Checkpoint {
            migration_version: self.catalog.introduced_by_schema_version.clone(),
            ..Checkpoint::default()
        };
        let Some(row) = state::load(&self.pool, &self.catalog.identity()).await? else {
            return Ok(checkpoint);
        };
        if row.canonical_definition_digest != self.catalog.canonical_definition_digest {
            bail!("task state definition digest does not match the fixed registry");
        }
        let Some(raw) = row.checkpoint else {
            return Ok(checkpoint);
        };
        let envelope: TaskCheckpointEnvelope<Checkpoint> =
            serde_json::from_str(&raw).context("parsing thread canonical keys checkpoint")?;
        if envelope.format != self.checkpoint_format()
            || envelope.task_identity != self.catalog.identity()
            || envelope.canonical_definition_digest != self.catalog.canonical_definition_digest
        {
            bail!("thread canonical keys checkpoint identity is invalid");
        }
        envelope.payload.validate()?;
        if envelope.payload.migration_version != checkpoint.migration_version {
            bail!(
                "thread canonical keys checkpoint was recorded for schema version {}",
                envelope.payload.migration_version
            );
        }
        if envelope.payload.adapter_version != ADAPTER_VERSION {
            bail!(
                "thread canonical keys checkpoint was recorded under adapter contract {}",
                envelope.payload.adapter_version
            );
        }
        if envelope.payload.policy_version != POLICY_VERSION {
            bail!(
                "thread canonical keys checkpoint was recorded under policy {}",
                envelope.payload.policy_version
            );
        }
        Ok(envelope.payload)
    }

    async fn save_checkpoint_tx(
        &self,
        tx: &mut RdbTransaction<'_>,
        lease: &TaskLease,
        checkpoint: &Checkpoint,
    ) -> Result<()> {
        checkpoint.validate()?;
        let envelope = TaskCheckpointEnvelope {
            format: self.checkpoint_format().to_owned(),
            task_identity: self.catalog.identity(),
            canonical_definition_digest: self.catalog.canonical_definition_digest.clone(),
            payload: checkpoint.clone(),
        };
        state::save_checkpoint_tx(
            tx,
            lease,
            &envelope,
            command_utils::util::datetime::now_millis(),
            self.lease_duration_ms,
        )
        .await
    }

    async fn renew_lease(&self, lease: &TaskLease) -> Result<()> {
        state::renew_lease(
            &self.pool,
            lease,
            command_utils::util::datetime::now_millis(),
            self.lease_duration_ms,
        )
        .await
    }

    async fn renew_lease_tx(&self, tx: &mut RdbTransaction<'_>, lease: &TaskLease) -> Result<()> {
        state::renew_lease_tx(
            tx,
            lease,
            command_utils::util::datetime::now_millis(),
            self.lease_duration_ms,
        )
        .await
    }

    async fn assign_source_backed_keys(
        &self,
        lease: &TaskLease,
        checkpoint: &mut Checkpoint,
    ) -> Result<u64> {
        if checkpoint.phase == KeyPhase::Manual || checkpoint.phase == KeyPhase::Verified {
            return Ok(0);
        }
        if checkpoint.phase == KeyPhase::Pending {
            checkpoint.phase = KeyPhase::SourceBacked;
            let mut tx = self.pool.begin().await?;
            self.save_checkpoint_tx(&mut tx, lease, checkpoint).await?;
            tx.commit().await?;
        }
        let mut assigned = 0_u64;
        loop {
            let ids = self
                .fetch_pending_ids(
                    SOURCE_BACKED_IDS_BASE_SQL,
                    checkpoint.source_backed_last_thread_id,
                )
                .await?;
            if ids.is_empty() {
                break;
            }
            let mut tx = self.pool.begin().await?;
            // Fenced heartbeat inside the same transaction that writes the
            // key rows: checkpoint and RDB writes commit together under the
            // caller's fencing token.
            self.renew_lease_tx(&mut tx, lease).await?;
            for thread_id in &ids {
                if self.thread_has_key_tx(&mut tx, *thread_id).await? {
                    continue;
                }
                let mapping = self
                    .smallest_resolved_mapping_tx(&mut tx, *thread_id)
                    .await?;
                let key = derive_source_backed_key(&mapping)
                    .with_context(|| format!("deriving the canonical key of thread {thread_id}"))?;
                self.assign_key_tx(
                    &mut tx,
                    *thread_id,
                    &mapping.owner_scope,
                    &key,
                    KEY_ORIGIN_SOURCE_IDENTITY,
                )
                .await?;
                assigned += 1;
            }
            checkpoint.source_backed_last_thread_id = ids.last().copied();
            self.save_checkpoint_tx(&mut tx, lease, checkpoint).await?;
            tx.commit().await?;
            self.note_batch_committed();
            self.interrupt_if_requested()?;
        }
        checkpoint.phase = KeyPhase::Manual;
        checkpoint.source_backed_last_thread_id = None;
        let mut tx = self.pool.begin().await?;
        self.save_checkpoint_tx(&mut tx, lease, checkpoint).await?;
        tx.commit().await?;
        Ok(assigned)
    }

    async fn assign_source_backed_keys_replacement(
        &self,
        lease: &TaskLease,
        checkpoint: &mut Checkpoint,
    ) -> Result<u64> {
        if checkpoint.phase == KeyPhase::Manual || checkpoint.phase == KeyPhase::Verified {
            return Ok(0);
        }
        if checkpoint.phase == KeyPhase::Pending {
            checkpoint.phase = KeyPhase::SourceBacked;
            let mut tx = self.pool.begin().await?;
            self.save_checkpoint_tx(&mut tx, lease, checkpoint).await?;
            tx.commit().await?;
        }
        let mut assigned = 0_u64;
        loop {
            let ids = self
                .fetch_pending_ids(
                    SOURCE_BACKED_IDS_BASE_SQL,
                    checkpoint.source_backed_last_thread_id,
                )
                .await?;
            if ids.is_empty() {
                break;
            }
            let mut tx = self.pool.begin().await?;
            self.renew_lease_tx(&mut tx, lease).await?;
            for thread_id in &ids {
                if self.thread_has_key_tx(&mut tx, *thread_id).await? {
                    continue;
                }
                let mappings = self
                    .resolved_mappings_for_thread_tx(&mut tx, *thread_id)
                    .await?;
                let owner_user_id: i64 = sqlx::query_scalar(THREAD_USER_ID_SQL)
                    .bind(thread_id)
                    .fetch_one(&mut *tx)
                    .await
                    .with_context(|| format!("reading owner of thread {thread_id}"))?;
                let owner_scope = format!("user:{owner_user_id}");
                let (key, origin) = if let [mapping] = mappings.as_slice() {
                    if mapping.owner_scope != owner_scope {
                        bail!(
                            "source identity owner mismatch for thread {thread_id}: {} does not match {owner_scope}",
                            mapping.owner_scope
                        );
                    }
                    (
                        derive_source_backed_key(mapping)
                            .with_context(|| format!("deriving key for thread {thread_id}"))?,
                        KEY_ORIGIN_SOURCE_IDENTITY,
                    )
                } else {
                    (random_key(), KEY_ORIGIN_BACKFILL_MAPPING)
                };
                self.assign_key_tx(&mut tx, *thread_id, &owner_scope, &key, origin)
                    .await?;
                assigned += 1;
            }
            checkpoint.source_backed_last_thread_id = ids.last().copied();
            self.save_checkpoint_tx(&mut tx, lease, checkpoint).await?;
            tx.commit().await?;
            self.note_batch_committed();
            self.interrupt_if_requested()?;
        }
        checkpoint.phase = KeyPhase::Manual;
        checkpoint.source_backed_last_thread_id = None;
        let mut tx = self.pool.begin().await?;
        self.save_checkpoint_tx(&mut tx, lease, checkpoint).await?;
        tx.commit().await?;
        Ok(assigned)
    }

    async fn assign_manual_keys(
        &self,
        lease: &TaskLease,
        checkpoint: &mut Checkpoint,
    ) -> Result<u64> {
        if checkpoint.phase == KeyPhase::Verified {
            return Ok(0);
        }
        if checkpoint.phase != KeyPhase::Manual {
            bail!("manual key assignment requires the source-backed phase to complete first");
        }
        let mut assigned = 0_u64;
        loop {
            let ids = self
                .fetch_pending_ids(MANUAL_IDS_BASE_SQL, checkpoint.manual_last_thread_id)
                .await?;
            if ids.is_empty() {
                break;
            }
            let mut tx = self.pool.begin().await?;
            self.renew_lease_tx(&mut tx, lease).await?;
            for thread_id in &ids {
                if self.thread_has_key_tx(&mut tx, *thread_id).await? {
                    continue;
                }
                let user_id: i64 = sqlx::query_scalar(THREAD_USER_ID_SQL)
                    .bind(thread_id)
                    .fetch_one(&mut *tx)
                    .await
                    .with_context(|| format!("reading the owner of thread {thread_id}"))?;
                self.assign_key_tx(
                    &mut tx,
                    *thread_id,
                    &format!("user:{user_id}"),
                    &random_key(),
                    KEY_ORIGIN_BACKFILL_MAPPING,
                )
                .await?;
                assigned += 1;
            }
            checkpoint.manual_last_thread_id = ids.last().copied();
            self.save_checkpoint_tx(&mut tx, lease, checkpoint).await?;
            tx.commit().await?;
            self.note_batch_committed();
            self.interrupt_if_requested()?;
        }
        checkpoint.phase = KeyPhase::Verified;
        checkpoint.manual_last_thread_id = None;
        let mut tx = self.pool.begin().await?;
        self.save_checkpoint_tx(&mut tx, lease, checkpoint).await?;
        tx.commit().await?;
        Ok(assigned)
    }

    async fn fetch_pending_ids(&self, base_sql: &str, after: Option<i64>) -> Result<Vec<i64>> {
        if self.batch_size <= 0 {
            bail!("canonical key assignment batch size must be positive");
        }
        let (cursor_clause, binds): (String, Vec<i64>) = match after {
            Some(after) => (format!(" AND t.id > {}", placeholder(1)), vec![after]),
            None => (String::new(), Vec::new()),
        };
        let limit = placeholder(binds.len() + 1);
        let sql = format!("{base_sql}{cursor_clause} ORDER BY t.id ASC LIMIT {limit}");
        let mut query = sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(sql));
        for bind in binds {
            query = query.bind(bind);
        }
        query
            .bind(self.batch_size)
            .fetch_all(&self.pool)
            .await
            .context("fetching pending canonical key assignment keyset page")
    }

    async fn thread_has_key_tx(&self, tx: &mut RdbTransaction<'_>, thread_id: i64) -> Result<bool> {
        let existing: Option<String> = sqlx::query_scalar(THREAD_HAS_KEY_SQL)
            .bind(thread_id)
            .fetch_optional(&mut **tx)
            .await
            .with_context(|| {
                format!("checking the existing canonical key of thread {thread_id}")
            })?;
        Ok(existing.is_some())
    }

    async fn smallest_resolved_mapping_tx(
        &self,
        tx: &mut RdbTransaction<'_>,
        thread_id: i64,
    ) -> Result<ResolvedMapping> {
        let sql = format!(
            "SELECT owner_scope, source, identity_scope, native_id FROM source_thread_identity \
             WHERE thread_id = {} ORDER BY owner_scope ASC, source ASC, identity_scope ASC, native_id ASC LIMIT 1",
            placeholder(1)
        );
        sqlx::query_as::<_, ResolvedMapping>(sqlx::AssertSqlSafe(sql))
            .bind(thread_id)
            .fetch_one(&mut **tx)
            .await
            .with_context(|| {
                format!("thread {thread_id} lost its resolved source identity mapping")
            })
    }

    async fn resolved_mappings_for_thread_tx(
        &self,
        tx: &mut RdbTransaction<'_>,
        thread_id: i64,
    ) -> Result<Vec<ResolvedMapping>> {
        let sql = format!(
            "SELECT owner_scope, source, identity_scope, native_id FROM source_thread_identity \
             WHERE thread_id = {} ORDER BY owner_scope ASC, source ASC, identity_scope ASC, native_id ASC",
            placeholder(1)
        );
        sqlx::query_as::<_, ResolvedMapping>(sqlx::AssertSqlSafe(sql))
            .bind(thread_id)
            .fetch_all(&mut **tx)
            .await
            .with_context(|| format!("reading resolved identities for thread {thread_id}"))
    }

    async fn resolved_mappings_for(
        &self,
        thread_ids: &[i64],
    ) -> Result<BTreeMap<i64, Vec<ResolvedMapping>>> {
        if thread_ids.is_empty() {
            return Ok(BTreeMap::new());
        }
        let binds = thread_ids
            .iter()
            .enumerate()
            .map(|(index, _)| placeholder(index + 1))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT thread_id, owner_scope, source, identity_scope, native_id \
             FROM source_thread_identity WHERE thread_id IN ({binds}) \
             ORDER BY thread_id ASC, owner_scope ASC, source ASC, identity_scope ASC, native_id ASC"
        );
        let mut query =
            sqlx::query_as::<_, (i64, String, String, String, String)>(sqlx::AssertSqlSafe(sql));
        for thread_id in thread_ids {
            query = query.bind(thread_id);
        }
        let rows = query
            .fetch_all(&self.pool)
            .await
            .context("fetching resolved source identity mappings for verification")?;
        let mut grouped = BTreeMap::new();
        for (thread_id, owner_scope, source, identity_scope, native_id) in rows {
            grouped
                .entry(thread_id)
                .or_insert_with(Vec::new)
                .push(ResolvedMapping {
                    owner_scope,
                    source,
                    identity_scope,
                    native_id,
                });
        }
        Ok(grouped)
    }

    async fn assign_key_tx(
        &self,
        tx: &mut RdbTransaction<'_>,
        thread_id: i64,
        owner_scope: &str,
        key: &str,
        origin: &str,
    ) -> Result<()> {
        sqlx::query(INSERT_KEY_SQL)
            .bind(thread_id)
            .bind(owner_scope)
            .bind(key)
            .bind(origin)
            .bind(command_utils::util::datetime::now_millis())
            .execute(&mut **tx)
            .await
            .with_context(|| {
                format!(
                    "assigning canonical key to thread {thread_id}; a UNIQUE failure here means \
                     another live thread already owns the key"
                )
            })?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, sqlx::FromRow)]
struct ResolvedMapping {
    owner_scope: String,
    source: String,
    identity_scope: String,
    native_id: String,
}

/// Source-backed keys derive from the canonical serialization v2 of the
/// resolved identity tuple; every resolved mapping carries a concrete
/// `identity_scope` value, so derivation always succeeds here.
fn derive_source_backed_key(mapping: &ResolvedMapping) -> Result<String> {
    let user_id = parse_legacy_owner_scope(&mapping.owner_scope)
        .context("resolved source identity mapping has an invalid legacy owner scope")?;
    source_thread_canonical_key(&SourceIdentity::new(
        user_id,
        mapping.source.clone(),
        IdentityScope::known(mapping.identity_scope.clone()),
        mapping.native_id.clone(),
    ))
    .context("resolved source identity mapping has an unusable identity scope")
}

fn is_valid_key(key: &str) -> bool {
    key.len() == 64
        && key
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn is_valid_origin(origin: &str) -> bool {
    matches!(
        origin,
        "source_identity" | "creation_uuid" | "backfill_mapping"
    )
}

/// Random key material as 64 lowercase hex characters. The value is stored
/// in `thread_canonical_key` on first assignment and reused verbatim on
/// every re-run, so randomness never becomes instability.
fn random_key() -> String {
    let mut bytes = [0_u8; 32];
    rand::rng().fill_bytes(&mut bytes);
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[async_trait]
impl DataMigrationTask for ThreadGroupsCanonicalKeysV1Task {
    fn task_identity(&self) -> String {
        self.catalog.identity()
    }

    async fn inspect(&self) -> Result<serde_json::Value> {
        Ok(serde_json::to_value(
            ThreadGroupsCanonicalKeysV1Task::inspect(self).await?,
        )?)
    }

    async fn dry_run(&self) -> Result<serde_json::Value> {
        Ok(serde_json::to_value(
            ThreadGroupsCanonicalKeysV1Task::dry_run(self).await?,
        )?)
    }

    async fn apply(&self, execution_id: &str, holder_id: &str) -> Result<serde_json::Value> {
        Ok(serde_json::to_value(
            ThreadGroupsCanonicalKeysV1Task::apply(self, execution_id, holder_id).await?,
        )?)
    }

    async fn verify(&self) -> Result<()> {
        ThreadGroupsCanonicalKeysV1Task::verify(self).await
    }
}

/// Replacement contract for canonical-key verification and new assignments.
/// The previous generation remains available only as an immutable history
/// entry; this generation scans past legacy checkpoints and keeps every key
/// already persisted by the old task.
pub struct ThreadGroupsCanonicalKeysV2Task {
    inner: ThreadGroupsCanonicalKeysV1Task,
}

impl ThreadGroupsCanonicalKeysV2Task {
    pub fn new(pool: RdbPool, catalog: TaskCatalogEntry) -> Result<Self> {
        if catalog.identity() != "thread-groups-canonical-keys-v1@2" {
            bail!(
                "unexpected replacement task catalog identity: {}",
                catalog.identity()
            );
        }
        Ok(Self {
            inner: ThreadGroupsCanonicalKeysV1Task::new(pool, catalog)?,
        })
    }

    pub async fn apply(&self, execution_id: &str, holder_id: &str) -> Result<RunResult> {
        self.inner.apply(execution_id, holder_id).await
    }

    pub async fn verify(&self) -> Result<()> {
        self.inner.verify().await
    }

    #[cfg(all(test, not(feature = "postgres")))]
    fn with_batch_size(mut self, batch_size: i64) -> Self {
        self.inner = self.inner.with_batch_size(batch_size);
        self
    }

    #[cfg(all(test, not(feature = "postgres")))]
    fn interrupt_after_batches(mut self, batches: u32) -> Self {
        self.inner = self.inner.interrupt_after_batches(batches);
        self
    }
}

#[async_trait]
impl DataMigrationTask for ThreadGroupsCanonicalKeysV2Task {
    fn task_identity(&self) -> String {
        self.inner.task_identity()
    }

    async fn inspect(&self) -> Result<serde_json::Value> {
        Ok(serde_json::to_value(
            ThreadGroupsCanonicalKeysV1Task::inspect(&self.inner).await?,
        )?)
    }

    async fn dry_run(&self) -> Result<serde_json::Value> {
        Ok(serde_json::to_value(
            ThreadGroupsCanonicalKeysV1Task::dry_run(&self.inner).await?,
        )?)
    }

    async fn apply(&self, execution_id: &str, holder_id: &str) -> Result<serde_json::Value> {
        Ok(serde_json::to_value(
            ThreadGroupsCanonicalKeysV1Task::apply(&self.inner, execution_id, holder_id).await?,
        )?)
    }

    async fn verify(&self) -> Result<()> {
        self.inner.verify().await
    }
}

#[cfg(all(test, not(feature = "postgres")))]
mod tests {
    use super::{
        Checkpoint, KeyPhase, ResolvedMapping, ThreadGroupsCanonicalKeysV1Task,
        ThreadGroupsCanonicalKeysV2Task, derive_source_backed_key,
    };
    use crate::db_migrate::{catalog, state, task_from_catalog};
    use common::thread_group_key::{IdentityScope, SourceIdentity, source_thread_canonical_key};
    use infra_utils::infra::rdb::RdbPool;
    use infra_utils::infra::test::TEST_RUNTIME;

    async fn test_pool() -> RdbPool {
        use sqlx::sqlite::SqlitePoolOptions;

        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::raw_sql(
            "CREATE TABLE thread (id BIGINT PRIMARY KEY, user_id BIGINT NOT NULL);\
             CREATE TABLE source_thread_identity (\
               owner_scope TEXT NOT NULL, source TEXT NOT NULL, identity_scope TEXT NOT NULL, native_id TEXT NOT NULL,\
               thread_id BIGINT NOT NULL, resolution_state TEXT NOT NULL,\
               first_seen_at BIGINT NOT NULL, last_seen_at BIGINT NOT NULL,\
               PRIMARY KEY (owner_scope, source, identity_scope, native_id)\
             );\
             CREATE TABLE thread_canonical_key (\
               thread_id BIGINT NOT NULL PRIMARY KEY, owner_scope TEXT NOT NULL,\
               key CHAR(64) NOT NULL, origin TEXT NOT NULL, assigned_at BIGINT NOT NULL\
             );\
             CREATE UNIQUE INDEX thread_canonical_key_key ON thread_canonical_key (key);\
             CREATE TABLE memories_data_migration_task_state (\
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

    fn task(pool: RdbPool) -> ThreadGroupsCanonicalKeysV1Task {
        ThreadGroupsCanonicalKeysV1Task::new(
            pool,
            catalog::thread_groups_canonical_keys_v1().unwrap(),
        )
        .unwrap()
    }

    fn replacement_task(pool: RdbPool) -> ThreadGroupsCanonicalKeysV2Task {
        ThreadGroupsCanonicalKeysV2Task::new(
            pool,
            catalog::thread_groups_canonical_keys_v2().unwrap(),
        )
        .unwrap()
    }

    async fn prepare_task_state(pool: &RdbPool) {
        sqlx::raw_sql(
            "CREATE TABLE IF NOT EXISTS memories_data_migration_task_state (\
               task_identity TEXT PRIMARY KEY, canonical_definition_digest TEXT NOT NULL, state TEXT NOT NULL,\
               execution_id TEXT, holder_id TEXT, fencing_token BIGINT NOT NULL, heartbeat_at BIGINT,\
               lease_expires_at BIGINT, attempt_count BIGINT NOT NULL, checkpoint TEXT,\
               failure_classification TEXT, started_at BIGINT, updated_at BIGINT NOT NULL, completed_at BIGINT\
             )",
        )
        .execute(pool)
        .await
        .unwrap();
        sqlx::query(
            "DELETE FROM memories_data_migration_task_state \
             WHERE task_identity IN ('thread-groups-canonical-keys-v1@1', 'thread-groups-canonical-keys-v1@2')",
        )
        .execute(pool)
        .await
        .unwrap();
    }

    async fn insert_thread(pool: &RdbPool, id: i64, user_id: i64) {
        sqlx::query("INSERT INTO thread (id, user_id) VALUES (?, ?)")
            .bind(id)
            .bind(user_id)
            .execute(pool)
            .await
            .unwrap();
    }

    async fn resolve_mapping(
        pool: &RdbPool,
        thread_id: i64,
        owner_scope: &str,
        source: &str,
        identity_scope: &str,
        native_id: &str,
    ) {
        sqlx::query(
            "INSERT INTO source_thread_identity \
             (owner_scope, source, identity_scope, native_id, thread_id, resolution_state, first_seen_at, last_seen_at) \
             VALUES (?, ?, ?, ?, ?, 'resolved', 1, 1)",
        )
        .bind(owner_scope)
        .bind(source)
        .bind(identity_scope)
        .bind(native_id)
        .bind(thread_id)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn key_rows(pool: &RdbPool) -> Vec<(i64, String, String, String)> {
        sqlx::query_as(
            "SELECT thread_id, owner_scope, key, origin FROM thread_canonical_key ORDER BY thread_id",
        )
        .fetch_all(pool)
        .await
        .unwrap()
    }

    async fn clear_task_state(pool: &RdbPool) {
        sqlx::query("DELETE FROM memories_data_migration_task_state")
            .execute(pool)
            .await
            .unwrap();
    }

    #[test]
    fn checkpoint_rejects_cursors_outside_their_phase() {
        let checkpoint = |phase, source, manual| Checkpoint {
            phase,
            source_backed_last_thread_id: source,
            manual_last_thread_id: manual,
            migration_version: "20260920000001".to_string(),
            ..Checkpoint::default()
        };
        assert!(checkpoint(KeyPhase::Pending, None, None).validate().is_ok());
        assert!(
            checkpoint(KeyPhase::Pending, Some(1), None)
                .validate()
                .is_err()
        );
        assert!(
            checkpoint(KeyPhase::SourceBacked, Some(1), None)
                .validate()
                .is_ok()
        );
        assert!(
            checkpoint(KeyPhase::SourceBacked, None, Some(1))
                .validate()
                .is_err()
        );
        assert!(
            checkpoint(KeyPhase::Manual, None, Some(1))
                .validate()
                .is_ok()
        );
        assert!(
            checkpoint(KeyPhase::Manual, Some(1), Some(1))
                .validate()
                .is_err()
        );
        assert!(
            checkpoint(KeyPhase::Verified, None, None)
                .validate()
                .is_ok()
        );
        assert!(
            checkpoint(KeyPhase::Verified, Some(1), None)
                .validate()
                .is_err()
        );

        let empty_versions = Checkpoint {
            phase: KeyPhase::Pending,
            migration_version: String::new(),
            ..Checkpoint::default()
        };
        assert!(empty_versions.validate().is_err());
    }

    #[test]
    fn source_backed_keys_are_derived_and_manual_keys_generated() {
        TEST_RUNTIME.block_on(async {
            let pool = test_pool().await;
            insert_thread(&pool, 1, 1).await;
            insert_thread(&pool, 2, 7).await;
            insert_thread(&pool, 3, 7).await;
            resolve_mapping(&pool, 1, "user:1", "codex", "", "session-1").await;
            resolve_mapping(&pool, 2, "user:7", "claude_code", "project-x", "sess-9").await;

            let result = task(pool.clone())
                .apply("execution-1", "test")
                .await
                .unwrap();
            assert_eq!(result.source_backed_assigned, 2);
            assert_eq!(result.manual_assigned, 1);

            let rows = key_rows(&pool).await;
            assert_eq!(rows.len(), 3);
            let expected_codex = source_thread_canonical_key(&SourceIdentity::new(
                1,
                "codex",
                IdentityScope::known(""),
                "session-1",
            ))
            .unwrap();
            let expected_claude = source_thread_canonical_key(&SourceIdentity::new(
                7,
                "claude_code",
                IdentityScope::known("project-x"),
                "sess-9",
            ))
            .unwrap();
            assert_eq!(
                rows[0],
                (
                    1,
                    "user:1".to_string(),
                    expected_codex,
                    "source_identity".to_string()
                )
            );
            assert_eq!(
                rows[1],
                (
                    2,
                    "user:7".to_string(),
                    expected_claude,
                    "source_identity".to_string()
                )
            );
            let (thread_id, owner_scope, key, origin) = &rows[2];
            assert_eq!(*thread_id, 3);
            assert_eq!(owner_scope, "user:7");
            assert_eq!(origin, "backfill_mapping");
            assert_eq!(key.len(), 64);
            assert!(
                key.chars()
                    .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase())
            );

            // Re-running a completed task reverifies instead of reassigning.
            let rerun = task(pool.clone())
                .apply("execution-2", "test")
                .await
                .unwrap();
            assert_eq!(rerun.outcome, "already_completed");
            assert_eq!(key_rows(&pool).await.len(), 3);
        });
    }

    #[test]
    fn manual_keys_are_stable_across_re_runs() {
        TEST_RUNTIME.block_on(async {
            let pool = test_pool().await;
            insert_thread(&pool, 1, 1).await;
            let first = task(pool.clone())
                .apply("execution-1", "test")
                .await
                .unwrap();
            assert_eq!(first.manual_assigned, 1);
            let (_, _, key_before, _) = key_rows(&pool).await.remove(0);

            // Simulate a fresh executor: drop the task state, re-apply, and
            // require the persistent correspondence mapping to be reused.
            clear_task_state(&pool).await;
            let second = task(pool.clone())
                .apply("execution-2", "test")
                .await
                .unwrap();
            assert_eq!(second.manual_assigned, 0);
            let rows = key_rows(&pool).await;
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].2, key_before);
        });
    }

    #[test]
    fn interrupted_run_resumes_from_its_checkpoint() {
        TEST_RUNTIME.block_on(async {
            let pool = test_pool().await;
            insert_thread(&pool, 1, 1).await;
            insert_thread(&pool, 2, 1).await;
            insert_thread(&pool, 3, 1).await;
            resolve_mapping(&pool, 1, "user:1", "codex", "", "session-1").await;

            let interrupted = task(pool.clone())
                .with_batch_size(1)
                .interrupt_after_batches(1)
                .apply("execution-1", "test")
                .await;
            assert!(interrupted.is_err());
            let state_row = state::load(&pool, "thread-groups-canonical-keys-v1@1")
                .await
                .unwrap()
                .unwrap();
            assert_eq!(state_row.state, "failed");
            // Exactly one key row was committed before the interruption.
            let partial = key_rows(&pool).await;
            assert_eq!(partial.len(), 1);
            assert_eq!(partial[0].0, 1);
            assert_eq!(partial[0].3, "source_identity");

            let resumed = task(pool.clone())
                .apply("execution-2", "test")
                .await
                .unwrap();
            assert_eq!(resumed.source_backed_assigned, 0);
            assert_eq!(resumed.manual_assigned, 2);
            let rows = key_rows(&pool).await;
            assert_eq!(rows.len(), 3);
            assert_eq!(rows[0].2, partial[0].2);
            assert_eq!(rows[1].3, "backfill_mapping");
            assert_eq!(rows[2].3, "backfill_mapping");
            task(pool.clone()).verify().await.unwrap();
        });
    }

    #[test]
    fn replacement_verify_keeps_a_completed_v1_key_after_a_smaller_alias_arrives() {
        TEST_RUNTIME.block_on(async {
            let pool = test_pool().await;
            insert_thread(&pool, 1, 1).await;
            resolve_mapping(&pool, 1, "user:1", "codex", "", "z-session").await;
            task(pool.clone())
                .apply("legacy-completed", "test")
                .await
                .unwrap();
            let saved = key_rows(&pool).await[0].2.clone();

            resolve_mapping(&pool, 1, "user:1", "codex", "", "a-alias").await;
            assert!(
                task(pool.clone()).verify().await.is_err(),
                "the retired generation exposes why it must no longer be re-run"
            );

            replacement_task(pool.clone())
                .apply("replacement-after-alias", "test")
                .await
                .unwrap();
            replacement_task(pool.clone()).verify().await.unwrap();
            assert_eq!(key_rows(&pool).await[0].2, saved);
        });
    }

    #[test]
    fn replacement_verification_treats_saved_key_as_authoritative_after_identity_rebinding() {
        TEST_RUNTIME.block_on(async {
            let pool = test_pool().await;
            insert_thread(&pool, 1, 1).await;
            resolve_mapping(&pool, 1, "user:1", "codex", "", "original-session").await;
            task(pool.clone())
                .apply("legacy-before-rebinding", "test")
                .await
                .unwrap();
            let saved_key = key_rows(&pool).await[0].2.clone();

            sqlx::query("DELETE FROM source_thread_identity WHERE thread_id = ?")
                .bind(1_i64)
                .execute(&pool)
                .await
                .unwrap();
            resolve_mapping(&pool, 1, "user:1", "codex", "", "rebound-session").await;

            replacement_task(pool.clone())
                .apply("replacement-after-rebinding", "test")
                .await
                .unwrap();
            replacement_task(pool.clone()).verify().await.unwrap();
            assert_eq!(key_rows(&pool).await[0].2, saved_key);
        });
    }

    #[test]
    fn replacement_uses_opaque_keys_for_multiple_new_mappings_and_legacy_run_cannot_replace_them() {
        TEST_RUNTIME.block_on(async {
            let pool = test_pool().await;
            insert_thread(&pool, 1, 1).await;
            resolve_mapping(&pool, 1, "user:1", "codex", "", "session-a").await;
            resolve_mapping(&pool, 1, "user:1", "codex", "", "session-b").await;

            replacement_task(pool.clone())
                .apply("replacement-multiple-mappings", "test")
                .await
                .unwrap();
            let saved = key_rows(&pool).await[0].clone();
            assert_eq!(saved.3, "backfill_mapping");
            let derived_keys = ["session-a", "session-b"].map(|native_id| {
                derive_source_backed_key(&ResolvedMapping {
                    owner_scope: "user:1".to_string(),
                    source: "codex".to_string(),
                    identity_scope: String::new(),
                    native_id: native_id.to_string(),
                })
                .unwrap()
            });
            assert!(derived_keys.iter().all(|derived| derived != &saved.2));

            task(pool.clone())
                .apply("legacy-after-replacement", "test")
                .await
                .unwrap();
            assert_eq!(key_rows(&pool).await[0], saved);
            replacement_task(pool.clone()).verify().await.unwrap();
        });
    }

    #[test]
    fn replacement_hands_off_from_an_unfinished_v1_checkpoint_and_resumes_opaque_keys() {
        TEST_RUNTIME.block_on(async {
            let pool = test_pool().await;
            insert_thread(&pool, 1, 1).await;
            insert_thread(&pool, 2, 1).await;
            resolve_mapping(&pool, 1, "user:1", "codex", "", "session-1").await;
            resolve_mapping(&pool, 2, "user:1", "codex", "", "session-2").await;
            resolve_mapping(&pool, 2, "user:1", "codex", "", "session-2-alias").await;

            let interrupted = task(pool.clone())
                .with_batch_size(1)
                .interrupt_after_batches(1)
                .apply("legacy-interrupted", "test")
                .await;
            assert!(interrupted.is_err());
            let legacy_state = state::load(&pool, "thread-groups-canonical-keys-v1@1")
                .await
                .unwrap()
                .unwrap();
            let legacy_checkpoint = legacy_state.checkpoint.clone();
            assert_eq!(key_rows(&pool).await.len(), 1);

            assert!(
                replacement_task(pool.clone())
                    .with_batch_size(1)
                    .interrupt_after_batches(1)
                    .apply("replacement-interrupted", "test")
                    .await
                    .is_err()
            );
            let after_handoff = key_rows(&pool).await;
            assert_eq!(after_handoff.len(), 2);
            assert_eq!(after_handoff[0].3, "source_identity");
            assert_eq!(after_handoff[1].3, "backfill_mapping");
            let opaque_key = after_handoff[1].2.clone();
            assert_eq!(
                state::load(&pool, "thread-groups-canonical-keys-v1@1")
                    .await
                    .unwrap()
                    .unwrap()
                    .checkpoint,
                legacy_checkpoint,
                "the replacement must not rewrite the old task checkpoint"
            );

            replacement_task(pool.clone())
                .apply("replacement-resumed", "test")
                .await
                .unwrap();
            replacement_task(pool.clone()).verify().await.unwrap();
            let rows = key_rows(&pool).await;
            assert_eq!(rows.len(), 2);
            assert_eq!(rows[1].2, opaque_key);
        });
    }

    #[test]
    fn replacement_verifies_an_empty_full_atlas_schema_fixture() {
        TEST_RUNTIME.block_on(async {
            let pool = infra::infra::thread_group::test_support::setup_thread_group_pool().await;
            prepare_task_state(pool).await;
            replacement_task(pool.clone())
                .apply("replacement-empty-atlas", "test")
                .await
                .unwrap();
            replacement_task(pool.clone()).verify().await.unwrap();
        });
    }

    #[test]
    fn verify_rejects_missing_and_mismatched_keys() {
        TEST_RUNTIME.block_on(async {
            let pool = test_pool().await;
            insert_thread(&pool, 1, 1).await;
            insert_thread(&pool, 2, 1).await;
            resolve_mapping(&pool, 1, "user:1", "codex", "", "session-1").await;
            task(pool.clone()).apply("execution-1", "test").await.unwrap();

            sqlx::query("DELETE FROM thread_canonical_key WHERE thread_id = 2")
                .execute(&pool)
                .await
                .unwrap();
            assert!(task(pool.clone()).verify().await.is_err());

            // Restore a key row for thread 2, then tamper with the
            // source-backed key of thread 1.
            sqlx::query(
                "INSERT INTO thread_canonical_key (thread_id, owner_scope, key, origin, assigned_at) \
                 VALUES (2, 'user:1', ?, 'backfill_mapping', 1)",
            )
            .bind("f".repeat(64))
            .execute(&pool)
            .await
            .unwrap();
            let codex_key: String =
                sqlx::query_scalar("SELECT key FROM thread_canonical_key WHERE thread_id = 1")
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            let first_char = if codex_key.starts_with('a') { 'b' } else { 'a' };
            let tampered = format!("{first_char}{}", &codex_key[1..]);
            sqlx::query("UPDATE thread_canonical_key SET key = ? WHERE thread_id = 1")
                .bind(tampered)
                .execute(&pool)
                .await
                .unwrap();
            assert!(task(pool.clone()).verify().await.is_err());
        });
    }

    #[test]
    fn preflight_fails_closed_before_any_lease_or_write() {
        TEST_RUNTIME.block_on(async {
            let pool = test_pool().await;
            insert_thread(&pool, 1, 1).await;
            sqlx::query(
                "INSERT INTO thread_canonical_key (thread_id, owner_scope, key, origin, assigned_at) \
                 VALUES (999, 'user:1', ?, 'backfill_mapping', 1)",
            )
            .bind("a".repeat(64))
            .execute(&pool)
            .await
            .unwrap();
            let orphan = task(pool.clone()).apply("execution-1", "test").await;
            assert!(orphan.is_err());
            assert!(
                state::load(&pool, "thread-groups-canonical-keys-v1@1")
                    .await
                    .unwrap()
                    .is_none()
            );

            sqlx::query("DELETE FROM thread_canonical_key")
                .execute(&pool)
                .await
                .unwrap();
            sqlx::query(
                "INSERT INTO thread_canonical_key (thread_id, owner_scope, key, origin, assigned_at) \
                 VALUES (1, 'user:1', 'short', 'backfill_mapping', 1)",
            )
            .execute(&pool)
            .await
            .unwrap();
            assert!(task(pool.clone()).apply("execution-1", "test").await.is_err());
            assert!(
                state::load(&pool, "thread-groups-canonical-keys-v1@1")
                    .await
                    .unwrap()
                    .is_none()
            );
        });
    }

    #[test]
    fn fixed_registry_constructs_the_task_from_its_catalog_entry() {
        TEST_RUNTIME.block_on(async {
            let pool = test_pool().await;
            let entry = catalog::thread_groups_canonical_keys_v1().unwrap();
            let built = task_from_catalog(pool.clone(), entry).unwrap();
            assert_eq!(built.task_identity(), "thread-groups-canonical-keys-v1@1");

            let inspection = built.inspect().await.unwrap();
            assert_eq!(inspection["threads"], serde_json::json!(0));
            assert_eq!(inspection["pending_source_backed"], serde_json::json!(0));
        });
    }
}
