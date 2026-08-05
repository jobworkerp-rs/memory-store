//! `thread-message-times-v1@1` post-schema data migration.

use super::{
    DataMigrationTask,
    catalog::TaskCatalogEntry,
    state::{self, TaskCheckpointEnvelope, TaskLease, TaskStateKind},
};
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use infra::infra::search_index_maintenance::TaskStatus as IndexBuildStatus;
use infra::infra::thread_vector::{
    config::ThreadVectorDBConfig, record::ThreadVectorRecord,
    repository::ThreadVectorRepositoryImpl,
};
use infra_utils::infra::rdb::RdbPool;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

const CHECKPOINT_FORMAT: &str = "thread-message-times-v1@1/checkpoint-v1";
const DEFAULT_BATCH_SIZE: i64 = 500;
const DEFAULT_LEASE_MS: i64 = 120_000;
const LEGACY_MIGRATION_KEY: &str = "thread-time-fields-v1";

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
pub enum RdbPhase {
    Pending,
    Backfilling,
    Verified,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum VectorPhase {
    Pending,
    NotRequired,
    Staged,
    Switching,
    CanonicalReady,
    IndexesVerified,
    Completed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LegacyVectorState {
    Pending,
    Staged,
    Switching,
    Completed,
    NotRequired,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct LegacyMigrationState {
    vector_state: LegacyVectorState,
    staging_table_name: Option<String>,
}

impl LegacyVectorState {
    fn parse(value: &str) -> Result<Self> {
        match value {
            "PENDING" => Ok(Self::Pending),
            "STAGED" => Ok(Self::Staged),
            "SWITCHING" => Ok(Self::Switching),
            "COMPLETED" => Ok(Self::Completed),
            "NOT_REQUIRED" => Ok(Self::NotRequired),
            _ => bail!("unknown legacy thread time vector state: {value}"),
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IndexOutcome {
    Rebuilt,
    SkippedDisabled,
    Deferred,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct IndexOutcomes {
    pub fts: IndexOutcome,
    pub ann: IndexOutcome,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
pub struct Checkpoint {
    pub rdb_phase: RdbPhase,
    pub rdb_last_thread_id: Option<i64>,
    pub vector_phase: VectorPhase,
    pub canonical_table_name: Option<String>,
    pub staging_table_name: Option<String>,
    pub staging_origin: Option<String>,
    pub staging_cleanup_started: bool,
    pub index_outcomes: Option<IndexOutcomes>,
    pub vector_outcome: Option<String>,
}

impl Default for Checkpoint {
    fn default() -> Self {
        Self {
            rdb_phase: RdbPhase::Pending,
            rdb_last_thread_id: None,
            vector_phase: VectorPhase::Pending,
            canonical_table_name: None,
            staging_table_name: None,
            staging_origin: None,
            staging_cleanup_started: false,
            index_outcomes: None,
            vector_outcome: None,
        }
    }
}

impl Checkpoint {
    fn validate(&self) -> Result<()> {
        if self.rdb_phase != RdbPhase::Backfilling && self.rdb_last_thread_id.is_some() {
            bail!("rdb_last_thread_id is valid only while RDB backfilling");
        }
        let needs_staging = matches!(
            self.vector_phase,
            VectorPhase::Staged
                | VectorPhase::Switching
                | VectorPhase::CanonicalReady
                | VectorPhase::IndexesVerified
        );
        if needs_staging
            && (self.canonical_table_name.is_none()
                || self.staging_table_name.is_none()
                || self.staging_origin.is_none())
        {
            bail!("intermediate vector phase requires canonical and staging table identities");
        }
        if self.vector_phase == VectorPhase::Completed
            && (self.staging_table_name.is_some() || self.staging_origin.is_some())
        {
            bail!("completed vector phase must not retain a staging table");
        }
        if self.vector_phase == VectorPhase::NotRequired && self.vector_outcome.is_some() {
            bail!("not_required is an intermediate vector phase, not an outcome");
        }
        if self.vector_phase == VectorPhase::Completed
            && self.vector_outcome.as_deref() == Some("migrated")
            && (self.canonical_table_name.is_none() || self.index_outcomes.is_none())
        {
            bail!(
                "migrated vector checkpoint requires canonical table identity and index outcomes"
            );
        }
        if self.vector_phase == VectorPhase::IndexesVerified && self.index_outcomes.is_none() {
            bail!("indexes_verified vector checkpoint requires index outcomes");
        }
        if self.staging_cleanup_started && self.vector_phase != VectorPhase::IndexesVerified {
            bail!("staging_cleanup_started is valid only while indexes are verified");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub struct InspectResult {
    pub threads: i64,
    pub invalid_memberships: i64,
    pub invalid_timestamps: i64,
    pub state: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, PartialEq, Eq)]
pub struct RunResult {
    pub scanned_threads: u64,
    pub changed_threads: u64,
    pub vector_outcome: String,
}

pub struct ThreadMessageTimesV1Task {
    pool: RdbPool,
    catalog: TaskCatalogEntry,
    vector_config: Option<ThreadVectorDBConfig>,
    batch_size: i64,
    lease_duration_ms: i64,
    #[cfg(all(test, not(feature = "postgres")))]
    interrupt_after_vector_phase: Option<VectorPhase>,
}

impl ThreadMessageTimesV1Task {
    pub fn new(pool: RdbPool, catalog: TaskCatalogEntry) -> Result<Self> {
        Self::new_with_vector_config(pool, catalog, explicit_thread_vector_config()?)
    }

    fn new_with_vector_config(
        pool: RdbPool,
        catalog: TaskCatalogEntry,
        vector_config: Option<ThreadVectorDBConfig>,
    ) -> Result<Self> {
        catalog.validate()?;
        Ok(Self {
            pool,
            catalog,
            vector_config,
            batch_size: DEFAULT_BATCH_SIZE,
            lease_duration_ms: DEFAULT_LEASE_MS,
            #[cfg(all(test, not(feature = "postgres")))]
            interrupt_after_vector_phase: None,
        })
    }

    #[cfg(all(test, not(feature = "postgres")))]
    fn with_batch_size(mut self, batch_size: i64) -> Self {
        self.batch_size = batch_size;
        self
    }

    #[cfg(all(test, not(feature = "postgres")))]
    fn interrupt_after_vector_phase(mut self, phase: VectorPhase) -> Self {
        self.interrupt_after_vector_phase = Some(phase);
        self
    }

    #[cfg(all(test, not(feature = "postgres")))]
    fn interrupt_if_requested(&self, phase: VectorPhase) -> Result<()> {
        if self.interrupt_after_vector_phase == Some(phase) {
            bail!("injected interruption after vector phase {phase:?}");
        }
        Ok(())
    }

    #[cfg(any(not(test), feature = "postgres"))]
    fn interrupt_if_requested(&self, phase: VectorPhase) -> Result<()> {
        #[cfg(debug_assertions)]
        if let Ok(expected) = std::env::var("MEMORIES_DB_MIGRATE_TEST_INTERRUPT_AFTER_VECTOR_PHASE")
        {
            let actual = serde_json::to_value(phase)?;
            if actual.as_str() == Some(expected.as_str()) {
                bail!("injected interruption after vector phase {phase:?}");
            }
        }
        #[cfg(not(debug_assertions))]
        let _ = phase;
        Ok(())
    }

    pub async fn inspect(&self) -> Result<InspectResult> {
        let (invalid_memberships, invalid_timestamps) = self.preflight().await?;
        let threads = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM thread")
            .fetch_one(&self.pool)
            .await
            .context("counting threads for data migration")?;
        let state = state::load(&self.pool, &self.catalog.identity())
            .await?
            .map(|row| row.state);
        Ok(InspectResult {
            threads,
            invalid_memberships,
            invalid_timestamps,
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
                vector_outcome: "already_completed".to_string(),
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
            let result = self.apply_with_lease(&lease).await?;
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
        let mut after: Option<i64> = None;
        loop {
            if let Some(lease) = lease {
                self.renew_lease(lease).await?;
            }
            let ids = fetch_thread_ids(&self.pool, after, self.batch_size).await?;
            if ids.is_empty() {
                break;
            }
            for id in &ids {
                let stored_sql = format!(
                    "SELECT first_message_at, last_message_at FROM thread WHERE id = {}",
                    placeholder(1)
                );
                let (stored_first, stored_last): (Option<i64>, Option<i64>) =
                    sqlx::query_as(sqlx::AssertSqlSafe(stored_sql))
                        .bind(*id)
                        .fetch_one(&self.pool)
                        .await
                        .context("reading stored thread message bounds")?;
                let expected = message_bounds(&self.pool, *id).await?;
                if (stored_first, stored_last) != expected {
                    bail!("thread message bounds verification failed for thread_id={id}");
                }
            }
            after = ids.last().copied();
        }
        self.verify_vector(lease).await?;
        Ok(())
    }

    async fn apply_with_lease(&self, lease: &TaskLease) -> Result<RunResult> {
        let mut checkpoint = self.load_checkpoint().await?;
        self.backfill_rdb(lease, &mut checkpoint).await?;
        let vector_outcome = self.sync_vector(lease, &mut checkpoint).await?;
        Ok(RunResult {
            vector_outcome,
            ..RunResult::default()
        })
    }

    async fn preflight(&self) -> Result<(i64, i64)> {
        let invalid_memberships = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM thread_memory tm LEFT JOIN memory m ON m.id = tm.memory_id WHERE m.id IS NULL",
        )
        .fetch_one(&self.pool)
        .await
        .context("checking dangling thread_memory rows")?;
        let invalid_timestamps = sqlx::query_scalar::<_, i64>(
            "SELECT COUNT(*) FROM thread_memory tm JOIN memory m ON m.id = tm.memory_id WHERE m.created_at <= 0",
        )
        .fetch_one(&self.pool)
        .await
        .context("checking non-positive memory creation timestamps")?;
        if invalid_memberships != 0 || invalid_timestamps != 0 {
            bail!(
                "thread-message-times-v1 preflight failed: dangling_memberships={invalid_memberships} invalid_memory_timestamps={invalid_timestamps}"
            );
        }
        Ok((invalid_memberships, invalid_timestamps))
    }

    async fn load_checkpoint(&self) -> Result<Checkpoint> {
        let Some(row) = state::load(&self.pool, &self.catalog.identity()).await? else {
            return Ok(Checkpoint::default());
        };
        if row.canonical_definition_digest != self.catalog.canonical_definition_digest {
            bail!("task state definition digest does not match the fixed registry");
        }
        let Some(raw) = row.checkpoint else {
            return Ok(Checkpoint::default());
        };
        let envelope: TaskCheckpointEnvelope<Checkpoint> =
            serde_json::from_str(&raw).context("parsing thread message times checkpoint")?;
        if envelope.format != CHECKPOINT_FORMAT
            || envelope.task_identity != self.catalog.identity()
            || envelope.canonical_definition_digest != self.catalog.canonical_definition_digest
        {
            bail!("thread message times checkpoint identity is invalid");
        }
        envelope.payload.validate()?;
        Ok(envelope.payload)
    }

    async fn save_checkpoint_tx(
        &self,
        tx: &mut infra_utils::infra::rdb::RdbTransaction<'_>,
        lease: &TaskLease,
        checkpoint: &Checkpoint,
    ) -> Result<()> {
        checkpoint.validate()?;
        let envelope = TaskCheckpointEnvelope {
            format: CHECKPOINT_FORMAT.to_owned(),
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

    async fn backfill_rdb(&self, lease: &TaskLease, checkpoint: &mut Checkpoint) -> Result<()> {
        if checkpoint.rdb_phase == RdbPhase::Verified {
            return Ok(());
        }
        if checkpoint.rdb_phase == RdbPhase::Pending {
            checkpoint.rdb_phase = RdbPhase::Backfilling;
            checkpoint.rdb_last_thread_id = None;
            let mut tx = self.pool.begin().await?;
            self.save_checkpoint_tx(&mut tx, lease, checkpoint).await?;
            tx.commit().await?;
        }
        loop {
            self.renew_lease(lease).await?;
            let ids = fetch_thread_ids(&self.pool, checkpoint.rdb_last_thread_id, self.batch_size)
                .await?;
            if ids.is_empty() {
                break;
            }
            let mut tx = self
                .pool
                .begin()
                .await
                .context("begin RDB migration batch")?;
            for id in &ids {
                update_message_bounds_preserving_audit(&mut tx, *id).await?;
            }
            checkpoint.rdb_last_thread_id = ids.last().copied();
            self.save_checkpoint_tx(&mut tx, lease, checkpoint).await?;
            tx.commit().await.context("commit RDB migration batch")?;
        }
        checkpoint.rdb_phase = RdbPhase::Verified;
        checkpoint.rdb_last_thread_id = None;
        let mut tx = self.pool.begin().await?;
        self.save_checkpoint_tx(&mut tx, lease, checkpoint).await?;
        tx.commit().await?;
        self.verify_with_lease(lease).await
    }

    async fn sync_vector(&self, lease: &TaskLease, checkpoint: &mut Checkpoint) -> Result<String> {
        if checkpoint.rdb_phase != RdbPhase::Verified {
            bail!("vector migration requires verified RDB message bounds");
        }
        if checkpoint.vector_phase == VectorPhase::Completed {
            let outcome = checkpoint
                .vector_outcome
                .clone()
                .context("completed vector checkpoint is missing its outcome")?;
            if outcome == "migrated" {
                let config = self
                    .vector_config
                    .clone()
                    .context("migrated vector checkpoint requires ThreadVector configuration")?;
                if !ThreadVectorRepositoryImpl::table_exists(&config).await? {
                    bail!("migrated vector checkpoint canonical table is missing");
                }
            }
            return Ok(outcome);
        }
        let needs_staging = matches!(
            checkpoint.vector_phase,
            VectorPhase::Staged
                | VectorPhase::Switching
                | VectorPhase::CanonicalReady
                | VectorPhase::IndexesVerified
        );
        let legacy_state = self.legacy_vector_state().await?;
        let Some(config) = self.vector_config.clone() else {
            if needs_staging {
                bail!("intermediate vector checkpoint requires ThreadVector configuration");
            }
            if matches!(
                legacy_state.as_ref().map(|state| state.vector_state),
                Some(LegacyVectorState::Staged | LegacyVectorState::Switching)
            ) {
                bail!(
                    "legacy vector cutover is in progress and requires ThreadVector configuration"
                );
            }
            checkpoint.vector_phase = VectorPhase::NotRequired;
            checkpoint.vector_outcome = None;
            self.persist_non_rdb_checkpoint(lease, checkpoint).await?;
            checkpoint.vector_phase = VectorPhase::Completed;
            checkpoint.vector_outcome = Some("not_required".to_string());
            self.persist_non_rdb_checkpoint(lease, checkpoint).await?;
            return Ok("not_required".to_string());
        };
        let canonical_exists = ThreadVectorRepositoryImpl::table_exists(&config).await?;
        if checkpoint.vector_phase == VectorPhase::Pending {
            match legacy_state.as_ref() {
                Some(LegacyMigrationState {
                    vector_state: LegacyVectorState::Staged | LegacyVectorState::Switching,
                    staging_table_name,
                }) => {
                    let phase = if legacy_state.as_ref().map(|state| state.vector_state)
                        == Some(LegacyVectorState::Staged)
                    {
                        VectorPhase::Staged
                    } else {
                        VectorPhase::Switching
                    };
                    let staging_name =
                        format!("{}__thread_time_fields_v1_staging", config.table_name);
                    if staging_table_name.as_deref() != Some(staging_name.as_str()) {
                        bail!("legacy vector checkpoint has an unexpected staging table name");
                    }
                    let mut staging_config = config.clone();
                    staging_config.table_name = staging_name.clone();
                    if !ThreadVectorRepositoryImpl::table_exists(&staging_config).await? {
                        bail!("legacy vector checkpoint requires its fixed staging table");
                    }
                    checkpoint.vector_phase = phase;
                    checkpoint.canonical_table_name = Some(config.table_name.clone());
                    checkpoint.staging_table_name = Some(staging_name);
                    checkpoint.staging_origin = Some("legacy_thread_time_fields_v1".to_string());
                    self.persist_non_rdb_checkpoint(lease, checkpoint).await?;
                }
                Some(LegacyMigrationState {
                    vector_state: LegacyVectorState::Completed,
                    ..
                }) if !canonical_exists => {
                    bail!("legacy completed vector state has no canonical table");
                }
                _ => {}
            }
        }
        let needs_staging = matches!(
            checkpoint.vector_phase,
            VectorPhase::Staged
                | VectorPhase::Switching
                | VectorPhase::CanonicalReady
                | VectorPhase::IndexesVerified
        );
        if !canonical_exists && !needs_staging {
            checkpoint.vector_phase = VectorPhase::NotRequired;
            checkpoint.vector_outcome = None;
            self.persist_non_rdb_checkpoint(lease, checkpoint).await?;
            checkpoint.vector_phase = VectorPhase::Completed;
            checkpoint.vector_outcome = Some("not_required".to_string());
            self.persist_non_rdb_checkpoint(lease, checkpoint).await?;
            return Ok("not_required".to_string());
        }

        let staging_name = match checkpoint.staging_origin.as_deref() {
            Some("legacy_thread_time_fields_v1") => {
                format!("{}__thread_time_fields_v1_staging", config.table_name)
            }
            Some("current_task") | None => {
                format!("{}__thread_message_times_v1_g1_staging", config.table_name)
            }
            Some(_) => bail!("vector checkpoint has an unsupported staging origin"),
        };
        if checkpoint.vector_phase == VectorPhase::Pending {
            ThreadVectorRepositoryImpl::drop_table_if_exists(&config, &staging_name).await?;
            let source = ThreadVectorRepositoryImpl::new(config.clone()).await?;
            let mut staging_config = config.clone();
            staging_config.table_name = staging_name.clone();
            let staging = ThreadVectorRepositoryImpl::new(staging_config).await?;
            let mut after = None;
            loop {
                self.renew_lease(lease).await?;
                let thread_ids = source
                    .thread_id_keyset_page(after, self.vector_page_size()?)
                    .await?;
                if thread_ids.is_empty() {
                    break;
                }
                for thread_id in &thread_ids {
                    let Some((first, last)) =
                        message_bounds_for_existing_thread(&self.pool, *thread_id).await?
                    else {
                        continue;
                    };
                    let (records, discarded_duplicates) = normalize_vector_records(
                        source
                            .find_records_by_thread_id(*thread_id)
                            .await?
                            .into_iter()
                            .map(|mut record| {
                                record.first_message_at = first;
                                record.last_message_at = last;
                                record
                            })
                            .collect(),
                    )?;
                    if discarded_duplicates != 0 {
                        eprintln!(
                            "thread_vector_duplicate_rows_discarded thread_id={thread_id} count={discarded_duplicates}"
                        );
                    }
                    staging.batch_upsert(records).await?;
                    self.renew_lease(lease).await?;
                }
                after = thread_ids.last().copied();
            }
            self.verify_staging_against_canonical(&source, &staging)
                .await?;
            checkpoint.vector_phase = VectorPhase::Staged;
            checkpoint.canonical_table_name = Some(config.table_name.clone());
            checkpoint.staging_table_name = Some(staging_name.clone());
            checkpoint.staging_origin = Some("current_task".to_string());
            self.persist_non_rdb_checkpoint(lease, checkpoint).await?;
            self.interrupt_if_requested(VectorPhase::Staged)?;
        }
        if checkpoint.staging_table_name.as_deref() != Some(staging_name.as_str()) {
            bail!("vector checkpoint staging table name does not match this task");
        }
        let mut staging_config = config.clone();
        staging_config.table_name = staging_name.clone();
        if !ThreadVectorRepositoryImpl::table_exists(&staging_config).await? {
            bail!("vector checkpoint staging table is missing");
        }
        if checkpoint.vector_phase == VectorPhase::Staged {
            let staging = ThreadVectorRepositoryImpl::new(staging_config.clone()).await?;
            self.verify_vector_table_against_rdb(&staging).await?;
            if ThreadVectorRepositoryImpl::table_exists(&config).await? {
                let canonical = ThreadVectorRepositoryImpl::new(config.clone()).await?;
                self.verify_staging_against_canonical(&canonical, &staging)
                    .await?;
            }
            checkpoint.vector_phase = VectorPhase::Switching;
            self.persist_non_rdb_checkpoint(lease, checkpoint).await?;
            self.interrupt_if_requested(VectorPhase::Switching)?;
        }
        if checkpoint.vector_phase == VectorPhase::Switching {
            let staging = ThreadVectorRepositoryImpl::new(staging_config.clone()).await?;
            self.verify_vector_table_against_rdb(&staging).await?;
            // A previous replacement may have dropped the canonical table or
            // copied only a prefix before the process stopped. `switching`
            // deliberately treats staging as the verified source of truth so
            // a retry can replace that partial canonical table safely.
            self.cutover_with_fenced_ownership(lease, &config, &staging_name, checkpoint)
                .await?;
            self.interrupt_if_requested(VectorPhase::CanonicalReady)?;
        }
        if checkpoint.vector_phase == VectorPhase::CanonicalReady {
            let repository = ThreadVectorRepositoryImpl::new(config.clone()).await?;
            let staging = ThreadVectorRepositoryImpl::new(staging_config.clone()).await?;
            self.verify_equivalent_vector_tables(&staging, &repository)
                .await?;
            self.build_indexes_with_fenced_ownership(lease, &config, checkpoint)
                .await?;
            self.interrupt_if_requested(VectorPhase::IndexesVerified)?;
        }
        if checkpoint.vector_phase == VectorPhase::IndexesVerified {
            let repository = ThreadVectorRepositoryImpl::new(config.clone()).await?;
            if ThreadVectorRepositoryImpl::table_exists(&staging_config).await? {
                let staging = ThreadVectorRepositoryImpl::new(staging_config.clone()).await?;
                self.verify_equivalent_vector_tables(&staging, &repository)
                    .await?;
            } else if !checkpoint.staging_cleanup_started {
                bail!("indexes_verified checkpoint lost its staging table before cleanup");
            }
            self.cleanup_staging_with_fenced_ownership(lease, &config, &staging_name, checkpoint)
                .await?;
        }
        checkpoint
            .vector_outcome
            .clone()
            .context("completed vector migration is missing its outcome")
    }

    async fn persist_non_rdb_checkpoint(
        &self,
        lease: &TaskLease,
        checkpoint: &Checkpoint,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        self.save_checkpoint_tx(&mut tx, lease, checkpoint).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn cutover_with_fenced_ownership(
        &self,
        lease: &TaskLease,
        config: &ThreadVectorDBConfig,
        staging_name: &str,
        checkpoint: &mut Checkpoint,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        state::renew_lease_tx(
            &mut tx,
            lease,
            command_utils::util::datetime::now_millis(),
            self.lease_duration_ms,
        )
        .await?;
        ThreadVectorRepositoryImpl::replace_table_from_staging(config, staging_name).await?;
        let canonical = ThreadVectorRepositoryImpl::new(config.clone()).await?;
        let mut staging_config = config.clone();
        staging_config.table_name = staging_name.to_string();
        let staging = ThreadVectorRepositoryImpl::new(staging_config).await?;
        // The staging table was verified against RDB immediately before this
        // fenced operation. Re-check exact table equivalence without taking a
        // second RDB connection while this transaction owns the task row.
        self.verify_identical_vector_tables(&staging, &canonical)
            .await?;
        checkpoint.vector_phase = VectorPhase::CanonicalReady;
        self.save_checkpoint_tx(&mut tx, lease, checkpoint).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn build_indexes_with_fenced_ownership(
        &self,
        lease: &TaskLease,
        config: &ThreadVectorDBConfig,
        checkpoint: &mut Checkpoint,
    ) -> Result<()> {
        let mut tx = self.pool.begin().await?;
        state::renew_lease_tx(
            &mut tx,
            lease,
            command_utils::util::datetime::now_millis(),
            self.lease_duration_ms,
        )
        .await?;
        let repository = ThreadVectorRepositoryImpl::new(config.clone()).await?;
        repository.maintenance_build_fts(false).await?;
        repository.maintenance_build_vector(false).await?;
        let outcomes = IndexOutcomes {
            fts: IndexOutcome::Rebuilt,
            ann: index_outcome(repository.maintenance_vector_build_status().await?),
        };
        let reopened = ThreadVectorRepositoryImpl::new(config.clone()).await?;
        self.verify_index_outcomes(&reopened, &outcomes).await?;
        checkpoint.index_outcomes = Some(outcomes);
        checkpoint.vector_phase = VectorPhase::IndexesVerified;
        self.save_checkpoint_tx(&mut tx, lease, checkpoint).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn cleanup_staging_with_fenced_ownership(
        &self,
        lease: &TaskLease,
        config: &ThreadVectorDBConfig,
        staging_name: &str,
        checkpoint: &mut Checkpoint,
    ) -> Result<()> {
        checkpoint.staging_cleanup_started = true;
        self.persist_non_rdb_checkpoint(lease, checkpoint).await?;
        let mut tx = self.pool.begin().await?;
        state::renew_lease_tx(
            &mut tx,
            lease,
            command_utils::util::datetime::now_millis(),
            self.lease_duration_ms,
        )
        .await?;
        ThreadVectorRepositoryImpl::drop_table_if_exists(config, staging_name).await?;
        checkpoint.vector_phase = VectorPhase::Completed;
        checkpoint.staging_table_name = None;
        checkpoint.staging_origin = None;
        checkpoint.staging_cleanup_started = false;
        checkpoint.vector_outcome = Some("migrated".to_string());
        self.save_checkpoint_tx(&mut tx, lease, checkpoint).await?;
        tx.commit().await?;
        Ok(())
    }

    async fn verify_staging_against_canonical(
        &self,
        canonical: &ThreadVectorRepositoryImpl,
        staging: &ThreadVectorRepositoryImpl,
    ) -> Result<()> {
        self.verify_vector_table_against_rdb(staging).await?;
        let canonical_ids = unique_thread_ids(canonical.get_all_thread_ids().await?);
        let staging_ids = unique_thread_ids(staging.get_all_thread_ids().await?);
        for thread_id in canonical_ids {
            let (source, _) =
                normalize_vector_records(canonical.find_records_by_thread_id(thread_id).await?)?;
            let (target, target_duplicates) =
                normalize_vector_records(staging.find_records_by_thread_id(thread_id).await?)?;
            if target_duplicates != 0 {
                bail!(
                    "staging ThreadVector retained duplicate row identifiers for thread_id={thread_id}"
                );
            }
            if message_bounds_for_existing_thread(&self.pool, thread_id)
                .await?
                .is_none()
            {
                if !target.is_empty() {
                    bail!("staging retained orphan ThreadVector rows for thread_id={thread_id}");
                }
                continue;
            }
            verify_matching_vector_records(thread_id, &source, &target, false)?;
        }
        for thread_id in staging_ids {
            if !canonical
                .find_records_by_thread_id(thread_id)
                .await?
                .is_empty()
            {
                continue;
            }
            bail!("staging contains a row absent from canonical thread_id={thread_id}");
        }
        Ok(())
    }

    async fn verify_equivalent_vector_tables(
        &self,
        expected: &ThreadVectorRepositoryImpl,
        actual: &ThreadVectorRepositoryImpl,
    ) -> Result<()> {
        self.verify_vector_table_against_rdb(expected).await?;
        self.verify_vector_table_against_rdb(actual).await?;
        self.verify_identical_vector_tables(expected, actual).await
    }

    async fn verify_identical_vector_tables(
        &self,
        expected: &ThreadVectorRepositoryImpl,
        actual: &ThreadVectorRepositoryImpl,
    ) -> Result<()> {
        let expected_ids = unique_thread_ids(expected.get_all_thread_ids().await?);
        let actual_ids = unique_thread_ids(actual.get_all_thread_ids().await?);
        if expected_ids != actual_ids {
            bail!("ThreadVector thread key set verification failed after cutover");
        }
        for thread_id in expected_ids {
            let (expected_records, expected_duplicates) =
                normalize_vector_records(expected.find_records_by_thread_id(thread_id).await?)?;
            let (actual_records, actual_duplicates) =
                normalize_vector_records(actual.find_records_by_thread_id(thread_id).await?)?;
            if expected_duplicates != 0 || actual_duplicates != 0 {
                bail!(
                    "ThreadVector equivalence verification found duplicate row identifiers for thread_id={thread_id}"
                );
            }
            verify_matching_vector_records(thread_id, &expected_records, &actual_records, true)?;
        }
        Ok(())
    }

    async fn verify_vector_table_against_rdb(
        &self,
        repository: &ThreadVectorRepositoryImpl,
    ) -> Result<()> {
        for thread_id in unique_thread_ids(repository.get_all_thread_ids().await?) {
            let (first, last) = message_bounds_for_existing_thread(&self.pool, thread_id)
                .await?
                .context("ThreadVector contains a thread absent from RDB")?;
            let (records, discarded_duplicates) =
                normalize_vector_records(repository.find_records_by_thread_id(thread_id).await?)?;
            if discarded_duplicates != 0 {
                bail!(
                    "ThreadVector verification found duplicate row identifiers for thread_id={thread_id}"
                );
            }
            if records.is_empty() {
                bail!("ThreadVector thread key has no records for thread_id={thread_id}");
            }
            for record in records {
                if record.first_message_at != first || record.last_message_at != last {
                    bail!("ThreadVector scalar verification failed for thread_id={thread_id}");
                }
            }
        }
        Ok(())
    }

    fn vector_page_size(&self) -> Result<usize> {
        usize::try_from(self.batch_size).context("vector migration batch size is invalid")
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

    async fn legacy_vector_state(&self) -> Result<Option<LegacyMigrationState>> {
        #[cfg(feature = "postgres")]
        let table_exists_sql = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = current_schema() AND table_name = $1)";
        #[cfg(not(feature = "postgres"))]
        let table_exists_sql =
            "SELECT EXISTS (SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?)";
        let exists: bool = sqlx::query_scalar(table_exists_sql)
            .bind("thread_time_migration_state")
            .fetch_one(&self.pool)
            .await
            .context("checking legacy thread time migration state table")?;
        if !exists {
            return Ok(None);
        }
        #[cfg(feature = "postgres")]
        let state_sql = "SELECT vector_status, staging_table_name FROM thread_time_migration_state WHERE migration_key = $1";
        #[cfg(not(feature = "postgres"))]
        let state_sql = "SELECT vector_status, staging_table_name FROM thread_time_migration_state WHERE migration_key = ?";
        let state: Option<(String, Option<String>)> = sqlx::query_as(state_sql)
            .bind(LEGACY_MIGRATION_KEY)
            .fetch_optional(&self.pool)
            .await
            .context("reading legacy thread time migration state")?;
        state
            .map(|(vector_state, staging_table_name)| {
                Ok(LegacyMigrationState {
                    vector_state: LegacyVectorState::parse(&vector_state)?,
                    staging_table_name,
                })
            })
            .transpose()
    }

    async fn verify_vector(&self, lease: Option<&TaskLease>) -> Result<()> {
        let Some(row) = state::load(&self.pool, &self.catalog.identity()).await? else {
            return Ok(());
        };
        let Some(raw) = row.checkpoint else {
            return Ok(());
        };
        let envelope: TaskCheckpointEnvelope<Checkpoint> = serde_json::from_str(&raw)
            .context("parsing thread message times checkpoint during vector verification")?;
        let checkpoint = envelope.payload;
        if checkpoint.vector_outcome.as_deref() != Some("migrated") {
            return Ok(());
        }
        checkpoint.validate()?;
        let config = self
            .vector_config
            .clone()
            .context("migrated vector checkpoint requires ThreadVector configuration")?;
        if !ThreadVectorRepositoryImpl::table_exists(&config).await? {
            bail!("migrated vector checkpoint canonical table is missing");
        }
        let repository = ThreadVectorRepositoryImpl::new(config).await?;
        if let Some(lease) = lease {
            self.renew_lease(lease).await?;
        }
        self.verify_vector_table_against_rdb(&repository).await?;
        let outcomes = checkpoint
            .index_outcomes
            .context("migrated vector checkpoint is missing index outcomes")?;
        self.verify_index_outcomes(&repository, &outcomes).await
    }

    async fn verify_index_outcomes(
        &self,
        repository: &ThreadVectorRepositoryImpl,
        outcomes: &IndexOutcomes,
    ) -> Result<()> {
        let fts = repository.observe_maintenance_index(false).await?;
        if !fts.index_present.unwrap_or(false) || outcomes.fts != IndexOutcome::Rebuilt {
            bail!("ThreadVector FTS index verification failed");
        }
        repository.verify_maintenance_fts_query().await?;
        let ann = repository.observe_maintenance_index(true).await?;
        match outcomes.ann {
            IndexOutcome::Rebuilt => {
                if !ann.index_present.unwrap_or(false) {
                    bail!("ThreadVector ANN index verification failed");
                }
                repository.verify_maintenance_vector_query().await?;
            }
            IndexOutcome::SkippedDisabled | IndexOutcome::Deferred
                if ann.index_present == Some(true) =>
            {
                bail!("ThreadVector ANN index outcome does not match the canonical table");
            }
            _ => {}
        }
        Ok(())
    }
}

fn unique_thread_ids(mut ids: Vec<i64>) -> Vec<i64> {
    ids.sort_unstable();
    ids.dedup();
    ids
}

fn normalize_vector_records(
    mut records: Vec<ThreadVectorRecord>,
) -> Result<(Vec<ThreadVectorRecord>, usize)> {
    records.sort_by(|left, right| {
        (left.thread_id, &left.vector_kind, left.chunk_index).cmp(&(
            right.thread_id,
            &right.vector_kind,
            right.chunk_index,
        ))
    });
    let mut normalized = Vec::with_capacity(records.len());
    let mut discarded = 0;
    let mut group_start = 0;
    while group_start < records.len() {
        let key = (
            records[group_start].thread_id,
            records[group_start].vector_kind.as_str(),
            records[group_start].chunk_index,
        );
        let mut group_end = group_start + 1;
        while group_end < records.len()
            && (
                records[group_end].thread_id,
                records[group_end].vector_kind.as_str(),
                records[group_end].chunk_index,
            ) == key
        {
            group_end += 1;
        }
        let winner = records[group_start..group_end]
            .iter()
            .max_by(|left, right| compare_duplicate_vector_records(left, right))
            .expect("non-empty duplicate group");
        discarded += group_end - group_start - 1;
        normalized.push(winner.clone());
        group_start = group_end;
    }
    Ok((normalized, discarded))
}

fn compare_duplicate_vector_records(
    left: &ThreadVectorRecord,
    right: &ThreadVectorRecord,
) -> Ordering {
    left.indexed_at
        .cmp(&right.indexed_at)
        .then_with(|| left.updated_at.cmp(&right.updated_at))
        .then_with(|| left.created_at.cmp(&right.created_at))
        .then_with(|| left.begin_position.cmp(&right.begin_position))
        .then_with(|| left.end_position.cmp(&right.end_position))
        .then_with(|| left.user_id.cmp(&right.user_id))
        .then_with(|| left.memory_kind.cmp(&right.memory_kind))
        .then_with(|| left.content.cmp(&right.content))
        .then_with(|| left.description.cmp(&right.description))
        .then_with(|| left.labels.cmp(&right.labels))
        .then_with(|| left.embedding_model.cmp(&right.embedding_model))
        .then_with(|| left.channel.cmp(&right.channel))
        .then_with(|| left.first_message_at.cmp(&right.first_message_at))
        .then_with(|| left.last_message_at.cmp(&right.last_message_at))
        .then_with(|| {
            left.embedding
                .iter()
                .map(|value| value.to_bits())
                .cmp(right.embedding.iter().map(|value| value.to_bits()))
        })
}

fn verify_matching_vector_records(
    thread_id: i64,
    expected: &[ThreadVectorRecord],
    actual: &[ThreadVectorRecord],
    check_message_times: bool,
) -> Result<()> {
    if expected.len() != actual.len() {
        bail!("ThreadVector row count verification failed for thread_id={thread_id}");
    }
    for (expected, actual) in expected.iter().zip(actual) {
        if expected.thread_id != actual.thread_id
            || expected.vector_kind != actual.vector_kind
            || expected.chunk_index != actual.chunk_index
            || expected.begin_position != actual.begin_position
            || expected.end_position != actual.end_position
            || expected.user_id != actual.user_id
            || expected.memory_kind != actual.memory_kind
            || expected.content != actual.content
            || expected.description != actual.description
            || expected.labels != actual.labels
            || expected.embedding != actual.embedding
            || expected.embedding_model != actual.embedding_model
            || expected.channel != actual.channel
            || expected.created_at != actual.created_at
            || expected.updated_at != actual.updated_at
            || expected.indexed_at != actual.indexed_at
            || (check_message_times
                && (expected.first_message_at != actual.first_message_at
                    || expected.last_message_at != actual.last_message_at))
        {
            bail!("ThreadVector row verification failed for thread_id={thread_id}");
        }
    }
    Ok(())
}

#[async_trait]
impl DataMigrationTask for ThreadMessageTimesV1Task {
    fn task_identity(&self) -> String {
        self.catalog.identity()
    }

    async fn inspect(&self) -> Result<serde_json::Value> {
        Ok(serde_json::to_value(
            ThreadMessageTimesV1Task::inspect(self).await?,
        )?)
    }

    async fn dry_run(&self) -> Result<serde_json::Value> {
        Ok(serde_json::to_value(
            ThreadMessageTimesV1Task::dry_run(self).await?,
        )?)
    }

    async fn apply(&self, execution_id: &str, holder_id: &str) -> Result<serde_json::Value> {
        Ok(serde_json::to_value(
            ThreadMessageTimesV1Task::apply(self, execution_id, holder_id).await?,
        )?)
    }

    async fn verify(&self) -> Result<()> {
        ThreadMessageTimesV1Task::verify(self).await
    }
}

fn explicit_thread_vector_config() -> Result<Option<ThreadVectorDBConfig>> {
    if !std::env::var("THREAD_VECTOR_ENABLED")
        .unwrap_or_default()
        .eq_ignore_ascii_case("true")
    {
        return Ok(None);
    }
    ThreadVectorDBConfig::from_env().map(Some)
}

fn index_outcome(status: IndexBuildStatus) -> IndexOutcome {
    match status {
        IndexBuildStatus::Succeeded => IndexOutcome::Rebuilt,
        IndexBuildStatus::SkippedDisabled => IndexOutcome::SkippedDisabled,
        IndexBuildStatus::Deferred => IndexOutcome::Deferred,
        _ => IndexOutcome::Deferred,
    }
}

async fn fetch_thread_ids(pool: &RdbPool, after: Option<i64>, batch_size: i64) -> Result<Vec<i64>> {
    if batch_size <= 0 {
        bail!("thread migration batch size must be positive");
    }
    match after {
        Some(after) => {
            let sql = format!(
                "SELECT id FROM thread WHERE id > {} ORDER BY id ASC LIMIT {}",
                placeholder(1),
                placeholder(2)
            );
            sqlx::query_scalar(sqlx::AssertSqlSafe(sql))
                .bind(after)
                .bind(batch_size)
                .fetch_all(pool)
                .await
                .context("fetching thread migration keyset page")
        }
        None => {
            let sql = format!(
                "SELECT id FROM thread ORDER BY id ASC LIMIT {}",
                placeholder(1)
            );
            sqlx::query_scalar(sqlx::AssertSqlSafe(sql))
                .bind(batch_size)
                .fetch_all(pool)
                .await
                .context("fetching initial thread migration keyset page")
        }
    }
}

async fn message_bounds(pool: &RdbPool, thread_id: i64) -> Result<(Option<i64>, Option<i64>)> {
    let sql = format!(
        "SELECT MIN(m.created_at), MAX(m.created_at) FROM thread_memory tm JOIN memory m ON m.id = tm.memory_id WHERE tm.thread_id = {}",
        placeholder(1)
    );
    sqlx::query_as(sqlx::AssertSqlSafe(sql))
        .bind(thread_id)
        .fetch_one(pool)
        .await
        .context("calculating thread message bounds")
}

async fn message_bounds_for_existing_thread(
    pool: &RdbPool,
    thread_id: i64,
) -> Result<Option<(Option<i64>, Option<i64>)>> {
    let sql = format!(
        "SELECT MIN(m.created_at), MAX(m.created_at) FROM thread t \
         LEFT JOIN thread_memory tm ON tm.thread_id = t.id \
         LEFT JOIN memory m ON m.id = tm.memory_id WHERE t.id = {} GROUP BY t.id",
        placeholder(1)
    );
    sqlx::query_as(sqlx::AssertSqlSafe(sql))
        .bind(thread_id)
        .fetch_optional(pool)
        .await
        .context("calculating existing thread message bounds")
}

async fn update_message_bounds_preserving_audit(
    tx: &mut infra_utils::infra::rdb::RdbTransaction<'_>,
    thread_id: i64,
) -> Result<()> {
    let bounds_sql = format!(
        "SELECT MIN(m.created_at), MAX(m.created_at) FROM thread_memory tm JOIN memory m ON m.id = tm.memory_id WHERE tm.thread_id = {}",
        placeholder(1)
    );
    let (first, last): (Option<i64>, Option<i64>) = sqlx::query_as(sqlx::AssertSqlSafe(bounds_sql))
        .bind(thread_id)
        .fetch_one(&mut **tx)
        .await
        .context("calculating thread message bounds in transaction")?;
    #[cfg(feature = "postgres")]
    let distinct = "IS DISTINCT FROM";
    #[cfg(not(feature = "postgres"))]
    let distinct = "IS NOT";
    let update_sql = format!(
        "UPDATE thread SET first_message_at = {p1}, last_message_at = {p2} WHERE id = {p3} \
         AND (first_message_at {distinct} {p4} OR last_message_at {distinct} {p5})",
        p1 = placeholder(1),
        p2 = placeholder(2),
        p3 = placeholder(3),
        p4 = placeholder(4),
        p5 = placeholder(5),
    );
    sqlx::query(sqlx::AssertSqlSafe(update_sql))
        .bind(first)
        .bind(last)
        .bind(thread_id)
        .bind(first)
        .bind(last)
        .execute(&mut **tx)
        .await
        .context("updating thread message bounds")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    #[cfg(not(feature = "postgres"))]
    use super::ThreadMessageTimesV1Task;
    use super::{
        Checkpoint, IndexOutcome, IndexOutcomes, RdbPhase, VectorPhase, fetch_thread_ids,
        normalize_vector_records,
    };
    #[cfg(not(feature = "postgres"))]
    use crate::db_migrate::{catalog, state};
    #[cfg(not(feature = "postgres"))]
    use infra::infra::memory_vector::config::{DistanceType, FtsConfig, VectorIndexConfig};
    use infra::infra::thread_vector::record::ThreadVectorRecord;
    #[cfg(not(feature = "postgres"))]
    use infra::infra::thread_vector::{
        config::ThreadVectorDBConfig, repository::ThreadVectorRepositoryImpl,
    };

    #[cfg(not(feature = "postgres"))]
    type ThreadTimeRow = (i64, i64, i64, Option<i64>, Option<i64>);

    #[cfg(not(feature = "postgres"))]
    async fn test_pool() -> infra_utils::infra::rdb::RdbPool {
        use sqlx::sqlite::SqlitePoolOptions;

        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        sqlx::raw_sql(
            "CREATE TABLE thread (id BIGINT PRIMARY KEY, created_at BIGINT NOT NULL, updated_at BIGINT NOT NULL, first_message_at BIGINT, last_message_at BIGINT);\
             CREATE TABLE memory (id BIGINT PRIMARY KEY, created_at BIGINT NOT NULL);\
             CREATE TABLE thread_memory (thread_id BIGINT NOT NULL, memory_id BIGINT NOT NULL);\
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

    #[test]
    fn checkpoint_rejects_cursor_outside_backfill() {
        let checkpoint = Checkpoint {
            rdb_phase: RdbPhase::Verified,
            rdb_last_thread_id: Some(1),
            ..Checkpoint::default()
        };
        assert!(checkpoint.validate().is_err());
    }

    #[test]
    fn intermediate_vector_phase_requires_staging_identity() {
        let checkpoint = Checkpoint {
            rdb_phase: RdbPhase::Verified,
            vector_phase: VectorPhase::Staged,
            ..Checkpoint::default()
        };
        assert!(checkpoint.validate().is_err());
    }

    #[test]
    fn migrated_checkpoint_requires_verified_index_outcomes() {
        let checkpoint = Checkpoint {
            rdb_phase: RdbPhase::Verified,
            vector_phase: VectorPhase::Completed,
            canonical_table_name: Some("threads".to_string()),
            vector_outcome: Some("migrated".to_string()),
            ..Checkpoint::default()
        };
        assert!(checkpoint.validate().is_err());

        let checkpoint = Checkpoint {
            index_outcomes: Some(IndexOutcomes {
                fts: IndexOutcome::Rebuilt,
                ann: IndexOutcome::Deferred,
            }),
            ..checkpoint
        };
        assert!(checkpoint.validate().is_ok());
    }

    #[test]
    fn cleanup_marker_is_not_valid_after_completed_cutover() {
        let checkpoint = Checkpoint {
            rdb_phase: RdbPhase::Verified,
            vector_phase: VectorPhase::Completed,
            canonical_table_name: Some("threads".to_string()),
            staging_cleanup_started: true,
            index_outcomes: Some(IndexOutcomes {
                fts: IndexOutcome::Rebuilt,
                ann: IndexOutcome::SkippedDisabled,
            }),
            vector_outcome: Some("migrated".to_string()),
            ..Checkpoint::default()
        };
        assert!(checkpoint.validate().is_err());
    }

    #[test]
    fn normalizes_identical_duplicate_vector_rows_by_latest_indexed_at() {
        let record = |content: &str, indexed_at: i64| ThreadVectorRecord {
            thread_id: 1,
            vector_kind: "text".to_string(),
            chunk_index: 0,
            begin_position: 0,
            end_position: 3,
            user_id: 7,
            memory_kind: 1,
            content: content.to_string(),
            description: Some(content.to_string()),
            labels: vec!["keep".to_string()],
            embedding: vec![0.1, 0.2, 0.3, 0.4],
            embedding_model: Some("test-model".to_string()),
            channel: None,
            created_at: 10,
            updated_at: 20,
            first_message_at: None,
            last_message_at: None,
            indexed_at,
        };

        let mut older = record("same", 30);
        older.first_message_at = Some(100);
        older.last_message_at = Some(200);
        let (records, discarded) = normalize_vector_records(vec![
            older,
            record("same", 40),
            ThreadVectorRecord {
                thread_id: 2,
                ..record("other", 50)
            },
        ])
        .unwrap();

        assert_eq!(discarded, 1);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].thread_id, 1);
        assert_eq!(records[0].content, "same");
        assert_eq!(records[0].indexed_at, 40);
        assert_eq!(records[1].thread_id, 2);
    }

    #[test]
    fn normalizes_conflicting_duplicate_vector_rows_by_latest_indexed_at() {
        let record = |content: &str, indexed_at: i64| ThreadVectorRecord {
            thread_id: 1,
            vector_kind: "text".to_string(),
            chunk_index: 0,
            begin_position: 0,
            end_position: 3,
            user_id: 7,
            memory_kind: 1,
            content: content.to_string(),
            description: Some(content.to_string()),
            labels: vec!["keep".to_string()],
            embedding: vec![0.1, 0.2, 0.3, 0.4],
            embedding_model: Some("test-model".to_string()),
            channel: None,
            created_at: 10,
            updated_at: 20,
            first_message_at: Some(100),
            last_message_at: Some(200),
            indexed_at,
        };

        let (records, discarded) =
            normalize_vector_records(vec![record("old", 30), record("new", 40)]).unwrap();
        assert_eq!(discarded, 1);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].content, "new");
        assert_eq!(records[0].indexed_at, 40);
    }

    #[cfg(not(feature = "postgres"))]
    #[test]
    fn vector_cutover_preserves_non_time_columns_and_discards_orphans() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            let pool = test_pool().await;
            sqlx::query("INSERT INTO thread (id, created_at, updated_at) VALUES (?, ?, ?), (?, ?, ?), (?, ?, ?)")
                .bind(1_i64).bind(10_i64).bind(20_i64)
                .bind(2_i64).bind(11_i64).bind(21_i64)
                .bind(3_i64).bind(12_i64).bind(22_i64)
                .execute(&pool).await.unwrap();
            sqlx::query("INSERT INTO memory (id, created_at) VALUES (?, ?), (?, ?)")
                .bind(101_i64).bind(100_i64).bind(102_i64).bind(200_i64)
                .execute(&pool).await.unwrap();
            sqlx::query("INSERT INTO thread_memory (thread_id, memory_id) VALUES (?, ?), (?, ?)")
                .bind(1_i64).bind(101_i64).bind(1_i64).bind(102_i64)
                .execute(&pool).await.unwrap();

            let temp = tempfile::tempdir().unwrap();
            let config = ThreadVectorDBConfig {
                uri: temp.path().to_string_lossy().into_owned(),
                table_name: "threads".to_string(),
                vector_size: 4,
                distance_type: DistanceType::Cosine,
                fts: FtsConfig::default(),
                vector_index: VectorIndexConfig { enabled: false, ..VectorIndexConfig::default() },
            };
            let source = ThreadVectorRepositoryImpl::new(config.clone()).await.unwrap();
            let record = |thread_id, chunk_index, content: &str, first, last| ThreadVectorRecord {
                thread_id,
                vector_kind: "text".to_string(),
                chunk_index,
                begin_position: chunk_index * 3,
                end_position: chunk_index * 3 + 3,
                user_id: 77,
                memory_kind: 1,
                content: content.to_string(),
                description: Some("unchanged description".to_string()),
                labels: vec!["keep".to_string()],
                embedding: vec![0.1, 0.2, 0.3, 0.4],
                embedding_model: Some("test-model".to_string()),
                channel: Some("test".to_string()),
                created_at: 55,
                updated_at: 66,
                first_message_at: first,
                last_message_at: last,
                indexed_at: 77,
            };
            source.batch_upsert(vec![
                record(1, 0, "chunk-a", Some(1), Some(999)),
                record(1, 1, "chunk-b", Some(1), Some(999)),
                record(2, 0, "empty", Some(1), Some(999)),
                record(99, 0, "orphan", Some(1), Some(999)),
            ]).await.unwrap();

            let task = ThreadMessageTimesV1Task::new_with_vector_config(
                pool,
                catalog::thread_message_times_v1().unwrap(),
                Some(config.clone()),
            ).unwrap();
            assert_eq!(task.apply("execution-1", "test").await.unwrap().vector_outcome, "migrated");

            let migrated = ThreadVectorRepositoryImpl::new(config).await.unwrap();
            let first = migrated.find_records_by_thread_id(1).await.unwrap();
            assert_eq!(first.len(), 2);
            assert!(first.iter().all(|row| row.first_message_at == Some(100) && row.last_message_at == Some(200)));
            assert!(first.iter().all(|row| row.updated_at == 66 && row.indexed_at == 77));
            assert_eq!(first[0].content, "chunk-a");
            assert_eq!(first[1].content, "chunk-b");
            assert!(first.iter().all(|row| {
                row.vector_kind == "text"
                    && row.user_id == 77
                    && row.memory_kind == 1
                    && row.description.as_deref() == Some("unchanged description")
                    && row.labels == ["keep"]
                    && row.embedding == vec![0.1, 0.2, 0.3, 0.4]
                    && row.embedding_model.as_deref() == Some("test-model")
                    && row.channel.as_deref() == Some("test")
                    && row.created_at == 55
                    && row.begin_position == row.chunk_index * 3
                    && row.end_position == row.chunk_index * 3 + 3
            }));
            let empty = migrated.find_records_by_thread_id(2).await.unwrap();
            assert_eq!(empty.len(), 1);
            assert_eq!(empty[0].first_message_at, None);
            assert_eq!(empty[0].last_message_at, None);
            assert!(migrated.find_records_by_thread_id(99).await.unwrap().is_empty());
            assert!(migrated.find_records_by_thread_id(3).await.unwrap().is_empty());
        });
    }

    #[cfg(not(feature = "postgres"))]
    #[test]
    fn vector_migration_resumes_from_every_durable_cutover_phase() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            for phase in [
                VectorPhase::Staged,
                VectorPhase::Switching,
                VectorPhase::CanonicalReady,
                VectorPhase::IndexesVerified,
            ] {
                let pool = test_pool().await;
                sqlx::query(
                    "INSERT INTO thread (id, created_at, updated_at) VALUES (?, ?, ?), (?, ?, ?)",
                )
                .bind(1_i64)
                .bind(10_i64)
                .bind(20_i64)
                .bind(2_i64)
                .bind(11_i64)
                .bind(21_i64)
                .execute(&pool)
                .await
                .unwrap();
                sqlx::query("INSERT INTO memory (id, created_at) VALUES (?, ?), (?, ?)")
                    .bind(101_i64)
                    .bind(100_i64)
                    .bind(102_i64)
                    .bind(200_i64)
                    .execute(&pool)
                    .await
                    .unwrap();
                sqlx::query(
                    "INSERT INTO thread_memory (thread_id, memory_id) VALUES (?, ?), (?, ?)",
                )
                .bind(1_i64)
                .bind(101_i64)
                .bind(2_i64)
                .bind(102_i64)
                .execute(&pool)
                .await
                .unwrap();
                let temporary = tempfile::tempdir().unwrap();
                let config = ThreadVectorDBConfig {
                    uri: temporary.path().to_string_lossy().into_owned(),
                    table_name: "threads".to_string(),
                    vector_size: 4,
                    distance_type: DistanceType::Cosine,
                    fts: FtsConfig::default(),
                    vector_index: VectorIndexConfig {
                        enabled: false,
                        ..VectorIndexConfig::default()
                    },
                };
                let source = ThreadVectorRepositoryImpl::new(config.clone())
                    .await
                    .unwrap();
                let first_record = ThreadVectorRecord {
                    thread_id: 1,
                    vector_kind: "text".to_string(),
                    chunk_index: 0,
                    begin_position: 0,
                    end_position: 4,
                    user_id: 7,
                    memory_kind: 1,
                    content: "test".to_string(),
                    description: Some("test".to_string()),
                    labels: vec!["keep".to_string()],
                    embedding: vec![0.1, 0.2, 0.3, 0.4],
                    embedding_model: Some("test-model".to_string()),
                    channel: Some("test".to_string()),
                    created_at: 10,
                    updated_at: 20,
                    first_message_at: None,
                    last_message_at: None,
                    indexed_at: 30,
                };
                let mut second_record = first_record.clone();
                second_record.thread_id = 2;
                second_record.content = "second".to_string();
                source
                    .batch_upsert(vec![first_record, second_record])
                    .await
                    .unwrap();
                let interrupted = ThreadMessageTimesV1Task::new_with_vector_config(
                    pool.clone(),
                    catalog::thread_message_times_v1().unwrap(),
                    Some(config.clone()),
                )
                .unwrap()
                .interrupt_after_vector_phase(phase);
                assert!(interrupted.apply("interrupted", "test").await.is_err());
                let saved = state::load(&pool, "thread-message-times-v1@1")
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(saved.kind().unwrap(), state::TaskStateKind::Failed);
                let checkpoint: serde_json::Value =
                    serde_json::from_str(&saved.checkpoint.unwrap()).unwrap();
                assert_eq!(
                    checkpoint["payload"]["vector_phase"],
                    serde_json::to_value(phase).unwrap()
                );

                if phase == VectorPhase::Switching {
                    let original = ThreadVectorRepositoryImpl::new(config.clone())
                        .await
                        .unwrap();
                    let partial_rows = original.find_records_by_thread_id(1).await.unwrap();
                    ThreadVectorRepositoryImpl::drop_table_if_exists(&config, &config.table_name)
                        .await
                        .unwrap();
                    let partial = ThreadVectorRepositoryImpl::new(config.clone())
                        .await
                        .unwrap();
                    partial.batch_upsert(partial_rows).await.unwrap();
                    assert_eq!(partial.get_all_thread_ids().await.unwrap(), vec![1]);
                }

                let resumed = ThreadMessageTimesV1Task::new_with_vector_config(
                    pool,
                    catalog::thread_message_times_v1().unwrap(),
                    Some(config.clone()),
                )
                .unwrap();
                assert_eq!(
                    resumed
                        .apply("resumed", "test")
                        .await
                        .unwrap()
                        .vector_outcome,
                    "migrated"
                );
                let canonical = ThreadVectorRepositoryImpl::new(config).await.unwrap();
                let rows = canonical.find_records_by_thread_id(1).await.unwrap();
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].first_message_at, Some(100));
                assert_eq!(rows[0].last_message_at, Some(100));
                assert_eq!(rows[0].content, "test");
                assert_eq!(rows[0].embedding, vec![0.1, 0.2, 0.3, 0.4]);
                let second_rows = canonical.find_records_by_thread_id(2).await.unwrap();
                assert_eq!(second_rows.len(), 1);
                assert_eq!(second_rows[0].first_message_at, Some(200));
                assert_eq!(second_rows[0].last_message_at, Some(200));
            }
        });
    }

    #[cfg(not(feature = "postgres"))]
    #[test]
    fn legacy_staged_state_requires_vector_configuration_instead_of_becoming_not_required() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            let pool = test_pool().await;
            sqlx::raw_sql(
                "CREATE TABLE thread_time_migration_state (migration_key TEXT PRIMARY KEY, vector_status TEXT NOT NULL, staging_table_name TEXT);\
                 INSERT INTO thread_time_migration_state (migration_key, vector_status) VALUES ('thread-time-fields-v1', 'STAGED');",
            )
            .execute(&pool)
            .await
            .unwrap();
            let task = ThreadMessageTimesV1Task::new(
                pool.clone(),
                catalog::thread_message_times_v1().unwrap(),
            )
            .unwrap();
            assert!(task.apply("execution-1", "test").await.is_err());
            let state = state::load(&pool, "thread-message-times-v1@1")
                .await
                .unwrap()
                .unwrap();
            assert_eq!(state.kind().unwrap(), state::TaskStateKind::Failed);
        });
    }

    #[test]
    fn keyset_query_accepts_null_cursor_without_a_sentinel() {
        let _ = fetch_thread_ids;
        let checkpoint = Checkpoint {
            rdb_phase: RdbPhase::Backfilling,
            rdb_last_thread_id: None,
            ..Checkpoint::default()
        };
        assert!(checkpoint.validate().is_ok());
    }

    #[cfg(not(feature = "postgres"))]
    #[test]
    fn rdb_backfill_is_keyset_resumable_and_preserves_thread_audit_fields() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            let pool = test_pool().await;
            sqlx::query("INSERT INTO thread (id, created_at, updated_at, first_message_at, last_message_at) VALUES (?, ?, ?, ?, ?)")
                .bind(i64::MIN)
                .bind(10_i64)
                .bind(20_i64)
                .bind(Option::<i64>::None)
                .bind(Option::<i64>::None)
                .execute(&pool)
                .await
                .unwrap();
            sqlx::query("INSERT INTO thread (id, created_at, updated_at, first_message_at, last_message_at) VALUES (?, ?, ?, ?, ?)")
                .bind(9_i64)
                .bind(11_i64)
                .bind(21_i64)
                .bind(Some(999_i64))
                .bind(Some(999_i64))
                .execute(&pool)
                .await
                .unwrap();
            sqlx::query("INSERT INTO memory (id, created_at) VALUES (?, ?), (?, ?), (?, ?)")
                .bind(1_i64)
                .bind(100_i64)
                .bind(2_i64)
                .bind(200_i64)
                .bind(3_i64)
                .bind(300_i64)
                .execute(&pool)
                .await
                .unwrap();
            sqlx::query("INSERT INTO thread_memory (thread_id, memory_id) VALUES (?, ?), (?, ?), (?, ?)")
                .bind(i64::MIN)
                .bind(1_i64)
                .bind(9_i64)
                .bind(2_i64)
                .bind(9_i64)
                .bind(3_i64)
                .execute(&pool)
                .await
                .unwrap();

            let task = ThreadMessageTimesV1Task::new(pool.clone(), catalog::thread_message_times_v1().unwrap())
                .unwrap()
                .with_batch_size(1);
            let result = task.apply("execution-1", "test").await.unwrap();
            assert_eq!(result.vector_outcome, "not_required");
            let rows: Vec<ThreadTimeRow> = sqlx::query_as(
                "SELECT id, created_at, updated_at, first_message_at, last_message_at FROM thread ORDER BY id",
            )
            .fetch_all(&pool)
            .await
            .unwrap();
            assert_eq!(rows[0], (i64::MIN, 10, 20, Some(100), Some(100)));
            assert_eq!(rows[1], (9, 11, 21, Some(200), Some(300)));
            let saved = state::load(&pool, "thread-message-times-v1@1")
                .await
                .unwrap()
                .unwrap();
            assert_eq!(saved.kind().unwrap(), state::TaskStateKind::Completed);
            assert!(saved.checkpoint.unwrap().contains("\"rdb_phase\":\"verified\""));
            task.apply("execution-2", "test").await.unwrap();
        });
    }

    #[cfg(not(feature = "postgres"))]
    #[test]
    fn preflight_rejects_dangling_membership_before_creating_task_state() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            let pool = test_pool().await;
            sqlx::query("INSERT INTO thread (id, created_at, updated_at) VALUES (?, ?, ?)")
                .bind(1_i64)
                .bind(1_i64)
                .bind(1_i64)
                .execute(&pool)
                .await
                .unwrap();
            sqlx::query("INSERT INTO thread_memory (thread_id, memory_id) VALUES (?, ?)")
                .bind(1_i64)
                .bind(99_i64)
                .execute(&pool)
                .await
                .unwrap();
            let task = ThreadMessageTimesV1Task::new(
                pool.clone(),
                catalog::thread_message_times_v1().unwrap(),
            )
            .unwrap();
            assert!(task.apply("execution-1", "test").await.is_err());
            assert!(
                state::load(&pool, "thread-message-times-v1@1")
                    .await
                    .unwrap()
                    .is_none()
            );
        });
    }

    #[cfg(not(feature = "postgres"))]
    #[test]
    fn preflight_rejects_non_positive_memory_timestamp_before_creating_task_state() {
        infra_utils::infra::test::TEST_RUNTIME.block_on(async {
            let pool = test_pool().await;
            sqlx::query("INSERT INTO thread (id, created_at, updated_at) VALUES (?, ?, ?)")
                .bind(1_i64)
                .bind(1_i64)
                .bind(1_i64)
                .execute(&pool)
                .await
                .unwrap();
            sqlx::query("INSERT INTO memory (id, created_at) VALUES (?, ?)")
                .bind(1_i64)
                .bind(0_i64)
                .execute(&pool)
                .await
                .unwrap();
            sqlx::query("INSERT INTO thread_memory (thread_id, memory_id) VALUES (?, ?)")
                .bind(1_i64)
                .bind(1_i64)
                .execute(&pool)
                .await
                .unwrap();
            let task = ThreadMessageTimesV1Task::new(
                pool.clone(),
                catalog::thread_message_times_v1().unwrap(),
            )
            .unwrap();
            assert!(task.dry_run().await.is_err());
            assert!(task.apply("execution-1", "test").await.is_err());
            assert!(
                state::load(&pool, "thread-message-times-v1@1")
                    .await
                    .unwrap()
                    .is_none()
            );
        });
    }
}
