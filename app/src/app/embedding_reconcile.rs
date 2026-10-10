//! Reconciliation of the vector tables with the RDB (spec §3.6 "照合"):
//! dispatch the targets that are missing, stale, or unverified (failed
//! ones on request, everything on `force`), delete orphans, and request
//! captions for image memories without a body.
//!
//! The rebuild task of a pending embedding migration ([`rebuild`]) runs
//! the same engine, keyed by the migration attempt.

pub mod rebuild;

use anyhow::Result;
use infra::infra::embedding_dispatch::{EmbeddingDispatch as _, ImageSearchMode};
use infra::infra::embedding_index::scan::{ScanConfig, ScanItem, ScanTable, Scanner};
use infra::infra::embedding_index::{EmbeddingIndex, TableLabel, TargetState};
use infra::infra::embedding_space::SpaceId;
use infra::infra::media_object::rdb::MediaObjectRepositoryImpl;
use infra::infra::memory::rdb::{MemoryRepository as _, MemoryRepositoryImpl};
use infra::infra::memory_vector::dispatcher::EmbeddingJobDispatcher;
use infra::infra::memory_vector::repository::MemoryVectorRepositoryImpl;
use infra::infra::reflection_intent_dispatch::ReflectionIntentDispatcher;
use infra::infra::reflection_intent_vector::repository::ReflectionIntentVectorRepository;
use infra::infra::thread::rdb::{ThreadRepository as _, ThreadRepositoryImpl};
use infra::infra::thread_vector::dispatcher::ThreadEmbeddingJobDispatcher;
use infra::infra::thread_vector::repository::ThreadVectorRepositoryImpl;
use infra_utils::infra::rdb::RdbPool;
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::sync::Mutex;

/// What a reconciliation re-dispatches beyond missing, stale, and
/// unverified targets.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReconcileOptions {
    /// Also dispatch failed targets.
    pub retry_failed: bool,
    /// Also dispatch complete targets (repairs results that are
    /// classified complete but are not, spec §3.1).
    pub force: Force,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum Force {
    #[default]
    None,
    All,
    /// Only these entities of one table.
    Entities {
        table: TableLabel,
        ids: BTreeSet<i64>,
    },
}

impl Force {
    fn covers(&self, table: TableLabel, entity_id: i64) -> bool {
        match self {
            Self::None => false,
            Self::All => true,
            Self::Entities { table: t, ids } => *t == table && ids.contains(&entity_id),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct KindProgress {
    /// Dispatches or deletions that failed for this item only.
    pub failed_to_dispatch: u64,
    pub missing: u64,
    pub stale: u64,
    pub unverified: u64,
    pub failed: u64,
    pub orphan: u64,
    pub dispatched: u64,
    pub complete: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStatus {
    Running,
    Completed,
    Failed,
}

#[derive(Debug, Clone)]
pub struct Progress {
    pub task_id: String,
    pub status: TaskStatus,
    /// Keyed by `memory_text`, `memory_media`, `thread`, `reflection_intent`.
    pub kinds: Vec<(&'static str, KindProgress)>,
    pub captions_requested: u64,
    pub error: Option<String>,
}

/// Everything a reconciliation reads from and dispatches to.
pub struct ReconcileDeps {
    pub pool: &'static RdbPool,
    pub memory_repo: MemoryRepositoryImpl,
    pub media_repo: MediaObjectRepositoryImpl,
    pub thread_repo: ThreadRepositoryImpl,
    pub memory_vector: Option<MemoryVectorRepositoryImpl>,
    pub thread_vector: Option<ThreadVectorRepositoryImpl>,
    pub intent_vector: Option<ReflectionIntentVectorRepository>,
    pub memory_dispatcher: Option<Arc<EmbeddingJobDispatcher>>,
    pub thread_dispatcher: Option<Arc<ThreadEmbeddingJobDispatcher>>,
    pub intent_dispatcher: Option<Arc<ReflectionIntentDispatcher>>,
    pub image_search_mode: ImageSearchMode,
    pub max_content_len: usize,
    pub page_size: i64,
}

struct Task {
    /// The rebuild attempt this task serves; `None` for a reconciliation.
    attempt_id: Option<String>,
    options: ReconcileOptions,
    progress: Mutex<Progress>,
    /// When dispatching ended, for the stall check of rebuilds.
    finished_at: Mutex<Option<std::time::Instant>>,
}

pub struct EmbeddingReconciler {
    deps: Arc<ReconcileDeps>,
    tasks: Mutex<HashMap<String, Arc<Task>>>,
    rebuild_watch: tokio::sync::Mutex<rebuild::Watch>,
    rebuild_timing: rebuild::Timing,
}

/// Why a reconciliation cannot start.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartError {
    /// A rebuild is pending; the rebuild task reconciles instead.
    RebuildPending,
    /// This process serves no embedding space.
    NoSpace,
}

impl EmbeddingReconciler {
    /// The rebuild stall threshold comes from the environment
    /// ([`rebuild::STALL_ENV`]).
    pub fn new(deps: ReconcileDeps) -> Arc<Self> {
        Self::with_rebuild_timing(deps, rebuild::Timing::from_env())
    }

    pub fn with_rebuild_timing(deps: ReconcileDeps, timing: rebuild::Timing) -> Arc<Self> {
        Arc::new(Self {
            deps: Arc::new(deps),
            tasks: Mutex::new(HashMap::new()),
            rebuild_watch: tokio::sync::Mutex::new(rebuild::Watch::default()),
            rebuild_timing: timing,
        })
    }

    /// Start a reconciliation, or return the running one (a resent start
    /// creates no second task).
    pub fn start(self: &Arc<Self>, options: ReconcileOptions) -> Result<String, StartError> {
        if infra::infra::embedding_space::token::rebuilding_attempt().is_some() {
            return Err(StartError::RebuildPending);
        }
        let space =
            infra::infra::embedding_space::workers::current_space().ok_or(StartError::NoSpace)?;
        Ok(self.start_task(None, options, space))
    }

    /// Spawn a task unless one is running, whose ID is then returned.
    fn start_task(
        self: &Arc<Self>,
        attempt_id: Option<String>,
        options: ReconcileOptions,
        space: SpaceId,
    ) -> String {
        let mut tasks = self.tasks.lock().unwrap_or_else(|e| e.into_inner());
        // One task at a time per instance: a second scan while the first
        // one's jobs are queued would dispatch them again.
        if let Some((id, _)) = tasks.iter().find(|(_, t)| {
            t.progress.lock().unwrap_or_else(|e| e.into_inner()).status == TaskStatus::Running
        }) {
            return id.clone();
        }
        let task_id = infra::infra::embedding_space::storage::new_identifier();
        let task = Arc::new(Task {
            attempt_id,
            finished_at: Mutex::new(None),
            options,
            progress: Mutex::new(Progress {
                task_id: task_id.clone(),
                status: TaskStatus::Running,
                kinds: infra::infra::embedding_index::ReportKind::ALL
                    .iter()
                    .map(|k| (k.as_str(), KindProgress::default()))
                    .collect(),
                captions_requested: 0,
                error: None,
            }),
        });
        tasks.insert(task_id.clone(), task.clone());
        let deps = self.deps.clone();
        let worker_task = task.clone();
        let worker = tokio::spawn(async move { run(&deps, &worker_task, space).await });
        tokio::spawn(async move {
            // A panic in the scan or a dispatch must not leave the task
            // `Running`, which would block every later start.
            let result = worker
                .await
                .unwrap_or_else(|e| Err(anyhow::anyhow!("embedding task panicked: {e}")));
            *task.finished_at.lock().unwrap_or_else(|e| e.into_inner()) =
                Some(std::time::Instant::now());
            let mut p = task.progress.lock().unwrap_or_else(|e| e.into_inner());
            match result {
                Ok(()) => p.status = TaskStatus::Completed,
                Err(e) => {
                    tracing::warn!("embedding reconciliation failed: {e:#}");
                    p.status = TaskStatus::Failed;
                    p.error = Some(format!("{e:#}"));
                }
            }
        });
        task_id
    }

    pub fn progress(&self, task_id: &str) -> Option<Progress> {
        self.tasks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(task_id)
            .map(|t| t.progress.lock().unwrap_or_else(|e| e.into_inner()).clone())
    }
}

/// Tables refreshed to their latest version, so rows written by other
/// instances are seen.
async fn scan_tables(deps: &ReconcileDeps) -> Result<Vec<ScanTable>> {
    let tables = table_handles(deps);
    for t in &tables {
        if let Some(table) = &t.table {
            table.checkout_latest().await?;
        }
    }
    Ok(tables)
}

fn table_handles(deps: &ReconcileDeps) -> Vec<ScanTable> {
    let mut out = Vec::new();
    if let Some(r) = &deps.memory_vector {
        out.push(ScanTable {
            label: TableLabel::Memory,
            table: Some(r.table_handle()),
            index: r.embedding_index().cloned(),
        });
    }
    if let Some(r) = &deps.intent_vector {
        out.push(ScanTable {
            label: TableLabel::ReflectionIntent,
            table: Some(r.table_handle()),
            index: r.embedding_index().cloned(),
        });
    }
    if let Some(r) = &deps.thread_vector {
        out.push(ScanTable {
            label: TableLabel::Thread,
            table: Some(r.table_handle()),
            index: r.embedding_index().cloned(),
        });
    }
    out
}

async fn run(deps: &ReconcileDeps, task: &Task, space: SpaceId) -> Result<()> {
    let mut scanner = Scanner::new(
        deps.pool,
        &deps.media_repo,
        ScanConfig {
            space,
            image_search_mode: deps.image_search_mode,
            max_content_len: deps.max_content_len,
            page_size: deps.page_size,
        },
        scan_tables(deps).await?,
    );
    while let Some(page) = scanner.next_page().await? {
        for item in page {
            let (table, kind) = match &item {
                ScanItem::Target {
                    table, vector_kind, ..
                } => (*table, vector_kind.to_string()),
                ScanItem::Orphan {
                    table, vector_kind, ..
                } => (*table, vector_kind.clone()),
                ScanItem::CaptionMissing { .. } => (TableLabel::Memory, "caption".to_string()),
            };
            match handle(deps, task, item).await {
                Ok(()) => {}
                // Workers not registered: nothing can be dispatched, stop.
                Err(e) if e.is::<Unregistered>() => return Err(e),
                Err(e) => {
                    tracing::warn!("embedding reconcile item failed: {e:#}");
                    bump(task, table, &kind, |k| k.failed_to_dispatch += 1);
                }
            }
        }
    }
    Ok(())
}

/// Dispatch is impossible until the workers are registered.
#[derive(Debug)]
struct Unregistered(String);

impl std::fmt::Display for Unregistered {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "embedding workers are not registered: {}", self.0)
    }
}

impl std::error::Error for Unregistered {}

fn dispatch_error(e: infra::infra::embedding_dispatch::DispatchError) -> anyhow::Error {
    match e {
        infra::infra::embedding_dispatch::DispatchError::Init(e) => {
            Unregistered(format!("{e:#}")).into()
        }
        other => anyhow::anyhow!("embedding dispatch failed: {other}"),
    }
}

fn bump(task: &Task, table: TableLabel, kind: &str, f: impl FnOnce(&mut KindProgress)) {
    let key = infra::infra::embedding_index::ReportKind::of(table, kind).as_str();
    let mut p = task.progress.lock().unwrap_or_else(|e| e.into_inner());
    if let Some((_, k)) = p.kinds.iter_mut().find(|(n, _)| *n == key) {
        f(k);
    }
}

async fn handle(deps: &ReconcileDeps, task: &Task, item: ScanItem) -> Result<()> {
    match item {
        ScanItem::Target {
            table,
            entity_id,
            vector_kind,
            state,
            ..
        } => {
            bump(task, table, vector_kind, |k| match state {
                TargetState::Complete => k.complete += 1,
                TargetState::Missing => k.missing += 1,
                TargetState::Stale => k.stale += 1,
                TargetState::Unverified => k.unverified += 1,
                TargetState::Failed => k.failed += 1,
            });
            let wanted = state.needs_dispatch(task.options.retry_failed)
                || task.options.force.covers(table, entity_id);
            if wanted && dispatch(deps, table, entity_id, vector_kind).await? {
                bump(task, table, vector_kind, |k| k.dispatched += 1);
            }
        }
        ScanItem::Orphan {
            table,
            entity_id,
            vector_kind,
            has_rows,
            has_entry,
        } => {
            bump(task, table, &vector_kind, |k| k.orphan += 1);
            delete_orphan(deps, table, entity_id, &vector_kind, has_rows, has_entry).await?;
        }
        ScanItem::CaptionMissing {
            entity_id,
            media_object_id,
        } => {
            if let Some(d) = &deps.memory_dispatcher {
                d.dispatch_caption_generation(entity_id, media_object_id)
                    .await
                    .map_err(dispatch_error)?;
                task.progress
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .captions_requested += 1;
            }
        }
    }
    Ok(())
}

/// Dispatch one target from the entity's current data. Returns whether a
/// job was enqueued (the entity may have changed since the scan).
async fn dispatch(
    deps: &ReconcileDeps,
    table: TableLabel,
    entity_id: i64,
    kind: &str,
) -> Result<bool> {
    let enqueued = match table {
        TableLabel::Memory => {
            let Some(d) = &deps.memory_dispatcher else {
                return Ok(false);
            };
            let Some(data) = memory_data(deps, entity_id).await? else {
                return Ok(false);
            };
            match kind {
                "image" => match data.media_object_id {
                    Some(mid) => d.dispatch_image(entity_id, mid.value).await,
                    None => return Ok(false),
                },
                "caption" => d.dispatch_caption(entity_id, &data.content).await,
                _ => d.dispatch(entity_id, &data.content).await,
            }
        }
        TableLabel::ReflectionIntent => {
            let Some(d) = &deps.intent_dispatcher else {
                return Ok(false);
            };
            let Some(data) = memory_data(deps, entity_id).await? else {
                return Ok(false);
            };
            let intent =
                infra::infra::embedding_target::reflection_task_intent(data.metadata.as_deref());
            d.dispatch(entity_id, &intent).await
        }
        TableLabel::Thread => {
            let Some(d) = &deps.thread_dispatcher else {
                return Ok(false);
            };
            let Some(desc) = deps
                .thread_repo
                .find(&protobuf::llm_memory::data::ThreadId { value: entity_id })
                .await?
                .and_then(|t| t.data)
                .and_then(|d| d.description)
            else {
                return Ok(false);
            };
            d.dispatch(entity_id, &desc).await
        }
    };
    enqueued.map(|job| job.is_some()).map_err(dispatch_error)
}

async fn memory_data(
    deps: &ReconcileDeps,
    id: i64,
) -> Result<Option<protobuf::llm_memory::data::MemoryData>> {
    Ok(deps
        .memory_repo
        .find(&protobuf::llm_memory::data::MemoryId { value: id }, false)
        .await?
        .and_then(|m| m.data))
}

async fn delete_orphan(
    deps: &ReconcileDeps,
    table: TableLabel,
    entity_id: i64,
    kind: &str,
    has_rows: bool,
    has_entry: bool,
) -> Result<()> {
    let index: Option<&EmbeddingIndex> = match table {
        TableLabel::Memory => deps
            .memory_vector
            .as_ref()
            .and_then(|r| r.embedding_index()),
        TableLabel::Thread => deps
            .thread_vector
            .as_ref()
            .and_then(|r| r.embedding_index()),
        TableLabel::ReflectionIntent => deps
            .intent_vector
            .as_ref()
            .and_then(|r| r.embedding_index()),
    };
    if has_rows {
        // Replacing the kind with nothing removes its rows and entry.
        match table {
            TableLabel::Memory => {
                if let Some(r) = &deps.memory_vector {
                    r.replace_kinds_upsert(entity_id, &[kind], Vec::new())
                        .await?;
                }
            }
            TableLabel::Thread => {
                if let Some(r) = &deps.thread_vector {
                    r.replace_kinds_upsert(entity_id, &[kind], Vec::new())
                        .await?;
                }
            }
            TableLabel::ReflectionIntent => {
                if let Some(r) = &deps.intent_vector {
                    r.replace_kinds_upsert(entity_id, &[kind], Vec::new())
                        .await?;
                }
            }
        }
    } else if has_entry && let Some(index) = index {
        index.delete(table, entity_id, &[kind]).await?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use infra::infra::embedding_index::source_version::{self, TextSource};
    use infra::infra::embedding_index::{EntryOutcome, IndexEntry};
    use infra::infra::embedding_space::registration::{RegistrationPlan, WorkerRegistry};
    use infra::infra::embedding_space::{token, workers};
    use infra::infra::jobworkerp_ops::fake::FakeJobworkerp;
    use infra::infra::memory_vector::config::{
        DistanceType, FtsConfig, VectorDBConfig, VectorIndexConfig,
    };
    use infra::infra::memory_vector::record::MemoryVectorRecord;
    use infra_utils::infra::rdb::UseRdbPool;
    use infra_utils::infra::test::{TEST_RUNTIME, setup_test_rdb_from};
    use protobuf::llm_memory::data::{ContentType, MemoryData, MessageRole, UserId};

    const DIM: usize = 4;

    fn space() -> SpaceId {
        SpaceId("ef".repeat(32))
    }

    struct Fixture {
        _dir: tempfile::TempDir,
        fake: Arc<FakeJobworkerp>,
        reconciler: Arc<EmbeddingReconciler>,
        vector: MemoryVectorRepositoryImpl,
        index: EmbeddingIndex,
        memory: MemoryRepositoryImpl,
    }

    async fn fixture() -> anyhow::Result<Fixture> {
        let pool = if cfg!(feature = "postgres") {
            let pool = setup_test_rdb_from("../infra/sql/postgres").await;
            sqlx::query("TRUNCATE TABLE memory, thread CASCADE;")
                .execute(pool)
                .await?;
            pool
        } else {
            let pool = setup_test_rdb_from("../infra/sql/sqlite").await;
            for t in ["thread_memory", "memory", "thread"] {
                sqlx::query(sqlx::AssertSqlSafe(format!("DELETE FROM {t}")))
                    .execute(pool)
                    .await?;
            }
            pool
        };
        workers::set_current_space(Some(space()));
        token::set_rebuilding_attempt(None);
        let dir = tempfile::tempdir()?;
        let uri = dir.path().join("lance").to_string_lossy().to_string();
        let index = EmbeddingIndex::open(&uri).await?;
        let vector = MemoryVectorRepositoryImpl::new(VectorDBConfig {
            uri,
            table_name: "memories".into(),
            vector_size: DIM,
            distance_type: DistanceType::Cosine,
            fts: FtsConfig::default(),
            vector_index: VectorIndexConfig::default(),
        })
        .await?
        .with_embedding_index(index.clone());

        let suffix = workers::space_suffix(&space());
        let yaml = dir.path().join("workers.yaml");
        std::fs::write(
            &yaml,
            format!(
                "workers:\n  - name: memories-auto-embedding{suffix}\n  - name: memories-auto-image-embedding{suffix}\n  - name: memories-auto-image-caption\n"
            ),
        )?;
        let fake = FakeJobworkerp::new();
        let registry = WorkerRegistry::new(
            Arc::new(fake.clone()),
            RegistrationPlan {
                worker_yamls: vec![yaml],
                ..Default::default()
            },
            Some(space()),
            Default::default(),
            std::time::Duration::from_millis(10),
        );
        registry.register_once().await?;
        let dispatcher = Arc::new(EmbeddingJobDispatcher::from_env(registry)?);
        let idg = infra::test_helper::shared_id_generator();
        let memory = MemoryRepositoryImpl::new(idg.clone(), pool);
        let reconciler = EmbeddingReconciler::with_rebuild_timing(
            ReconcileDeps {
                pool,
                memory_repo: MemoryRepositoryImpl::new(idg.clone(), pool),
                media_repo: MediaObjectRepositoryImpl::new(idg.clone(), pool),
                thread_repo: ThreadRepositoryImpl::new(idg, pool),
                memory_vector: Some(vector.clone()),
                thread_vector: None,
                intent_vector: None,
                memory_dispatcher: Some(dispatcher),
                thread_dispatcher: None,
                intent_dispatcher: None,
                image_search_mode: ImageSearchMode::None,
                max_content_len: 100,
                page_size: 2,
            },
            // Every poll recounts; anything not settled counts as stalled.
            rebuild::Timing {
                stall_after: std::time::Duration::ZERO,
                count_reuse: std::time::Duration::ZERO,
            },
        );
        Ok(Fixture {
            _dir: dir,
            fake,
            reconciler,
            vector,
            index,
            memory,
        })
    }

    impl Fixture {
        async fn memory(&self, content: &str) -> anyhow::Result<(i64, MemoryData)> {
            let data = MemoryData {
                user_id: Some(UserId { value: 1 }),
                content: content.into(),
                content_type: ContentType::Text as i32,
                role: MessageRole::RoleUser as i32,
                ..Default::default()
            };
            let mut tx = self.memory.db_pool().begin().await?;
            let id = self.memory.create(&mut *tx, &data).await?;
            tx.commit().await?;
            Ok((id.value, data))
        }

        async fn rows(&self, id: i64, data: &MemoryData) -> anyhow::Result<()> {
            let r = MemoryVectorRecord::from_chunk_with_content(
                id,
                data,
                &[0.5; DIM],
                Some("m"),
                "text",
                0,
                0,
                1,
                "x".into(),
            );
            self.vector
                .replace_kinds_upsert(id, &["text"], vec![r])
                .await?;
            Ok(())
        }

        async fn entry(&self, id: i64, text: &str, outcome: EntryOutcome) -> anyhow::Result<()> {
            self.index
                .put(&[IndexEntry {
                    table: TableLabel::Memory,
                    entity_id: id,
                    vector_kind: "text".into(),
                    space_id: space(),
                    source_version: source_version::text(TextSource::MemoryText, text),
                    generation_id: "g".into(),
                    outcome,
                    media_digest: None,
                    recorded_at: 0,
                }])
                .await
        }

        async fn run(&self, options: ReconcileOptions) -> Progress {
            let id = self.reconciler.start(options).unwrap();
            loop {
                let p = self.reconciler.progress(&id).unwrap();
                if p.status != TaskStatus::Running {
                    assert_eq!(p.status, TaskStatus::Completed, "{:?}", p.error);
                    return p;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        }

        async fn rebuild(&self, attempt: &str, retry_failed: bool) -> rebuild::RebuildProgress {
            self.reconciler
                .start_rebuild(attempt, retry_failed)
                .unwrap();
            loop {
                let p = self.reconciler.rebuild_progress(attempt).await.unwrap();
                if p.status != rebuild::RebuildStatus::Running {
                    assert!(p.error.is_none(), "{:?}", p.error);
                    return p;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        }

        fn dispatched_attempts(&self) -> Vec<String> {
            self.fake
                .enqueued
                .lock()
                .unwrap()
                .iter()
                .map(|r| {
                    let args: serde_json::Value = serde_json::from_slice(&r.args).unwrap();
                    let input: serde_json::Value =
                        serde_json::from_str(args["input"].as_str().unwrap()).unwrap();
                    input["token"]["attempt_id"].as_str().unwrap().to_string()
                })
                .collect()
        }

        fn dispatched_ids(&self) -> Vec<i64> {
            let mut ids: Vec<i64> = self
                .fake
                .enqueued
                .lock()
                .unwrap()
                .iter()
                .map(|r| {
                    let args: serde_json::Value = serde_json::from_slice(&r.args).unwrap();
                    let input: serde_json::Value =
                        serde_json::from_str(args["input"].as_str().unwrap()).unwrap();
                    assert!(
                        input.get("token").is_some(),
                        "reconcile dispatches carry tokens"
                    );
                    input["memory_id"].as_str().unwrap().parse().unwrap()
                })
                .collect();
            ids.sort();
            ids
        }
    }

    fn sorted(mut ids: Vec<i64>) -> Vec<i64> {
        ids.sort();
        ids
    }

    fn text(p: &Progress) -> KindProgress {
        p.kinds.iter().find(|(k, _)| *k == "memory_text").unwrap().1
    }

    #[test]
    fn dispatches_what_is_not_complete_and_deletes_orphans() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let f = fixture().await?;
            let (complete, d1) = f.memory("complete").await?;
            let (missing, _) = f.memory("missing").await?;
            let (failed, _) = f.memory("failed").await?;
            f.rows(complete, &d1).await?;
            f.entry(
                complete,
                "complete",
                EntryOutcome::Success { chunk_count: 1 },
            )
            .await?;
            f.entry(
                failed,
                "failed",
                EntryOutcome::Failure {
                    reason: "x".into(),
                    class: "permanent".into(),
                },
            )
            .await?;
            let gone = failed + 1_000_000;
            f.rows(gone, &d1).await?;

            let p = f.run(ReconcileOptions::default()).await;
            let k = text(&p);
            assert_eq!(
                (k.complete, k.missing, k.failed, k.orphan, k.dispatched),
                (1, 1, 1, 1, 1)
            );
            assert_eq!(f.dispatched_ids(), vec![missing]);
            assert!(
                f.vector
                    .get_all_memory_ids()
                    .await?
                    .iter()
                    .all(|id| *id != gone),
                "orphan rows are deleted"
            );

            f.fake.enqueued.lock().unwrap().clear();
            f.run(ReconcileOptions {
                retry_failed: true,
                ..Default::default()
            })
            .await;
            assert_eq!(f.dispatched_ids(), sorted(vec![missing, failed]));

            f.fake.enqueued.lock().unwrap().clear();
            f.run(ReconcileOptions {
                force: Force::Entities {
                    table: TableLabel::Memory,
                    ids: [complete].into(),
                },
                ..Default::default()
            })
            .await;
            assert_eq!(f.dispatched_ids(), sorted(vec![complete, missing]));
            workers::set_current_space(None);
            Ok(())
        })
    }

    #[test]
    fn stops_when_workers_are_not_registered() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let f = fixture().await?;
            f.memory("needs embedding").await?;
            let unregistered = WorkerRegistry::new(
                Arc::new(FakeJobworkerp::new()),
                RegistrationPlan::default(),
                Some(space()),
                Default::default(),
                std::time::Duration::from_millis(10),
            );
            let base = &f.reconciler.deps;
            let idg = infra::test_helper::shared_id_generator();
            let reconciler = EmbeddingReconciler::new(ReconcileDeps {
                pool: base.pool,
                memory_repo: MemoryRepositoryImpl::new(idg.clone(), base.pool),
                media_repo: MediaObjectRepositoryImpl::new(idg.clone(), base.pool),
                thread_repo: ThreadRepositoryImpl::new(idg, base.pool),
                memory_vector: base.memory_vector.clone(),
                thread_vector: None,
                intent_vector: None,
                memory_dispatcher: Some(Arc::new(EmbeddingJobDispatcher::from_env(unregistered)?)),
                thread_dispatcher: None,
                intent_dispatcher: None,
                image_search_mode: ImageSearchMode::None,
                max_content_len: 100,
                page_size: 2,
            });
            let id = reconciler.start(ReconcileOptions::default()).unwrap();
            let p = loop {
                let p = reconciler.progress(&id).unwrap();
                if p.status != TaskStatus::Running {
                    break p;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            };
            assert_eq!(p.status, TaskStatus::Failed);
            assert!(p.error.unwrap().contains("not registered"));
            workers::set_current_space(None);
            Ok(())
        })
    }

    #[test]
    fn refused_while_a_rebuild_is_pending() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let f = fixture().await?;
            token::set_rebuilding_attempt(Some("att".into()));
            assert_eq!(
                f.reconciler.start(ReconcileOptions::default()).unwrap_err(),
                StartError::RebuildPending
            );
            token::set_rebuilding_attempt(None);
            workers::set_current_space(None);
            Ok(())
        })
    }

    #[test]
    fn rebuild_requires_the_pending_attempt() -> anyhow::Result<()> {
        use rebuild::RebuildError;
        TEST_RUNTIME.block_on(async {
            let f = fixture().await?;
            assert_eq!(
                f.reconciler.start_rebuild("att", false).unwrap_err(),
                RebuildError::NotPending
            );
            token::set_rebuilding_attempt(Some("att".into()));
            assert_eq!(
                f.reconciler.start_rebuild("other", false).unwrap_err(),
                RebuildError::AttemptMismatch {
                    current: "att".into()
                }
            );
            assert!(f.reconciler.rebuild_progress("other").await.is_err());
            let p = f.reconciler.rebuild_progress("att").await?;
            assert_eq!(p.status, rebuild::RebuildStatus::NotStarted);
            assert!(p.completion_expected, "nothing to rebuild");
            token::set_rebuilding_attempt(None);
            workers::set_current_space(None);
            Ok(())
        })
    }

    #[test]
    fn rebuild_dispatches_what_is_not_complete_with_the_attempt() -> anyhow::Result<()> {
        use rebuild::RebuildStatus;
        TEST_RUNTIME.block_on(async {
            let f = fixture().await?;
            token::set_rebuilding_attempt(Some("att".into()));
            let (complete, d1) = f.memory("complete").await?;
            let (missing, d2) = f.memory("missing").await?;
            let (failed, _) = f.memory("failed").await?;
            f.rows(complete, &d1).await?;
            f.entry(
                complete,
                "complete",
                EntryOutcome::Success { chunk_count: 1 },
            )
            .await?;
            f.entry(
                failed,
                "failed",
                EntryOutcome::Failure {
                    reason: "x".into(),
                    class: "permanent".into(),
                },
            )
            .await?;
            let gone = failed + 1_000_000;
            f.rows(gone, &d1).await?;

            let p = f.rebuild("att", false).await;
            assert_eq!(f.dispatched_ids(), vec![missing]);
            assert_eq!(f.dispatched_attempts(), vec!["att".to_string()]);
            let text = p.kinds.iter().find(|(k, _)| *k == "memory_text").unwrap().1;
            assert_eq!(
                (
                    text.counts.required,
                    text.counts.complete,
                    text.counts.missing,
                    text.counts.failed_permanent,
                    text.dispatched
                ),
                (3, 1, 1, 1, 1)
            );
            assert_eq!(p.orphan, 0, "the task deleted the orphan");
            assert!(!p.completion_expected);
            assert_eq!(p.status, RebuildStatus::Stalled, "nothing settled since");

            // The dispatched result arrives.
            f.rows(missing, &d2).await?;
            f.entry(missing, "missing", EntryOutcome::Success { chunk_count: 1 })
                .await?;
            let p = f.reconciler.rebuild_progress("att").await?;
            assert!(p.completion_expected);
            assert_eq!((p.failed, p.status), (1, RebuildStatus::Dispatched));

            // A second start dispatches only what is still not complete:
            // nothing, unless failed targets are retried.
            f.fake.enqueued.lock().unwrap().clear();
            f.rebuild("att", false).await;
            assert!(f.dispatched_ids().is_empty());
            f.rebuild("att", true).await;
            assert_eq!(f.dispatched_ids(), vec![failed]);

            token::set_rebuilding_attempt(None);
            workers::set_current_space(None);
            Ok(())
        })
    }

    #[test]
    fn rebuild_redispatches_a_target_whose_text_changed() -> anyhow::Result<()> {
        TEST_RUNTIME.block_on(async {
            let f = fixture().await?;
            token::set_rebuilding_attempt(Some("att".into()));
            // Completed for an earlier text, as after a rollback of the body
            // or an edit during the rebuild.
            let (id, data) = f.memory("current text").await?;
            f.rows(id, &data).await?;
            f.entry(id, "earlier text", EntryOutcome::Success { chunk_count: 1 })
                .await?;
            let p = f.rebuild("att", false).await;
            let text = p.kinds.iter().find(|(k, _)| *k == "memory_text").unwrap().1;
            assert_eq!((text.counts.stale, text.dispatched), (1, 1));
            assert_eq!(f.dispatched_ids(), vec![id]);
            assert!(!p.completion_expected);
            token::set_rebuilding_attempt(None);
            workers::set_current_space(None);
            Ok(())
        })
    }
}
