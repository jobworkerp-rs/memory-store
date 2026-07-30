//! Runtime configuration shared by the search-index maintenance control plane.
//!
//! This module deliberately has no repository dependency.  Parsing the
//! configuration before opening LanceDB keeps a stale write-count setting from
//! starting a process that would otherwise run maintenance with ambiguous
//! semantics.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use futures::FutureExt;
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};

pub mod config;
pub mod repository_executor;

pub use config::{SearchIndexMaintenanceConfig, TableMaintenanceConfig, reject_legacy_environment};

/// A logical maintenance target.  It deliberately contains no table name or
/// URI: callers must not be able to turn the management endpoint into a DDL
/// proxy for arbitrary LanceDB tables.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MaintenanceTarget {
    MemoryFts,
    MemoryVector,
    MemoryTable,
    ThreadFts,
    ThreadVector,
    ThreadTable,
}

impl MaintenanceTarget {
    pub fn physical_table(self) -> Self {
        match self {
            Self::MemoryFts | Self::MemoryVector | Self::MemoryTable => Self::MemoryTable,
            Self::ThreadFts | Self::ThreadVector | Self::ThreadTable => Self::ThreadTable,
        }
    }

    pub fn is_component(self) -> bool {
        matches!(
            self,
            Self::MemoryFts | Self::MemoryVector | Self::ThreadFts | Self::ThreadVector
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MaintenanceAction {
    Check,
    Build,
    Optimize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptimizeAction {
    Index,
    Compact,
    Prune,
}

fn normalize_optimize_actions(actions: &[OptimizeAction]) -> Vec<OptimizeAction> {
    let compact = actions.contains(&OptimizeAction::Compact);
    let index = compact || actions.contains(&OptimizeAction::Index);
    let prune = actions.contains(&OptimizeAction::Prune);
    [
        compact.then_some(OptimizeAction::Compact),
        index.then_some(OptimizeAction::Index),
        prune.then_some(OptimizeAction::Prune),
    ]
    .into_iter()
    .flatten()
    .collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskStatus {
    Running,
    Succeeded,
    Failed,
    Skipped,
    SkippedDisabled,
    Deferred,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceTask {
    pub task_id: String,
    pub requested_target: MaintenanceTarget,
    pub physical_table: MaintenanceTarget,
    pub action: MaintenanceAction,
    pub status: TaskStatus,
    pub started_at_unix_ms: i64,
    pub finished_at_unix_ms: Option<i64>,
    pub error_summary: String,
    pub observations: Vec<IndexObservation>,
    pub warnings: Vec<MaintenanceWarning>,
    pub sub_actions: Vec<SubActionResult>,
}

#[derive(Debug, Clone)]
pub struct TaskCompletion {
    pub status: TaskStatus,
    pub error_summary: String,
    pub observations: Vec<IndexObservation>,
    pub warnings: Vec<MaintenanceWarning>,
    pub sub_actions: Vec<SubActionResult>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObservationStatus {
    Observed,
    Unavailable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexObservation {
    pub target: MaintenanceTarget,
    pub observed_at_unix_ms: i64,
    pub status: ObservationStatus,
    pub index_present: Option<bool>,
    pub unindexed_rows: Option<u64>,
    pub error_summary: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MaintenanceWarning {
    pub category: String,
    pub recorded_at_unix_ms: i64,
    pub summary: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubActionResult {
    pub action: OptimizeAction,
    pub status: TaskStatus,
    pub error_summary: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkipReason {
    CandidateNone,
    Running,
    CheckRunning,
    ReconcileRunning,
    Backoff,
    NonRetryable,
    Deferred,
    Disabled,
    ObservationUnavailable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkippedTarget {
    pub target: MaintenanceTarget,
    pub reason: SkipReason,
}

#[derive(Debug, Clone, Default)]
pub struct ReconcileResult {
    pub started: Option<StartTask>,
    pub skipped_targets: Vec<SkippedTarget>,
}

impl TaskCompletion {
    pub fn succeeded() -> Self {
        Self {
            status: TaskStatus::Succeeded,
            error_summary: String::new(),
            observations: vec![],
            warnings: vec![],
            sub_actions: vec![],
        }
    }
}

fn unavailable_observation(
    target: MaintenanceTarget,
    summary: impl Into<String>,
) -> IndexObservation {
    IndexObservation {
        target,
        observed_at_unix_ms: now_unix_ms(),
        status: ObservationStatus::Unavailable,
        index_present: None,
        unindexed_rows: None,
        error_summary: summary.into(),
    }
}

fn check_timeout_completion(target: MaintenanceTarget) -> TaskCompletion {
    let targets = match target {
        MaintenanceTarget::MemoryTable => {
            vec![
                MaintenanceTarget::MemoryFts,
                MaintenanceTarget::MemoryVector,
            ]
        }
        MaintenanceTarget::ThreadTable => {
            vec![
                MaintenanceTarget::ThreadFts,
                MaintenanceTarget::ThreadVector,
            ]
        }
        component => vec![component],
    };
    let summary = "check deadline exceeded";
    TaskCompletion {
        status: TaskStatus::Succeeded,
        error_summary: String::new(),
        observations: targets
            .into_iter()
            .map(|component| unavailable_observation(component, summary))
            .collect(),
        warnings: vec![MaintenanceWarning {
            category: "CHECK_TIMEOUT".into(),
            recorded_at_unix_ms: now_unix_ms(),
            summary: summary.into(),
        }],
        sub_actions: vec![],
    }
}

/// Repository-specific maintenance operations.  Normal query and write paths
/// never receive this trait; it is owned solely by `MaintenanceCoordinator`.
#[async_trait]
pub trait MaintenanceExecutor: Send + Sync + 'static {
    async fn check(&self, target: MaintenanceTarget) -> anyhow::Result<TaskCompletion>;
    /// Returns a read-only component build eligibility result when the
    /// repository can determine one without attempting DDL.
    async fn build_eligibility(&self, _: MaintenanceTarget) -> anyhow::Result<Option<TaskStatus>> {
        Ok(None)
    }
    /// Returns whether an existing component index must be rebuilt on the
    /// next maintenance pass without performing DDL.
    async fn force_build_required(&self, _: MaintenanceTarget) -> anyhow::Result<bool> {
        Ok(false)
    }
    async fn build(&self, target: MaintenanceTarget, force: bool)
    -> anyhow::Result<TaskCompletion>;
    async fn optimize(
        &self,
        target: MaintenanceTarget,
        actions: Vec<OptimizeAction>,
        prune_older_than_secs: u64,
    ) -> anyhow::Result<TaskCompletion>;
}

#[derive(Default)]
struct CoordinatorState {
    running: HashMap<String, MaintenanceTask>,
    history: VecDeque<MaintenanceTask>,
    last_results: HashMap<MaintenanceTarget, MaintenanceTask>,
    reconcile_running: bool,
    reconcile_cursor: usize,
    internal_checks: HashSet<MaintenanceTarget>,
    candidates: HashMap<MaintenanceTarget, CandidateState>,
}

#[derive(Debug, Clone, Default)]
struct CandidateState {
    last_index_at_unix_ms: Option<i64>,
    last_compact_at_unix_ms: Option<i64>,
    last_prune_at_unix_ms: Option<i64>,
    retry_after_unix_ms: Option<i64>,
    non_retryable: bool,
}

/// In-process coordination boundary for all runtime search-index DDL.
///
/// DDL permits are intentionally process-wide (capacity one), while check
/// tasks have their own table slot and can run beside a build/optimize task.
pub struct MaintenanceCoordinator {
    executor: Arc<dyn MaintenanceExecutor>,
    state: Arc<Mutex<CoordinatorState>>,
    ddl_semaphore: Arc<Semaphore>,
    task_counter: AtomicU64,
    history_limit: usize,
    check_deadline: Duration,
    backoff_initial: Duration,
    backoff_multiplier: f64,
    backoff_max: Duration,
    table_config: Option<(TableMaintenanceConfig, TableMaintenanceConfig)>,
    started_at_unix_ms: i64,
}

#[derive(Debug, Clone)]
pub enum StartError {
    AlreadyRunning(Vec<MaintenanceTask>),
    InvalidRequest(&'static str),
}

enum ReconcileStartOutcome {
    Blocked,
    Started(StartTask),
}

impl MaintenanceCoordinator {
    /// Performs one read-only round-robin observation and starts at most one
    /// change task. Internal observations are never task records.
    pub async fn reconcile_once(&self) -> Result<ReconcileResult, StartError> {
        let targets = [
            MaintenanceTarget::MemoryFts,
            MaintenanceTarget::MemoryVector,
            MaintenanceTarget::MemoryTable,
            MaintenanceTarget::ThreadFts,
            MaintenanceTarget::ThreadVector,
            MaintenanceTarget::ThreadTable,
        ];
        let start = {
            let mut state = self.state.lock().await;
            if state.reconcile_running {
                return Ok(ReconcileResult {
                    started: None,
                    skipped_targets: vec![SkippedTarget {
                        target: targets[0],
                        reason: SkipReason::ReconcileRunning,
                    }],
                });
            }
            state.reconcile_running = true;
            state.reconcile_cursor % targets.len()
        };
        let deadline = tokio::time::Instant::now() + self.check_deadline;
        let result = async {
            let mut skipped_targets = Vec::new();
            for offset in 0..targets.len() {
                let target = targets[(start + offset) % targets.len()];
                if !target.is_component() {
                    let mut actions = self.due_optimize_actions(target).await;
                    if let Some(reason) = self.candidate_skip_reason(target).await {
                        skipped_targets.push(SkippedTarget { target, reason });
                        continue;
                    }
                    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                    if remaining.is_zero() {
                        if actions.is_empty() {
                            skipped_targets.push(SkippedTarget {
                                target,
                                reason: SkipReason::ObservationUnavailable,
                            });
                            return Ok(ReconcileResult {
                                started: None,
                                skipped_targets,
                            });
                        }
                    } else {
                        match self.reconcile_check(target, remaining).await {
                            Ok(completion) => {
                                if self
                                    .table_index_row_threshold_due(target, &completion)
                                    .await
                                {
                                    actions.push(OptimizeAction::Index);
                                } else if actions.is_empty()
                                    && completion.observations.iter().any(|observation| {
                                        observation.status == ObservationStatus::Unavailable
                                    })
                                {
                                    skipped_targets.push(SkippedTarget {
                                        target,
                                        reason: SkipReason::ObservationUnavailable,
                                    });
                                    continue;
                                }
                            }
                            Err(reason) => {
                                skipped_targets.push(SkippedTarget { target, reason });
                                continue;
                            }
                        }
                    }
                    actions = normalize_optimize_actions(&actions);
                    if !actions.is_empty() {
                        let started = match self
                            .start_reconcile_candidate(
                                target,
                                MaintenanceAction::Optimize,
                                false,
                                actions,
                                &mut skipped_targets,
                            )
                            .await?
                        {
                            ReconcileStartOutcome::Started(task) => Some(task),
                            ReconcileStartOutcome::Blocked => None,
                        };
                        return Ok(ReconcileResult {
                            started,
                            skipped_targets,
                        });
                    }
                    skipped_targets.push(SkippedTarget {
                        target,
                        reason: SkipReason::CandidateNone,
                    });
                    continue;
                }
                if let Some(reason) = self.candidate_skip_reason(target).await {
                    skipped_targets.push(SkippedTarget { target, reason });
                    continue;
                }
                let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                if remaining.is_zero() {
                    skipped_targets.push(SkippedTarget {
                        target,
                        reason: SkipReason::ObservationUnavailable,
                    });
                    return Ok(ReconcileResult {
                        started: None,
                        skipped_targets,
                    });
                }
                let completion = match self.reconcile_check(target, remaining).await {
                    Ok(completion) => completion,
                    Err(reason) => {
                        skipped_targets.push(SkippedTarget { target, reason });
                        continue;
                    }
                };
                let force_build = match self.executor.force_build_required(target).await {
                    Ok(force_build) => force_build,
                    Err(_) => {
                        skipped_targets.push(SkippedTarget {
                            target,
                            reason: SkipReason::ObservationUnavailable,
                        });
                        continue;
                    }
                };
                let index_missing = completion.observations.first().is_some_and(|observation| {
                    observation.status == ObservationStatus::Observed
                        && observation.index_present == Some(false)
                });
                if index_missing || force_build {
                    let eligibility = match self.executor.build_eligibility(target).await {
                        Ok(eligibility) => eligibility,
                        Err(_) => {
                            skipped_targets.push(SkippedTarget {
                                target,
                                reason: SkipReason::ObservationUnavailable,
                            });
                            continue;
                        }
                    };
                    if let Some(status) = eligibility {
                        let reason = match status {
                            TaskStatus::Deferred => Some(SkipReason::Deferred),
                            TaskStatus::SkippedDisabled => Some(SkipReason::Disabled),
                            _ => None,
                        };
                        if let Some(reason) = reason {
                            skipped_targets.push(SkippedTarget { target, reason });
                            continue;
                        }
                    }
                    let started = match self
                        .start_reconcile_candidate(
                            target,
                            MaintenanceAction::Build,
                            force_build,
                            vec![],
                            &mut skipped_targets,
                        )
                        .await?
                    {
                        ReconcileStartOutcome::Started(task) => Some(task),
                        ReconcileStartOutcome::Blocked => None,
                    };
                    return Ok(ReconcileResult {
                        started,
                        skipped_targets,
                    });
                }
                skipped_targets.push(SkippedTarget {
                    target,
                    reason: SkipReason::CandidateNone,
                });
            }
            Ok(ReconcileResult {
                started: None,
                skipped_targets,
            })
        }
        .await;
        let mut state = self.state.lock().await;
        // Resume immediately after the unit that won this round.  Advancing
        // merely from the scan start would repeatedly favour units before a
        // late candidate whenever that candidate remains due.
        state.reconcile_cursor = result
            .as_ref()
            .ok()
            .and_then(|result| result.started.as_ref())
            .and_then(|started| {
                targets
                    .iter()
                    .position(|target| *target == started.task.requested_target)
            })
            .map(|selected| (selected + 1) % targets.len())
            .unwrap_or_else(|| (start + 1) % targets.len());
        state.reconcile_running = false;
        result
    }

    async fn start_reconcile_candidate(
        &self,
        target: MaintenanceTarget,
        action: MaintenanceAction,
        force: bool,
        optimize_actions: Vec<OptimizeAction>,
        skipped_targets: &mut Vec<SkippedTarget>,
    ) -> Result<ReconcileStartOutcome, StartError> {
        match self.start(target, action, force, optimize_actions).await {
            Ok(task) => Ok(ReconcileStartOutcome::Started(task)),
            Err(StartError::AlreadyRunning(_)) => {
                skipped_targets.push(SkippedTarget {
                    target,
                    reason: SkipReason::Running,
                });
                Ok(ReconcileStartOutcome::Blocked)
            }
            Err(error) => Err(error),
        }
    }

    async fn reconcile_check(
        &self,
        target: MaintenanceTarget,
        deadline: Duration,
    ) -> Result<TaskCompletion, SkipReason> {
        let physical = target.physical_table();
        {
            let mut state = self.state.lock().await;
            let public_check_running = state.running.values().any(|task| {
                task.physical_table == physical && task.action == MaintenanceAction::Check
            });
            if public_check_running || state.internal_checks.contains(&physical) {
                return Err(SkipReason::CheckRunning);
            }
            state.internal_checks.insert(physical);
        }
        let result = tokio::time::timeout(deadline, self.executor.check(target)).await;
        self.state.lock().await.internal_checks.remove(&physical);
        match result {
            Ok(Ok(completion)) => Ok(completion),
            Ok(Err(_)) => Err(SkipReason::ObservationUnavailable),
            Err(_) => Ok(check_timeout_completion(target)),
        }
    }

    async fn candidate_skip_reason(&self, target: MaintenanceTarget) -> Option<SkipReason> {
        let state = self.state.lock().await;
        let candidate = state.candidates.get(&target)?;
        if candidate.non_retryable {
            Some(SkipReason::NonRetryable)
        } else if candidate
            .retry_after_unix_ms
            .is_some_and(|retry_after| retry_after > now_unix_ms())
        {
            Some(SkipReason::Backoff)
        } else {
            None
        }
    }

    async fn table_index_row_threshold_due(
        &self,
        target: MaintenanceTarget,
        completion: &TaskCompletion,
    ) -> bool {
        let Some(config) = self.table_maintenance_config(target) else {
            return false;
        };
        config.index_update_unindexed_rows > 0
            && completion.observations.iter().any(|observation| {
                observation.status == ObservationStatus::Observed
                    && observation.index_present == Some(true)
                    && observation
                        .unindexed_rows
                        .is_some_and(|rows| rows >= config.index_update_unindexed_rows)
            })
    }

    fn table_maintenance_config(
        &self,
        target: MaintenanceTarget,
    ) -> Option<TableMaintenanceConfig> {
        let (memory, thread) = self.table_config?;
        match target {
            MaintenanceTarget::MemoryTable => Some(memory),
            MaintenanceTarget::ThreadTable => Some(thread),
            _ => None,
        }
    }

    fn prune_older_than_secs_for(&self, target: MaintenanceTarget) -> u64 {
        self.table_maintenance_config(target)
            .map(|config| config.prune_older_than.as_secs())
            .unwrap_or_default()
    }
    pub fn new(executor: Arc<dyn MaintenanceExecutor>, history_limit: usize) -> Self {
        Self::with_policy(
            executor,
            history_limit,
            Duration::from_secs(30),
            Duration::from_secs(1),
            2.0,
            Duration::from_secs(60),
            None,
        )
    }

    pub fn with_config(
        executor: Arc<dyn MaintenanceExecutor>,
        config: &SearchIndexMaintenanceConfig,
    ) -> Self {
        Self::with_policy(
            executor,
            config.task_history_limit,
            config.check_deadline,
            config.backoff_initial,
            config.backoff_multiplier,
            config.backoff_max,
            Some((config.memory, config.thread)),
        )
    }

    fn with_policy(
        executor: Arc<dyn MaintenanceExecutor>,
        history_limit: usize,
        check_deadline: Duration,
        backoff_initial: Duration,
        backoff_multiplier: f64,
        backoff_max: Duration,
        table_config: Option<(TableMaintenanceConfig, TableMaintenanceConfig)>,
    ) -> Self {
        Self {
            executor,
            state: Arc::new(Mutex::new(CoordinatorState::default())),
            ddl_semaphore: Arc::new(Semaphore::new(1)),
            task_counter: AtomicU64::new(1),
            history_limit,
            check_deadline,
            backoff_initial,
            backoff_multiplier,
            backoff_max,
            table_config,
            started_at_unix_ms: now_unix_ms(),
        }
    }

    async fn due_optimize_actions(&self, target: MaintenanceTarget) -> Vec<OptimizeAction> {
        let Some((memory, thread)) = self.table_config else {
            return vec![];
        };
        let config = match target {
            MaintenanceTarget::MemoryTable => memory,
            MaintenanceTarget::ThreadTable => thread,
            _ => return vec![],
        };
        let now = now_unix_ms();
        let state = self.state.lock().await;
        let candidate = state.candidates.get(&target).cloned().unwrap_or_default();
        let due = |interval: Duration, last: Option<i64>| {
            !interval.is_zero()
                && now
                    >= last.unwrap_or(self.started_at_unix_ms)
                        + interval.as_millis().min(i64::MAX as u128) as i64
        };
        let compact_due = due(
            config.compaction_interval,
            candidate.last_compact_at_unix_ms,
        );
        let index_due = due(
            config.index_update_interval,
            candidate.last_index_at_unix_ms,
        );
        let prune_due = due(config.prune_interval, candidate.last_prune_at_unix_ms);
        [
            compact_due.then_some(OptimizeAction::Compact),
            (compact_due || index_due).then_some(OptimizeAction::Index),
            prune_due.then_some(OptimizeAction::Prune),
        ]
        .into_iter()
        .flatten()
        .collect()
    }

    pub async fn start(
        &self,
        target: MaintenanceTarget,
        action: MaintenanceAction,
        force: bool,
        mut optimize_actions: Vec<OptimizeAction>,
    ) -> Result<StartTask, StartError> {
        match action {
            MaintenanceAction::Build if !target.is_component() => {
                return Err(StartError::InvalidRequest(
                    "build requires a component target",
                ));
            }
            MaintenanceAction::Optimize if target.is_component() => {
                return Err(StartError::InvalidRequest(
                    "optimize requires a table target",
                ));
            }
            MaintenanceAction::Optimize if optimize_actions.is_empty() => {
                return Err(StartError::InvalidRequest("optimize requires actions"));
            }
            _ => {}
        }
        let physical = target.physical_table();
        let mut state = self.state.lock().await;
        let blocked: Vec<_> = state
            .running
            .values()
            .filter(|task| {
                task.physical_table == physical
                    && match action {
                        MaintenanceAction::Check => task.action == MaintenanceAction::Check,
                        MaintenanceAction::Build | MaintenanceAction::Optimize => {
                            task.action != MaintenanceAction::Check
                        }
                    }
            })
            .cloned()
            .collect();
        if !blocked.is_empty() {
            return Err(StartError::AlreadyRunning(blocked));
        }
        // Acquire before accepting the task.  Waiting on the semaphore in a
        // spawned future would create an invisible DDL queue and let a second
        // request look RUNNING even though it has not started.
        let ddl_permit: Option<OwnedSemaphorePermit> = if action == MaintenanceAction::Check {
            None
        } else {
            match self.ddl_semaphore.clone().try_acquire_owned() {
                Ok(permit) => Some(permit),
                Err(_) => {
                    let blockers = state
                        .running
                        .values()
                        .filter(|task| task.action != MaintenanceAction::Check)
                        .cloned()
                        .collect();
                    return Err(StartError::AlreadyRunning(blockers));
                }
            }
        };
        if matches!(action, MaintenanceAction::Optimize) {
            optimize_actions = normalize_optimize_actions(&optimize_actions);
        }
        let task_id = format!("sim-{}", self.task_counter.fetch_add(1, Ordering::Relaxed));
        let task = MaintenanceTask {
            task_id: task_id.clone(),
            requested_target: target,
            physical_table: physical,
            action,
            status: TaskStatus::Running,
            started_at_unix_ms: now_unix_ms(),
            finished_at_unix_ms: None,
            error_summary: String::new(),
            observations: vec![],
            warnings: vec![],
            sub_actions: vec![],
        };
        state.running.insert(task_id.clone(), task.clone());
        drop(state);

        let executor = self.executor.clone();
        let prune_older_than_secs = self.prune_older_than_secs_for(target);
        let state = self.state.clone();
        let history_limit = self.history_limit;
        let check_deadline = self.check_deadline;
        let backoff_initial = self.backoff_initial;
        let backoff_multiplier = self.backoff_multiplier;
        let backoff_max = self.backoff_max;
        tokio::spawn(async move {
            let execution = async {
                let completion = match action {
                    MaintenanceAction::Check => {
                        match tokio::time::timeout(check_deadline, executor.check(target)).await {
                            Ok(result) => result,
                            Err(_) => Ok(check_timeout_completion(target)),
                        }
                    }
                    MaintenanceAction::Build => executor.build(target, force).await,
                    MaintenanceAction::Optimize => {
                        executor
                            .optimize(target, optimize_actions, prune_older_than_secs)
                            .await
                    }
                };
                completion.unwrap_or_else(|error| TaskCompletion {
                    status: TaskStatus::Failed,
                    error_summary: format!("{error:#}"),
                    observations: vec![],
                    warnings: vec![],
                    sub_actions: vec![],
                })
            };
            let completion = match std::panic::AssertUnwindSafe(execution).catch_unwind().await {
                Ok(value) => value,
                Err(_) => TaskCompletion {
                    status: TaskStatus::Failed,
                    error_summary: "TASK_PANIC".into(),
                    observations: vec![],
                    warnings: vec![],
                    sub_actions: vec![],
                },
            };
            let mut state = state.lock().await;
            if let Some(mut task) = state.running.remove(&task_id) {
                task.status = completion.status;
                task.error_summary = completion.error_summary;
                task.observations = completion.observations;
                task.warnings = completion.warnings;
                task.sub_actions = completion.sub_actions;
                task.finished_at_unix_ms = Some(now_unix_ms());
                let retryable_failures = (task.status == TaskStatus::Failed
                    && is_retryable_error(&task.error_summary))
                .then(|| {
                    state
                        .history
                        .iter()
                        .rev()
                        .take_while(|previous| {
                            previous.requested_target == task.requested_target
                                && previous.status == TaskStatus::Failed
                                && is_retryable_error(&previous.error_summary)
                        })
                        .count() as i32
                });
                let candidate = state.candidates.entry(task.requested_target).or_default();
                if task.status == TaskStatus::Succeeded {
                    candidate.retry_after_unix_ms = None;
                    candidate.non_retryable = false;
                    let completed_at = task.finished_at_unix_ms.unwrap_or_default();
                    match task.action {
                        MaintenanceAction::Build => {
                            candidate.last_index_at_unix_ms = Some(completed_at)
                        }
                        MaintenanceAction::Optimize => {
                            for sub_action in &task.sub_actions {
                                if sub_action.status == TaskStatus::Succeeded {
                                    match sub_action.action {
                                        OptimizeAction::Index => {
                                            candidate.last_index_at_unix_ms = Some(completed_at)
                                        }
                                        OptimizeAction::Compact => {
                                            candidate.last_compact_at_unix_ms = Some(completed_at)
                                        }
                                        OptimizeAction::Prune => {
                                            candidate.last_prune_at_unix_ms = Some(completed_at)
                                        }
                                    }
                                }
                            }
                        }
                        MaintenanceAction::Check => {}
                    }
                } else if task.status == TaskStatus::Failed {
                    if is_retryable_error(&task.error_summary) {
                        let delay = backoff_initial
                            .mul_f64(
                                backoff_multiplier.powi(retryable_failures.unwrap_or_default()),
                            )
                            .min(backoff_max);
                        candidate.retry_after_unix_ms =
                            Some(now_unix_ms() + delay.as_millis() as i64);
                    } else {
                        candidate.non_retryable = true;
                    }
                }
                state
                    .last_results
                    .insert(task.requested_target, task.clone());
                state.history.push_back(task);
                while state.history.len() > history_limit {
                    state.history.pop_front();
                }
            }
            // Keep the permit until the terminal record is visible.  The
            // next DDL candidate must observe success/failure/backoff before
            // it can be accepted.
            drop(ddl_permit);
        });
        Ok(StartTask { task })
    }

    pub async fn status(
        &self,
        task_id: Option<&str>,
        target: Option<MaintenanceTarget>,
    ) -> Option<CoordinatorStatus> {
        let state = self.state.lock().await;
        if let Some(task_id) = task_id {
            let task = state.running.get(task_id).cloned().or_else(|| {
                state
                    .history
                    .iter()
                    .find(|task| task.task_id == task_id)
                    .cloned()
            })?;
            return Some(CoordinatorStatus {
                running_tasks: (task.status == TaskStatus::Running)
                    .then_some(task.clone())
                    .into_iter()
                    .collect(),
                last_results: vec![task],
            });
        }
        let physical = target.map(MaintenanceTarget::physical_table);
        let running_tasks = state
            .running
            .values()
            .filter(|task| physical.is_none_or(|table| task.physical_table == table))
            .cloned()
            .collect::<Vec<_>>();
        let last_results = state
            .last_results
            .values()
            .filter(|task| physical.is_none_or(|table| task.physical_table == table))
            .cloned()
            .collect::<Vec<_>>();
        let mut running_tasks = running_tasks;
        let mut last_results = last_results;
        let order = |left: &MaintenanceTask, right: &MaintenanceTask| {
            (left.requested_target as u8, &left.task_id)
                .cmp(&(right.requested_target as u8, &right.task_id))
        };
        running_tasks.sort_by(order);
        last_results.sort_by(order);
        Some(CoordinatorStatus {
            running_tasks,
            last_results,
        })
    }
}

#[derive(Debug, Clone)]
pub struct StartTask {
    pub task: MaintenanceTask,
}
#[derive(Debug, Clone)]
pub struct CoordinatorStatus {
    pub running_tasks: Vec<MaintenanceTask>,
    pub last_results: Vec<MaintenanceTask>,
}

fn now_unix_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

// LanceDB does not expose a typed retryable/non-retryable error distinction,
// so this classifies by substring against the formatted `anyhow::Error`
// (e.g. commit conflicts, transient unavailability, deadline timeouts).
fn is_retryable_error(error: &str) -> bool {
    let error = error.to_ascii_lowercase();
    error.contains("conflict")
        || error.contains("temporar")
        || error.contains("unavailable")
        || error.contains("timeout")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use std::sync::atomic::{AtomicBool, AtomicUsize};

    struct TestExecutor {
        calls: AtomicUsize,
    }

    struct MissingIndexExecutor;

    struct SlowCheckExecutor;

    struct ThresholdExecutor;

    struct RetryableFailureExecutor;

    struct IneligibleVectorExecutor {
        build_calls: AtomicUsize,
        status: TaskStatus,
    }

    struct ForceFtsExecutor {
        build_calls: AtomicUsize,
        received_force: AtomicBool,
    }

    struct PruneRecordingExecutor {
        prune_older_than_secs: AtomicU64,
    }

    #[async_trait]
    impl MaintenanceExecutor for SlowCheckExecutor {
        async fn check(&self, _: MaintenanceTarget) -> anyhow::Result<TaskCompletion> {
            tokio::time::sleep(Duration::from_secs(1)).await;
            Ok(TaskCompletion::succeeded())
        }

        async fn build(&self, _: MaintenanceTarget, _: bool) -> anyhow::Result<TaskCompletion> {
            Ok(TaskCompletion::succeeded())
        }

        async fn optimize(
            &self,
            _: MaintenanceTarget,
            _: Vec<OptimizeAction>,
            _: u64,
        ) -> anyhow::Result<TaskCompletion> {
            Ok(TaskCompletion::succeeded())
        }
    }

    #[async_trait]
    impl MaintenanceExecutor for MissingIndexExecutor {
        async fn check(&self, target: MaintenanceTarget) -> anyhow::Result<TaskCompletion> {
            Ok(TaskCompletion {
                status: TaskStatus::Succeeded,
                error_summary: String::new(),
                observations: vec![IndexObservation {
                    target,
                    observed_at_unix_ms: now_unix_ms(),
                    status: ObservationStatus::Observed,
                    index_present: Some(false),
                    unindexed_rows: None,
                    error_summary: String::new(),
                }],
                warnings: vec![],
                sub_actions: vec![],
            })
        }

        async fn build(&self, _: MaintenanceTarget, _: bool) -> anyhow::Result<TaskCompletion> {
            Ok(TaskCompletion::succeeded())
        }

        async fn optimize(
            &self,
            _: MaintenanceTarget,
            _: Vec<OptimizeAction>,
            _: u64,
        ) -> anyhow::Result<TaskCompletion> {
            Ok(TaskCompletion::succeeded())
        }
    }

    #[async_trait]
    impl MaintenanceExecutor for ThresholdExecutor {
        async fn check(&self, target: MaintenanceTarget) -> anyhow::Result<TaskCompletion> {
            let observations = match target {
                MaintenanceTarget::MemoryTable => vec![
                    observed(MaintenanceTarget::MemoryFts, true, Some(3)),
                    observed(MaintenanceTarget::MemoryVector, true, Some(0)),
                ],
                target => vec![observed(target, true, Some(0))],
            };
            Ok(TaskCompletion {
                status: TaskStatus::Succeeded,
                error_summary: String::new(),
                observations,
                warnings: vec![],
                sub_actions: vec![],
            })
        }

        async fn build(&self, _: MaintenanceTarget, _: bool) -> anyhow::Result<TaskCompletion> {
            Ok(TaskCompletion::succeeded())
        }

        async fn optimize(
            &self,
            _: MaintenanceTarget,
            _: Vec<OptimizeAction>,
            _: u64,
        ) -> anyhow::Result<TaskCompletion> {
            Ok(TaskCompletion::succeeded())
        }
    }

    #[async_trait]
    impl MaintenanceExecutor for RetryableFailureExecutor {
        async fn check(&self, target: MaintenanceTarget) -> anyhow::Result<TaskCompletion> {
            Ok(TaskCompletion {
                status: TaskStatus::Succeeded,
                error_summary: String::new(),
                observations: vec![observed(target, false, None)],
                warnings: vec![],
                sub_actions: vec![],
            })
        }

        async fn build(&self, _: MaintenanceTarget, _: bool) -> anyhow::Result<TaskCompletion> {
            anyhow::bail!("temporary storage unavailable")
        }

        async fn optimize(
            &self,
            _: MaintenanceTarget,
            _: Vec<OptimizeAction>,
            _: u64,
        ) -> anyhow::Result<TaskCompletion> {
            Ok(TaskCompletion::succeeded())
        }
    }

    #[async_trait]
    impl MaintenanceExecutor for IneligibleVectorExecutor {
        async fn check(&self, target: MaintenanceTarget) -> anyhow::Result<TaskCompletion> {
            Ok(TaskCompletion {
                status: TaskStatus::Succeeded,
                error_summary: String::new(),
                observations: vec![observed(
                    target,
                    target != MaintenanceTarget::MemoryVector,
                    None,
                )],
                warnings: vec![],
                sub_actions: vec![],
            })
        }

        async fn build_eligibility(
            &self,
            target: MaintenanceTarget,
        ) -> anyhow::Result<Option<TaskStatus>> {
            if target == MaintenanceTarget::MemoryVector {
                Ok(Some(self.status))
            } else {
                Ok(None)
            }
        }

        async fn build(&self, _: MaintenanceTarget, _: bool) -> anyhow::Result<TaskCompletion> {
            self.build_calls.fetch_add(1, Ordering::Relaxed);
            Ok(TaskCompletion {
                status: TaskStatus::Deferred,
                error_summary: String::new(),
                observations: vec![],
                warnings: vec![],
                sub_actions: vec![],
            })
        }

        async fn optimize(
            &self,
            _: MaintenanceTarget,
            _: Vec<OptimizeAction>,
            _: u64,
        ) -> anyhow::Result<TaskCompletion> {
            Ok(TaskCompletion::succeeded())
        }
    }

    #[async_trait]
    impl MaintenanceExecutor for ForceFtsExecutor {
        async fn check(&self, target: MaintenanceTarget) -> anyhow::Result<TaskCompletion> {
            Ok(TaskCompletion {
                status: TaskStatus::Succeeded,
                error_summary: String::new(),
                observations: vec![observed(target, true, None)],
                warnings: vec![],
                sub_actions: vec![],
            })
        }

        async fn force_build_required(&self, target: MaintenanceTarget) -> anyhow::Result<bool> {
            Ok(target == MaintenanceTarget::MemoryFts)
        }

        async fn build(
            &self,
            target: MaintenanceTarget,
            force: bool,
        ) -> anyhow::Result<TaskCompletion> {
            assert_eq!(target, MaintenanceTarget::MemoryFts);
            self.build_calls.fetch_add(1, Ordering::Relaxed);
            self.received_force.store(force, Ordering::Relaxed);
            Ok(TaskCompletion::succeeded())
        }

        async fn optimize(
            &self,
            _: MaintenanceTarget,
            _: Vec<OptimizeAction>,
            _: u64,
        ) -> anyhow::Result<TaskCompletion> {
            Ok(TaskCompletion::succeeded())
        }
    }

    #[async_trait]
    impl MaintenanceExecutor for PruneRecordingExecutor {
        async fn check(&self, _: MaintenanceTarget) -> anyhow::Result<TaskCompletion> {
            Ok(TaskCompletion::succeeded())
        }

        async fn build(&self, _: MaintenanceTarget, _: bool) -> anyhow::Result<TaskCompletion> {
            Ok(TaskCompletion::succeeded())
        }

        async fn optimize(
            &self,
            _: MaintenanceTarget,
            _: Vec<OptimizeAction>,
            prune_older_than_secs: u64,
        ) -> anyhow::Result<TaskCompletion> {
            self.prune_older_than_secs
                .store(prune_older_than_secs, Ordering::Relaxed);
            Ok(TaskCompletion::succeeded())
        }
    }

    fn observed(
        target: MaintenanceTarget,
        index_present: bool,
        unindexed_rows: Option<u64>,
    ) -> IndexObservation {
        IndexObservation {
            target,
            observed_at_unix_ms: now_unix_ms(),
            status: ObservationStatus::Observed,
            index_present: Some(index_present),
            unindexed_rows,
            error_summary: String::new(),
        }
    }

    #[async_trait]
    impl MaintenanceExecutor for TestExecutor {
        async fn check(&self, _: MaintenanceTarget) -> anyhow::Result<TaskCompletion> {
            Ok(TaskCompletion::succeeded())
        }
        async fn build(&self, _: MaintenanceTarget, _: bool) -> anyhow::Result<TaskCompletion> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Ok(TaskCompletion::succeeded())
        }
        async fn optimize(
            &self,
            _: MaintenanceTarget,
            _: Vec<OptimizeAction>,
            _: u64,
        ) -> anyhow::Result<TaskCompletion> {
            Ok(TaskCompletion::succeeded())
        }
    }

    const NAMES: &[&str] = &[
        "MEMORY_INDEX_UPDATE_INTERVAL_SECS",
        "MEMORY_COMPACTION_INTERVAL_SECS",
        "MEMORY_PRUNE_INTERVAL_SECS",
        "MEMORY_PRUNE_OLDER_THAN_SECS",
        "MEMORY_INDEX_UPDATE_UNINDEXED_ROWS",
        "THREAD_INDEX_UPDATE_INTERVAL_SECS",
        "THREAD_COMPACTION_INTERVAL_SECS",
        "THREAD_PRUNE_INTERVAL_SECS",
        "THREAD_PRUNE_OLDER_THAN_SECS",
        "THREAD_INDEX_UPDATE_UNINDEXED_ROWS",
        "SEARCH_INDEX_MAINTENANCE_CHECK_DEADLINE_SECS",
        "SEARCH_INDEX_MAINTENANCE_BACKOFF_INITIAL_SECS",
        "SEARCH_INDEX_MAINTENANCE_BACKOFF_MULTIPLIER",
        "SEARCH_INDEX_MAINTENANCE_BACKOFF_MAX_SECS",
        "SEARCH_INDEX_MAINTENANCE_TASK_HISTORY_LIMIT",
        "MEMORY_AUTO_OPTIMIZE_INTERVAL",
        "MEMORY_OPTIMIZE_COMPACT_INTERVAL",
        "MEMORY_OPTIMIZE_PRUNE_INTERVAL",
        "MEMORY_OPTIMIZE_PRUNE_OLDER_THAN_SECS",
        "MEMORY_OPTIMIZE_PRUNE_ON_STARTUP",
        "THREAD_AUTO_OPTIMIZE_INTERVAL",
        "THREAD_OPTIMIZE_COMPACT_INTERVAL",
        "THREAD_OPTIMIZE_PRUNE_INTERVAL",
        "THREAD_OPTIMIZE_PRUNE_OLDER_THAN_SECS",
        "THREAD_OPTIMIZE_PRUNE_ON_STARTUP",
    ];

    fn set_env(name: impl AsRef<std::ffi::OsStr>, value: impl AsRef<std::ffi::OsStr>) {
        // serial_test holds the process-wide environment mutation lock here.
        unsafe { std::env::set_var(name, value) };
    }

    fn remove_env(name: impl AsRef<std::ffi::OsStr>) {
        // serial_test holds the process-wide environment mutation lock here.
        unsafe { std::env::remove_var(name) };
    }

    fn set_valid() {
        for prefix in ["MEMORY_", "THREAD_"] {
            set_env(format!("{prefix}INDEX_UPDATE_INTERVAL_SECS"), "0");
            set_env(format!("{prefix}COMPACTION_INTERVAL_SECS"), "0");
            set_env(format!("{prefix}PRUNE_INTERVAL_SECS"), "0");
            set_env(format!("{prefix}PRUNE_OLDER_THAN_SECS"), "0");
            set_env(format!("{prefix}INDEX_UPDATE_UNINDEXED_ROWS"), "0");
        }
        set_env("SEARCH_INDEX_MAINTENANCE_CHECK_DEADLINE_SECS", "30");
        set_env("SEARCH_INDEX_MAINTENANCE_BACKOFF_INITIAL_SECS", "1");
        set_env("SEARCH_INDEX_MAINTENANCE_BACKOFF_MULTIPLIER", "2");
        set_env("SEARCH_INDEX_MAINTENANCE_BACKOFF_MAX_SECS", "60");
    }
    fn clear() {
        for name in NAMES {
            remove_env(name);
        }
    }

    #[test]
    #[serial]
    fn accepts_zero_only_for_disabled_actions_and_prune_retention() {
        clear();
        set_valid();
        let cfg = SearchIndexMaintenanceConfig::from_env().unwrap();
        assert!(cfg.memory.index_update_interval.is_zero());
        assert!(cfg.thread.prune_older_than.is_zero());
        clear();
    }

    #[test]
    #[serial]
    fn rejects_legacy_operation_count_configuration() {
        clear();
        set_valid();
        set_env("MEMORY_OPTIMIZE_COMPACT_INTERVAL", "1000");
        let err = SearchIndexMaintenanceConfig::from_env()
            .unwrap_err()
            .to_string();
        assert!(err.contains("MEMORY_OPTIMIZE_COMPACT_INTERVAL"));
        clear();
    }

    #[test]
    #[serial]
    fn validates_positive_global_values_and_backoff_relation() {
        clear();
        set_valid();
        set_env("SEARCH_INDEX_MAINTENANCE_CHECK_DEADLINE_SECS", "0");
        assert!(SearchIndexMaintenanceConfig::from_env().is_err());
        set_env("SEARCH_INDEX_MAINTENANCE_CHECK_DEADLINE_SECS", "30");
        set_env("SEARCH_INDEX_MAINTENANCE_BACKOFF_MAX_SECS", "0");
        assert!(SearchIndexMaintenanceConfig::from_env().is_err());
        set_env("SEARCH_INDEX_MAINTENANCE_BACKOFF_MAX_SECS", "1");
        set_env("SEARCH_INDEX_MAINTENANCE_BACKOFF_INITIAL_SECS", "2");
        assert!(SearchIndexMaintenanceConfig::from_env().is_err());
        clear();
    }

    #[tokio::test]
    async fn reconcile_scans_the_six_fixed_targets_and_reports_skips() {
        let coordinator = MaintenanceCoordinator::new(
            Arc::new(TestExecutor {
                calls: AtomicUsize::new(0),
            }),
            4,
        );
        let result = coordinator.reconcile_once().await.unwrap();
        assert!(result.started.is_none());
        assert_eq!(result.skipped_targets.len(), 6);
        assert!(
            result
                .skipped_targets
                .iter()
                .any(|skip| skip.target == MaintenanceTarget::MemoryTable)
        );
    }

    #[tokio::test]
    async fn reconcile_starts_only_one_missing_component_build() {
        let coordinator = MaintenanceCoordinator::new(Arc::new(MissingIndexExecutor), 4);
        let result = coordinator.reconcile_once().await.unwrap();
        assert_eq!(
            result.started.unwrap().task.requested_target,
            MaintenanceTarget::MemoryFts
        );
    }

    #[tokio::test]
    async fn reconcile_selects_due_table_optimize_after_component_checks() {
        let config = SearchIndexMaintenanceConfig {
            memory: TableMaintenanceConfig {
                index_update_interval: Duration::ZERO,
                compaction_interval: Duration::from_nanos(1),
                prune_interval: Duration::ZERO,
                prune_older_than: Duration::ZERO,
                index_update_unindexed_rows: 0,
            },
            thread: TableMaintenanceConfig {
                index_update_interval: Duration::ZERO,
                compaction_interval: Duration::ZERO,
                prune_interval: Duration::ZERO,
                prune_older_than: Duration::ZERO,
                index_update_unindexed_rows: 0,
            },
            check_deadline: Duration::from_secs(1),
            backoff_initial: Duration::from_secs(1),
            backoff_multiplier: 2.0,
            backoff_max: Duration::from_secs(2),
            task_history_limit: 4,
        };
        let coordinator = MaintenanceCoordinator::with_config(
            Arc::new(TestExecutor {
                calls: AtomicUsize::new(0),
            }),
            &config,
        );
        let result = coordinator.reconcile_once().await.unwrap();
        let task = result.started.expect("due optimize must start").task;
        assert_eq!(task.requested_target, MaintenanceTarget::MemoryTable);
        assert_eq!(task.action, MaintenanceAction::Optimize);
    }

    #[tokio::test]
    async fn optimize_passes_the_table_prune_retention_to_the_executor() {
        let config = SearchIndexMaintenanceConfig {
            memory: TableMaintenanceConfig {
                index_update_interval: Duration::ZERO,
                compaction_interval: Duration::ZERO,
                prune_interval: Duration::ZERO,
                prune_older_than: Duration::from_secs(123),
                index_update_unindexed_rows: 0,
            },
            thread: TableMaintenanceConfig {
                index_update_interval: Duration::ZERO,
                compaction_interval: Duration::ZERO,
                prune_interval: Duration::ZERO,
                prune_older_than: Duration::ZERO,
                index_update_unindexed_rows: 0,
            },
            check_deadline: Duration::from_secs(1),
            backoff_initial: Duration::from_secs(1),
            backoff_multiplier: 2.0,
            backoff_max: Duration::from_secs(2),
            task_history_limit: 4,
        };
        let executor = Arc::new(PruneRecordingExecutor {
            prune_older_than_secs: AtomicU64::new(0),
        });
        let coordinator = MaintenanceCoordinator::with_config(executor.clone(), &config);
        let started = coordinator
            .start(
                MaintenanceTarget::MemoryTable,
                MaintenanceAction::Optimize,
                false,
                vec![OptimizeAction::Prune],
            )
            .await
            .unwrap();
        for _ in 0..20 {
            if coordinator
                .status(Some(&started.task.task_id), None)
                .await
                .unwrap()
                .last_results[0]
                .status
                != TaskStatus::Running
            {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(executor.prune_older_than_secs.load(Ordering::Relaxed), 123);
    }

    #[tokio::test]
    async fn reconcile_uses_table_observations_for_the_index_row_threshold() {
        let config = SearchIndexMaintenanceConfig {
            memory: TableMaintenanceConfig {
                index_update_interval: Duration::ZERO,
                compaction_interval: Duration::ZERO,
                prune_interval: Duration::ZERO,
                prune_older_than: Duration::ZERO,
                index_update_unindexed_rows: 3,
            },
            thread: TableMaintenanceConfig {
                index_update_interval: Duration::ZERO,
                compaction_interval: Duration::ZERO,
                prune_interval: Duration::ZERO,
                prune_older_than: Duration::ZERO,
                index_update_unindexed_rows: u64::MAX,
            },
            check_deadline: Duration::from_secs(1),
            backoff_initial: Duration::from_secs(1),
            backoff_multiplier: 2.0,
            backoff_max: Duration::from_secs(2),
            task_history_limit: 4,
        };
        let coordinator = MaintenanceCoordinator::with_config(Arc::new(ThresholdExecutor), &config);
        let result = coordinator.reconcile_once().await.unwrap();
        let task = result.started.expect("row threshold must start index").task;
        assert_eq!(task.requested_target, MaintenanceTarget::MemoryTable);
        assert_eq!(task.action, MaintenanceAction::Optimize);
    }

    #[tokio::test]
    async fn reconcile_reports_check_running_without_creating_an_internal_task() {
        let coordinator = MaintenanceCoordinator::with_policy(
            Arc::new(SlowCheckExecutor),
            4,
            Duration::from_secs(1),
            Duration::from_secs(1),
            2.0,
            Duration::from_secs(2),
            None,
        );
        let check = coordinator
            .start(
                MaintenanceTarget::MemoryFts,
                MaintenanceAction::Check,
                false,
                vec![],
            )
            .await
            .unwrap();
        let result = coordinator.reconcile_once().await.unwrap();
        assert!(result.skipped_targets.iter().any(|skipped| {
            skipped.target == MaintenanceTarget::MemoryFts
                && skipped.reason == SkipReason::CheckRunning
        }));
        let status = coordinator.status(None, None).await.unwrap();
        assert_eq!(status.running_tasks.len(), 1);
        assert_eq!(status.running_tasks[0].task_id, check.task.task_id);
    }

    #[tokio::test]
    async fn reconcile_reports_backoff_for_a_retryable_component_failure() {
        let coordinator = MaintenanceCoordinator::with_policy(
            Arc::new(RetryableFailureExecutor),
            4,
            Duration::from_secs(1),
            Duration::from_secs(60),
            2.0,
            Duration::from_secs(60),
            None,
        );
        let task = coordinator
            .start(
                MaintenanceTarget::MemoryFts,
                MaintenanceAction::Build,
                false,
                vec![],
            )
            .await
            .unwrap();
        for _ in 0..20 {
            if coordinator
                .status(Some(&task.task.task_id), None)
                .await
                .unwrap()
                .last_results[0]
                .status
                != TaskStatus::Running
            {
                break;
            }
            tokio::task::yield_now().await;
        }
        let result = coordinator.reconcile_once().await.unwrap();
        assert!(result.skipped_targets.iter().any(|skipped| {
            skipped.target == MaintenanceTarget::MemoryFts && skipped.reason == SkipReason::Backoff
        }));
    }

    #[tokio::test]
    async fn reconcile_resumes_after_the_selected_target() {
        let config = SearchIndexMaintenanceConfig {
            memory: TableMaintenanceConfig {
                index_update_interval: Duration::ZERO,
                compaction_interval: Duration::from_nanos(1),
                prune_interval: Duration::ZERO,
                prune_older_than: Duration::ZERO,
                index_update_unindexed_rows: u64::MAX,
            },
            thread: TableMaintenanceConfig {
                index_update_interval: Duration::ZERO,
                compaction_interval: Duration::from_nanos(1),
                prune_interval: Duration::ZERO,
                prune_older_than: Duration::ZERO,
                index_update_unindexed_rows: u64::MAX,
            },
            check_deadline: Duration::from_secs(1),
            backoff_initial: Duration::from_secs(1),
            backoff_multiplier: 2.0,
            backoff_max: Duration::from_secs(2),
            task_history_limit: 8,
        };
        let coordinator = MaintenanceCoordinator::with_config(
            Arc::new(TestExecutor {
                calls: AtomicUsize::new(0),
            }),
            &config,
        );
        let first = coordinator.reconcile_once().await.unwrap().started.unwrap();
        assert_eq!(first.task.requested_target, MaintenanceTarget::MemoryTable);
        for _ in 0..20 {
            if coordinator
                .status(Some(&first.task.task_id), None)
                .await
                .unwrap()
                .last_results[0]
                .status
                != TaskStatus::Running
            {
                break;
            }
            tokio::task::yield_now().await;
        }

        let second = coordinator.reconcile_once().await.unwrap();
        assert_eq!(
            second.skipped_targets.first().map(|skipped| skipped.target),
            Some(MaintenanceTarget::ThreadFts)
        );
        assert_eq!(
            second.started.unwrap().task.requested_target,
            MaintenanceTarget::ThreadTable
        );
    }

    #[tokio::test]
    async fn same_table_builds_are_single_flight_and_terminal_tasks_are_retained() {
        let coordinator = MaintenanceCoordinator::new(
            Arc::new(TestExecutor {
                calls: AtomicUsize::new(0),
            }),
            1,
        );
        let accepted = coordinator
            .start(
                MaintenanceTarget::MemoryFts,
                MaintenanceAction::Build,
                false,
                vec![],
            )
            .await
            .unwrap();
        let blocked = coordinator
            .start(
                MaintenanceTarget::MemoryVector,
                MaintenanceAction::Build,
                false,
                vec![],
            )
            .await
            .unwrap_err();
        let StartError::AlreadyRunning(blocked) = blocked else {
            panic!("same table request must report its blocking task");
        };
        assert_eq!(blocked.len(), 1);
        assert_eq!(blocked[0].task_id, accepted.task.task_id);
        for _ in 0..20 {
            if coordinator
                .status(Some(&accepted.task.task_id), None)
                .await
                .unwrap()
                .last_results[0]
                .status
                != TaskStatus::Running
            {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(
            coordinator
                .status(Some(&accepted.task.task_id), None)
                .await
                .unwrap()
                .last_results[0]
                .status,
            TaskStatus::Succeeded
        );
    }

    #[tokio::test]
    async fn check_deadline_reports_unavailable_observation_but_succeeds() {
        let coordinator = MaintenanceCoordinator::with_policy(
            Arc::new(SlowCheckExecutor),
            4,
            Duration::from_millis(1),
            Duration::from_secs(1),
            2.0,
            Duration::from_secs(2),
            None,
        );
        let task = coordinator
            .start(
                MaintenanceTarget::MemoryTable,
                MaintenanceAction::Check,
                false,
                vec![],
            )
            .await
            .unwrap();
        for _ in 0..50 {
            let status = coordinator
                .status(Some(&task.task.task_id), None)
                .await
                .unwrap();
            if status.last_results[0].status != TaskStatus::Running {
                let result = &status.last_results[0];
                assert_eq!(result.status, TaskStatus::Succeeded);
                assert_eq!(result.warnings[0].category, "CHECK_TIMEOUT");
                assert!(result.observations.iter().all(|observation| {
                    observation.status == ObservationStatus::Unavailable
                        && observation.index_present.is_none()
                        && observation.unindexed_rows.is_none()
                }));
                return;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        panic!("check task did not finish");
    }

    #[tokio::test]
    async fn reconcile_skips_an_ineligible_vector_without_creating_a_task() {
        let executor = Arc::new(IneligibleVectorExecutor {
            build_calls: AtomicUsize::new(0),
            status: TaskStatus::Deferred,
        });
        let coordinator = MaintenanceCoordinator::new(executor.clone(), 4);

        let result = coordinator.reconcile_once().await.unwrap();

        assert!(result.started.is_none());
        assert!(result.skipped_targets.iter().any(|skipped| {
            skipped.target == MaintenanceTarget::MemoryVector
                && skipped.reason == SkipReason::Deferred
        }));
        assert_eq!(executor.build_calls.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn reconcile_skips_a_disabled_vector_without_creating_a_task() {
        let executor = Arc::new(IneligibleVectorExecutor {
            build_calls: AtomicUsize::new(0),
            status: TaskStatus::SkippedDisabled,
        });
        let coordinator = MaintenanceCoordinator::new(executor.clone(), 4);

        let result = coordinator.reconcile_once().await.unwrap();

        assert!(result.started.is_none());
        assert!(result.skipped_targets.iter().any(|skipped| {
            skipped.target == MaintenanceTarget::MemoryVector
                && skipped.reason == SkipReason::Disabled
        }));
        assert_eq!(executor.build_calls.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn reconcile_force_rebuilds_an_existing_fts_index() {
        let executor = Arc::new(ForceFtsExecutor {
            build_calls: AtomicUsize::new(0),
            received_force: AtomicBool::new(false),
        });
        let coordinator = MaintenanceCoordinator::new(executor.clone(), 4);

        let result = coordinator.reconcile_once().await.unwrap();

        assert_eq!(
            result.started.unwrap().task.requested_target,
            MaintenanceTarget::MemoryFts
        );
        for _ in 0..50 {
            if executor.build_calls.load(Ordering::Relaxed) == 1 {
                assert!(executor.received_force.load(Ordering::Relaxed));
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("forced FTS build did not start");
    }

    #[test]
    fn normalizes_optimize_actions_to_the_required_execution_order() {
        assert_eq!(
            normalize_optimize_actions(&[
                OptimizeAction::Prune,
                OptimizeAction::Compact,
                OptimizeAction::Prune,
            ]),
            vec![
                OptimizeAction::Compact,
                OptimizeAction::Index,
                OptimizeAction::Prune,
            ]
        );
    }
}
