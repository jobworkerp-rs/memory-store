//! The rebuild task of a pending embedding migration (spec §3.6
//! "再構築 task"): the reconciliation engine run for one attempt while the
//! vector tables carry its `pending` marker, and progress counted from
//! the tables themselves, so results that arrive after dispatching are
//! seen.

use super::{EmbeddingReconciler, ReconcileDeps, ReconcileOptions, TaskStatus, scan_tables};
use anyhow::Result;
use infra::infra::embedding_index::counts::{CountKind, Counts, StateCounts};
use infra::infra::embedding_index::scan::ScanConfig;
use infra::infra::embedding_space::SpaceId;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// Seconds without a newly settled (complete or failed) target, after
/// dispatching ended, before a rebuild is reported as stalled.
pub const STALL_ENV: &str = "MEMORY_EMBEDDING_REBUILD_STALL_SECS";
pub const STALL_DEFAULT: Duration = Duration::from_secs(15 * 60);

/// Progress polls within this interval share one count, since each count
/// scans every target.
const COUNT_REUSE: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Copy)]
pub struct Timing {
    pub stall_after: Duration,
    pub count_reuse: Duration,
}

impl Timing {
    pub fn from_env() -> Self {
        Self {
            stall_after: std::env::var(STALL_ENV)
                .ok()
                .and_then(|v| v.parse::<u64>().ok())
                .map(Duration::from_secs)
                .unwrap_or(STALL_DEFAULT),
            count_reuse: COUNT_REUSE,
        }
    }
}

/// Why a rebuild task cannot start or report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RebuildError {
    /// No rebuild is pending in this process.
    NotPending,
    /// The tables are pending for another attempt (the current one).
    AttemptMismatch { current: String },
}

impl std::fmt::Display for RebuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotPending => f.write_str("no embedding rebuild is pending"),
            Self::AttemptMismatch { current } => write!(
                f,
                "the pending embedding rebuild belongs to attempt {current}"
            ),
        }
    }
}

impl std::error::Error for RebuildError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RebuildStatus {
    /// No task ran in this process since it started.
    NotStarted,
    /// Targets are being dispatched.
    Running,
    /// Every target was dispatched; results are arriving.
    Dispatched,
    /// Dispatched, but nothing settled for the stall threshold.
    Stalled,
    /// The task stopped (e.g. workers not registered); start it again.
    Failed,
}

impl RebuildStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::NotStarted => "not_started",
            Self::Running => "running",
            Self::Dispatched => "dispatched",
            Self::Stalled => "stalled",
            Self::Failed => "failed",
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RebuildKindProgress {
    pub counts: StateCounts,
    pub dispatched: u64,
    pub failed_to_dispatch: u64,
}

#[derive(Debug, Clone)]
pub struct RebuildProgress {
    pub attempt_id: String,
    pub status: RebuildStatus,
    pub kinds: Vec<(&'static str, RebuildKindProgress)>,
    pub orphan: u64,
    /// Image memories whose caption is still to be generated.
    pub caption_missing: u64,
    pub captions_requested: u64,
    /// Every target is complete or failed and nothing is orphaned. Only
    /// `finalize` decides completion.
    pub completion_expected: bool,
    pub failed: u64,
    pub error: Option<String>,
}

/// Count history behind the stall check and the shared count.
#[derive(Default)]
pub(super) struct Watch {
    attempt_id: String,
    counted: Option<(Instant, Counts)>,
    settled: u64,
    advanced_at: Option<Instant>,
}

fn settled(c: &Counts) -> u64 {
    let t = c.total();
    t.complete + t.failed()
}

/// Whether the tables are pending for `attempt_id`.
fn check_attempt(attempt_id: &str) -> Result<(), RebuildError> {
    match infra::infra::embedding_space::token::rebuilding_attempt() {
        None => Err(RebuildError::NotPending),
        Some(current) if current != attempt_id => Err(RebuildError::AttemptMismatch { current }),
        Some(_) => Ok(()),
    }
}

impl EmbeddingReconciler {
    /// Start the rebuild task of `attempt_id`, or return the running one.
    /// Calling again after it ended (or after a restart) dispatches only
    /// what is still not complete.
    pub fn start_rebuild(
        self: &Arc<Self>,
        attempt_id: &str,
        retry_failed: bool,
    ) -> Result<String, RebuildError> {
        check_attempt(attempt_id)?;
        let space = infra::infra::embedding_space::workers::current_space()
            .ok_or(RebuildError::NotPending)?;
        Ok(self.start_task(
            Some(attempt_id.to_string()),
            ReconcileOptions {
                retry_failed,
                force: super::Force::None,
            },
            space,
        ))
    }

    pub async fn rebuild_progress(&self, attempt_id: &str) -> Result<RebuildProgress> {
        check_attempt(attempt_id)?;
        let space = infra::infra::embedding_space::workers::current_space()
            .ok_or(RebuildError::NotPending)?;
        let now = Instant::now();
        let counts = {
            let mut watch = self.rebuild_watch.lock().await;
            if watch.attempt_id != attempt_id {
                *watch = Watch {
                    attempt_id: attempt_id.to_string(),
                    ..Watch::default()
                };
            }
            let fresh = watch
                .counted
                .as_ref()
                .filter(|(at, _)| now.duration_since(*at) < self.rebuild_timing.count_reuse)
                .map(|(_, c)| c.clone());
            match fresh {
                Some(c) => c,
                None => {
                    let c = count(&self.deps, space).await?;
                    let s = settled(&c);
                    if watch.advanced_at.is_none() || s > watch.settled {
                        watch.advanced_at = Some(now);
                    }
                    watch.settled = s;
                    watch.counted = Some((now, c.clone()));
                    c
                }
            }
        };
        let advanced_at = self.rebuild_watch.lock().await.advanced_at;
        Ok(self.assemble(attempt_id, counts, advanced_at, now))
    }

    fn assemble(
        &self,
        attempt_id: &str,
        counts: Counts,
        advanced_at: Option<Instant>,
        now: Instant,
    ) -> RebuildProgress {
        let task = self
            .tasks
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .filter(|t| t.attempt_id.as_deref() == Some(attempt_id))
            .max_by_key(|t| {
                t.finished_at
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .unwrap_or(now)
            })
            .cloned();
        let total = counts.total();
        let completion_expected =
            total.complete + total.failed() == total.required && counts.orphan == 0;
        let (task_progress, finished_at) = match &task {
            Some(t) => (
                Some(t.progress.lock().unwrap_or_else(|e| e.into_inner()).clone()),
                *t.finished_at.lock().unwrap_or_else(|e| e.into_inner()),
            ),
            None => (None, None),
        };
        let status = rebuild_status(
            task_progress.as_ref().map(|p| p.status),
            finished_at.map(|f| advanced_at.map_or(f, |a| f.max(a))),
            completion_expected,
            now,
            self.rebuild_timing.stall_after,
        );
        let kinds = CountKind::ALL
            .iter()
            .map(|k| {
                let dispatch = task_progress
                    .as_ref()
                    .and_then(|p| p.kinds.iter().find(|(n, _)| *n == k.as_str()))
                    .map(|(_, d)| *d)
                    .unwrap_or_default();
                (
                    k.as_str(),
                    RebuildKindProgress {
                        counts: *counts.kind(*k),
                        dispatched: dispatch.dispatched,
                        failed_to_dispatch: dispatch.failed_to_dispatch,
                    },
                )
            })
            .collect();
        RebuildProgress {
            attempt_id: attempt_id.to_string(),
            status,
            kinds,
            orphan: counts.orphan,
            caption_missing: counts.caption_missing,
            captions_requested: task_progress
                .as_ref()
                .map(|p| p.captions_requested)
                .unwrap_or_default(),
            completion_expected,
            failed: total.failed(),
            error: task_progress.and_then(|p| p.error),
        }
    }
}

/// `quiet_since` is the later of the end of dispatching and the last
/// increase of settled targets.
fn rebuild_status(
    task: Option<TaskStatus>,
    quiet_since: Option<Instant>,
    completion_expected: bool,
    now: Instant,
    stall_after: Duration,
) -> RebuildStatus {
    match task {
        None => RebuildStatus::NotStarted,
        Some(TaskStatus::Running) => RebuildStatus::Running,
        Some(TaskStatus::Failed) => RebuildStatus::Failed,
        Some(TaskStatus::Completed) => {
            let quiet = quiet_since.map_or(Duration::ZERO, |q| now.saturating_duration_since(q));
            if !completion_expected && quiet >= stall_after {
                RebuildStatus::Stalled
            } else {
                RebuildStatus::Dispatched
            }
        }
    }
}

/// Classify every target and orphan without changing anything.
async fn count(deps: &ReconcileDeps, space: SpaceId) -> Result<Counts> {
    Counts::scan(
        deps.pool,
        &deps.media_repo,
        ScanConfig {
            space,
            image_search_mode: deps.image_search_mode,
            max_content_len: deps.max_content_len,
            page_size: deps.page_size,
        },
        scan_tables(deps).await?,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_reports_a_stall_only_after_the_threshold() {
        let t0 = Instant::now();
        let min = Duration::from_secs(60);
        let at = |secs| t0 + Duration::from_secs(secs);
        let done = Some(TaskStatus::Completed);
        let cases = [
            (None, None, false, 0, RebuildStatus::NotStarted),
            (
                Some(TaskStatus::Running),
                None,
                false,
                999,
                RebuildStatus::Running,
            ),
            (
                Some(TaskStatus::Failed),
                Some(t0),
                false,
                999,
                RebuildStatus::Failed,
            ),
            (done, Some(t0), false, 59, RebuildStatus::Dispatched),
            (done, Some(t0), false, 60, RebuildStatus::Stalled),
            (done, Some(t0), true, 999, RebuildStatus::Dispatched),
        ];
        for (task, quiet, expected_done, secs, want) in cases {
            assert_eq!(
                rebuild_status(task, quiet, expected_done, at(secs), min),
                want,
                "{task:?} quiet {secs}s"
            );
        }
    }
}
