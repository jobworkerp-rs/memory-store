//! LanceDB repository adapter for the search-index maintenance coordinator.

use super::*;
use crate::infra::memory_vector::repository::MemoryVectorRepositoryImpl;
use crate::infra::thread_vector::repository::ThreadVectorRepositoryImpl;

/// Binds the maintenance control plane to the two supported LanceDB tables.
///
/// This adapter is intentionally the only owner of repository maintenance
/// capabilities. Application query and write services never receive them.
pub struct RepositoryMaintenanceExecutor {
    memory: Option<MemoryVectorRepositoryImpl>,
    thread: Option<ThreadVectorRepositoryImpl>,
}

impl RepositoryMaintenanceExecutor {
    pub fn new(
        memory: Option<MemoryVectorRepositoryImpl>,
        thread: Option<ThreadVectorRepositoryImpl>,
    ) -> Self {
        Self { memory, thread }
    }

    fn repository_for(
        &self,
        target: MaintenanceTarget,
    ) -> anyhow::Result<(&dyn IndexMaintenanceRepository, TableMaintenanceTarget)> {
        match target.physical_table() {
            MaintenanceTarget::MemoryTable => self
                .memory
                .as_ref()
                .map(|repository| {
                    (
                        repository as &dyn IndexMaintenanceRepository,
                        MEMORY_TARGETS,
                    )
                })
                .ok_or_else(|| anyhow::anyhow!("memory vector repository is disabled")),
            MaintenanceTarget::ThreadTable => self
                .thread
                .as_ref()
                .map(|repository| {
                    (
                        repository as &dyn IndexMaintenanceRepository,
                        THREAD_TARGETS,
                    )
                })
                .ok_or_else(|| anyhow::anyhow!("thread vector repository is disabled")),
            _ => unreachable!("physical_table always returns a table target"),
        }
    }
}

fn effective_build_force(
    target: MaintenanceTarget,
    requested_force: bool,
    configured_fts_force_rebuild: bool,
) -> bool {
    requested_force
        || (matches!(
            target,
            MaintenanceTarget::MemoryFts | MaintenanceTarget::ThreadFts
        ) && configured_fts_force_rebuild)
}

#[async_trait::async_trait]
trait IndexMaintenanceRepository: Send + Sync {
    fn maintenance_fts_force_rebuild_enabled(&self) -> bool;
    async fn observe_maintenance_index(&self, vector: bool) -> anyhow::Result<IndexObservation>;
    async fn maintenance_vector_build_status(&self) -> anyhow::Result<TaskStatus>;
    async fn maintenance_build_fts(&self, force: bool) -> anyhow::Result<()>;
    async fn maintenance_build_vector(&self, force: bool) -> anyhow::Result<()>;
    async fn maintenance_optimize_action(
        &self,
        action: OptimizeAction,
        prune_older_than_secs: u64,
    ) -> anyhow::Result<()>;
}

#[async_trait::async_trait]
impl IndexMaintenanceRepository for MemoryVectorRepositoryImpl {
    fn maintenance_fts_force_rebuild_enabled(&self) -> bool {
        MemoryVectorRepositoryImpl::maintenance_fts_force_rebuild_enabled(self)
    }

    async fn observe_maintenance_index(&self, vector: bool) -> anyhow::Result<IndexObservation> {
        MemoryVectorRepositoryImpl::observe_maintenance_index(self, vector).await
    }

    async fn maintenance_vector_build_status(&self) -> anyhow::Result<TaskStatus> {
        MemoryVectorRepositoryImpl::maintenance_vector_build_status(self).await
    }

    async fn maintenance_build_fts(&self, force: bool) -> anyhow::Result<()> {
        MemoryVectorRepositoryImpl::maintenance_build_fts(self, force).await
    }

    async fn maintenance_build_vector(&self, force: bool) -> anyhow::Result<()> {
        MemoryVectorRepositoryImpl::maintenance_build_vector(self, force).await
    }

    async fn maintenance_optimize_action(
        &self,
        action: OptimizeAction,
        prune_older_than_secs: u64,
    ) -> anyhow::Result<()> {
        MemoryVectorRepositoryImpl::maintenance_optimize_action(self, action, prune_older_than_secs)
            .await
    }
}

#[async_trait::async_trait]
impl IndexMaintenanceRepository for ThreadVectorRepositoryImpl {
    fn maintenance_fts_force_rebuild_enabled(&self) -> bool {
        ThreadVectorRepositoryImpl::maintenance_fts_force_rebuild_enabled(self)
    }

    async fn observe_maintenance_index(&self, vector: bool) -> anyhow::Result<IndexObservation> {
        ThreadVectorRepositoryImpl::observe_maintenance_index(self, vector).await
    }

    async fn maintenance_vector_build_status(&self) -> anyhow::Result<TaskStatus> {
        ThreadVectorRepositoryImpl::maintenance_vector_build_status(self).await
    }

    async fn maintenance_build_fts(&self, force: bool) -> anyhow::Result<()> {
        ThreadVectorRepositoryImpl::maintenance_build_fts(self, force).await
    }

    async fn maintenance_build_vector(&self, force: bool) -> anyhow::Result<()> {
        ThreadVectorRepositoryImpl::maintenance_build_vector(self, force).await
    }

    async fn maintenance_optimize_action(
        &self,
        action: OptimizeAction,
        prune_older_than_secs: u64,
    ) -> anyhow::Result<()> {
        ThreadVectorRepositoryImpl::maintenance_optimize_action(self, action, prune_older_than_secs)
            .await
    }
}

async fn maintain_index_via(
    repo: &dyn IndexMaintenanceRepository,
    target: MaintenanceTarget,
    force: bool,
    actions: &[OptimizeAction],
    prune_older_than_secs: u64,
    targets: TableMaintenanceTarget,
) -> anyhow::Result<Vec<SubActionResult>> {
    match target {
        target if target == targets.fts => {
            repo.maintenance_build_fts(force).await?;
            Ok(vec![])
        }
        target if target == targets.vector => {
            repo.maintenance_build_vector(force).await?;
            Ok(vec![])
        }
        target if target == targets.table => {
            let mut results = Vec::with_capacity(actions.len());
            for action in actions {
                match repo
                    .maintenance_optimize_action(*action, prune_older_than_secs)
                    .await
                {
                    Ok(()) => results.push(SubActionResult {
                        action: *action,
                        status: TaskStatus::Succeeded,
                        error_summary: String::new(),
                    }),
                    Err(error) => {
                        results.push(SubActionResult {
                            action: *action,
                            status: TaskStatus::Failed,
                            error_summary: format!("{error:#}"),
                        });
                        return Ok(results);
                    }
                }
            }
            Ok(results)
        }
        _ => anyhow::bail!(targets.unexpected_target),
    }
}

#[derive(Clone, Copy)]
struct TableMaintenanceTarget {
    fts: MaintenanceTarget,
    vector: MaintenanceTarget,
    table: MaintenanceTarget,
    unexpected_target: &'static str,
}

const MEMORY_TARGETS: TableMaintenanceTarget = TableMaintenanceTarget {
    fts: MaintenanceTarget::MemoryFts,
    vector: MaintenanceTarget::MemoryVector,
    table: MaintenanceTarget::MemoryTable,
    unexpected_target: "memory maintenance received a thread target",
};

const THREAD_TARGETS: TableMaintenanceTarget = TableMaintenanceTarget {
    fts: MaintenanceTarget::ThreadFts,
    vector: MaintenanceTarget::ThreadVector,
    table: MaintenanceTarget::ThreadTable,
    unexpected_target: "thread maintenance received a memory target",
};

#[async_trait::async_trait]
impl MaintenanceExecutor for RepositoryMaintenanceExecutor {
    async fn check(&self, target: MaintenanceTarget) -> anyhow::Result<TaskCompletion> {
        let (repository, targets) = self.repository_for(target)?;
        if target == targets.table {
            return Ok(check_completion(vec![
                observation_or_unavailable(
                    targets.fts,
                    repository.observe_maintenance_index(false).await,
                ),
                observation_or_unavailable(
                    targets.vector,
                    repository.observe_maintenance_index(true).await,
                ),
            ]));
        }
        let observation = repository
            .observe_maintenance_index(target == targets.vector)
            .await?;
        Ok(TaskCompletion {
            status: TaskStatus::Succeeded,
            error_summary: String::new(),
            observations: vec![observation],
            warnings: vec![],
            sub_actions: vec![],
        })
    }

    async fn build_eligibility(
        &self,
        target: MaintenanceTarget,
    ) -> anyhow::Result<Option<TaskStatus>> {
        let (repository, targets) = self.repository_for(target)?;
        if target != targets.vector {
            return Ok(None);
        }
        Ok(Some(repository.maintenance_vector_build_status().await?))
    }

    async fn force_build_required(&self, target: MaintenanceTarget) -> anyhow::Result<bool> {
        let (repository, targets) = self.repository_for(target)?;
        Ok(target == targets.fts && repository.maintenance_fts_force_rebuild_enabled())
    }

    async fn build(
        &self,
        target: MaintenanceTarget,
        force: bool,
    ) -> anyhow::Result<TaskCompletion> {
        if !target.is_component() {
            anyhow::bail!("build requires a component target");
        }
        let (repository, targets) = self.repository_for(target)?;
        let effective_force = effective_build_force(
            target,
            force,
            target == targets.fts && repository.maintenance_fts_force_rebuild_enabled(),
        );
        if !effective_force {
            let observed = self.check(target).await?;
            if observed
                .observations
                .first()
                .is_some_and(|observation| observation.index_present == Some(true))
            {
                return Ok(TaskCompletion {
                    status: TaskStatus::Skipped,
                    error_summary: String::new(),
                    observations: observed.observations,
                    warnings: vec![],
                    sub_actions: vec![],
                });
            }
        }
        let vector_status = if target == targets.vector {
            repository.maintenance_vector_build_status().await?
        } else {
            TaskStatus::Succeeded
        };
        if matches!(
            vector_status,
            TaskStatus::SkippedDisabled | TaskStatus::Deferred
        ) {
            return Ok(TaskCompletion {
                status: vector_status,
                error_summary: String::new(),
                observations: vec![],
                warnings: vec![],
                sub_actions: vec![],
            });
        }
        maintain_index_via(repository, target, effective_force, &[], 0, targets).await?;
        Ok(TaskCompletion::succeeded())
    }

    async fn optimize(
        &self,
        target: MaintenanceTarget,
        actions: Vec<OptimizeAction>,
        prune_older_than_secs: u64,
    ) -> anyhow::Result<TaskCompletion> {
        let (repository, targets) = self.repository_for(target)?;
        if target != targets.table {
            anyhow::bail!("optimize requires a table target");
        }
        let sub_actions = maintain_index_via(
            repository,
            target,
            false,
            &actions,
            prune_older_than_secs,
            targets,
        )
        .await?;
        let failed = sub_actions
            .iter()
            .find(|result| result.status == TaskStatus::Failed);
        Ok(TaskCompletion {
            status: if failed.is_some() {
                TaskStatus::Failed
            } else {
                TaskStatus::Succeeded
            },
            error_summary: failed
                .map(|result| result.error_summary.clone())
                .unwrap_or_default(),
            observations: vec![],
            warnings: vec![],
            sub_actions,
        })
    }
}

fn observation_or_unavailable(
    target: MaintenanceTarget,
    result: anyhow::Result<IndexObservation>,
) -> IndexObservation {
    result.unwrap_or_else(|error| IndexObservation {
        target,
        observed_at_unix_ms: command_utils::util::datetime::now_millis(),
        status: ObservationStatus::Unavailable,
        index_present: None,
        unindexed_rows: None,
        error_summary: format!("{error:#}"),
    })
}

fn check_completion(observations: Vec<IndexObservation>) -> TaskCompletion {
    let warnings = observations
        .iter()
        .filter(|observation| observation.status == ObservationStatus::Unavailable)
        .map(|observation| MaintenanceWarning {
            category: "OBSERVATION_UNAVAILABLE".into(),
            recorded_at_unix_ms: command_utils::util::datetime::now_millis(),
            summary: observation.error_summary.clone(),
        })
        .collect();
    TaskCompletion {
        status: TaskStatus::Succeeded,
        error_summary: String::new(),
        observations,
        warnings,
        sub_actions: vec![],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_target_sets_keep_components_with_their_physical_table() {
        assert_eq!(MEMORY_TARGETS.fts.physical_table(), MEMORY_TARGETS.table);
        assert_eq!(MEMORY_TARGETS.vector.physical_table(), MEMORY_TARGETS.table);
        assert_eq!(THREAD_TARGETS.fts.physical_table(), THREAD_TARGETS.table);
        assert_eq!(THREAD_TARGETS.vector.physical_table(), THREAD_TARGETS.table);
    }

    #[test]
    fn configured_force_applies_only_to_fts_builds() {
        assert!(effective_build_force(
            MaintenanceTarget::MemoryFts,
            false,
            true
        ));
        assert!(effective_build_force(
            MaintenanceTarget::ThreadFts,
            false,
            true
        ));
        assert!(!effective_build_force(
            MaintenanceTarget::MemoryVector,
            false,
            true
        ));
        assert!(effective_build_force(
            MaintenanceTarget::MemoryVector,
            true,
            false
        ));
    }
}
