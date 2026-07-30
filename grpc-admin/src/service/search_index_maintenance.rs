use crate::protobuf::llm_memory::service as proto;
use infra::infra::search_index_maintenance as maintenance;
use std::sync::Arc;
use tonic::{Request, Response, Status};

pub struct SearchIndexMaintenanceGrpcImpl {
    coordinator: Arc<maintenance::MaintenanceCoordinator>,
}

impl SearchIndexMaintenanceGrpcImpl {
    pub fn new(
        executor: Arc<dyn maintenance::MaintenanceExecutor>,
        config: &maintenance::SearchIndexMaintenanceConfig,
    ) -> Self {
        Self {
            coordinator: Arc::new(maintenance::MaintenanceCoordinator::with_config(
                executor, config,
            )),
        }
    }
}

#[tonic::async_trait]
impl proto::search_index_maintenance_service_server::SearchIndexMaintenanceService
    for SearchIndexMaintenanceGrpcImpl
{
    async fn start_search_index_maintenance(
        &self,
        request: Request<proto::StartSearchIndexMaintenanceRequest>,
    ) -> Result<Response<proto::StartSearchIndexMaintenanceResponse>, Status> {
        let request = request.into_inner();
        let target = target_from_proto(request.target)?;
        let action = action_from_proto(request.action)?;
        validate_request(target, action, request.force, &request.optimize_actions)?;
        let actions = request
            .optimize_actions
            .into_iter()
            .map(optimize_from_proto)
            .collect::<Result<Vec<_>, _>>()?;
        match self
            .coordinator
            .start(target, action, request.force, actions)
            .await
        {
            Ok(started) => Ok(Response::new(proto::StartSearchIndexMaintenanceResponse {
                task_id: Some(started.task.task_id),
                target: request.target,
                action: request.action,
                disposition: proto::SearchIndexMaintenanceDisposition::Accepted as i32,
                blocking_tasks: vec![],
            })),
            Err(maintenance::StartError::AlreadyRunning(blocking)) => {
                Ok(Response::new(proto::StartSearchIndexMaintenanceResponse {
                    task_id: None,
                    target: request.target,
                    action: request.action,
                    disposition: proto::SearchIndexMaintenanceDisposition::AlreadyRunning as i32,
                    blocking_tasks: blocking.into_iter().map(task_to_proto).collect(),
                }))
            }
            Err(maintenance::StartError::InvalidRequest(message)) => {
                Err(Status::invalid_argument(message))
            }
        }
    }

    async fn get_search_index_maintenance_status(
        &self,
        request: Request<proto::GetSearchIndexMaintenanceStatusRequest>,
    ) -> Result<Response<proto::GetSearchIndexMaintenanceStatusResponse>, Status> {
        let request = request.into_inner();
        let target = request.target.map(target_from_proto).transpose()?;
        let status = self
            .coordinator
            .status(request.task_id.as_deref(), target)
            .await
            .ok_or_else(|| Status::not_found("maintenance task was not found"))?;
        Ok(Response::new(
            proto::GetSearchIndexMaintenanceStatusResponse {
                running_tasks: status
                    .running_tasks
                    .into_iter()
                    .map(task_to_proto)
                    .collect(),
                last_results: status.last_results.into_iter().map(task_to_proto).collect(),
            },
        ))
    }

    async fn reconcile_search_indices(
        &self,
        _: Request<proto::ReconcileSearchIndicesRequest>,
    ) -> Result<Response<proto::ReconcileSearchIndicesResponse>, Status> {
        let result = self
            .coordinator
            .reconcile_once()
            .await
            .map_err(|error| match error {
                maintenance::StartError::AlreadyRunning(_) => {
                    Status::internal("unexpected reconcile blocker")
                }
                maintenance::StartError::InvalidRequest(message) => Status::unavailable(message),
            })?;
        Ok(Response::new(proto::ReconcileSearchIndicesResponse {
            started_task_id: result
                .started
                .as_ref()
                .map(|task| task.task.task_id.clone()),
            started_action: result.started.map(|task| action_to_proto(task.task.action)),
            skipped_targets: result
                .skipped_targets
                .into_iter()
                .map(|skipped| proto::SearchIndexMaintenanceSkippedTarget {
                    target: target_to_proto(skipped.target),
                    reason: skip_reason_to_proto(skipped.reason),
                })
                .collect(),
        }))
    }
}

fn validate_request(
    target: maintenance::MaintenanceTarget,
    action: maintenance::MaintenanceAction,
    force: bool,
    optimize_actions: &[i32],
) -> Result<(), Status> {
    match action {
        maintenance::MaintenanceAction::Build if !target.is_component() => Err(
            Status::invalid_argument("build requires a component target"),
        ),
        maintenance::MaintenanceAction::Optimize if target.is_component() => {
            Err(Status::invalid_argument("optimize requires a table target"))
        }
        maintenance::MaintenanceAction::Build if !optimize_actions.is_empty() => Err(
            Status::invalid_argument("optimize_actions are valid only for optimize"),
        ),
        maintenance::MaintenanceAction::Check if force || !optimize_actions.is_empty() => Err(
            Status::invalid_argument("check accepts neither force nor optimize_actions"),
        ),
        maintenance::MaintenanceAction::Optimize if force || optimize_actions.is_empty() => Err(
            Status::invalid_argument("optimize requires non-empty optimize_actions and no force"),
        ),
        maintenance::MaintenanceAction::Optimize => {
            for value in optimize_actions {
                optimize_from_proto(*value)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn target_from_proto(value: i32) -> Result<maintenance::MaintenanceTarget, Status> {
    match proto::SearchIndexMaintenanceTarget::try_from(value).ok() {
        Some(proto::SearchIndexMaintenanceTarget::MemoryFts) => {
            Ok(maintenance::MaintenanceTarget::MemoryFts)
        }
        Some(proto::SearchIndexMaintenanceTarget::MemoryVector) => {
            Ok(maintenance::MaintenanceTarget::MemoryVector)
        }
        Some(proto::SearchIndexMaintenanceTarget::MemoryTable) => {
            Ok(maintenance::MaintenanceTarget::MemoryTable)
        }
        Some(proto::SearchIndexMaintenanceTarget::ThreadFts) => {
            Ok(maintenance::MaintenanceTarget::ThreadFts)
        }
        Some(proto::SearchIndexMaintenanceTarget::ThreadVector) => {
            Ok(maintenance::MaintenanceTarget::ThreadVector)
        }
        Some(proto::SearchIndexMaintenanceTarget::ThreadTable) => {
            Ok(maintenance::MaintenanceTarget::ThreadTable)
        }
        _ => Err(Status::invalid_argument("target is required")),
    }
}
fn action_from_proto(value: i32) -> Result<maintenance::MaintenanceAction, Status> {
    match proto::SearchIndexMaintenanceAction::try_from(value).ok() {
        Some(proto::SearchIndexMaintenanceAction::Check) => {
            Ok(maintenance::MaintenanceAction::Check)
        }
        Some(proto::SearchIndexMaintenanceAction::Build) => {
            Ok(maintenance::MaintenanceAction::Build)
        }
        Some(proto::SearchIndexMaintenanceAction::Optimize) => {
            Ok(maintenance::MaintenanceAction::Optimize)
        }
        _ => Err(Status::invalid_argument("action is required")),
    }
}
fn optimize_from_proto(value: i32) -> Result<maintenance::OptimizeAction, Status> {
    match proto::SearchIndexOptimizeAction::try_from(value).ok() {
        Some(proto::SearchIndexOptimizeAction::Index) => Ok(maintenance::OptimizeAction::Index),
        Some(proto::SearchIndexOptimizeAction::Compact) => Ok(maintenance::OptimizeAction::Compact),
        Some(proto::SearchIndexOptimizeAction::Prune) => Ok(maintenance::OptimizeAction::Prune),
        _ => Err(Status::invalid_argument("invalid optimize action")),
    }
}
fn task_to_proto(task: maintenance::MaintenanceTask) -> proto::SearchIndexMaintenanceTask {
    proto::SearchIndexMaintenanceTask {
        task_id: task.task_id,
        requested_target: target_to_proto(task.requested_target),
        physical_table: target_to_proto(task.physical_table),
        action: action_to_proto(task.action),
        status: status_to_proto(task.status),
        started_at_unix_ms: task.started_at_unix_ms,
        finished_at_unix_ms: task.finished_at_unix_ms,
        error_summary: task.error_summary,
        observations: task
            .observations
            .into_iter()
            .map(|observation| proto::SearchIndexMaintenanceObservation {
                target: target_to_proto(observation.target),
                observed_at_unix_ms: observation.observed_at_unix_ms,
                index_present: observation.index_present,
                unindexed_rows: observation.unindexed_rows,
                status: match observation.status {
                    maintenance::ObservationStatus::Observed => {
                        proto::SearchIndexObservationStatus::Observed as i32
                    }
                    maintenance::ObservationStatus::Unavailable => {
                        proto::SearchIndexObservationStatus::Unavailable as i32
                    }
                },
                error_summary: observation.error_summary,
            })
            .collect(),
        warnings: task
            .warnings
            .into_iter()
            .map(|warning| proto::SearchIndexMaintenanceWarning {
                category: warning.category,
                recorded_at_unix_ms: warning.recorded_at_unix_ms,
                summary: warning.summary,
            })
            .collect(),
        sub_actions: task
            .sub_actions
            .into_iter()
            .map(|result| proto::SearchIndexMaintenanceSubActionResult {
                action: optimize_to_proto(result.action),
                status: status_to_proto(result.status),
                error_summary: result.error_summary,
            })
            .collect(),
    }
}
fn target_to_proto(target: maintenance::MaintenanceTarget) -> i32 {
    match target {
        maintenance::MaintenanceTarget::MemoryFts => {
            proto::SearchIndexMaintenanceTarget::MemoryFts as i32
        }
        maintenance::MaintenanceTarget::MemoryVector => {
            proto::SearchIndexMaintenanceTarget::MemoryVector as i32
        }
        maintenance::MaintenanceTarget::MemoryTable => {
            proto::SearchIndexMaintenanceTarget::MemoryTable as i32
        }
        maintenance::MaintenanceTarget::ThreadFts => {
            proto::SearchIndexMaintenanceTarget::ThreadFts as i32
        }
        maintenance::MaintenanceTarget::ThreadVector => {
            proto::SearchIndexMaintenanceTarget::ThreadVector as i32
        }
        maintenance::MaintenanceTarget::ThreadTable => {
            proto::SearchIndexMaintenanceTarget::ThreadTable as i32
        }
    }
}
fn action_to_proto(action: maintenance::MaintenanceAction) -> i32 {
    match action {
        maintenance::MaintenanceAction::Check => proto::SearchIndexMaintenanceAction::Check as i32,
        maintenance::MaintenanceAction::Build => proto::SearchIndexMaintenanceAction::Build as i32,
        maintenance::MaintenanceAction::Optimize => {
            proto::SearchIndexMaintenanceAction::Optimize as i32
        }
    }
}

fn optimize_to_proto(action: maintenance::OptimizeAction) -> i32 {
    match action {
        maintenance::OptimizeAction::Index => proto::SearchIndexOptimizeAction::Index as i32,
        maintenance::OptimizeAction::Compact => proto::SearchIndexOptimizeAction::Compact as i32,
        maintenance::OptimizeAction::Prune => proto::SearchIndexOptimizeAction::Prune as i32,
    }
}
fn status_to_proto(status: maintenance::TaskStatus) -> i32 {
    match status {
        maintenance::TaskStatus::Running => proto::SearchIndexMaintenanceStatus::Running as i32,
        maintenance::TaskStatus::Succeeded => proto::SearchIndexMaintenanceStatus::Succeeded as i32,
        maintenance::TaskStatus::Failed => proto::SearchIndexMaintenanceStatus::Failed as i32,
        maintenance::TaskStatus::Skipped => proto::SearchIndexMaintenanceStatus::Skipped as i32,
        maintenance::TaskStatus::SkippedDisabled => {
            proto::SearchIndexMaintenanceStatus::SkippedDisabled as i32
        }
        maintenance::TaskStatus::Deferred => proto::SearchIndexMaintenanceStatus::Deferred as i32,
    }
}

fn skip_reason_to_proto(reason: maintenance::SkipReason) -> i32 {
    match reason {
        maintenance::SkipReason::CandidateNone => {
            proto::SearchIndexMaintenanceSkipReason::CandidateNone as i32
        }
        maintenance::SkipReason::Running => {
            proto::SearchIndexMaintenanceSkipReason::RunningSkip as i32
        }
        maintenance::SkipReason::CheckRunning => {
            proto::SearchIndexMaintenanceSkipReason::CheckRunning as i32
        }
        maintenance::SkipReason::ReconcileRunning => {
            proto::SearchIndexMaintenanceSkipReason::ReconcileRunning as i32
        }
        maintenance::SkipReason::Backoff => proto::SearchIndexMaintenanceSkipReason::Backoff as i32,
        maintenance::SkipReason::NonRetryable => {
            proto::SearchIndexMaintenanceSkipReason::NonRetryable as i32
        }
        maintenance::SkipReason::Deferred => {
            proto::SearchIndexMaintenanceSkipReason::DeferredSkip as i32
        }
        maintenance::SkipReason::Disabled => {
            proto::SearchIndexMaintenanceSkipReason::Disabled as i32
        }
        maintenance::SkipReason::ObservationUnavailable => {
            proto::SearchIndexMaintenanceSkipReason::ObservationUnavailable as i32
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_invalid_action_target_and_option_combinations() {
        let build_table = validate_request(
            maintenance::MaintenanceTarget::MemoryTable,
            maintenance::MaintenanceAction::Build,
            false,
            &[],
        );
        assert_eq!(
            build_table.unwrap_err().code(),
            tonic::Code::InvalidArgument
        );

        let optimize_component = validate_request(
            maintenance::MaintenanceTarget::MemoryFts,
            maintenance::MaintenanceAction::Optimize,
            false,
            &[proto::SearchIndexOptimizeAction::Index as i32],
        );
        assert_eq!(
            optimize_component.unwrap_err().code(),
            tonic::Code::InvalidArgument
        );

        let force_check = validate_request(
            maintenance::MaintenanceTarget::MemoryFts,
            maintenance::MaintenanceAction::Check,
            true,
            &[],
        );
        assert_eq!(
            force_check.unwrap_err().code(),
            tonic::Code::InvalidArgument
        );

        let empty_optimize = validate_request(
            maintenance::MaintenanceTarget::MemoryTable,
            maintenance::MaintenanceAction::Optimize,
            false,
            &[],
        );
        assert_eq!(
            empty_optimize.unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }

    #[test]
    fn maps_every_reconcile_skip_reason_to_a_wire_value() {
        for reason in [
            maintenance::SkipReason::CandidateNone,
            maintenance::SkipReason::Running,
            maintenance::SkipReason::CheckRunning,
            maintenance::SkipReason::ReconcileRunning,
            maintenance::SkipReason::Backoff,
            maintenance::SkipReason::NonRetryable,
            maintenance::SkipReason::Deferred,
            maintenance::SkipReason::Disabled,
            maintenance::SkipReason::ObservationUnavailable,
        ] {
            assert_ne!(
                skip_reason_to_proto(reason),
                proto::SearchIndexMaintenanceSkipReason::Unspecified as i32
            );
        }
    }
}
