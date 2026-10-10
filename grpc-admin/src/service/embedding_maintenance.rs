//! `EmbeddingMaintenanceService`: start and follow reconciliation of the
//! vector tables (management listener only).

use app::app::embedding_reconcile::rebuild::{RebuildError, RebuildProgress};
use app::app::embedding_reconcile::{
    EmbeddingReconciler, Force, ReconcileOptions, StartError, TaskStatus,
};
use infra::infra::embedding_index::TableLabel;
use protobuf::llm_memory::service::embedding_maintenance_service_server::EmbeddingMaintenanceService;
use protobuf::llm_memory::service::{
    EmbeddingRebuildKindProgress, EmbeddingReconcileKindProgress,
    GetEmbeddingRebuildProgressRequest, GetEmbeddingRebuildProgressResponse,
    GetEmbeddingReconcileProgressRequest, GetEmbeddingReconcileProgressResponse,
    StartEmbeddingRebuildRequest, StartEmbeddingRebuildResponse, StartEmbeddingReconcileRequest,
    StartEmbeddingReconcileResponse,
};
use std::sync::Arc;
use tonic::{Response, Status};

#[derive(Clone)]
pub(crate) struct EmbeddingMaintenanceGrpcImpl {
    reconciler: Option<Arc<EmbeddingReconciler>>,
}

impl EmbeddingMaintenanceGrpcImpl {
    pub fn new(reconciler: Option<Arc<EmbeddingReconciler>>) -> Self {
        Self { reconciler }
    }

    fn reconciler(&self) -> Result<&Arc<EmbeddingReconciler>, Status> {
        self.reconciler
            .as_ref()
            .ok_or_else(|| Status::failed_precondition("no vector store is enabled"))
    }
}

pub(crate) fn options(req: &StartEmbeddingReconcileRequest) -> Result<ReconcileOptions, Status> {
    let force = match (req.force, req.force_entity_ids.is_empty()) {
        (false, true) => Force::None,
        (false, false) => {
            return Err(Status::invalid_argument("force_entity_ids requires force"));
        }
        (true, true) => Force::All,
        (true, false) => Force::Entities {
            table: TableLabel::parse(&req.force_table).ok_or_else(|| {
                Status::invalid_argument("force_table must be memory, thread, or reflection_intent")
            })?,
            ids: req.force_entity_ids.iter().copied().collect(),
        },
    };
    Ok(ReconcileOptions {
        retry_failed: req.retry_failed,
        force,
    })
}

fn rebuild_status(e: RebuildError) -> Status {
    Status::failed_precondition(e.to_string())
}

pub(crate) fn rebuild_response(p: RebuildProgress) -> GetEmbeddingRebuildProgressResponse {
    GetEmbeddingRebuildProgressResponse {
        attempt_id: p.attempt_id,
        status: p.status.as_str().to_string(),
        kinds: p
            .kinds
            .iter()
            .map(|(kind, k)| EmbeddingRebuildKindProgress {
                kind: kind.to_string(),
                targets: k.counts.required,
                complete: k.counts.complete,
                missing: k.counts.missing,
                stale: k.counts.stale,
                unverified: k.counts.unverified,
                failed_transient: k.counts.failed_transient,
                failed_permanent: k.counts.failed_permanent,
                dispatched: k.dispatched,
                failed_to_dispatch: k.failed_to_dispatch,
            })
            .collect(),
        orphan: p.orphan,
        caption_missing: p.caption_missing,
        captions_requested: p.captions_requested,
        completion_expected: p.completion_expected,
        failed: p.failed,
        write_rejections: infra::infra::embedding_space::token::rejection_counts()
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect(),
        error: p.error,
    }
}

#[tonic::async_trait]
impl EmbeddingMaintenanceService for EmbeddingMaintenanceGrpcImpl {
    async fn start_rebuild(
        &self,
        request: tonic::Request<StartEmbeddingRebuildRequest>,
    ) -> Result<tonic::Response<StartEmbeddingRebuildResponse>, Status> {
        let req = request.into_inner();
        let task_id = self
            .reconciler()?
            .start_rebuild(&req.attempt_id, req.retry_failed)
            .map_err(rebuild_status)?;
        Ok(Response::new(StartEmbeddingRebuildResponse {
            attempt_id: req.attempt_id,
            task_id,
        }))
    }

    async fn get_rebuild_progress(
        &self,
        request: tonic::Request<GetEmbeddingRebuildProgressRequest>,
    ) -> Result<tonic::Response<GetEmbeddingRebuildProgressResponse>, Status> {
        let reconciler = self.reconciler()?;
        let attempt_id = &request.get_ref().attempt_id;
        let p = match reconciler.rebuild_progress(attempt_id).await {
            Ok(p) => p,
            Err(e) => {
                return Err(match e.downcast::<RebuildError>() {
                    Ok(e) => rebuild_status(e),
                    Err(e) => Status::internal(format!("{e:#}")),
                });
            }
        };
        Ok(Response::new(rebuild_response(p)))
    }

    async fn start_reconcile(
        &self,
        request: tonic::Request<StartEmbeddingReconcileRequest>,
    ) -> Result<tonic::Response<StartEmbeddingReconcileResponse>, Status> {
        let options = options(request.get_ref())?;
        match self.reconciler()?.start(options) {
            Ok(task_id) => Ok(Response::new(StartEmbeddingReconcileResponse { task_id })),
            Err(StartError::RebuildPending) => Err(Status::failed_precondition(
                "a rebuild is pending; use the rebuild task",
            )),
            Err(StartError::NoSpace) => {
                Err(Status::failed_precondition("no embedding space is served"))
            }
        }
    }

    async fn get_reconcile_progress(
        &self,
        request: tonic::Request<GetEmbeddingReconcileProgressRequest>,
    ) -> Result<tonic::Response<GetEmbeddingReconcileProgressResponse>, Status> {
        let p = self
            .reconciler()?
            .progress(&request.get_ref().task_id)
            .ok_or_else(|| Status::not_found("unknown reconcile task"))?;
        Ok(Response::new(GetEmbeddingReconcileProgressResponse {
            task_id: p.task_id,
            status: match p.status {
                TaskStatus::Running => "running",
                TaskStatus::Completed => "completed",
                TaskStatus::Failed => "failed",
            }
            .to_string(),
            kinds: p
                .kinds
                .iter()
                .map(|(kind, k)| EmbeddingReconcileKindProgress {
                    kind: kind.to_string(),
                    missing: k.missing,
                    stale: k.stale,
                    unverified: k.unverified,
                    failed: k.failed,
                    orphan: k.orphan,
                    dispatched: k.dispatched,
                    complete: k.complete,
                    failed_to_dispatch: k.failed_to_dispatch,
                })
                .collect(),
            captions_requested: p.captions_requested,
            error: p.error,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn force_options_are_validated() {
        let req = |force, table: &str, ids: Vec<i64>| StartEmbeddingReconcileRequest {
            retry_failed: true,
            force,
            force_table: table.into(),
            force_entity_ids: ids,
        };
        assert_eq!(options(&req(false, "", vec![])).unwrap().force, Force::None);
        assert_eq!(options(&req(true, "", vec![])).unwrap().force, Force::All);
        assert_eq!(
            options(&req(true, "thread", vec![3])).unwrap().force,
            Force::Entities {
                table: TableLabel::Thread,
                ids: [3].into()
            }
        );
        assert!(options(&req(false, "thread", vec![3])).is_err());
        assert!(options(&req(true, "bogus", vec![3])).is_err());
        assert!(options(&req(false, "", vec![])).unwrap().retry_failed);
    }

    #[test]
    fn rebuild_progress_maps_counts_and_refusals() {
        use app::app::embedding_reconcile::rebuild::{RebuildKindProgress, RebuildStatus};
        let mut k = RebuildKindProgress::default();
        k.counts.required = 3;
        k.counts.complete = 1;
        k.counts.failed_transient = 1;
        k.dispatched = 2;
        let r = rebuild_response(RebuildProgress {
            attempt_id: "a".into(),
            status: RebuildStatus::Stalled,
            kinds: vec![("memory_text", k)],
            orphan: 0,
            caption_missing: 1,
            captions_requested: 1,
            completion_expected: false,
            failed: 1,
            error: None,
        });
        assert_eq!((r.status.as_str(), r.failed), ("stalled", 1));
        let kind = &r.kinds[0];
        assert_eq!(
            (
                kind.targets,
                kind.complete,
                kind.failed_transient,
                kind.dispatched
            ),
            (3, 1, 1, 2)
        );
        assert!(r.write_rejections.contains_key("attempt_mismatch"));
        assert_eq!(
            rebuild_status(RebuildError::NotPending).code(),
            tonic::Code::FailedPrecondition
        );
    }
}
