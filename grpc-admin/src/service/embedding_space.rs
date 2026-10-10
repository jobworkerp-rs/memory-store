//! `EmbeddingSpaceService`: read-only view of the embedding space, the
//! mm-embedding worker name of that space, and worker registration state.
//!
//! The proto code lives in the shared `protobuf` crate only.

use infra::infra::embedding_space::bootstrap::SpaceState;
use infra::infra::embedding_space::registration::{RegistrationStatus, WorkerRegistry};
use protobuf::llm_memory::service::embedding_space_service_server::EmbeddingSpaceService;
use protobuf::llm_memory::service::{
    EmbeddingTableRecord, FailureReportOutcome, GetEmbeddingSpaceRequest,
    GetEmbeddingSpaceResponse, ReportEmbeddingFailureRequest, ReportEmbeddingFailureResponse,
    WorkerRegistrationState,
};
use std::sync::Arc;
use tonic::Response;

#[derive(Clone)]
pub(crate) struct EmbeddingSpaceGrpcImpl {
    registry: Arc<WorkerRegistry>,
    space: Option<SpaceState>,
    memory_vector: Option<Arc<app::app::memory_vector::MemoryVectorAppImpl>>,
    thread_vector: Option<Arc<app::app::thread_vector::ThreadVectorAppImpl>>,
    reflection: Option<Arc<app::app::reflection::ReflectionAppImpl>>,
}

impl EmbeddingSpaceGrpcImpl {
    pub fn new(registry: Arc<WorkerRegistry>, space: Option<SpaceState>) -> Self {
        Self {
            registry,
            space,
            memory_vector: None,
            thread_vector: None,
            reflection: None,
        }
    }

    /// The apps owning each vector table, for failure reports.
    pub fn with_apps(
        mut self,
        memory_vector: Option<Arc<app::app::memory_vector::MemoryVectorAppImpl>>,
        thread_vector: Option<Arc<app::app::thread_vector::ThreadVectorAppImpl>>,
        reflection: Option<Arc<app::app::reflection::ReflectionAppImpl>>,
    ) -> Self {
        self.memory_vector = memory_vector;
        self.thread_vector = thread_vector;
        self.reflection = reflection;
        self
    }

    async fn report(
        &self,
        req: &ReportEmbeddingFailureRequest,
    ) -> Result<FailureReportOutcome, tonic::Status> {
        use infra::infra::embedding_index::TableLabel;
        use infra::infra::embedding_index::write::FailureReport;
        let token = req
            .token
            .as_ref()
            .map(infra::infra::embedding_space::token::DispatchToken::from)
            .ok_or_else(|| tonic::Status::invalid_argument("token is required"))?;
        let unavailable = || tonic::Status::failed_precondition("that vector store is not enabled");
        let result = match TableLabel::parse(&req.table) {
            Some(TableLabel::Memory) => {
                self.memory_vector
                    .as_ref()
                    .ok_or_else(unavailable)?
                    .report_embedding_failure(
                        req.entity_id,
                        &req.vector_kind,
                        &token,
                        &req.reason,
                        &req.message,
                    )
                    .await
            }
            Some(TableLabel::Thread) => {
                self.thread_vector
                    .as_ref()
                    .ok_or_else(unavailable)?
                    .report_embedding_failure(
                        req.entity_id,
                        &req.vector_kind,
                        &token,
                        &req.reason,
                        &req.message,
                    )
                    .await
            }
            Some(TableLabel::ReflectionIntent) => {
                self.reflection
                    .as_ref()
                    .ok_or_else(unavailable)?
                    .report_intent_embedding_failure(
                        req.entity_id,
                        &req.vector_kind,
                        &token,
                        &req.reason,
                        &req.message,
                    )
                    .await
            }
            None => {
                return Err(tonic::Status::invalid_argument(format!(
                    "unknown table {:?}",
                    req.table
                )));
            }
        };
        match result {
            Ok(FailureReport::Recorded) => Ok(FailureReportOutcome::Recorded),
            Ok(FailureReport::AlreadyComplete) => Ok(FailureReportOutcome::AlreadyComplete),
            Ok(FailureReport::Rejected(_)) => Ok(FailureReportOutcome::Rejected),
            Err(e) => Err(crate::service::error_handle::handle_error(&e)),
        }
    }

    fn response(&self) -> GetEmbeddingSpaceResponse {
        let current_space = self.space.as_ref().map(|s| {
            let c = &s.current;
            protobuf::llm_memory::data::EmbeddingSpace {
                space_id: c.space_id().to_string(),
                model_id: c.model_id.clone(),
                tokenizer_model_id: c.tokenizer_model_id.clone(),
                revision: c.revision.clone(),
                dimension: c.dimension,
                distance: c.distance.clone(),
            }
        });
        let tables = self
            .space
            .iter()
            .flat_map(|s| s.tables.iter())
            .map(|(label, record)| EmbeddingTableRecord {
                table: label.to_string(),
                record: Some(record.into()),
            })
            .collect();
        GetEmbeddingSpaceResponse {
            current_space,
            mm_embedding_worker_name: infra::infra::embedding_dispatch::mm_embedding_worker_name(),
            registration_state: match self.registry.status() {
                RegistrationStatus::Registered => WorkerRegistrationState::Registered,
                RegistrationStatus::Pending => WorkerRegistrationState::Pending,
            } as i32,
            tables,
            rpc_migration_supported: false,
            write_rejections: infra::infra::embedding_space::token::rejection_counts()
                .into_iter()
                .map(|(reason, count)| (reason.to_string(), count))
                .collect(),
        }
    }
}

#[tonic::async_trait]
impl EmbeddingSpaceService for EmbeddingSpaceGrpcImpl {
    async fn get_embedding_space(
        &self,
        _request: tonic::Request<GetEmbeddingSpaceRequest>,
    ) -> Result<tonic::Response<GetEmbeddingSpaceResponse>, tonic::Status> {
        Ok(Response::new(self.response()))
    }

    async fn report_embedding_failure(
        &self,
        request: tonic::Request<ReportEmbeddingFailureRequest>,
    ) -> Result<tonic::Response<ReportEmbeddingFailureResponse>, tonic::Status> {
        let outcome = self.report(request.get_ref()).await?;
        Ok(Response::new(ReportEmbeddingFailureResponse {
            outcome: outcome as i32,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use infra::infra::embedding_space::SpaceComponents;
    use infra::infra::embedding_space::record::{SpaceRecord, TableRecord};
    use infra::infra::embedding_space::registration::RegistrationPlan;
    use infra::infra::jobworkerp_ops::fake::FakeJobworkerp;

    fn components() -> SpaceComponents {
        SpaceComponents {
            model_id: "m".into(),
            tokenizer_model_id: String::new(),
            revision: "r1".into(),
            dimension: 4,
            distance: "cosine".into(),
        }
    }

    #[tokio::test]
    async fn reports_space_tables_and_registration_state() {
        let fake = FakeJobworkerp::new();
        let registry = WorkerRegistry::new(
            Arc::new(fake),
            RegistrationPlan::default(),
            None,
            Default::default(),
            std::time::Duration::from_millis(10),
        );
        let state = SpaceState {
            current: components(),
            rebuilding_attempt: None,
            tables: vec![(
                "memory",
                TableRecord {
                    space: Some(SpaceRecord::new(&components(), true)),
                    marker: None,
                    rebuild_chunking: None,
                },
            )],
        };
        let svc = EmbeddingSpaceGrpcImpl::new(registry.clone(), Some(state));

        let pending = svc.response();
        assert_eq!(
            pending.registration_state,
            WorkerRegistrationState::Pending as i32
        );
        assert!(!pending.mm_embedding_worker_name.is_empty());
        let current = pending.current_space.unwrap();
        assert_eq!(current.space_id, components().space_id().to_string());
        assert_eq!(current.revision, "r1");
        assert_eq!(pending.tables.len(), 1);
        let rec = pending.tables[0].record.clone().unwrap();
        assert!(rec.legacy_accept);
        assert!(rec.migration_marker.is_none());
        assert!(!pending.rpc_migration_supported);
        assert!(
            pending
                .write_rejections
                .contains_key("source_version_mismatch")
        );

        registry.register_once().await.unwrap();
        assert_eq!(
            svc.response().registration_state,
            WorkerRegistrationState::Registered as i32
        );
    }

    #[test]
    fn without_vector_stores_no_space_is_reported() {
        let registry = WorkerRegistry::new(
            Arc::new(FakeJobworkerp::new()),
            RegistrationPlan::default(),
            None,
            Default::default(),
            std::time::Duration::from_millis(10),
        );
        let resp = EmbeddingSpaceGrpcImpl::new(registry, None).response();
        assert!(resp.current_space.is_none());
        assert!(resp.tables.is_empty());
    }
}
