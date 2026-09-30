//! ThreadGroup gRPC service (design 8.1).
//!
//! Additive to `ThreadService`. The read model and the import /
//! event-processing paths delegate to the app layer; the transport only
//! maps between proto and the app types and never decides policy.

use std::time::{SystemTime, UNIX_EPOCH};

use app::app::memory::MemoryAppImpl;
use app::app::memory_vector::MemoryVectorAppImpl;
use app::app::thread_group::memory_relation::ThreadGroupMemoryRelationService;
use app::app::thread_group::orphan_cleanup::ThreadGroupOrphanCleanupService;
use app::app::thread_group::{
    GroupSearchMode, GroupSearchTarget, ObservationInput, ObservedEndpoint, ThreadDisplayView,
    ThreadGroupMemberSearchHit, ThreadGroupMemberSearchProvider, ThreadGroupMemberSearchQuery,
    ThreadGroupOperatorService, ThreadGroupPurgeService, ThreadGroupReadService,
    ThreadGroupReconciliationService, ThreadGroupView, ThreadRelationEndpointView,
};
use common::thread_group_key::{IdentityScope, legacy_owner_scope};
use infra::infra::thread_group::outbox::{
    ThreadGroupEventOutboxRepository, ThreadGroupEventOutboxRepositoryImpl,
};
use infra_utils::infra::rdb::RdbPool;

use crate::protobuf::llm_memory::data::{
    ManualCollection, ManualCollectionMember, ThreadCandidateState, ThreadEvidenceConfidence,
    ThreadEvidenceKind, ThreadGroup, ThreadGroupCandidateAssociation, ThreadGroupEndpoint,
    ThreadGroupId, ThreadGroupLineage, ThreadGroupMember, ThreadGroupMemberRole,
    ThreadGroupMemberState, ThreadGroupStatus, ThreadGroupThreadDisplay, ThreadGroupingAuthority,
    ThreadObservation, ThreadObservationPolarity, ThreadObservationState, ThreadRelation,
    ThreadRelationState, ThreadRelationType, ThreadSelectionBasis,
};
use crate::protobuf::llm_memory::service::thread_group_service_server::ThreadGroupService;
use crate::protobuf::llm_memory::service::{
    AttachManualCollectionMemberRequest, AttachThreadGroupMemberRequest,
    CountThreadGroupSummariesRequest, CountThreadGroupSummariesResponse,
    CreateManualCollectionRequest, CreateManualCollectionResponse, DeleteManualCollectionRequest,
    DeleteThreadGroupInactiveHistoryRequest, DeleteThreadGroupInactiveHistoryResponse,
    DetachManualCollectionMemberRequest, ExecuteGlobalOrphanCleanupRequest,
    FindManualCollectionListRequest, FindManualCollectionMembersRequest,
    FindThreadGroupListRequest, FindThreadGroupReconciliationReportRequest,
    FindThreadGroupSummaryRequest, GlobalOrphanCleanupPreview, GlobalOrphanCleanupResponse,
    LinkThreadGroupMemoryRequest, MergeThreadGroupsRequest, MergeThreadGroupsResponse,
    PreviewGlobalOrphanCleanupRequest, PreviewThreadGroupImportRequest,
    PreviewThreadGroupImportResponse, ProcessThreadGroupEventRequest,
    ProcessThreadGroupEventResponse, RecordThreadGroupObservationsRequest,
    RecordThreadGroupObservationsResponse, RecordThreadGroupOperatorDecisionRequest,
    RecordThreadGroupOperatorDecisionResponse, RenameManualCollectionRequest,
    SearchThreadGroupsRequest, SearchThreadGroupsResponse, SplitThreadGroupRequest,
    SplitThreadGroupResponse, SuccessResponse, ThreadGroupCapabilitiesRequest,
    ThreadGroupCapabilitiesResponse, ThreadGroupPurgePreview, ThreadGroupReconciliationReport,
    ThreadGroupSearchMode, ThreadGroupSearchTarget, ThreadGroupSearchWitness,
    ThreadGroupSummaryResponse,
};
use crate::service::error_handle::handle_error;
use async_stream::stream;
use futures::stream::BoxStream;
use infra::infra::thread_group::rows::{
    ThreadGroupCandidateAssociationRow, ThreadGroupMemberRow, ThreadObservationRow,
    ThreadRelationRow,
};
use std::sync::Arc;
use tonic::Response;

pub struct ThreadGroupGrpcImpl {
    read_app: ThreadGroupReadService,
    reconcile_app: ThreadGroupReconciliationService,
    operator_app: ThreadGroupOperatorService,
    purge_app: ThreadGroupPurgeService,
    memory_relation_app: ThreadGroupMemoryRelationService,
    orphan_cleanup: Option<ThreadGroupOrphanCleanupService>,
    pool: &'static RdbPool,
    outbox: ThreadGroupEventOutboxRepositoryImpl,
    memory_vector_app: Option<Arc<MemoryVectorAppImpl>>,
    thread_vector_app: Option<Arc<app::app::thread_vector::ThreadVectorAppImpl>>,
}

impl ThreadGroupGrpcImpl {
    pub fn new(pool: &'static RdbPool) -> Self {
        Self {
            read_app: ThreadGroupReadService::new(pool),
            reconcile_app: ThreadGroupReconciliationService::new(pool),
            operator_app: ThreadGroupOperatorService::new(pool),
            purge_app: ThreadGroupPurgeService::new(pool),
            memory_relation_app: ThreadGroupMemoryRelationService::new(pool),
            orphan_cleanup: None,
            pool,
            outbox: ThreadGroupEventOutboxRepositoryImpl::new(pool),
            memory_vector_app: None,
            thread_vector_app: None,
        }
    }

    pub fn with_search_apps(
        mut self,
        memory_vector_app: Option<Arc<MemoryVectorAppImpl>>,
        thread_vector_app: Option<Arc<app::app::thread_vector::ThreadVectorAppImpl>>,
    ) -> Self {
        if let Some(cleanup) = self.orphan_cleanup.take() {
            self.orphan_cleanup =
                Some(cleanup.with_vectors(memory_vector_app.clone(), thread_vector_app.clone()));
        }
        self.memory_vector_app = memory_vector_app;
        self.thread_vector_app = thread_vector_app;
        self
    }

    pub fn with_memory_app(mut self, app: Arc<MemoryAppImpl>) -> Self {
        self.purge_app = self.purge_app.with_memory_app(app.clone());
        self.orphan_cleanup = Some(
            ThreadGroupOrphanCleanupService::new(self.pool, app).with_vectors(
                self.memory_vector_app.clone(),
                self.thread_vector_app.clone(),
            ),
        );
        self
    }
}

fn require_group_owner_user_id(value: Option<i64>) -> Result<i64, tonic::Status> {
    match value {
        Some(user_id) if user_id > 0 => Ok(user_id),
        Some(_) => Err(tonic::Status::invalid_argument(
            "group_owner_user_id must be greater than zero",
        )),
        None => Err(tonic::Status::invalid_argument(
            "group_owner_user_id is required",
        )),
    }
}

pub(crate) fn decode_group_memory_delete_policy(
    value: i32,
) -> Result<app::app::thread_group::memory_relation::GroupMemoryDeletePolicy, tonic::Status> {
    use crate::protobuf::llm_memory::data::GroupMemoryDeletePolicy as Wire;
    use app::app::thread_group::memory_relation::GroupMemoryDeletePolicy as Policy;
    match Wire::try_from(value) {
        Ok(Wire::Retain) => Ok(Policy::Retain),
        Ok(Wire::Delete) => Ok(Policy::Delete),
        _ => Err(tonic::Status::invalid_argument(
            "on_group_delete must be RETAIN or DELETE",
        )),
    }
}

struct ThreadGroupVectorSearchProvider {
    memory: Option<Arc<MemoryVectorAppImpl>>,
    thread: Option<Arc<app::app::thread_vector::ThreadVectorAppImpl>>,
}

/// Keep the group boundary outside each target-specific vector API. The
/// delegated services know how to search a member, not which enclosing group
/// the caller is currently evaluating.
fn group_filter_allows(
    group_id: i64,
    filter: &crate::protobuf::llm_memory::data::ThreadSearchFilter,
) -> bool {
    !filter
        .thread_group_id
        .is_some_and(|filter_group_id| filter_group_id != group_id)
}

/// Both vector backends expose a scored first result; only the app-layer hit
/// shape is shared here so their target-specific search contracts stay clear.
fn member_search_hit(thread_id: i64, score: Option<f32>) -> Option<ThreadGroupMemberSearchHit> {
    score.map(|score| ThreadGroupMemberSearchHit { thread_id, score })
}

fn hybrid_options_or_default(
    proto: Option<&crate::protobuf::llm_memory::data::HybridSearchOptions>,
) -> infra::infra::memory_vector::repository::HybridOptions {
    super::vector_decode::decode_hybrid_options(proto).unwrap_or(
        infra::infra::memory_vector::repository::HybridOptions {
            strategy: infra::infra::memory_vector::repository::HybridStrategy::Rrf,
            vector_weight: None,
            rrf_k: None,
        },
    )
}

#[tonic::async_trait]
impl ThreadGroupMemberSearchProvider for ThreadGroupVectorSearchProvider {
    async fn search_member(
        &self,
        query: &ThreadGroupMemberSearchQuery,
        group_id: i64,
        thread_id: i64,
    ) -> anyhow::Result<Option<ThreadGroupMemberSearchHit>> {
        match query.target {
            GroupSearchTarget::Thread => {
                let Some(app) = &self.thread else {
                    return Err(anyhow::Error::new(
                        infra::error::LlmMemoryError::Unimplemented(
                            "ThreadGroup Thread search requires ThreadVectorService".to_string(),
                        ),
                    ));
                };
                let mut thread_filter = query.thread_filter.clone().unwrap_or_default();
                if !group_filter_allows(group_id, &thread_filter) {
                    return Ok(None);
                }
                thread_filter.thread_id = Some(thread_id);
                let filter =
                    infra::infra::thread_vector::safe_filter::ThreadSafeFilter::from_proto_filter(
                        &thread_filter,
                    );
                let result = match query.mode {
                    GroupSearchMode::Keyword => {
                        app.search_by_text(&query.query_text, 1, filter.as_ref(), false)
                            .await?
                    }
                    GroupSearchMode::Semantic => {
                        app.search_by_vector(&query.query_vectors, 1, filter.as_ref(), false, None)
                            .await?
                    }
                    GroupSearchMode::Hybrid => {
                        let options = hybrid_options_or_default(query.hybrid_options.as_ref());
                        app.hybrid_search(
                            &query.query_vectors[0],
                            &query.query_text,
                            1,
                            filter.as_ref(),
                            false,
                            &options,
                        )
                        .await?
                    }
                };
                Ok(member_search_hit(
                    thread_id,
                    result.first().map(|item| item.score),
                ))
            }
            GroupSearchTarget::Memory => {
                let Some(app) = &self.memory else {
                    return Err(anyhow::Error::new(
                        infra::error::LlmMemoryError::Unimplemented(
                            "ThreadGroup Memory search requires MemoryVectorService".to_string(),
                        ),
                    ));
                };
                let mut memory_filter = query.memory_filter.clone().unwrap_or_default();
                let mut thread_filter = memory_filter.thread_filter.take().unwrap_or_default();
                if !group_filter_allows(group_id, &thread_filter) {
                    return Ok(None);
                }
                thread_filter.thread_id = Some(thread_id);
                memory_filter.thread_filter = Some(thread_filter.clone());
                let filter =
                    infra::infra::memory_vector::safe_filter::SafeFilter::from_proto_filter(
                        &memory_filter,
                    );
                let user_id = memory_filter.user_id;
                let result = match query.mode {
                    GroupSearchMode::Keyword => {
                        app.search_by_text(
                            &query.query_text,
                            filter.as_ref(),
                            Some(&thread_filter),
                            user_id,
                            1,
                            false,
                        )
                        .await?
                    }
                    GroupSearchMode::Semantic => {
                        app.search_semantic(
                            &query.query_text,
                            filter.as_ref(),
                            Some(&thread_filter),
                            user_id,
                            1,
                            false,
                        )
                        .await?
                    }
                    GroupSearchMode::Hybrid => {
                        let options = hybrid_options_or_default(query.hybrid_options.as_ref());
                        app.hybrid_search(
                            &query.query_vectors,
                            &query.query_text,
                            filter.as_ref(),
                            Some(&thread_filter),
                            user_id,
                            1,
                            &options,
                            false,
                        )
                        .await?
                    }
                };
                Ok(member_search_hit(
                    thread_id,
                    result.first().map(|item| item.score),
                ))
            }
        }
    }
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

pub(super) fn owner_user_id_from_proto(
    user_id: Option<i64>,
    owner_scope: &str,
    field_name: &str,
) -> Result<i64, tonic::Status> {
    let legacy_user_id = if owner_scope.is_empty() {
        None
    } else {
        let user_id =
            common::thread_group_key::parse_legacy_owner_scope(owner_scope).ok_or_else(|| {
                tonic::Status::invalid_argument(format!(
                    "{field_name} must use the legacy user:<id> format"
                ))
            })?;
        Some(user_id)
    };
    match (user_id, legacy_user_id) {
        (Some(current), Some(legacy)) if current != legacy => Err(tonic::Status::invalid_argument(
            format!("{field_name} and its legacy owner_scope disagree"),
        )),
        (Some(current), _) | (None, Some(current)) => Ok(current),
        (None, None) => Err(tonic::Status::invalid_argument(format!(
            "{field_name} is required"
        ))),
    }
}

fn endpoint_from_proto(
    endpoint: Option<&ThreadGroupEndpoint>,
) -> Result<ObservedEndpoint, tonic::Status> {
    let endpoint =
        endpoint.ok_or_else(|| tonic::Status::invalid_argument("subject endpoint is required"))?;
    Ok(ObservedEndpoint {
        source: endpoint.source.clone(),
        identity_scope: if endpoint.identity_scope_known {
            IdentityScope::known(endpoint.identity_scope.clone())
        } else {
            IdentityScope::unknown()
        },
        user_id: owner_user_id_from_proto(endpoint.user_id, &endpoint.owner_scope, "user_id")?,
        native_id: endpoint.native_id.clone(),
    })
}

fn evidence_kind_str(value: i32) -> String {
    match ThreadEvidenceKind::try_from(value).unwrap_or(ThreadEvidenceKind::Unspecified) {
        ThreadEvidenceKind::SourceEvent => "source_event",
        ThreadEvidenceKind::SourceField => "source_field",
        ThreadEvidenceKind::NegativeOrConflict => "negative_or_conflict",
        ThreadEvidenceKind::DerivedRejection => "derived_rejection",
        ThreadEvidenceKind::DerivedConflict => "derived_conflict",
        ThreadEvidenceKind::Unspecified => "source_field",
    }
    .to_string()
}

fn polarity_str(value: i32) -> String {
    match ThreadObservationPolarity::try_from(value)
        .unwrap_or(ThreadObservationPolarity::Unspecified)
    {
        ThreadObservationPolarity::Supports => "supports",
        ThreadObservationPolarity::Negates => "negates",
        ThreadObservationPolarity::Conflicts => "conflicts",
        ThreadObservationPolarity::Unspecified => "supports",
    }
    .to_string()
}

fn confidence_str(value: Option<i32>) -> Option<String> {
    value.map(|value| {
        match ThreadEvidenceConfidence::try_from(value)
            .unwrap_or(ThreadEvidenceConfidence::Unspecified)
        {
            ThreadEvidenceConfidence::Exact => "exact",
            ThreadEvidenceConfidence::Strong => "strong",
            ThreadEvidenceConfidence::Heuristic => "heuristic",
            ThreadEvidenceConfidence::Unsupported => "unsupported",
            ThreadEvidenceConfidence::Unspecified => "unsupported",
        }
        .to_string()
    })
}

fn status_proto(value: &str) -> i32 {
    (match value {
        "active" => ThreadGroupStatus::Active,
        "redirected" => ThreadGroupStatus::Redirected,
        "split" => ThreadGroupStatus::Split,
        _ => ThreadGroupStatus::Unspecified,
    }) as i32
}

fn authority_proto(value: &str) -> i32 {
    (match value {
        "reconciler" => ThreadGroupingAuthority::Reconciler,
        "operator" => ThreadGroupingAuthority::Operator,
        _ => ThreadGroupingAuthority::Unspecified,
    }) as i32
}

fn member_role_proto(value: &str) -> i32 {
    (match value {
        "root" => ThreadGroupMemberRole::Root,
        "member" => ThreadGroupMemberRole::Member,
        _ => ThreadGroupMemberRole::Unspecified,
    }) as i32
}

fn member_state_proto(value: &str) -> i32 {
    (match value {
        "active" => ThreadGroupMemberState::Active,
        "deleted" => ThreadGroupMemberState::Deleted,
        "redirected" => ThreadGroupMemberState::Redirected,
        _ => ThreadGroupMemberState::Unspecified,
    }) as i32
}

fn relation_type_proto(value: &str) -> i32 {
    (match value {
        "delegated" => ThreadRelationType::Delegated,
        "fork" => ThreadRelationType::Fork,
        "continuation" => ThreadRelationType::Continuation,
        _ => ThreadRelationType::Unspecified,
    }) as i32
}

fn relation_state_proto(value: &str) -> i32 {
    (match value {
        "active" => ThreadRelationState::Active,
        "retracted" => ThreadRelationState::Retracted,
        "superseded" => ThreadRelationState::Superseded,
        _ => ThreadRelationState::Unspecified,
    }) as i32
}

fn selection_basis_proto(value: &str) -> i32 {
    (match value {
        "source_exact" => ThreadSelectionBasis::SourceExact,
        "source_strong" => ThreadSelectionBasis::SourceStrong,
        "operator_confirmation" => ThreadSelectionBasis::OperatorConfirmation,
        _ => ThreadSelectionBasis::Unspecified,
    }) as i32
}

fn confidence_proto(value: Option<&str>) -> Option<i32> {
    value.map(|value| {
        (match value {
            "exact" => ThreadEvidenceConfidence::Exact,
            "strong" => ThreadEvidenceConfidence::Strong,
            "heuristic" => ThreadEvidenceConfidence::Heuristic,
            _ => ThreadEvidenceConfidence::Unsupported,
        }) as i32
    })
}

fn observation_state_proto(value: &str) -> i32 {
    (match value {
        "pending" => ThreadObservationState::Pending,
        "candidate" => ThreadObservationState::Candidate,
        "selected" => ThreadObservationState::Selected,
        "conflict" => ThreadObservationState::Conflict,
        "superseded" => ThreadObservationState::Superseded,
        "unsupported" => ThreadObservationState::Unsupported,
        _ => ThreadObservationState::Unspecified,
    }) as i32
}

fn candidate_state_proto(value: &str) -> i32 {
    (match value {
        "pending" => ThreadCandidateState::Pending,
        "candidate" => ThreadCandidateState::Candidate,
        "ambiguous" => ThreadCandidateState::Ambiguous,
        "conflict" => ThreadCandidateState::Conflict,
        "unsupported" => ThreadCandidateState::Unsupported,
        "superseded" => ThreadCandidateState::Superseded,
        _ => ThreadCandidateState::Unspecified,
    }) as i32
}

fn thread_display_proto(view: &ThreadDisplayView) -> ThreadGroupThreadDisplay {
    ThreadGroupThreadDisplay {
        thread_id: view.thread_id,
        thread_canonical_key: view.thread_canonical_key.clone(),
        description: view.description.clone(),
        source: view.source.clone(),
        created_at: view.created_at,
        last_message_at: view.last_message_at,
        deleted_at: view.deleted_at,
    }
}

fn group_proto(view: &ThreadGroupView) -> ThreadGroup {
    ThreadGroup {
        id: Some(ThreadGroupId {
            value: view.id,
            user_id: Some(view.user_id),
        }),
        group_canonical_key: view.group_canonical_key.clone(),
        title: view.title.clone(),
        status: status_proto(&view.status),
        grouping_authority: authority_proto(&view.grouping_authority),
        redirect_to_group_id: view.redirect_to_group_id,
        latest_activity_at: view.latest_activity_at,
        root_thread_id: view.root_thread_id,
        root_thread_canonical_key: view.root_thread_canonical_key.clone(),
        root_display: view.root_display.as_ref().map(thread_display_proto),
        active_member_count: view.active_member_count,
        deleted_member_count: view.deleted_member_count,
        unresolved_count: view.unresolved_count,
        membership_snapshot_digest: view.membership_snapshot_digest.clone(),
        user_id: Some(view.user_id),
    }
}

fn member_proto(
    row: &ThreadGroupMemberRow,
    display: Option<&ThreadDisplayView>,
) -> ThreadGroupMember {
    ThreadGroupMember {
        group_id: row.group_id,
        thread_id: row.thread_id,
        thread_canonical_key: row.thread_canonical_key.clone(),
        owner_scope: legacy_owner_scope(row.user_id),
        source: row.source.clone(),
        identity_scope: row.identity_scope.clone(),
        native_id: row.native_id.clone(),
        role: member_role_proto(&row.role),
        state: member_state_proto(&row.state),
        provenance: authority_proto(&row.provenance),
        deleted_at: row.deleted_at,
        display: display.map(thread_display_proto),
        user_id: Some(row.user_id),
    }
}

fn relation_proto(
    row: &ThreadRelationRow,
    endpoint: Option<&ThreadRelationEndpointView>,
) -> ThreadRelation {
    ThreadRelation {
        id: row.id,
        parent_thread_id: row.parent_thread_id,
        child_thread_id: row.child_thread_id,
        parent_thread_canonical_key: row.parent_thread_canonical_key.clone(),
        child_thread_canonical_key: row.child_thread_canonical_key.clone(),
        relation_type: relation_type_proto(&row.relation_type),
        state: relation_state_proto(&row.state),
        selection_basis: selection_basis_proto(&row.selection_basis),
        source_confidence: confidence_proto(row.source_confidence.as_deref()),
        selected_observation_id: row.selected_observation_id,
        selected_operator_decision_id: row.selected_operator_decision_id,
        parent_group_id: endpoint.and_then(|value| value.parent_group_id),
        child_group_id: endpoint.and_then(|value| value.child_group_id),
        parent_display: endpoint
            .and_then(|value| value.parent_display.as_ref())
            .map(thread_display_proto),
        child_display: endpoint
            .and_then(|value| value.child_display.as_ref())
            .map(thread_display_proto),
        parent_user_id: Some(row.parent_user_id),
        child_user_id: Some(row.child_user_id),
    }
}

fn candidate_proto(row: &ThreadGroupCandidateAssociationRow) -> ThreadGroupCandidateAssociation {
    ThreadGroupCandidateAssociation {
        id: row.id,
        subject_thread_id: row.subject_thread_id,
        subject_source: row.subject_source.clone(),
        subject_identity_scope_known: row.subject_identity_scope_known,
        subject_identity_scope_value: row.subject_identity_scope_value.clone(),
        subject_owner_scope: legacy_owner_scope(row.subject_user_id),
        subject_native_id: row.subject_native_id.clone(),
        candidate_group_id: row.candidate_group_id,
        candidate_parent_thread_id: row.candidate_parent_thread_id,
        state: candidate_state_proto(&row.state),
        selected_observation_id: row.selected_observation_id,
        subject_user_id: Some(row.subject_user_id),
    }
}

fn evidence_kind_proto(value: &str) -> i32 {
    (match value {
        "source_event" => ThreadEvidenceKind::SourceEvent,
        "source_field" => ThreadEvidenceKind::SourceField,
        "negative_or_conflict" => ThreadEvidenceKind::NegativeOrConflict,
        "derived_rejection" => ThreadEvidenceKind::DerivedRejection,
        "derived_conflict" => ThreadEvidenceKind::DerivedConflict,
        _ => ThreadEvidenceKind::Unspecified,
    }) as i32
}

fn polarity_proto(value: &str) -> i32 {
    (match value {
        "supports" => ThreadObservationPolarity::Supports,
        "negates" => ThreadObservationPolarity::Negates,
        "conflicts" => ThreadObservationPolarity::Conflicts,
        _ => ThreadObservationPolarity::Unspecified,
    }) as i32
}

fn observation_proto(row: &ThreadObservationRow) -> ThreadObservation {
    ThreadObservation {
        id: row.id,
        subject_source: row.subject_source.clone(),
        subject_identity_scope_known: row.subject_identity_scope_known,
        subject_identity_scope_value: row.subject_identity_scope_value.clone(),
        subject_owner_scope: legacy_owner_scope(row.subject_user_id),
        subject_native_id: row.subject_native_id.clone(),
        candidate_parent_present: row.candidate_parent_present,
        candidate_parent_source: Some(row.candidate_parent_source.clone()),
        candidate_parent_identity_scope_known: row.candidate_parent_identity_scope_known,
        candidate_parent_identity_scope_value: Some(
            row.candidate_parent_identity_scope_value.clone(),
        ),
        candidate_parent_owner_scope: row.candidate_parent_user_id.map(legacy_owner_scope),
        candidate_parent_native_id: Some(row.candidate_parent_native_id.clone()),
        relation_kind: row.relation_kind.clone(),
        evidence_kind: evidence_kind_proto(&row.evidence_kind),
        polarity: polarity_proto(&row.polarity),
        source_confidence: confidence_proto(row.source_confidence.as_deref()),
        state: observation_state_proto(&row.state),
        adapter_version: String::new(),
        source_record_ref: row.source_record_ref.clone().unwrap_or_default(),
        observed_at: row.observed_at,
        subject_user_id: Some(row.subject_user_id),
        candidate_parent_user_id: row.candidate_parent_user_id,
    }
}

fn manual_collection_proto(
    row: &infra::infra::thread_group::rows::ManualCollectionRow,
) -> ManualCollection {
    ManualCollection {
        id: row.id,
        owner_scope: legacy_owner_scope(row.user_id),
        title: row.title.clone(),
        created_at: row.created_at,
        updated_at: row.updated_at,
        user_id: Some(row.user_id),
    }
}

fn manual_collection_member_proto(
    row: &infra::infra::thread_group::rows::ManualCollectionMemberRow,
) -> ManualCollectionMember {
    ManualCollectionMember {
        collection_id: row.collection_id,
        thread_id: row.thread_id,
        owner_scope: legacy_owner_scope(row.user_id),
        user_id: Some(row.user_id),
    }
}

fn observation_input_from_proto(
    input: &crate::protobuf::llm_memory::data::ThreadGroupObservationInput,
    now: i64,
) -> Result<ObservationInput, tonic::Status> {
    Ok(ObservationInput {
        subject: endpoint_from_proto(input.subject.as_ref())?,
        candidate_parent: input
            .candidate_parent
            .as_ref()
            .map(|parent| endpoint_from_proto(Some(parent)))
            .transpose()?,
        relation_kind: input.relation_kind.clone(),
        evidence_kind: evidence_kind_str(input.evidence_kind),
        polarity: polarity_str(input.polarity),
        source_confidence: confidence_str(input.source_confidence),
        adapter_version: input.adapter_version.clone(),
        source_record_ref: input.source_record_ref.clone(),
        import_run_id: None,
        observed_at: input.observed_at.unwrap_or(now),
    })
}

#[derive(Clone, Debug, PartialEq)]
struct MappedObservationBatch {
    subject: ObservedEndpoint,
    observations: Vec<ObservationInput>,
}

/// Record and preview must convert the same request payload identically;
/// keeping this batch mapper shared prevents a transport-only dry-run from
/// silently drifting from the write path.
fn observation_batch_from_proto(
    subject: Option<&crate::protobuf::llm_memory::data::ThreadGroupEndpoint>,
    observations: &[crate::protobuf::llm_memory::data::ThreadGroupObservationInput],
    now: i64,
) -> Result<MappedObservationBatch, tonic::Status> {
    Ok(MappedObservationBatch {
        subject: endpoint_from_proto(subject)?,
        observations: observations
            .iter()
            .map(|input| observation_input_from_proto(input, now))
            .collect::<Result<Vec<_>, _>>()?,
    })
}

fn member_search_query_from_proto(
    query: crate::protobuf::llm_memory::service::ThreadGroupMemberSearchQuery,
) -> anyhow::Result<ThreadGroupMemberSearchQuery> {
    let target = match ThreadGroupSearchTarget::try_from(query.target) {
        Ok(ThreadGroupSearchTarget::Thread) => GroupSearchTarget::Thread,
        Ok(ThreadGroupSearchTarget::Memory) => GroupSearchTarget::Memory,
        _ => {
            return Err(anyhow::Error::new(
                infra::error::LlmMemoryError::InvalidArgument(
                    "thread-group search target must be THREAD or MEMORY".to_string(),
                ),
            ));
        }
    };
    let mode = match ThreadGroupSearchMode::try_from(query.mode) {
        Ok(ThreadGroupSearchMode::Keyword) => GroupSearchMode::Keyword,
        Ok(ThreadGroupSearchMode::Semantic) => GroupSearchMode::Semantic,
        Ok(ThreadGroupSearchMode::Hybrid) => GroupSearchMode::Hybrid,
        _ => {
            return Err(anyhow::Error::new(
                infra::error::LlmMemoryError::InvalidArgument(
                    "thread-group search mode must be KEYWORD, SEMANTIC, or HYBRID".to_string(),
                ),
            ));
        }
    };
    Ok(ThreadGroupMemberSearchQuery {
        target,
        mode,
        query_text: query.query_text,
        query_vectors: query
            .query_vectors
            .into_iter()
            .map(|vector| vector.values)
            .collect(),
        thread_filter: query.thread_filter,
        memory_filter: query.memory_filter,
        hybrid_options: query.hybrid_options,
    })
}

fn search_result_proto(
    result: &app::app::thread_group::ThreadGroupSearchResult,
) -> crate::protobuf::llm_memory::service::ThreadGroupSearchResult {
    crate::protobuf::llm_memory::service::ThreadGroupSearchResult {
        group: Some(group_proto(&result.group)),
        relevance_score: result.relevance_score,
        witnesses: result
            .witnesses
            .iter()
            .map(|witness| ThreadGroupSearchWitness {
                predicate_index: witness.predicate_index as i32,
                target: match witness.target {
                    GroupSearchTarget::Thread => ThreadGroupSearchTarget::Thread as i32,
                    GroupSearchTarget::Memory => ThreadGroupSearchTarget::Memory as i32,
                },
                thread_id: witness.member.thread_id.unwrap_or_default(),
                thread_canonical_key: witness.member.thread_canonical_key.clone(),
                score: witness.score,
                member: Some(thread_display_proto(&witness.member)),
            })
            .collect(),
        root_member: result.group.root_display.as_ref().map(thread_display_proto),
        current_member_count: result.group.active_member_count + result.group.deleted_member_count,
        active_member_count: result.group.active_member_count,
        deleted_member_count: result.group.deleted_member_count,
    }
}

#[tonic::async_trait]
impl ThreadGroupService for ThreadGroupGrpcImpl {
    type FindThreadGroupListStream = BoxStream<'static, Result<ThreadGroup, tonic::Status>>;

    async fn get_thread_group_capabilities(
        &self,
        _request: tonic::Request<ThreadGroupCapabilitiesRequest>,
    ) -> Result<Response<ThreadGroupCapabilitiesResponse>, tonic::Status> {
        Ok(Response::new(ThreadGroupCapabilitiesResponse {
            supports_independent_group_owner_scope: true,
        }))
    }

    #[tracing::instrument(skip(self, request))]
    async fn find_thread_group_list(
        &self,
        request: tonic::Request<FindThreadGroupListRequest>,
    ) -> Result<Response<Self::FindThreadGroupListStream>, tonic::Status> {
        let request = request.into_inner();
        let views = self
            .read_app
            .list_groups(
                request.include_inactive,
                request.limit.map(i64::from),
                request.offset,
                request.user_id,
            )
            .await
            .map_err(|e| handle_error(&e))?;
        let stream = stream! {
            for view in views {
                yield Ok(group_proto(&view));
            }
        };
        Ok(Response::new(Box::pin(stream)))
    }

    #[tracing::instrument(skip(self, request))]
    async fn search_thread_groups(
        &self,
        request: tonic::Request<SearchThreadGroupsRequest>,
    ) -> Result<Response<SearchThreadGroupsResponse>, tonic::Status> {
        let request = request.into_inner();
        let queries = request
            .predicates
            .into_iter()
            .map(member_search_query_from_proto)
            .collect::<anyhow::Result<Vec<_>>>()
            .map_err(|error| handle_error(&error))?;
        let page_size = request.page_size.unwrap_or(20);
        if page_size <= 0 {
            return Err(tonic::Status::invalid_argument(
                "thread-group search page_size must be greater than zero",
            ));
        }
        let provider = ThreadGroupVectorSearchProvider {
            memory: self.memory_vector_app.clone(),
            thread: self.thread_vector_app.clone(),
        };
        let page = self
            .read_app
            .search_groups(
                &provider,
                &queries,
                request.allow_cross_member,
                page_size as usize,
                request.page_token.as_deref(),
                request.browse_filter.as_ref(),
                request.user_id,
            )
            .await
            .map_err(|error| handle_error(&error))?;
        Ok(Response::new(SearchThreadGroupsResponse {
            results: page.results.iter().map(search_result_proto).collect(),
            next_page_token: page.next_page_token,
        }))
    }

    #[tracing::instrument(skip(self, request))]
    async fn find_thread_group_lineage(
        &self,
        request: tonic::Request<ThreadGroupId>,
    ) -> Result<Response<ThreadGroupLineage>, tonic::Status> {
        let request = request.into_inner();
        let group_id = request.value;
        let lineage = self
            .read_app
            .get_lineage(group_id, request.user_id)
            .await
            .map_err(|e| handle_error(&e))?
            .ok_or_else(|| {
                tonic::Status::not_found(format!("thread group {group_id} not found"))
            })?;
        Ok(Response::new(ThreadGroupLineage {
            group: Some(group_proto(&lineage.group)),
            members: lineage
                .members
                .iter()
                .zip(lineage.member_displays.iter())
                .map(|(member, display)| member_proto(member, Some(display)))
                .collect(),
            relations: lineage
                .relations
                .iter()
                .map(|relation| {
                    relation_proto(
                        relation,
                        lineage
                            .relation_endpoints
                            .iter()
                            .find(|endpoint| endpoint.relation_id == relation.id),
                    )
                })
                .collect(),
            unresolved: lineage.unresolved.iter().map(candidate_proto).collect(),
            observations: lineage.observations.iter().map(observation_proto).collect(),
        }))
    }

    #[tracing::instrument(skip(self, request))]
    async fn find_thread_group_summary(
        &self,
        request: tonic::Request<FindThreadGroupSummaryRequest>,
    ) -> Result<Response<ThreadGroupSummaryResponse>, tonic::Status> {
        let request = request.into_inner();
        let summary = self
            .read_app
            .find_group_summary(request.group_id, request.user_id)
            .await
            .map_err(|e| handle_error(&e))?;
        Ok(Response::new(ThreadGroupSummaryResponse { summary }))
    }

    #[tracing::instrument(skip(self, request))]
    async fn count_thread_group_summaries(
        &self,
        request: tonic::Request<CountThreadGroupSummariesRequest>,
    ) -> Result<Response<CountThreadGroupSummariesResponse>, tonic::Status> {
        let request = request.into_inner();
        let count = self
            .read_app
            .count_group_summaries(request.user_id)
            .await
            .map_err(|e| handle_error(&e))?;
        Ok(Response::new(CountThreadGroupSummariesResponse { count }))
    }

    #[tracing::instrument(skip(self, request))]
    async fn record_thread_group_observations(
        &self,
        request: tonic::Request<RecordThreadGroupObservationsRequest>,
    ) -> Result<Response<RecordThreadGroupObservationsResponse>, tonic::Status> {
        let request = request.into_inner();
        let group_owner_user_id = require_group_owner_user_id(request.group_owner_user_id)?;
        let now = now_millis();
        let mapped =
            observation_batch_from_proto(request.subject.as_ref(), &request.observations, now)?;
        let operation_id = request
            .operation_id
            .unwrap_or_else(|| format!("record:{}", request.subject_thread_id));
        let outcome = self
            .reconcile_app
            .reconcile_imported_subject(
                request.subject_thread_id,
                &mapped.subject,
                &mapped.observations,
                group_owner_user_id,
                &operation_id,
                now,
            )
            .await
            .map_err(|e| handle_error(&e))?;
        Ok(Response::new(RecordThreadGroupObservationsResponse {
            observation_ids: outcome.observation_ids,
            relation_selected: outcome.relation_selected,
            group_id: outcome.group_id,
            conflict: outcome.conflict,
            pending: outcome.pending as i32,
        }))
    }

    #[tracing::instrument(skip(self, request))]
    async fn process_thread_group_event(
        &self,
        request: tonic::Request<ProcessThreadGroupEventRequest>,
    ) -> Result<Response<ProcessThreadGroupEventResponse>, tonic::Status> {
        let event_id = request.into_inner().event_id;
        if event_id.is_empty() {
            return Err(tonic::Status::invalid_argument(
                "event_id must not be empty",
            ));
        }
        // State transitions are applied transactionally when the event
        // row is produced, so a recorded event is already processed.
        // Delivery retries therefore observe the same immutable row and
        // are a no-op (design 8.3).
        let row = self
            .outbox
            .find_by_event_id(&event_id)
            .await
            .map_err(|e| handle_error(&e))?;
        match row {
            Some(row) => Ok(Response::new(ProcessThreadGroupEventResponse {
                already_processed: true,
                event_type: row.event_type,
            })),
            None => Ok(Response::new(ProcessThreadGroupEventResponse {
                already_processed: false,
                event_type: String::new(),
            })),
        }
    }

    #[tracing::instrument(skip(self, request))]
    async fn preview_thread_group_import(
        &self,
        request: tonic::Request<PreviewThreadGroupImportRequest>,
    ) -> Result<Response<PreviewThreadGroupImportResponse>, tonic::Status> {
        let request = request.into_inner();
        let _group_owner_user_id = require_group_owner_user_id(request.group_owner_user_id)?;
        let now = now_millis();
        let mapped =
            observation_batch_from_proto(request.subject.as_ref(), &request.observations, now)?;
        let preview = self
            .reconcile_app
            .preview_import(
                &mapped.subject,
                &mapped.observations,
                request.explicit_override,
            )
            .await
            .map_err(|e| handle_error(&e))?;
        Ok(Response::new(PreviewThreadGroupImportResponse {
            suppressed: preview.suppressed,
            would_revive: preview.would_revive,
            planned_observations: preview.planned_observations,
            planned_relations: preview.planned_relations,
            pending: preview.pending,
            conflict: preview.conflict,
        }))
    }

    #[tracing::instrument(skip(self, request))]
    async fn record_thread_group_operator_decision(
        &self,
        request: tonic::Request<RecordThreadGroupOperatorDecisionRequest>,
    ) -> Result<Response<RecordThreadGroupOperatorDecisionResponse>, tonic::Status> {
        let request = request.into_inner();
        if request.decision.trim().is_empty() || request.reason.trim().is_empty() {
            return Err(tonic::Status::invalid_argument(
                "decision and reason must not be empty",
            ));
        }
        let outcome = self
            .reconcile_app
            .record_operator_decision(
                request.candidate_association_id,
                &request.decision,
                &request.actor_id,
                &request.reason,
                now_millis(),
            )
            .await
            .map_err(|e| handle_error(&e))?;
        Ok(Response::new(RecordThreadGroupOperatorDecisionResponse {
            decision_id: outcome.decision_id,
            relation_id: outcome.relation_id,
            state: outcome.state,
        }))
    }

    #[tracing::instrument(skip(self, request))]
    async fn merge_thread_groups(
        &self,
        request: tonic::Request<MergeThreadGroupsRequest>,
    ) -> Result<Response<MergeThreadGroupsResponse>, tonic::Status> {
        let request = request.into_inner();
        if request.reason.trim().is_empty() {
            return Err(tonic::Status::invalid_argument("reason must not be empty"));
        }
        let outcome = self
            .operator_app
            .merge_groups(
                request.source_group_id,
                request.target_group_id,
                &request.actor_id,
                &request.reason,
                now_millis(),
            )
            .await
            .map_err(|e| handle_error(&e))?;
        Ok(Response::new(MergeThreadGroupsResponse {
            target_group_id: outcome.target_group_id,
            moved_members: outcome.moved_members as i32,
            already_merged: outcome.already_merged,
        }))
    }

    #[tracing::instrument(skip(self, request))]
    async fn split_thread_group(
        &self,
        request: tonic::Request<SplitThreadGroupRequest>,
    ) -> Result<Response<SplitThreadGroupResponse>, tonic::Status> {
        let request = request.into_inner();
        if request.reason.trim().is_empty() {
            return Err(tonic::Status::invalid_argument("reason must not be empty"));
        }
        let partitions: Vec<Vec<String>> = request
            .partitions
            .iter()
            .map(|partition| partition.thread_canonical_keys.clone())
            .collect();
        let successors = self
            .operator_app
            .split_group(
                request.source_group_id,
                &partitions,
                &request.actor_id,
                &request.reason,
                now_millis(),
            )
            .await
            .map_err(|e| handle_error(&e))?;
        Ok(Response::new(SplitThreadGroupResponse {
            successor_group_ids: successors,
        }))
    }

    #[tracing::instrument(skip(self, request))]
    async fn attach_thread_group_member(
        &self,
        request: tonic::Request<AttachThreadGroupMemberRequest>,
    ) -> Result<Response<SuccessResponse>, tonic::Status> {
        let request = request.into_inner();
        let now = now_millis();
        let endpoint = self
            .reconcile_app
            .source_endpoint_for_thread(request.thread_id)
            .await
            .map_err(|e| handle_error(&e))?;
        // Thread ownership is the typed creator id, whether source-backed
        // or manually created.
        let user_id = match endpoint.as_ref() {
            Some(endpoint) => endpoint.user_id,
            None => self
                .reconcile_app
                .thread_user_id(request.thread_id)
                .await
                .map_err(|e| handle_error(&e))?
                .ok_or_else(|| tonic::Status::not_found("thread owner not found"))?,
        };
        // Resolve (or generate) the thread's canonical key server-side;
        // an optional caller-supplied key is validated, never trusted.
        let canonical_key = self
            .reconcile_app
            .ensure_thread_canonical_key(request.thread_id, user_id, now)
            .await
            .map_err(|e| handle_error(&e))?;
        if !request.thread_canonical_key.is_empty() && request.thread_canonical_key != canonical_key
        {
            return Err(tonic::Status::invalid_argument(
                "thread_canonical_key does not match the resolved thread key",
            ));
        }
        self.operator_app
            .attach_member(
                request.group_id,
                request.thread_id,
                &canonical_key,
                endpoint.as_ref(),
                now,
            )
            .await
            .map_err(|e| handle_error(&e))?;
        Ok(Response::new(SuccessResponse { is_success: true }))
    }

    #[tracing::instrument(skip(self, request))]
    async fn link_thread_group_memory(
        &self,
        request: tonic::Request<LinkThreadGroupMemoryRequest>,
    ) -> Result<Response<SuccessResponse>, tonic::Status> {
        let request = request.into_inner();
        let relation = request
            .relation
            .ok_or_else(|| tonic::Status::invalid_argument("relation is required"))?;
        let group_id = relation
            .group_id
            .ok_or_else(|| tonic::Status::invalid_argument("group_id is required"))?;
        let memory_id = request
            .memory_id
            .ok_or_else(|| tonic::Status::invalid_argument("memory_id is required"))?;
        let policy = decode_group_memory_delete_policy(relation.on_group_delete)?;
        self.memory_relation_app
            .link_existing(group_id.value, memory_id.value, &relation.purpose, policy)
            .await
            .map_err(|e| handle_error(&e))?;
        Ok(Response::new(SuccessResponse { is_success: true }))
    }

    #[tracing::instrument(skip(self, request))]
    async fn create_manual_collection(
        &self,
        request: tonic::Request<CreateManualCollectionRequest>,
    ) -> Result<Response<CreateManualCollectionResponse>, tonic::Status> {
        let request = request.into_inner();
        if request.title.trim().is_empty()
            || (request.user_id.is_none() && request.owner_scope.trim().is_empty())
        {
            return Err(tonic::Status::invalid_argument(
                "user_id (or legacy owner_scope) and title are required",
            ));
        }
        let user_id = owner_user_id_from_proto(request.user_id, &request.owner_scope, "user_id")?;
        let id = self
            .operator_app
            .create_manual_collection(user_id, &request.title, now_millis())
            .await
            .map_err(|e| handle_error(&e))?;
        Ok(Response::new(CreateManualCollectionResponse { id }))
    }

    type FindManualCollectionListStream =
        BoxStream<'static, Result<ManualCollection, tonic::Status>>;

    #[tracing::instrument(skip(self, request))]
    async fn find_manual_collection_list(
        &self,
        request: tonic::Request<FindManualCollectionListRequest>,
    ) -> Result<Response<Self::FindManualCollectionListStream>, tonic::Status> {
        let request = request.into_inner();
        let user_id = owner_user_id_from_proto(request.user_id, &request.owner_scope, "user_id")?;
        let rows = self
            .operator_app
            .list_manual_collections(user_id, request.limit.map(i64::from), request.offset)
            .await
            .map_err(|e| handle_error(&e))?;
        let stream = stream! {
            for row in rows {
                yield Ok(manual_collection_proto(&row));
            }
        };
        Ok(Response::new(Box::pin(stream)))
    }

    #[tracing::instrument(skip(self, request))]
    async fn rename_manual_collection(
        &self,
        request: tonic::Request<RenameManualCollectionRequest>,
    ) -> Result<Response<SuccessResponse>, tonic::Status> {
        let request = request.into_inner();
        let renamed = self
            .operator_app
            .rename_manual_collection(
                request.collection_id,
                request.user_id,
                &request.title,
                now_millis(),
            )
            .await
            .map_err(|e| handle_error(&e))?;
        if !renamed {
            return Err(tonic::Status::not_found("manual collection not found"));
        }
        Ok(Response::new(SuccessResponse { is_success: true }))
    }

    #[tracing::instrument(skip(self, request))]
    async fn delete_manual_collection(
        &self,
        request: tonic::Request<DeleteManualCollectionRequest>,
    ) -> Result<Response<SuccessResponse>, tonic::Status> {
        let request = request.into_inner();
        let deleted = self
            .operator_app
            .delete_manual_collection(request.collection_id, request.user_id)
            .await
            .map_err(|e| handle_error(&e))?;
        if !deleted {
            return Err(tonic::Status::not_found("manual collection not found"));
        }
        Ok(Response::new(SuccessResponse { is_success: true }))
    }

    #[tracing::instrument(skip(self, request))]
    async fn attach_manual_collection_member(
        &self,
        request: tonic::Request<AttachManualCollectionMemberRequest>,
    ) -> Result<Response<SuccessResponse>, tonic::Status> {
        let request = request.into_inner();
        let user_id = owner_user_id_from_proto(request.user_id, &request.owner_scope, "user_id")?;
        self.operator_app
            .attach_manual_collection_member(
                request.collection_id,
                request.thread_id,
                user_id,
                now_millis(),
            )
            .await
            .map_err(|e| handle_error(&e))?;
        Ok(Response::new(SuccessResponse { is_success: true }))
    }

    #[tracing::instrument(skip(self, request))]
    async fn detach_manual_collection_member(
        &self,
        request: tonic::Request<DetachManualCollectionMemberRequest>,
    ) -> Result<Response<SuccessResponse>, tonic::Status> {
        let request = request.into_inner();
        self.operator_app
            .detach_manual_collection_member(
                request.collection_id,
                request.thread_id,
                request.user_id,
                now_millis(),
            )
            .await
            .map_err(|e| handle_error(&e))?;
        Ok(Response::new(SuccessResponse { is_success: true }))
    }

    #[tracing::instrument(skip(self, _request))]
    async fn find_thread_group_reconciliation_report(
        &self,
        _request: tonic::Request<FindThreadGroupReconciliationReportRequest>,
    ) -> Result<Response<ThreadGroupReconciliationReport>, tonic::Status> {
        let report = self
            .read_app
            .reconciliation_report()
            .await
            .map_err(|e| handle_error(&e))?;
        Ok(Response::new(ThreadGroupReconciliationReport {
            active_groups: report.active_groups,
            redirected_groups: report.redirected_groups,
            split_groups: report.split_groups,
            pending_candidates: report.pending_candidates,
            ambiguous_candidates: report.ambiguous_candidates,
            conflict_candidates: report.conflict_candidates,
            unsupported_observations: report.unsupported_observations,
        }))
    }

    type FindManualCollectionMembersStream =
        BoxStream<'static, Result<ManualCollectionMember, tonic::Status>>;

    #[tracing::instrument(skip(self, request))]
    async fn find_manual_collection_members(
        &self,
        request: tonic::Request<FindManualCollectionMembersRequest>,
    ) -> Result<Response<Self::FindManualCollectionMembersStream>, tonic::Status> {
        let request = request.into_inner();
        let rows = self
            .operator_app
            .list_manual_collection_members(request.collection_id, request.user_id)
            .await
            .map_err(|e| handle_error(&e))?;
        let stream = stream! {
            for row in rows {
                yield Ok(manual_collection_member_proto(&row));
            }
        };
        Ok(Response::new(Box::pin(stream)))
    }

    #[tracing::instrument(skip(self, _request))]
    async fn preview_global_orphan_cleanup(
        &self,
        _request: tonic::Request<PreviewGlobalOrphanCleanupRequest>,
    ) -> Result<Response<GlobalOrphanCleanupPreview>, tonic::Status> {
        let cleanup = self
            .orphan_cleanup
            .as_ref()
            .ok_or_else(|| tonic::Status::unavailable("cleanup is not configured"))?;
        let preview = cleanup
            .preview()
            .await
            .map_err(|error| handle_error(&error))?;
        Ok(Response::new(GlobalOrphanCleanupPreview {
            digest: preview.digest,
            orphan_relation_count: preview.orphan_relation_count,
            safe_memory_ids: preview.safe_memory_ids,
            unsafe_memory_ids: preview.unsafe_memory_ids,
            orphan_memory_index_ids: preview.orphan_memory_index_ids,
            orphan_thread_index_ids: preview.orphan_thread_index_ids,
            memory_index_available: preview.memory_index_available,
            thread_index_available: preview.thread_index_available,
        }))
    }

    #[tracing::instrument(skip(self, request))]
    async fn execute_global_orphan_cleanup(
        &self,
        request: tonic::Request<ExecuteGlobalOrphanCleanupRequest>,
    ) -> Result<Response<GlobalOrphanCleanupResponse>, tonic::Status> {
        let request = request.into_inner();
        if request.actor_id.trim().is_empty() || request.reason.trim().is_empty() {
            return Err(tonic::Status::invalid_argument(
                "actor_id and reason are required",
            ));
        }
        let cleanup = self
            .orphan_cleanup
            .as_ref()
            .ok_or_else(|| tonic::Status::unavailable("cleanup is not configured"))?;
        tracing::info!(actor_id = %request.actor_id, reason = %request.reason, "global orphan cleanup explicitly requested");
        let outcome = cleanup
            .execute(&request.expected_digest)
            .await
            .map_err(|error| handle_error(&error))?;
        tracing::info!(actor_id = %request.actor_id, deleted_relations = outcome.deleted_relation_count, deleted_memories = outcome.deleted_memory_ids.len(), "global orphan cleanup RDB phase completed");
        Ok(Response::new(GlobalOrphanCleanupResponse {
            deleted_relation_count: outcome.deleted_relation_count,
            deleted_memory_ids: outcome.deleted_memory_ids,
            failed_memory_index_ids: outcome.failed_memory_index_ids,
            failed_thread_index_ids: outcome.failed_thread_index_ids,
            remaining_memory_index_ids: outcome.remaining_memory_index_ids,
            remaining_thread_index_ids: outcome.remaining_thread_index_ids,
            memory_index_available: outcome.memory_index_available,
            thread_index_available: outcome.thread_index_available,
        }))
    }

    #[tracing::instrument(skip(self, request))]
    async fn preview_thread_group_purge(
        &self,
        request: tonic::Request<ThreadGroupId>,
    ) -> Result<Response<ThreadGroupPurgePreview>, tonic::Status> {
        let group_id = request.into_inner().value;
        let preview = self
            .purge_app
            .preview(group_id)
            .await
            .map_err(|e| handle_error(&e))?
            .ok_or_else(|| {
                tonic::Status::not_found(format!("thread group {group_id} not found"))
            })?;
        Ok(Response::new(ThreadGroupPurgePreview {
            group_id: preview.group_id,
            group_status: preview.group_status,
            inactive_memberships: preview.inactive_memberships,
            active_memberships: preview.active_memberships,
            inactive_relations: preview.inactive_relations,
            active_relations: preview.active_relations,
            audit_rows: preview.audit_rows,
            retained_deletion_markers: preview.retained_deletion_markers,
            digest: preview.digest,
            purgeable_deletion_markers: preview.purgeable_deletion_markers,
            dangling_redirects: preview.dangling_redirects,
            summary_memory_id: preview.summary_memory_id,
            delete_memory_count: preview.delete_memory_count,
            retain_memory_count: preview.retain_memory_count,
            blocked_reason: preview.blocked_reason,
        }))
    }

    #[tracing::instrument(skip(self, request))]
    async fn delete_thread_group_inactive_history(
        &self,
        request: tonic::Request<DeleteThreadGroupInactiveHistoryRequest>,
    ) -> Result<Response<DeleteThreadGroupInactiveHistoryResponse>, tonic::Status> {
        let request = request.into_inner();
        if request.reason.trim().is_empty() {
            return Err(tonic::Status::invalid_argument("reason must not be empty"));
        }
        let outcome = self
            .purge_app
            .delete(request.group_id, &request.expected_digest)
            .await
            .map_err(|e| handle_error(&e))?;
        let mut vector_cleanup_failed_ids = Vec::new();
        let mut vector_cleanup_unverified_ids = Vec::new();
        for id in &outcome.deleted_memory_ids {
            if let Some(vector) = &self.memory_vector_app {
                if let Err(error) = vector.delete_vector(*id).await {
                    tracing::error!(memory_id = id, "post-purge vector cleanup failed: {error}");
                    vector_cleanup_failed_ids.push(*id);
                }
            } else {
                vector_cleanup_unverified_ids.push(*id);
            }
        }
        Ok(Response::new(DeleteThreadGroupInactiveHistoryResponse {
            deleted_memberships: outcome.deleted_memberships as i64,
            deleted_relations: outcome.deleted_relations as i64,
            deleted_audit_rows: outcome.deleted_audit_rows as i64,
            group_deleted: outcome.group_deleted,
            deleted_deletion_markers: outcome.deleted_deletion_markers as i64,
            repointed_redirects: outcome.repointed_redirects as i64,
            deleted_memory_ids: outcome.deleted_memory_ids,
            vector_cleanup_failed_ids,
            vector_cleanup_unverified_ids,
        }))
    }
}

#[cfg(test)]
mod search_contract_tests {
    use super::*;
    use crate::protobuf::llm_memory::data::ThreadGroupObservationInput;

    #[test]
    fn group_owner_is_required_positive_and_independent_from_source_owner() {
        assert_eq!(
            require_group_owner_user_id(Some(7)).unwrap(),
            7,
            "a caller may choose a group owner different from the source Thread owner"
        );
        assert_eq!(
            require_group_owner_user_id(None).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
        assert_eq!(
            require_group_owner_user_id(Some(0)).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
    }

    #[test]
    fn hybrid_options_default_when_absent_and_preserve_specified_values() {
        let defaults = hybrid_options_or_default(None);
        assert!(matches!(
            defaults.strategy,
            infra::infra::memory_vector::repository::HybridStrategy::Rrf
        ));
        assert_eq!(defaults.vector_weight, None);
        assert_eq!(defaults.rrf_k, None);

        let proto = crate::protobuf::llm_memory::data::HybridSearchOptions {
            strategy: crate::protobuf::llm_memory::data::HybridStrategy::Weighted as i32,
            vector_weight: Some(0.35),
            rrf_k: Some(43.0),
        };
        let specified = hybrid_options_or_default(Some(&proto));
        assert!(matches!(
            specified.strategy,
            infra::infra::memory_vector::repository::HybridStrategy::Weighted
        ));
        assert_eq!(specified.vector_weight, Some(0.35));
        assert_eq!(specified.rrf_k, Some(43.0));
    }

    #[test]
    fn rrf_and_unknown_strategy_values_fall_back_to_rrf_and_keep_options() {
        for strategy in [
            crate::protobuf::llm_memory::data::HybridStrategy::Rrf as i32,
            i32::MAX,
        ] {
            let proto = crate::protobuf::llm_memory::data::HybridSearchOptions {
                strategy,
                vector_weight: Some(0.2),
                rrf_k: Some(17.0),
            };
            let options = hybrid_options_or_default(Some(&proto));

            assert!(matches!(
                options.strategy,
                infra::infra::memory_vector::repository::HybridStrategy::Rrf
            ));
            assert_eq!(options.vector_weight, Some(0.2));
            assert_eq!(options.rrf_k, Some(17.0));
        }
    }

    #[test]
    fn proto_search_query_maps_target_mode_and_vector_without_exposing_cursor_shape() {
        let request = crate::protobuf::llm_memory::service::ThreadGroupMemberSearchQuery {
            target: ThreadGroupSearchTarget::Memory as i32,
            mode: ThreadGroupSearchMode::Hybrid as i32,
            query_text: "cursor contract".to_string(),
            query_vectors: vec![crate::protobuf::llm_memory::data::EmbeddingVector {
                values: vec![0.25, 0.75],
            }],
            thread_filter: None,
            memory_filter: None,
            hybrid_options: None,
        };
        let mapped = member_search_query_from_proto(request).expect("valid query");
        assert_eq!(mapped.target, GroupSearchTarget::Memory);
        assert_eq!(mapped.mode, GroupSearchMode::Hybrid);
        assert_eq!(mapped.query_vectors, vec![vec![0.25, 0.75]]);

        let response = SearchThreadGroupsResponse {
            results: Vec::new(),
            next_page_token: Some("opaque-keyset-token".to_string()),
        };
        assert_eq!(
            response.next_page_token.as_deref(),
            Some("opaque-keyset-token")
        );
    }

    #[test]
    fn observation_batch_mapping_matches_element_mapping() {
        let subject = ThreadGroupEndpoint {
            source: "codex".into(),
            identity_scope_known: true,
            identity_scope: String::new(),
            owner_scope: "user:1".into(),
            native_id: "child".into(),
            user_id: Some(1),
        };
        let inputs = vec![ThreadGroupObservationInput {
            subject: Some(subject.clone()),
            candidate_parent: None,
            relation_kind: None,
            evidence_kind: ThreadEvidenceKind::SourceField as i32,
            polarity: ThreadObservationPolarity::Supports as i32,
            source_confidence: None,
            adapter_version: "adapter@1".into(),
            source_record_ref: "record:1".into(),
            observed_at: Some(7),
        }];

        let mapped = observation_batch_from_proto(Some(&subject), &inputs, 100)
            .expect("valid typed endpoints");
        assert_eq!(mapped.subject, endpoint_from_proto(Some(&subject)).unwrap());
        assert_eq!(
            mapped.observations,
            inputs
                .iter()
                .map(|input| observation_input_from_proto(input, 100).unwrap())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn group_filter_guard_rejects_mismatched_thread_and_memory_targets() {
        let thread_filter = crate::protobuf::llm_memory::data::ThreadSearchFilter {
            thread_group_id: Some(7),
            ..Default::default()
        };
        assert!(!group_filter_allows(8, &thread_filter));
        assert!(group_filter_allows(7, &thread_filter));

        let memory_filter = crate::protobuf::llm_memory::data::MemorySearchFilter {
            thread_filter: Some(thread_filter),
            ..Default::default()
        };
        let memory_thread_filter = memory_filter.thread_filter.as_ref().expect("filter");
        assert!(!group_filter_allows(8, memory_thread_filter));
        assert!(group_filter_allows(7, memory_thread_filter));
    }

    #[test]
    fn search_query_mapping_preserves_both_targets_and_all_modes() {
        for target in [
            ThreadGroupSearchTarget::Thread,
            ThreadGroupSearchTarget::Memory,
        ] {
            for mode in [
                ThreadGroupSearchMode::Keyword,
                ThreadGroupSearchMode::Semantic,
                ThreadGroupSearchMode::Hybrid,
            ] {
                let mapped = member_search_query_from_proto(
                    crate::protobuf::llm_memory::service::ThreadGroupMemberSearchQuery {
                        target: target as i32,
                        mode: mode as i32,
                        query_text: "query".into(),
                        query_vectors: vec![crate::protobuf::llm_memory::data::EmbeddingVector {
                            values: vec![0.25, 0.75],
                        }],
                        thread_filter: None,
                        memory_filter: None,
                        hybrid_options: None,
                    },
                )
                .expect("valid target and mode");
                assert_eq!(
                    mapped.target,
                    match target {
                        ThreadGroupSearchTarget::Thread => GroupSearchTarget::Thread,
                        ThreadGroupSearchTarget::Memory => GroupSearchTarget::Memory,
                        ThreadGroupSearchTarget::Unspecified => unreachable!(),
                    }
                );
                assert_eq!(
                    mapped.mode,
                    match mode {
                        ThreadGroupSearchMode::Keyword => GroupSearchMode::Keyword,
                        ThreadGroupSearchMode::Semantic => GroupSearchMode::Semantic,
                        ThreadGroupSearchMode::Hybrid => GroupSearchMode::Hybrid,
                        ThreadGroupSearchMode::Unspecified => unreachable!(),
                    }
                );
            }
        }
    }

    #[test]
    fn member_search_hit_conversion_is_empty_result_safe() {
        assert_eq!(
            member_search_hit(42, Some(0.75)),
            Some(ThreadGroupMemberSearchHit {
                thread_id: 42,
                score: 0.75,
            })
        );
        assert_eq!(member_search_hit(42, None), None);
    }

    #[test]
    fn unspecified_target_and_mode_are_rejected_at_service_boundary() {
        let request = crate::protobuf::llm_memory::service::ThreadGroupMemberSearchQuery::default();
        let error = member_search_query_from_proto(request).expect_err("invalid query");
        assert!(error.to_string().contains("target"));
    }

    #[test]
    fn relation_proto_carries_batch_resolved_cross_group_endpoint_display() {
        let row = ThreadRelationRow {
            id: 91,
            parent_thread_id: Some(11),
            child_thread_id: Some(22),
            parent_thread_canonical_key: "parent".into(),
            child_thread_canonical_key: "child".into(),
            parent_user_id: 1,
            parent_source: Some("codex".into()),
            parent_identity_scope: None,
            parent_native_id: None,
            child_user_id: 1,
            child_source: Some("codex".into()),
            child_identity_scope: None,
            child_native_id: None,
            relation_type: "continuation".into(),
            state: "active".into(),
            selection_basis: "source_exact".into(),
            source_confidence: Some("exact".into()),
            selected_observation_id: None,
            selected_operator_decision_id: None,
            created_at: 0,
            updated_at: 0,
        };
        let endpoint = ThreadRelationEndpointView {
            relation_id: 91,
            parent_group_id: Some(7),
            child_group_id: Some(8),
            parent_display: Some(ThreadDisplayView {
                description: Some("Parent".into()),
                ..Default::default()
            }),
            child_display: Some(ThreadDisplayView {
                description: Some("Child".into()),
                ..Default::default()
            }),
        };
        let mapped = relation_proto(&row, Some(&endpoint));
        assert_eq!(mapped.parent_group_id, Some(7));
        assert_eq!(mapped.child_group_id, Some(8));
        assert_eq!(
            mapped.parent_display.unwrap().description.as_deref(),
            Some("Parent")
        );
        assert_eq!(
            mapped.child_display.unwrap().description.as_deref(),
            Some("Child")
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protobuf::llm_memory::data::ThreadGroupObservationInput;

    #[test]
    fn endpoint_scope_known_and_unknown_stay_distinct() {
        let known = endpoint_from_proto(Some(&ThreadGroupEndpoint {
            source: "codex".into(),
            identity_scope_known: true,
            identity_scope: String::new(),
            owner_scope: "user:1".into(),
            native_id: "s1".into(),
            user_id: None,
        }))
        .expect("legacy owner scope is accepted");
        assert_eq!(known.identity_scope, IdentityScope::known(""));
        assert_eq!(known.user_id, 1);

        let unknown = endpoint_from_proto(Some(&ThreadGroupEndpoint {
            source: "codex".into(),
            identity_scope_known: false,
            identity_scope: "ignored".into(),
            owner_scope: String::new(),
            native_id: "s1".into(),
            user_id: Some(1),
        }))
        .expect("typed owner is accepted");
        assert_eq!(unknown.identity_scope, IdentityScope::unknown());
        assert_eq!(unknown.user_id, 1);
    }

    #[test]
    fn endpoint_owner_fields_reject_disagreement() {
        let error = endpoint_from_proto(Some(&ThreadGroupEndpoint {
            source: "codex".into(),
            identity_scope_known: true,
            identity_scope: String::new(),
            owner_scope: "user:1".into(),
            native_id: "s1".into(),
            user_id: Some(2),
        }))
        .expect_err("conflicting owner representations are rejected");
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }

    #[test]
    fn observation_input_maps_evidence_kind_polarity_and_confidence() {
        let input = ThreadGroupObservationInput {
            subject: Some(ThreadGroupEndpoint {
                source: "codex".into(),
                identity_scope_known: true,
                identity_scope: String::new(),
                owner_scope: "user:1".into(),
                native_id: "child".into(),
                user_id: Some(1),
            }),
            candidate_parent: Some(ThreadGroupEndpoint {
                source: "codex".into(),
                identity_scope_known: true,
                identity_scope: String::new(),
                owner_scope: "user:1".into(),
                native_id: "parent".into(),
                user_id: Some(1),
            }),
            relation_kind: Some("delegated".into()),
            evidence_kind: ThreadEvidenceKind::SourceField as i32,
            polarity: ThreadObservationPolarity::Supports as i32,
            source_confidence: Some(ThreadEvidenceConfidence::Exact as i32),
            adapter_version: "codex-adapter@1".into(),
            source_record_ref: "codex:session_meta:subagent".into(),
            observed_at: Some(7),
        };
        let mapped = observation_input_from_proto(&input, 100).expect("valid typed endpoints");
        assert_eq!(mapped.evidence_kind, "source_field");
        assert_eq!(mapped.polarity, "supports");
        assert_eq!(mapped.source_confidence.as_deref(), Some("exact"));
        assert_eq!(mapped.observed_at, 7);
        assert_eq!(
            mapped
                .candidate_parent
                .as_ref()
                .map(|parent| parent.native_id.as_str()),
            Some("parent")
        );
    }

    #[test]
    fn enum_mappings_cover_the_storage_tokens() {
        assert_eq!(status_proto("active"), ThreadGroupStatus::Active as i32);
        assert_eq!(
            authority_proto("operator"),
            ThreadGroupingAuthority::Operator as i32
        );
        assert_eq!(
            member_role_proto("root"),
            ThreadGroupMemberRole::Root as i32
        );
        assert_eq!(
            member_state_proto("deleted"),
            ThreadGroupMemberState::Deleted as i32
        );
        assert_eq!(relation_type_proto("fork"), ThreadRelationType::Fork as i32);
        assert_eq!(
            relation_state_proto("retracted"),
            ThreadRelationState::Retracted as i32
        );
        assert_eq!(
            selection_basis_proto("source_strong"),
            ThreadSelectionBasis::SourceStrong as i32
        );
        assert_eq!(
            confidence_proto(Some("unsupported")),
            Some(ThreadEvidenceConfidence::Unsupported as i32)
        );
    }

    #[test]
    fn thread_display_mapping_preserves_placeholder_and_live_fields() {
        let display = ThreadDisplayView {
            thread_id: None,
            thread_canonical_key: "canonical-key".into(),
            description: None,
            source: Some("codex".into()),
            created_at: None,
            last_message_at: None,
            deleted_at: Some(42),
        };
        let mapped = thread_display_proto(&display);
        assert_eq!(mapped.thread_id, None);
        assert_eq!(mapped.thread_canonical_key, "canonical-key");
        assert_eq!(mapped.source.as_deref(), Some("codex"));
        assert_eq!(mapped.deleted_at, Some(42));
    }
}
