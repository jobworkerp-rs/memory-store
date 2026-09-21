//! Shared imports for the private ThreadGroup responsibility modules.

pub(crate) use std::collections::{BTreeMap, HashSet};

pub(crate) use async_trait::async_trait;
pub(crate) use base64::Engine;
pub(crate) use common::thread_group_key::{
    IdentityScope, SourceIdentity, evidence_fingerprint, reconciler_group_canonical_key,
    sha256_hex, source_thread_canonical_key, split_group_canonical_key,
};
pub(crate) use infra::infra::IdGeneratorWrapper;
pub(crate) use infra::infra::memory::rdb::{MemoryRepository, MemoryRepositoryImpl};
pub(crate) use infra::infra::thread::rdb::{ThreadRepository, ThreadRepositoryImpl};
pub(crate) use infra::infra::thread_group::audit::{
    ThreadGroupAuditRepository, ThreadGroupAuditRepositoryImpl,
};
pub(crate) use infra::infra::thread_group::candidate::{
    ThreadGroupCandidateAssociationRepository, ThreadGroupCandidateAssociationRepositoryImpl,
};
pub(crate) use infra::infra::thread_group::canonical_key::{
    ThreadCanonicalKeyRepository, ThreadCanonicalKeyRepositoryImpl,
};
pub(crate) use infra::infra::thread_group::collection::{
    ManualCollectionRepository, ManualCollectionRepositoryImpl,
};
pub(crate) use infra::infra::thread_group::deletion_marker::{
    ThreadDeletionMarkerRepository, ThreadDeletionMarkerRepositoryImpl,
};
pub(crate) use infra::infra::thread_group::group::{
    ThreadGroupRepository, ThreadGroupRepositoryImpl,
};
pub(crate) use infra::infra::thread_group::lock::{
    ThreadGroupLockRepository, ThreadGroupLockRepositoryImpl,
};
pub(crate) use infra::infra::thread_group::member::{
    ThreadGroupMemberRepository, ThreadGroupMemberRepositoryImpl,
};
pub(crate) use infra::infra::thread_group::observation::{
    ThreadObservationRepository, ThreadObservationRepositoryImpl,
};
pub(crate) use infra::infra::thread_group::operator_decision::{
    OperatorDecisionRepository, OperatorDecisionRepositoryImpl,
};
pub(crate) use infra::infra::thread_group::outbox::{
    ThreadGroupEventOutboxRepository, ThreadGroupEventOutboxRepositoryImpl,
};
pub(crate) use infra::infra::thread_group::relation::{
    ThreadRelationRepository, ThreadRelationRepositoryImpl,
};
pub(crate) use infra::infra::thread_group::revision::{
    ThreadGroupReadModelRevision, ThreadGroupReadModelRevisionRepository,
    ThreadGroupReadModelRevisionRepositoryImpl,
};
pub(crate) use infra::infra::thread_group::rows::{
    ManualCollectionMemberRow, ManualCollectionRow, NewGroupAuditMerge, NewGroupAuditSplit,
    NewManualCollection, NewOperatorDecision, NewThreadGroup, NewThreadGroupCandidateAssociation,
    NewThreadGroupEvent, NewThreadGroupMember, NewThreadObservation, NewThreadRelation,
    ObservationIdentity, SourceIdentityKey, ThreadGroupCandidateAssociationRow,
    ThreadGroupMemberRow, ThreadGroupRow, ThreadObservationRow, ThreadRelationRow, values,
};
pub(crate) use infra::infra::thread_group::source_identity::{
    SourceThreadIdentityRepository, SourceThreadIdentityRepositoryImpl,
};
pub(crate) use infra::infra::thread_label::rdb::{
    ThreadLabelRepository, ThreadLabelRepositoryImpl,
};
pub(crate) use infra_utils::infra::rdb::{RdbPool, RdbTransaction};
pub(crate) use protobuf::llm_memory::data::{LabelMatchMode, MemoryKind, ThreadSearchFilter};
