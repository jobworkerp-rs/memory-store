// The ThreadGroup integration fixtures build a SQLite temp database from
// the committed migration; PostgreSQL runs need a live server and are
// exercised by the repository / migration suites instead.
#![cfg(not(feature = "postgres"))]

use app::app::memory::{MemoryApp, MemoryAppImpl};
use app::app::thread::{ThreadApp, ThreadAppImpl};
use app::app::thread_group::memory_relation::{
    GroupMemoryDeletePolicy, ThreadGroupMemoryRelationService,
};
use app::app::thread_group::orphan_cleanup::ThreadGroupOrphanCleanupService;
use app::app::thread_group::{
    Candidate, CandidateEvidence, CandidateSelection, Group, GroupError, GroupStatus, Membership,
    MembershipProvenance, MembershipRole, MembershipState, ObservationInput, ObservedEndpoint,
    RelationType, SourceConfidence, SourceIdentityInput, SuppressionDecision, ThreadGroupEventSink,
    ThreadGroupEventType, ThreadGroupObservationService, ThreadGroupOperatorService,
    ThreadGroupOutboxDispatcher, ThreadGroupPurgeService, ThreadGroupReadService,
    ThreadGroupReconciliationService, decide_suppression, event_id_for, new_observation,
    observation_fingerprint, select_parent_candidate, split_group, would_create_cycle,
};
use common::thread_group_key::{IdentityScope, source_thread_canonical_key};
use infra::infra::IdGeneratorWrapper;
use infra::infra::memory::rdb::{MemoryRepository, MemoryRepositoryImpl};
use infra::infra::memory_rating::rdb::MemoryRatingRepositoryImpl;
use infra::infra::thread::rdb::{ThreadRepository, ThreadRepositoryImpl};
use infra::infra::thread_group::audit::{
    ThreadGroupAuditRepository, ThreadGroupAuditRepositoryImpl,
};
use infra::infra::thread_group::candidate::{
    ThreadGroupCandidateAssociationRepository, ThreadGroupCandidateAssociationRepositoryImpl,
};
use infra::infra::thread_group::deletion_marker::{
    ThreadDeletionMarkerRepository, ThreadDeletionMarkerRepositoryImpl,
};
use infra::infra::thread_group::group::{ThreadGroupRepository, ThreadGroupRepositoryImpl};
use infra::infra::thread_group::member::{
    ThreadGroupMemberRepository, ThreadGroupMemberRepositoryImpl,
};
use infra::infra::thread_group::memory_relation::ThreadGroupMemoryRelationRepositoryImpl;
use infra::infra::thread_group::observation::{
    ThreadObservationRepository, ThreadObservationRepositoryImpl,
};
use infra::infra::thread_group::operator_decision::{
    OperatorDecisionRepository, OperatorDecisionRepositoryImpl,
};
use infra::infra::thread_group::outbox::{
    ThreadGroupEventOutboxRepository, ThreadGroupEventOutboxRepositoryImpl,
};
use infra::infra::thread_group::relation::{
    ThreadRelationRepository, ThreadRelationRepositoryImpl,
};
use infra::infra::thread_group::rows::values;
use infra::infra::thread_group::rows::{
    NewGroupAuditMerge, NewThreadDeletionMarker, NewThreadRelation, SourceIdentityKey,
};
use infra::infra::thread_group::test_support::{
    insert_thread, key, new_group, new_member, setup_thread_group_pool,
};
use infra::infra::thread_label::rdb::ThreadLabelRepositoryImpl;
use infra::infra::thread_memory::rdb::{ThreadMemoryRepository, ThreadMemoryRepositoryImpl};

fn candidate(parent: &str, confidence: SourceConfidence) -> Candidate {
    Candidate {
        parent_key: parent.into(),
        relation_type: RelationType::Delegated,
        evidence: CandidateEvidence::Source { confidence },
    }
}

#[test]
fn exact_evidence_wins_over_strong_evidence() {
    let result = select_parent_candidate(&[
        candidate("parent-strong", SourceConfidence::Strong),
        candidate("parent-exact", SourceConfidence::Exact),
    ]);

    assert_eq!(
        result,
        CandidateSelection::Selected(candidate("parent-exact", SourceConfidence::Exact))
    );
}

#[test]
fn operator_confirmation_wins_over_strong_but_not_exact_evidence() {
    let operator = Candidate {
        parent_key: "parent-operator".into(),
        relation_type: RelationType::Delegated,
        evidence: CandidateEvidence::OperatorConfirmation,
    };

    assert_eq!(
        select_parent_candidate(&[
            candidate("parent-strong", SourceConfidence::Strong),
            operator.clone()
        ]),
        CandidateSelection::Selected(operator.clone())
    );
    assert_eq!(
        select_parent_candidate(&[operator, candidate("parent-exact", SourceConfidence::Exact)]),
        CandidateSelection::Selected(candidate("parent-exact", SourceConfidence::Exact))
    );
}

#[test]
fn equal_ranked_distinct_parents_become_a_conflict_without_fallback() {
    let result = select_parent_candidate(&[
        candidate("parent-a", SourceConfidence::Strong),
        candidate("parent-b", SourceConfidence::Strong),
        candidate("parent-lower", SourceConfidence::Unsupported),
    ]);

    assert_eq!(
        result,
        CandidateSelection::Conflict(vec![
            candidate("parent-a", SourceConfidence::Strong),
            candidate("parent-b", SourceConfidence::Strong),
        ])
    );
}

#[test]
fn unsupported_and_heuristic_evidence_do_not_create_a_relation() {
    let result = select_parent_candidate(&[
        candidate("parent-heuristic", SourceConfidence::Heuristic),
        candidate("parent-unsupported", SourceConfidence::Unsupported),
    ]);

    assert_eq!(result, CandidateSelection::NoSelection);
}

#[test]
fn strong_evidence_is_selected_when_lower_ranked_candidates_are_present() {
    assert_eq!(
        select_parent_candidate(&[
            candidate("parent-strong", SourceConfidence::Strong),
            candidate("parent-heuristic", SourceConfidence::Heuristic),
        ]),
        CandidateSelection::Selected(candidate("parent-strong", SourceConfidence::Strong))
    );
}

#[test]
fn conflicting_relation_types_for_the_same_parent_are_not_silently_selected() {
    let delegated = candidate("parent", SourceConfidence::Strong);
    let continuation = Candidate {
        parent_key: "parent".into(),
        relation_type: RelationType::Continuation,
        evidence: CandidateEvidence::Source {
            confidence: SourceConfidence::Strong,
        },
    };

    assert_eq!(
        select_parent_candidate(&[delegated.clone(), continuation.clone()]),
        CandidateSelection::Conflict(vec![delegated, continuation])
    );
}

#[test]
fn equal_ranked_operator_confirmations_conflict() {
    let first = Candidate {
        parent_key: "parent-a".into(),
        relation_type: RelationType::Delegated,
        evidence: CandidateEvidence::OperatorConfirmation,
    };
    let second = Candidate {
        parent_key: "parent-b".into(),
        relation_type: RelationType::Delegated,
        evidence: CandidateEvidence::OperatorConfirmation,
    };

    assert_eq!(
        select_parent_candidate(&[first.clone(), second.clone()]),
        CandidateSelection::Conflict(vec![first, second])
    );
}

#[test]
fn cycle_check_rejects_a_child_that_is_already_an_ancestor() {
    let active_edges = vec![
        ("a".to_owned(), "b".to_owned()),
        ("b".to_owned(), "c".to_owned()),
    ];

    assert!(would_create_cycle(&active_edges, "c", "a"));
    assert!(!would_create_cycle(&active_edges, "a", "c"));
    assert!(would_create_cycle(&active_edges, "a", "a"));
}

fn active_member(key: &str, state: MembershipState) -> Membership {
    Membership {
        thread_canonical_key: key.into(),
        role: MembershipRole::Member,
        state,
        provenance: MembershipProvenance::Reconciler,
        deleted_at: None,
    }
}

#[test]
fn split_preserves_deleted_state_and_pins_each_successor_membership() {
    let group = Group {
        canonical_key: "source-group".into(),
        status: GroupStatus::Active,
        memberships: vec![
            active_member("a", MembershipState::Active),
            active_member("b", MembershipState::Deleted),
        ],
    };

    let outcome = split_group(&group, &[vec!["a"], vec!["b"]]).expect("valid split");

    assert_eq!(outcome.source.status, GroupStatus::Split);
    assert!(
        outcome
            .source
            .memberships
            .iter()
            .all(|member| member.state == MembershipState::Redirected)
    );
    assert_eq!(outcome.successors.len(), 2);
    assert_eq!(
        outcome.successors[1].memberships[0].state,
        MembershipState::Deleted
    );
    assert!(
        outcome
            .successors
            .iter()
            .flat_map(|group| &group.memberships)
            .all(|member| member.provenance == MembershipProvenance::Operator)
    );
}

#[test]
fn split_rejects_missing_duplicate_and_empty_partitions_without_mutating_source() {
    let group = Group {
        canonical_key: "source-group".into(),
        status: GroupStatus::Active,
        memberships: vec![
            active_member("a", MembershipState::Active),
            active_member("b", MembershipState::Active),
        ],
    };

    assert_eq!(
        split_group(&group, &[vec!["a"], vec![]]),
        Err(GroupError::EmptyPartition)
    );
    assert_eq!(
        split_group(&group, &[vec!["a"], vec!["a", "b"]]),
        Err(GroupError::DuplicateMember("a".into()))
    );
    assert_eq!(
        split_group(&group, &[vec!["a"], vec!["unknown"]]),
        Err(GroupError::MembershipMismatch)
    );
    assert_eq!(group.status, GroupStatus::Active);
}

#[test]
fn split_rejects_inactive_or_incomplete_partitions() {
    let group = Group {
        canonical_key: "source-group".into(),
        status: GroupStatus::Active,
        memberships: vec![
            active_member("a", MembershipState::Active),
            active_member("b", MembershipState::Active),
            active_member("c", MembershipState::Active),
        ],
    };
    assert_eq!(
        split_group(&group, &[vec!["a"]]),
        Err(GroupError::TooFewPartitions)
    );
    assert_eq!(
        split_group(&group, &[vec!["a"], vec!["b"]]),
        Err(GroupError::MembershipMismatch)
    );
    let mut inactive = group.clone();
    inactive.status = GroupStatus::Split;
    assert_eq!(
        split_group(&inactive, &[vec!["a"], vec!["b"], vec!["c"]]),
        Err(GroupError::SourceNotActive)
    );
}

fn endpoint(source: &str, scope: IdentityScope, native_id: &str) -> ObservedEndpoint {
    ObservedEndpoint {
        source: source.into(),
        identity_scope: scope,
        owner_scope: "user:1".into(),
        native_id: native_id.into(),
    }
}

fn observation(subject: ObservedEndpoint, parent: Option<ObservedEndpoint>) -> ObservationInput {
    ObservationInput {
        subject,
        candidate_parent: parent,
        relation_kind: Some("delegated".into()),
        evidence_kind: values::evidence_kind::SOURCE_FIELD.into(),
        polarity: values::polarity::SUPPORTS.into(),
        source_confidence: Some(values::source_confidence::EXACT.into()),
        adapter_version: "codex-adapter@1".into(),
        source_record_ref: "codex:session_meta:subagent".into(),
        import_run_id: None,
        observed_at: 42,
    }
}

#[test]
fn observation_fingerprint_is_stable_and_distinguishes_scope_states() {
    let known_empty = observation(
        endpoint("codex", IdentityScope::known(""), "child"),
        Some(endpoint("codex", IdentityScope::known(""), "parent")),
    );
    assert_eq!(
        observation_fingerprint(&known_empty, values::observation_origin::ADAPTER),
        observation_fingerprint(&known_empty, values::observation_origin::ADAPTER)
    );

    let unknown = observation(
        endpoint("codex", IdentityScope::unknown(), "child"),
        Some(endpoint("codex", IdentityScope::known(""), "parent")),
    );
    assert_ne!(
        observation_fingerprint(&known_empty, values::observation_origin::ADAPTER),
        observation_fingerprint(&unknown, values::observation_origin::ADAPTER),
        "known empty scope and unknown scope must not share a fingerprint"
    );

    let no_parent = observation(endpoint("codex", IdentityScope::known(""), "child"), None);
    assert_ne!(
        observation_fingerprint(&known_empty, values::observation_origin::ADAPTER),
        observation_fingerprint(&no_parent, values::observation_origin::ADAPTER),
        "no-parent and present-parent must not share a fingerprint"
    );
}

#[test]
fn new_observation_records_presence_and_known_state_columns() {
    let input = observation(
        endpoint("codex", IdentityScope::known(""), "child"),
        Some(endpoint("codex", IdentityScope::known("proj"), "parent")),
    );
    let row = new_observation(
        &input,
        values::observation_origin::ADAPTER,
        values::observation_state::CANDIDATE,
        100,
    );

    assert!(row.subject_identity_scope_known);
    assert_eq!(row.subject_identity_scope_value, "");
    assert!(row.candidate_parent_present);
    assert!(row.candidate_parent_identity_scope_known);
    assert_eq!(row.candidate_parent_identity_scope_value, "proj");
    assert_eq!(row.candidate_parent_native_id, "parent");
    assert_eq!(row.source_confidence.as_deref(), Some("exact"));
    assert_eq!(row.origin, values::observation_origin::ADAPTER);
    // Only a redacted locator reaches storage, never the raw payload.
    assert_eq!(
        row.source_record_ref.as_deref(),
        Some("codex:session_meta:subagent")
    );
    assert_eq!(row.created_at, 100);
    assert_eq!(row.updated_at, 100);

    let absent = new_observation(
        &observation(endpoint("codex", IdentityScope::unknown(), "child"), None),
        values::observation_origin::ADAPTER,
        values::observation_state::UNSUPPORTED,
        100,
    );
    assert!(!absent.subject_identity_scope_known);
    assert!(!absent.candidate_parent_present);
    assert_eq!(absent.candidate_parent_source, "");
}

#[test]
fn event_id_is_deterministic_and_type_specific() {
    let first = event_id_for(ThreadGroupEventType::RelationSelected, "fingerprint-1");
    assert_eq!(
        first,
        event_id_for(ThreadGroupEventType::RelationSelected, "fingerprint-1")
    );
    assert_ne!(
        first,
        event_id_for(ThreadGroupEventType::RelationSelected, "fingerprint-2")
    );
    assert_ne!(
        first,
        event_id_for(ThreadGroupEventType::ConflictDetected, "fingerprint-1")
    );
}

#[test]
fn record_observation_is_idempotent_and_emits_one_event() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("current-thread runtime");
    runtime.block_on(async {
        let pool = setup_thread_group_pool().await;
        let service = ThreadGroupObservationService::with_id_generator(
            pool,
            infra::test_helper::shared_id_generator(),
        );
        let input = observation(
            endpoint("codex", IdentityScope::known(""), "child-idem"),
            Some(endpoint("codex", IdentityScope::known(""), "parent-idem")),
        );

        let first = service
            .record_observation(&input, values::observation_state::CANDIDATE, "op-1", 100)
            .await
            .expect("first record");
        assert!(first.inserted);
        assert!(first.event_written);

        // Replaying the same evidence fingerprint must not create a
        // second observation row or a second event.
        let second = service
            .record_observation(&input, values::observation_state::CANDIDATE, "op-1", 200)
            .await
            .expect("replay");
        assert_eq!(second.observation_id, first.observation_id);
        assert!(!second.inserted);
        assert!(!second.event_written);

        let fingerprint = observation_fingerprint(&input, values::observation_origin::ADAPTER);
        let event_id = event_id_for(ThreadGroupEventType::ObservationRecorded, &fingerprint);
        let outbox = ThreadGroupEventOutboxRepositoryImpl::new(pool);
        let event = outbox
            .find_by_event_id(&event_id)
            .await
            .expect("event lookup")
            .expect("event row exists");
        assert_eq!(event.event_type, values::event_type::OBSERVATION_RECORDED);
        assert_eq!(event.source.as_deref(), Some("codex"));
    });
}

#[test]
fn suppression_decision_matrix() {
    assert_eq!(decide_suppression(None, false), SuppressionDecision::Allow);
    assert_eq!(decide_suppression(None, true), SuppressionDecision::Allow);
    // Revivable marker: normal import may recreate content.
    assert_eq!(
        decide_suppression(Some(false), false),
        SuppressionDecision::Allow
    );
    // Forbidden marker without override blocks content writes.
    assert_eq!(
        decide_suppression(Some(true), false),
        SuppressionDecision::Suppress
    );
    // Only the explicit override consumes a forbidden marker.
    assert_eq!(
        decide_suppression(Some(true), true),
        SuppressionDecision::Override
    );
}

#[test]
fn split_preserves_member_role_and_deletion_timestamp() {
    let group = Group {
        canonical_key: "source-group".into(),
        status: GroupStatus::Active,
        memberships: vec![
            Membership {
                thread_canonical_key: "root".into(),
                role: MembershipRole::Root,
                state: MembershipState::Deleted,
                provenance: MembershipProvenance::Reconciler,
                deleted_at: Some(42),
            },
            active_member("child", MembershipState::Active),
        ],
    };
    let outcome = split_group(&group, &[vec!["root"], vec!["child"]]).expect("valid split");
    let root = &outcome.successors[0].memberships[0];
    assert_eq!(root.role, MembershipRole::Root);
    assert_eq!(root.state, MembershipState::Deleted);
    assert_eq!(root.deleted_at, Some(42));
}

fn run<F: std::future::Future>(future: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .expect("current-thread runtime")
        .block_on(future)
}

fn recon_endpoint(native_id: &str) -> ObservedEndpoint {
    ObservedEndpoint {
        source: "codex".into(),
        identity_scope: IdentityScope::known(""),
        owner_scope: "user:1".into(),
        native_id: native_id.into(),
    }
}

fn key_of(endpoint: &ObservedEndpoint) -> String {
    source_thread_canonical_key(&common::thread_group_key::SourceIdentity::new(
        endpoint.owner_scope.clone(),
        endpoint.source.clone(),
        endpoint.identity_scope.clone(),
        endpoint.native_id.clone(),
    ))
    .expect("known scope")
}

async fn candidate_rows_for(
    pool: &'static infra_utils::infra::rdb::RdbPool,
    subject: &ObservedEndpoint,
) -> Vec<infra::infra::thread_group::rows::ThreadGroupCandidateAssociationRow> {
    ThreadGroupCandidateAssociationRepositoryImpl::new(
        infra::test_helper::shared_id_generator(),
        pool,
    )
    .list_by_subject_identity(
        &subject.owner_scope,
        &subject.source,
        matches!(&subject.identity_scope, &IdentityScope::Known(_)),
        match &subject.identity_scope {
            IdentityScope::Known(scope) => scope.as_str(),
            IdentityScope::Unknown => "",
        },
        &subject.native_id,
    )
    .await
    .unwrap()
}

#[test]
fn reconcile_attaches_child_to_parent_group() {
    run(async {
        let pool = setup_thread_group_pool().await;
        let service = ThreadGroupReconciliationService::with_id_generator(
            pool,
            infra::test_helper::shared_id_generator(),
        );
        let parent = recon_endpoint("parent-a");
        let child = recon_endpoint("child-a");
        insert_thread(pool, 20_001, None).await;
        insert_thread(pool, 20_002, None).await;

        service
            .reconcile_subject(20_001, &parent, &[], "op", 1_000)
            .await
            .expect("parent key assignment");

        let outcome = service
            .reconcile_subject(
                20_002,
                &child,
                &[observation(child.clone(), Some(parent.clone()))],
                "op",
                1_001,
            )
            .await
            .expect("child reconcile");

        assert!(outcome.relation_selected.is_some());
        let group_id = outcome.group_id.expect("group membership");
        assert!(!outcome.conflict);

        let child_key = key_of(&child);
        let member = ThreadGroupMemberRepositoryImpl::new(pool)
            .find_current_by_thread_canonical_key(&child_key)
            .await
            .expect("member lookup")
            .expect("child membership");
        assert_eq!(member.group_id, group_id);
        assert_eq!(member.provenance, values::grouping_authority::RECONCILER);

        let relation = ThreadRelationRepositoryImpl::new(IdGeneratorWrapper::new(), pool)
            .find_active_by_child_canonical_key(&child_key)
            .await
            .expect("relation lookup")
            .expect("active relation");
        assert_eq!(relation.relation_type, values::relation_type::DELEGATED);
        assert_eq!(
            relation.selection_basis,
            values::selection_basis::SOURCE_EXACT
        );
    });
}

#[test]
fn reconcile_subject_is_idempotent() {
    run(async {
        let pool = setup_thread_group_pool().await;
        let service = ThreadGroupReconciliationService::with_id_generator(
            pool,
            infra::test_helper::shared_id_generator(),
        );
        let parent = recon_endpoint("parent-b");
        let child = recon_endpoint("child-b");
        insert_thread(pool, 20_011, None).await;
        insert_thread(pool, 20_012, None).await;
        service
            .reconcile_subject(20_011, &parent, &[], "op", 1_000)
            .await
            .unwrap();
        let evidence = vec![observation(child.clone(), Some(parent.clone()))];

        let first = service
            .reconcile_subject(20_012, &child, &evidence, "op", 1_001)
            .await
            .unwrap();
        let second = service
            .reconcile_subject(20_012, &child, &evidence, "op", 1_002)
            .await
            .unwrap();

        assert_eq!(first.relation_selected, second.relation_selected);
        assert_eq!(first.group_id, second.group_id);
        let child_key = key_of(&child);
        let relations = ThreadRelationRepositoryImpl::new(IdGeneratorWrapper::new(), pool)
            .list_by_child_canonical_key(&child_key, None)
            .await
            .unwrap();
        assert_eq!(relations.len(), 1, "replay must not duplicate the edge");
    });
}

#[test]
fn late_parent_discovery_reconciles_all_pending_children() {
    run(async {
        let pool = setup_thread_group_pool().await;
        let service = ThreadGroupReconciliationService::with_id_generator(
            pool,
            infra::test_helper::shared_id_generator(),
        );
        let parent = recon_endpoint("late-parent-many");
        let children = [
            recon_endpoint("late-child-one"),
            recon_endpoint("late-child-two"),
        ];
        insert_thread(pool, 26_001, None).await;
        insert_thread(pool, 26_002, None).await;
        insert_thread(pool, 26_003, None).await;

        let mut singleton_group_ids = Vec::new();
        for (index, child) in children.iter().enumerate() {
            let outcome = service
                .reconcile_subject(
                    26_002 + index as i64,
                    child,
                    &[observation(child.clone(), Some(parent.clone()))],
                    "late-parent",
                    1_001 + index as i64,
                )
                .await
                .unwrap();
            assert_eq!(outcome.pending, 1);
            singleton_group_ids.push(outcome.group_id.expect("orphan singleton group"));
        }
        for child in &children {
            assert_eq!(
                candidate_rows_for(pool, child)
                    .await
                    .iter()
                    .filter(|row| row.state == values::candidate_state::PENDING)
                    .count(),
                1
            );
        }

        let groups =
            ThreadGroupRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool);
        let inbound_id = groups
            .create_tx(pool, &new_group(33_807_001))
            .await
            .unwrap();
        assert!(
            groups
                .redirect_tx(pool, inbound_id, singleton_group_ids[0], 1_009)
                .await
                .unwrap()
        );
        let read = ThreadGroupReadService::new(pool);
        let active_before = read.reconciliation_report().await.unwrap().active_groups;

        let parent_outcome = service
            .reconcile_subject(26_001, &parent, &[], "late-parent", 1_010)
            .await
            .unwrap();
        let target_group = parent_outcome.group_id.unwrap();
        assert_eq!(
            read.reconciliation_report().await.unwrap().active_groups,
            active_before - 1,
            "the new parent group replaces both former active singleton groups"
        );
        let visible = read.list_groups(false, None, None).await.unwrap();
        assert!(visible.iter().any(|group| group.id == target_group));
        for old_group_id in singleton_group_ids {
            assert!(visible.iter().all(|group| group.id != old_group_id));
            let old = groups.find_by_id(old_group_id).await.unwrap().unwrap();
            assert_eq!(old.status, values::group_status::REDIRECTED);
            assert_eq!(old.redirect_to_group_id, Some(target_group));
        }
        assert_eq!(
            groups
                .find_by_id(inbound_id)
                .await
                .unwrap()
                .unwrap()
                .redirect_to_group_id,
            Some(target_group),
            "redirected history must not form a chain through the moved singleton"
        );
        let relations = ThreadRelationRepositoryImpl::new(IdGeneratorWrapper::new(), pool);
        let members = ThreadGroupMemberRepositoryImpl::new(pool);
        for child in &children {
            let relation = relations
                .find_active_by_child_canonical_key(&key_of(child))
                .await
                .unwrap()
                .expect("late-discovered parent relation");
            assert_eq!(relation.parent_thread_canonical_key, key_of(&parent));
            assert_eq!(
                members
                    .find_current_by_thread_canonical_key(&key_of(child))
                    .await
                    .unwrap()
                    .unwrap()
                    .group_id,
                parent_outcome.group_id.unwrap()
            );
        }
        for child in &children {
            assert!(
                candidate_rows_for(pool, child)
                    .await
                    .iter()
                    .all(|row| row.state != values::candidate_state::PENDING)
            );
        }
    });
}

#[test]
fn automatic_group_stays_active_while_another_current_member_remains() {
    run(async {
        let pool = setup_thread_group_pool().await;
        let service = ThreadGroupReconciliationService::with_id_generator(
            pool,
            infra::test_helper::shared_id_generator(),
        );
        let parent = recon_endpoint("partial-move-parent");
        let child = recon_endpoint("partial-move-child");
        insert_thread(pool, 26_051, None).await;
        insert_thread(pool, 26_052, None).await;
        insert_thread(pool, 26_053, None).await;
        let orphan = service
            .reconcile_subject(
                26_052,
                &child,
                &[observation(child.clone(), Some(parent.clone()))],
                "partial-move",
                1_000,
            )
            .await
            .unwrap();
        let old_group = orphan.group_id.unwrap();
        let remaining = new_member(old_group, Some(26_053), key(26_053));
        ThreadGroupMemberRepositoryImpl::new(pool)
            .insert_tx(pool, &remaining)
            .await
            .unwrap();

        let parent_group = service
            .reconcile_subject(26_051, &parent, &[], "partial-move", 1_001)
            .await
            .unwrap()
            .group_id
            .unwrap();
        let groups =
            ThreadGroupRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool);
        let old = groups.find_by_id(old_group).await.unwrap().unwrap();
        assert_eq!(old.status, values::group_status::ACTIVE);
        assert_eq!(old.redirect_to_group_id, None);
        assert_eq!(
            ThreadGroupMemberRepositoryImpl::new(pool)
                .find_current_by_thread_canonical_key(&key_of(&child))
                .await
                .unwrap()
                .unwrap()
                .group_id,
            parent_group
        );
    });
}

#[test]
fn automatic_move_does_not_redirect_an_operator_owned_group() {
    run(async {
        let pool = setup_thread_group_pool().await;
        let service = ThreadGroupReconciliationService::with_id_generator(
            pool,
            infra::test_helper::shared_id_generator(),
        );
        let parent = recon_endpoint("manual-group-parent");
        let child = recon_endpoint("manual-group-child");
        insert_thread(pool, 26_054, None).await;
        insert_thread(pool, 26_055, None).await;
        let old_group = service
            .reconcile_subject(
                26_055,
                &child,
                &[observation(child.clone(), Some(parent.clone()))],
                "manual-group",
                1_000,
            )
            .await
            .unwrap()
            .group_id
            .unwrap();
        let groups =
            ThreadGroupRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool);
        groups
            .set_grouping_authority_tx(pool, old_group, values::grouping_authority::OPERATOR, 1_001)
            .await
            .unwrap();

        service
            .reconcile_subject(26_054, &parent, &[], "manual-group", 1_002)
            .await
            .unwrap();
        let old = groups.find_by_id(old_group).await.unwrap().unwrap();
        assert_eq!(old.status, values::group_status::ACTIVE);
        assert_eq!(old.redirect_to_group_id, None);
    });
}

#[test]
fn pending_replay_is_idempotent_and_parent_discovery_drains_pending_count() {
    run(async {
        let pool = setup_thread_group_pool().await;
        let service = ThreadGroupReconciliationService::with_id_generator(
            pool,
            infra::test_helper::shared_id_generator(),
        );
        let parent = recon_endpoint("late-parent-replay");
        let child = recon_endpoint("late-child-replay");
        let input = observation(child.clone(), Some(parent.clone()));
        insert_thread(pool, 26_004, None).await;
        insert_thread(pool, 26_005, None).await;

        let first = service
            .reconcile_subject(
                26_005,
                &child,
                std::slice::from_ref(&input),
                "late-replay",
                1_100,
            )
            .await
            .unwrap();
        let replay = service
            .reconcile_subject(
                26_005,
                &child,
                std::slice::from_ref(&input),
                "late-replay",
                1_101,
            )
            .await
            .unwrap();
        assert_eq!(first.observation_ids, replay.observation_ids);
        assert_eq!(first.pending, 1);
        assert_eq!(replay.pending, 1);

        let pending = candidate_rows_for(pool, &child)
            .await
            .into_iter()
            .filter(|row| row.state == values::candidate_state::PENDING)
            .collect::<Vec<_>>();
        assert_eq!(
            pending.len(),
            1,
            "one observation owns one pending association"
        );

        service
            .reconcile_subject(26_004, &parent, &[], "late-replay", 1_102)
            .await
            .unwrap();
        service
            .reconcile_subject(26_004, &parent, &[], "late-replay", 1_103)
            .await
            .unwrap();
        let resolved_candidates = candidate_rows_for(pool, &child).await;
        assert!(
            resolved_candidates
                .iter()
                .all(|row| row.state != values::candidate_state::PENDING)
        );
        assert_eq!(resolved_candidates.len(), 1);
        assert_eq!(
            resolved_candidates[0].state,
            values::candidate_state::CANDIDATE
        );
        assert_eq!(
            ThreadRelationRepositoryImpl::new(IdGeneratorWrapper::new(), pool)
                .list_by_child_canonical_key(&key_of(&child), None)
                .await
                .unwrap()
                .len(),
            1,
            "parent replay must not duplicate the relation"
        );
    });
}

#[test]
fn selected_relation_replay_resolves_only_matching_stale_pending_evidence() {
    run(async {
        let pool = setup_thread_group_pool().await;
        let service = ThreadGroupReconciliationService::with_id_generator(
            pool,
            infra::test_helper::shared_id_generator(),
        );
        let parent = recon_endpoint("replay-pending-parent");
        let child = recon_endpoint("replay-pending-child");
        let evidence = observation(child.clone(), Some(parent.clone()));
        insert_thread(pool, 26_041, None).await;
        insert_thread(pool, 26_042, None).await;
        service
            .reconcile_subject(26_041, &parent, &[], "pending-replay", 1_000)
            .await
            .unwrap();
        let first = service
            .reconcile_subject(
                26_042,
                &child,
                std::slice::from_ref(&evidence),
                "pending-replay",
                1_001,
            )
            .await
            .unwrap();
        let mut stale = infra::infra::thread_group::test_support::new_candidate(26_041);
        stale.subject_thread_id = Some(26_042);
        stale.subject_owner_scope = child.owner_scope.clone();
        stale.subject_source = child.source.clone();
        stale.subject_native_id = child.native_id.clone();
        stale.selected_observation_id = first.observation_ids.first().copied();
        let candidates = ThreadGroupCandidateAssociationRepositoryImpl::new(
            infra::test_helper::shared_id_generator(),
            pool,
        );
        candidates.insert_tx(pool, &stale).await.unwrap();
        let mut unrelated = infra::infra::thread_group::test_support::new_candidate(26_042);
        unrelated.subject_thread_id = Some(26_042);
        unrelated.subject_owner_scope = child.owner_scope.clone();
        unrelated.subject_source = child.source.clone();
        unrelated.subject_native_id = child.native_id.clone();
        candidates.insert_tx(pool, &unrelated).await.unwrap();

        let replay = service
            .reconcile_subject(26_042, &child, &[evidence], "pending-replay", 1_002)
            .await
            .unwrap();
        assert_eq!(replay.relation_selected, first.relation_selected);
        let rows = candidate_rows_for(pool, &child).await;
        assert_eq!(rows.len(), 2);
        assert!(rows.iter().any(|row| {
            row.selected_observation_id == first.observation_ids.first().copied()
                && row.state == values::candidate_state::CANDIDATE
                && row.candidate_parent_thread_id == Some(26_041)
        }));
        assert!(rows.iter().any(|row| {
            row.selected_observation_id.is_none() && row.state == values::candidate_state::PENDING
        }));
    });
}

#[test]
fn a_missing_parent_remains_pending_without_creating_a_relation() {
    run(async {
        let pool = setup_thread_group_pool().await;
        let service = ThreadGroupReconciliationService::with_id_generator(
            pool,
            infra::test_helper::shared_id_generator(),
        );
        let parent = recon_endpoint("never-imported-parent");
        let child = recon_endpoint("orphan-child");
        insert_thread(pool, 26_006, None).await;

        let outcome = service
            .reconcile_subject(
                26_006,
                &child,
                &[observation(child.clone(), Some(parent))],
                "missing-parent",
                1_200,
            )
            .await
            .unwrap();
        assert_eq!(outcome.pending, 1);
        let orphan_group =
            ThreadGroupRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool)
                .find_by_id(
                    outcome
                        .group_id
                        .expect("pending child keeps a singleton group"),
                )
                .await
                .unwrap()
                .unwrap();
        assert_eq!(orphan_group.status, values::group_status::ACTIVE);
        assert_eq!(orphan_group.redirect_to_group_id, None);
        assert!(
            ThreadRelationRepositoryImpl::new(IdGeneratorWrapper::new(), pool)
                .find_active_by_child_canonical_key(&key_of(&child))
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            candidate_rows_for(pool, &child)
                .await
                .iter()
                .filter(|row| row.state == values::candidate_state::PENDING)
                .count(),
            1
        );
    });
}

#[test]
fn distinct_missing_parents_are_retained_and_become_a_conflict_when_discovered() {
    run(async {
        let pool = setup_thread_group_pool().await;
        let service = ThreadGroupReconciliationService::with_id_generator(
            pool,
            infra::test_helper::shared_id_generator(),
        );
        let parent_a = recon_endpoint("late-ambiguous-parent-a");
        let parent_b = recon_endpoint("late-ambiguous-parent-b");
        let child = recon_endpoint("late-ambiguous-child");
        insert_thread(pool, 26_007, None).await;
        insert_thread(pool, 26_008, None).await;
        insert_thread(pool, 26_009, None).await;

        let outcome = service
            .reconcile_subject(
                26_009,
                &child,
                &[
                    observation(child.clone(), Some(parent_a.clone())),
                    observation(child.clone(), Some(parent_b.clone())),
                ],
                "late-ambiguous",
                1_300,
            )
            .await
            .unwrap();
        assert_eq!(outcome.pending, 2);
        let candidates = candidate_rows_for(pool, &child).await;
        assert_eq!(
            candidates.len(),
            2,
            "pending associations are keyed by observation, not unresolved parent id"
        );

        service
            .reconcile_subject(26_007, &parent_a, &[], "late-ambiguous", 1_301)
            .await
            .unwrap();
        service
            .reconcile_subject(26_008, &parent_b, &[], "late-ambiguous", 1_302)
            .await
            .unwrap();

        assert!(
            ThreadRelationRepositoryImpl::new(IdGeneratorWrapper::new(), pool)
                .find_active_by_child_canonical_key(&key_of(&child))
                .await
                .unwrap()
                .is_none(),
            "equal-ranked discovered parents must not leave an active edge"
        );
        let resolved_candidates = candidate_rows_for(pool, &child).await;
        assert!(
            resolved_candidates
                .iter()
                .all(|row| row.state != values::candidate_state::PENDING)
        );
        assert_eq!(
            resolved_candidates
                .iter()
                .filter(|row| row.state == values::candidate_state::CONFLICT)
                .count(),
            2,
            "both selected parents are retained as conflicting candidates"
        );
    });
}

#[test]
fn late_discovery_does_not_promote_an_operator_rejected_pending_candidate() {
    run(async {
        let pool = setup_thread_group_pool().await;
        let service = ThreadGroupReconciliationService::with_id_generator(
            pool,
            infra::test_helper::shared_id_generator(),
        );
        let parent = recon_endpoint("late-rejected-parent");
        let child = recon_endpoint("late-rejected-child");
        insert_thread(pool, 26_010, None).await;
        insert_thread(pool, 26_011, None).await;
        service
            .reconcile_subject(
                26_011,
                &child,
                &[observation(child.clone(), Some(parent.clone()))],
                "late-rejected",
                1_700,
            )
            .await
            .unwrap();
        let association = candidate_rows_for(pool, &child)
            .await
            .into_iter()
            .find(|row| row.state == values::candidate_state::PENDING)
            .expect("pending association");
        service
            .record_operator_decision(
                association.id,
                values::operator_decision::REJECT,
                "operator",
                "not a parent",
                1_701,
            )
            .await
            .unwrap();

        service
            .reconcile_subject(26_010, &parent, &[], "late-rejected", 1_702)
            .await
            .unwrap();
        assert!(
            ThreadRelationRepositoryImpl::new(IdGeneratorWrapper::new(), pool)
                .find_active_by_child_canonical_key(&key_of(&child))
                .await
                .unwrap()
                .is_none(),
            "a rejected association must not be reactivated by parent discovery"
        );
        assert!(candidate_rows_for(pool, &child).await.iter().any(
            |row| row.id == association.id && row.state == values::candidate_state::SUPERSEDED
        ));
    });
}

#[test]
fn unknown_parent_scope_does_not_resolve_to_known_empty_scope() {
    run(async {
        let pool = setup_thread_group_pool().await;
        let service = ThreadGroupReconciliationService::with_id_generator(
            pool,
            infra::test_helper::shared_id_generator(),
        );
        let known_parent = recon_endpoint("scope-collision-parent");
        let unknown_parent = ObservedEndpoint {
            identity_scope: IdentityScope::unknown(),
            ..known_parent.clone()
        };
        let child = recon_endpoint("scope-collision-child");
        insert_thread(pool, 26_012, None).await;
        insert_thread(pool, 26_013, None).await;
        service
            .reconcile_subject(26_012, &known_parent, &[], "unknown-parent", 1_400)
            .await
            .unwrap();

        let outcome = service
            .reconcile_subject(
                26_013,
                &child,
                &[observation(child.clone(), Some(unknown_parent))],
                "unknown-parent",
                1_401,
            )
            .await
            .unwrap();
        assert_eq!(outcome.pending, 1);
        assert!(
            ThreadRelationRepositoryImpl::new(IdGeneratorWrapper::new(), pool)
                .find_active_by_child_canonical_key(&key_of(&child))
                .await
                .unwrap()
                .is_none()
        );
    });
}

#[test]
fn late_parent_discovery_respects_deletion_marker() {
    run(async {
        let pool = setup_thread_group_pool().await;
        let service = ThreadGroupReconciliationService::with_id_generator(
            pool,
            infra::test_helper::shared_id_generator(),
        );
        let parent = recon_endpoint("late-guarded-parent");
        let child = recon_endpoint("late-operator-child");
        insert_thread(pool, 26_014, None).await;
        insert_thread(pool, 26_015, None).await;
        service
            .reconcile_subject(
                26_015,
                &child,
                &[observation(child.clone(), Some(parent.clone()))],
                "late-guarded",
                1_500,
            )
            .await
            .unwrap();

        let identity = SourceIdentityKey {
            owner_scope: &parent.owner_scope,
            source: &parent.source,
            identity_scope: "",
            native_id: &parent.native_id,
        };
        ThreadDeletionMarkerRepositoryImpl::new(pool)
            .put_tx(
                pool,
                &NewThreadDeletionMarker {
                    identity,
                    forbid_reimport: true,
                    recursive: false,
                    actor_id: "operator".into(),
                    reason: Some("deleted parent".into()),
                    deleted_at: 1_502,
                },
            )
            .await
            .unwrap();

        service
            .reconcile_subject(26_014, &parent, &[], "late-guarded", 1_503)
            .await
            .unwrap();
        let member = ThreadGroupMemberRepositoryImpl::new(pool)
            .find_current_by_thread_canonical_key(&key_of(&child))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(member.provenance, values::grouping_authority::RECONCILER);
        assert!(
            ThreadRelationRepositoryImpl::new(IdGeneratorWrapper::new(), pool)
                .find_active_by_child_canonical_key(&key_of(&child))
                .await
                .unwrap()
                .is_none(),
            "a deletion marker blocks automatic late adoption"
        );
        assert_eq!(
            candidate_rows_for(pool, &child)
                .await
                .iter()
                .filter(|row| row.state == values::candidate_state::PENDING)
                .count(),
            1
        );
    });
}

#[test]
fn imported_subject_registration_rejects_forbidden_marker_without_creating_membership() {
    run(async {
        let pool = setup_thread_group_pool().await;
        let subject = recon_endpoint("import-guard-root");
        insert_thread(pool, 26_018, None).await;
        ThreadDeletionMarkerRepositoryImpl::new(pool)
            .put_tx(
                pool,
                &NewThreadDeletionMarker {
                    identity: SourceIdentityKey {
                        owner_scope: &subject.owner_scope,
                        source: &subject.source,
                        identity_scope: "",
                        native_id: &subject.native_id,
                    },
                    forbid_reimport: true,
                    recursive: false,
                    actor_id: "operator".into(),
                    reason: None,
                    deleted_at: 1_700,
                },
            )
            .await
            .unwrap();
        let service = ThreadGroupReconciliationService::with_id_generator(
            pool,
            infra::test_helper::shared_id_generator(),
        );
        assert!(
            service
                .reconcile_imported_subject(26_018, &subject, &[], "import-guard", 1_701)
                .await
                .is_err()
        );
        assert!(
            ThreadGroupMemberRepositoryImpl::new(pool)
                .find_current_by_thread_canonical_key(&key_of(&subject))
                .await
                .unwrap()
                .is_none()
        );
    });
}

#[test]
fn imported_subject_registration_checks_owner_and_existing_identity() {
    run(async {
        let pool = setup_thread_group_pool().await;
        let service = ThreadGroupReconciliationService::with_id_generator(
            pool,
            infra::test_helper::shared_id_generator(),
        );
        let subject = recon_endpoint("import-guard-owner-root");
        insert_thread(pool, 26_019, None).await;
        insert_thread(pool, 26_020, None).await;
        let mut other_owner = subject.clone();
        other_owner.owner_scope = "user:2".into();
        assert!(
            service
                .reconcile_imported_subject(26_019, &other_owner, &[], "import-owner", 1_702)
                .await
                .is_err()
        );
        assert!(
            service
                .reconcile_imported_subject(26_019, &subject, &[], "import-owner", 1_703)
                .await
                .unwrap()
                .group_id
                .is_some()
        );
        assert!(
            service
                .reconcile_imported_subject(26_020, &subject, &[], "import-owner", 1_704)
                .await
                .is_err()
        );
        assert_eq!(
            ThreadGroupMemberRepositoryImpl::new(pool)
                .find_current_by_thread_canonical_key(&key_of(&subject))
                .await
                .unwrap()
                .unwrap()
                .thread_id,
            Some(26_019)
        );
    });
}

#[test]
fn unknown_scope_import_keeps_evidence_without_promoting_identity() {
    run(async {
        let pool = setup_thread_group_pool().await;
        let service = ThreadGroupReconciliationService::with_id_generator(
            pool,
            infra::test_helper::shared_id_generator(),
        );
        insert_thread(pool, 26_021, None).await;
        let mut subject = recon_endpoint("claude-unknown-fork");
        subject.identity_scope = IdentityScope::unknown();
        let parent = recon_endpoint("claude-known-parent");
        let result = service
            .reconcile_imported_subject(
                26_021,
                &subject,
                &[observation(subject.clone(), Some(parent))],
                "unknown-fork",
                1_705,
            )
            .await
            .unwrap();
        assert_eq!(result.observation_ids.len(), 1);
        assert!(result.group_id.is_none());
        let membership_count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM thread_group_member WHERE thread_id = ?")
                .bind(26_021_i64)
                .fetch_one(pool)
                .await
                .unwrap();
        assert_eq!(membership_count, 0);
    });
}

#[test]
fn late_parent_relation_does_not_move_operator_owned_child_membership() {
    run(async {
        let pool = setup_thread_group_pool().await;
        let service = ThreadGroupReconciliationService::with_id_generator(
            pool,
            infra::test_helper::shared_id_generator(),
        );
        let parent = recon_endpoint("late-operator-parent");
        let child = recon_endpoint("late-operator-child-fixed");
        insert_thread(pool, 26_016, None).await;
        insert_thread(pool, 26_017, None).await;
        service
            .reconcile_subject(
                26_017,
                &child,
                &[observation(child.clone(), Some(parent.clone()))],
                "late-operator",
                1_600,
            )
            .await
            .unwrap();

        let member_repo = ThreadGroupMemberRepositoryImpl::new(pool);
        let existing = member_repo
            .find_current_by_thread_canonical_key(&key_of(&child))
            .await
            .unwrap()
            .unwrap();
        let operator_group = existing.group_id;
        let mut tx = pool.begin().await.unwrap();
        member_repo
            .set_role_provenance_tx(
                &mut *tx,
                &key_of(&child),
                &existing.role,
                values::grouping_authority::OPERATOR,
                1_601,
            )
            .await
            .unwrap();
        tx.commit().await.unwrap();

        service
            .reconcile_subject(26_016, &parent, &[], "late-operator", 1_602)
            .await
            .unwrap();
        let member = ThreadGroupMemberRepositoryImpl::new(pool)
            .find_current_by_thread_canonical_key(&key_of(&child))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(member.group_id, operator_group);
        assert_eq!(member.provenance, values::grouping_authority::OPERATOR);
        assert!(
            ThreadRelationRepositoryImpl::new(IdGeneratorWrapper::new(), pool)
                .find_active_by_child_canonical_key(&key_of(&child))
                .await
                .unwrap()
                .is_some(),
            "the relation may resolve while operator-owned membership stays fixed"
        );
    });
}

#[test]
fn reconcile_ignores_unsupported_evidence() {
    run(async {
        let pool = setup_thread_group_pool().await;
        let service = ThreadGroupReconciliationService::with_id_generator(
            pool,
            infra::test_helper::shared_id_generator(),
        );
        let parent = recon_endpoint("parent-c");
        let child = recon_endpoint("child-c");
        insert_thread(pool, 20_021, None).await;
        insert_thread(pool, 20_022, None).await;
        service
            .reconcile_subject(20_021, &parent, &[], "op", 1_000)
            .await
            .unwrap();

        let mut unsupported = observation(child.clone(), Some(parent.clone()));
        unsupported.source_confidence = Some(values::source_confidence::UNSUPPORTED.into());
        let outcome = service
            .reconcile_subject(20_022, &child, &[unsupported], "op", 1_001)
            .await
            .unwrap();

        assert!(outcome.relation_selected.is_none());
        let child_key = key_of(&child);
        assert!(
            ThreadRelationRepositoryImpl::new(IdGeneratorWrapper::new(), pool)
                .find_active_by_child_canonical_key(&child_key)
                .await
                .unwrap()
                .is_none()
        );
    });
}

#[test]
fn equal_ranked_parents_conflict_without_active_parent() {
    run(async {
        let pool = setup_thread_group_pool().await;
        let service = ThreadGroupReconciliationService::with_id_generator(
            pool,
            infra::test_helper::shared_id_generator(),
        );
        let parent_a = recon_endpoint("parent-d1");
        let parent_b = recon_endpoint("parent-d2");
        let child = recon_endpoint("child-d");
        insert_thread(pool, 20_031, None).await;
        insert_thread(pool, 20_032, None).await;
        insert_thread(pool, 20_033, None).await;
        service
            .reconcile_subject(20_031, &parent_a, &[], "op", 1_000)
            .await
            .unwrap();
        service
            .reconcile_subject(20_032, &parent_b, &[], "op", 1_000)
            .await
            .unwrap();

        let outcome = service
            .reconcile_subject(
                20_033,
                &child,
                &[
                    observation(child.clone(), Some(parent_a)),
                    observation(child.clone(), Some(parent_b)),
                ],
                "op",
                1_001,
            )
            .await
            .unwrap();

        assert!(outcome.conflict);
        assert!(outcome.relation_selected.is_none());
        let child_key = key_of(&child);
        assert!(
            ThreadRelationRepositoryImpl::new(IdGeneratorWrapper::new(), pool)
                .find_active_by_child_canonical_key(&child_key)
                .await
                .unwrap()
                .is_none()
        );
    });
}

#[test]
fn self_edge_is_rejected() {
    run(async {
        let pool = setup_thread_group_pool().await;
        let service = ThreadGroupReconciliationService::with_id_generator(
            pool,
            infra::test_helper::shared_id_generator(),
        );
        let child = recon_endpoint("child-e");
        insert_thread(pool, 20_041, None).await;
        let outcome = service
            .reconcile_subject(
                20_041,
                &child,
                &[observation(child.clone(), Some(child.clone()))],
                "op",
                1_000,
            )
            .await
            .unwrap();
        assert!(outcome.relation_selected.is_none());
    });
}

#[test]
fn cycle_is_rejected() {
    run(async {
        let pool = setup_thread_group_pool().await;
        let service = ThreadGroupReconciliationService::with_id_generator(
            pool,
            infra::test_helper::shared_id_generator(),
        );
        let a = recon_endpoint("node-f1");
        let b = recon_endpoint("node-f2");
        insert_thread(pool, 20_051, None).await;
        insert_thread(pool, 20_052, None).await;

        // A -> B (B is A's parent).
        service
            .reconcile_subject(20_052, &b, &[], "op", 1_000)
            .await
            .unwrap();
        service
            .reconcile_subject(
                20_051,
                &a,
                &[observation(a.clone(), Some(b.clone()))],
                "op",
                1_001,
            )
            .await
            .unwrap();

        // B -> A would close the cycle.
        let outcome = service
            .reconcile_subject(20_052, &b, &[observation(b.clone(), Some(a))], "op", 1_002)
            .await
            .unwrap();
        assert!(outcome.relation_selected.is_none());
    });
}

/// Build a reconciler group with a parent and a delegated child, and
/// return `(group_id, parent_key, child_key)`.
async fn setup_pair(
    pool: &'static infra_utils::infra::rdb::RdbPool,
    service: &ThreadGroupReconciliationService,
    parent: &ObservedEndpoint,
    child: &ObservedEndpoint,
    parent_id: i64,
    child_id: i64,
) -> (i64, String, String) {
    insert_thread(pool, parent_id, None).await;
    insert_thread(pool, child_id, None).await;
    service
        .reconcile_subject(parent_id, parent, &[], "op", 1_000)
        .await
        .unwrap();
    let outcome = service
        .reconcile_subject(
            child_id,
            child,
            &[observation(child.clone(), Some(parent.clone()))],
            "op",
            1_001,
        )
        .await
        .unwrap();
    (
        outcome.group_id.expect("group"),
        key_of(parent),
        key_of(child),
    )
}

#[test]
fn merge_moves_members_and_redirects_source() {
    run(async {
        let pool = setup_thread_group_pool().await;
        let reconcile = ThreadGroupReconciliationService::with_id_generator(
            pool,
            infra::test_helper::shared_id_generator(),
        );
        let operator = ThreadGroupOperatorService::with_id_generator(
            pool,
            infra::test_helper::shared_id_generator(),
        );
        let (group_a, _, _) = setup_pair(
            pool,
            &reconcile,
            &recon_endpoint("m-parent-a"),
            &recon_endpoint("m-child-a"),
            20_101,
            20_102,
        )
        .await;
        let (group_b, _, _) = setup_pair(
            pool,
            &reconcile,
            &recon_endpoint("m-parent-b"),
            &recon_endpoint("m-child-b"),
            20_111,
            20_112,
        )
        .await;

        let outcome = operator
            .merge_groups(group_b, group_a, "operator-a", "duplicate work", 2_000)
            .await
            .unwrap();
        assert_eq!(outcome.target_group_id, group_a);
        assert_eq!(outcome.moved_members, 2);
        assert!(!outcome.already_merged);

        let groups = ThreadGroupRepositoryImpl::new(IdGeneratorWrapper::new(), pool);
        let source = groups.find_by_id(group_b).await.unwrap().unwrap();
        assert_eq!(source.status, values::group_status::REDIRECTED);
        assert_eq!(source.redirect_to_group_id, Some(group_a));

        let target_members = ThreadGroupMemberRepositoryImpl::new(pool)
            .list_current_by_group_id(group_a)
            .await
            .unwrap();
        assert_eq!(target_members.len(), 4);
        assert!(
            target_members
                .iter()
                .all(|m| m.provenance == values::grouping_authority::OPERATOR)
        );

        // Idempotent replay.
        let replay = operator
            .merge_groups(group_b, group_a, "operator-a", "duplicate work", 2_001)
            .await
            .unwrap();
        assert!(replay.already_merged);
    });
}

#[test]
fn split_returns_successors_and_rejects_a_different_partition() {
    run(async {
        let pool = setup_thread_group_pool().await;
        let reconcile = ThreadGroupReconciliationService::with_id_generator(
            pool,
            infra::test_helper::shared_id_generator(),
        );
        let operator = ThreadGroupOperatorService::with_id_generator(
            pool,
            infra::test_helper::shared_id_generator(),
        );
        let (group_a, parent_a, child_a) = setup_pair(
            pool,
            &reconcile,
            &recon_endpoint("s-parent-a"),
            &recon_endpoint("s-child-a"),
            20_201,
            20_202,
        )
        .await;
        let (group_b, parent_b, child_b) = setup_pair(
            pool,
            &reconcile,
            &recon_endpoint("s-parent-b"),
            &recon_endpoint("s-child-b"),
            20_211,
            20_212,
        )
        .await;
        operator
            .merge_groups(group_b, group_a, "operator-a", "combine", 2_100)
            .await
            .unwrap();

        let partition = vec![
            vec![parent_a.clone(), child_a.clone()],
            vec![parent_b.clone(), child_b.clone()],
        ];
        let successors = operator
            .split_group(group_a, &partition, "operator-a", "separate", 2_200)
            .await
            .unwrap();
        assert_eq!(successors.len(), 2);
        for successor in &successors {
            let members = ThreadGroupMemberRepositoryImpl::new(pool)
                .list_current_by_group_id(*successor)
                .await
                .unwrap();
            assert_eq!(members.len(), 2);
            assert!(
                members
                    .iter()
                    .all(|m| m.provenance == values::grouping_authority::OPERATOR)
            );
        }

        let groups = ThreadGroupRepositoryImpl::new(IdGeneratorWrapper::new(), pool);
        assert_eq!(
            groups.find_by_id(group_a).await.unwrap().unwrap().status,
            values::group_status::SPLIT
        );

        // Equivalent partitions retry in a different outer/key order but
        // must still return the original successors.
        let reordered = vec![
            vec![child_b.clone(), parent_b.clone()],
            vec![child_a.clone(), parent_a.clone()],
        ];
        let retry = operator
            .split_group(group_a, &reordered, "operator-a", "separate", 2_201)
            .await
            .unwrap();
        assert_eq!(retry, successors);

        // A different partition is rejected.
        let conflicting = vec![
            vec![parent_a.clone(), parent_b.clone()],
            vec![child_a.clone(), child_b.clone()],
        ];
        assert!(
            operator
                .split_group(group_a, &conflicting, "operator-a", "different", 2_202)
                .await
                .is_err()
        );
    });
}

#[test]
fn attach_member_is_idempotent_and_operator_fixed() {
    run(async {
        let pool = setup_thread_group_pool().await;
        let reconcile = ThreadGroupReconciliationService::with_id_generator(
            pool,
            infra::test_helper::shared_id_generator(),
        );
        let operator = ThreadGroupOperatorService::with_id_generator(
            pool,
            infra::test_helper::shared_id_generator(),
        );
        let (group, _, _) = setup_pair(
            pool,
            &reconcile,
            &recon_endpoint("a-parent"),
            &recon_endpoint("a-child"),
            20_301,
            20_302,
        )
        .await;
        let manual = recon_endpoint("a-manual");
        insert_thread(pool, 20_303, None).await;

        operator
            .attach_member(group, 20_303, &key_of(&manual), Some(&manual), 2_300)
            .await
            .unwrap();
        operator
            .attach_member(group, 20_303, &key_of(&manual), Some(&manual), 2_301)
            .await
            .unwrap();

        let member = ThreadGroupMemberRepositoryImpl::new(pool)
            .find_current_by_thread_canonical_key(&key_of(&manual))
            .await
            .unwrap()
            .expect("attached");
        assert_eq!(member.group_id, group);
        assert_eq!(member.provenance, values::grouping_authority::OPERATOR);
    });
}

#[test]
fn manual_collection_lifecycle_does_not_touch_lineage() {
    run(async {
        let pool = setup_thread_group_pool().await;
        let operator = ThreadGroupOperatorService::with_id_generator(
            pool,
            infra::test_helper::shared_id_generator(),
        );
        let collection = operator
            .create_manual_collection("user:1", "favorites", 2_400)
            .await
            .unwrap();
        let listed = operator
            .list_manual_collections("user:1", None, None)
            .await
            .unwrap();
        assert!(listed.iter().any(|row| row.id == collection));
        assert_eq!(listed[0].id, collection, "updated_at DESC, id ASC");

        assert!(
            operator
                .attach_manual_collection_member(collection, 20_401, "user:1", 2_401)
                .await
                .unwrap()
        );
        assert!(
            !operator
                .attach_manual_collection_member(collection, 20_401, "user:1", 2_402)
                .await
                .unwrap()
        );
        assert_eq!(
            operator
                .list_manual_collection_members(collection)
                .await
                .unwrap()
                .len(),
            1
        );
        assert!(
            operator
                .detach_manual_collection_member(collection, 20_401, 2_403)
                .await
                .unwrap()
        );
        assert!(
            operator
                .list_manual_collection_members(collection)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(operator.delete_manual_collection(collection).await.unwrap());
        assert!(
            operator
                .list_manual_collections("user:1", None, None)
                .await
                .unwrap()
                .iter()
                .all(|row| row.id != collection)
        );
    });
}

#[derive(Default)]
struct RecordingSink {
    events: std::sync::Mutex<Vec<(String, String, String)>>,
}

#[async_trait::async_trait]
impl ThreadGroupEventSink for RecordingSink {
    async fn enqueue(&self, event_id: &str, event_type: &str, payload: &str) -> anyhow::Result<()> {
        self.events.lock().unwrap().push((
            event_id.to_string(),
            event_type.to_string(),
            payload.to_string(),
        ));
        Ok(())
    }
}

#[test]
fn outbox_dispatcher_delivers_then_deletes() {
    run(async {
        let pool = setup_thread_group_pool().await;
        let service = ThreadGroupObservationService::with_id_generator(
            pool,
            infra::test_helper::shared_id_generator(),
        );
        let input = observation(
            endpoint("codex", IdentityScope::known(""), "dispatch-subject"),
            Some(endpoint(
                "codex",
                IdentityScope::known(""),
                "dispatch-parent",
            )),
        );
        service
            .record_observation(
                &input,
                values::observation_state::CANDIDATE,
                "op-dispatch",
                3_000,
            )
            .await
            .unwrap();

        let fingerprint = observation_fingerprint(&input, values::observation_origin::ADAPTER);
        let expected_event = event_id_for(ThreadGroupEventType::ObservationRecorded, &fingerprint);

        let dispatcher = ThreadGroupOutboxDispatcher::new(pool);
        let sink = RecordingSink::default();
        dispatcher.dispatch_once(&sink, None).await.unwrap();

        let recorded = sink.events.lock().unwrap().clone();
        assert!(
            recorded.iter().any(|(id, _, _)| id == &expected_event),
            "dispatcher must enqueue the recorded event"
        );

        let outbox = ThreadGroupEventOutboxRepositoryImpl::new(pool);
        assert!(
            outbox
                .find_by_event_id(&expected_event)
                .await
                .unwrap()
                .is_none(),
            "delivered event must be deleted from the outbox"
        );
    });
}

#[test]
fn reconciliation_report_counts_unsupported_observations() {
    run(async {
        let pool = setup_thread_group_pool().await;
        let reconcile = ThreadGroupReconciliationService::with_id_generator(
            pool,
            infra::test_helper::shared_id_generator(),
        );
        let read = ThreadGroupReadService::new(pool);
        let before = read.reconciliation_report().await.unwrap();

        let parent = recon_endpoint("report-parent");
        let child = recon_endpoint("report-child");
        insert_thread(pool, 20_501, None).await;
        insert_thread(pool, 20_502, None).await;
        reconcile
            .reconcile_subject(20_501, &parent, &[], "op", 3_100)
            .await
            .unwrap();
        let mut unsupported = observation(child.clone(), Some(parent.clone()));
        unsupported.source_confidence = Some(values::source_confidence::UNSUPPORTED.into());
        reconcile
            .reconcile_subject(20_502, &child, &[unsupported], "op", 3_101)
            .await
            .unwrap();

        let after = read.reconciliation_report().await.unwrap();
        assert!(
            after.unsupported_observations > before.unsupported_observations,
            "unsupported evidence must be visible in the report"
        );
    });
}

/// Build a conflicting child (two equal-ranked parents) and return the
/// recorded conflict candidate ids.
async fn setup_conflict_candidates(
    pool: &'static infra_utils::infra::rdb::RdbPool,
    reconcile: &ThreadGroupReconciliationService,
    child_native: &str,
    child_thread_id: i64,
    parent_a_native: &str,
    parent_b_native: &str,
) -> Vec<i64> {
    let parent_a = recon_endpoint(parent_a_native);
    let parent_b = recon_endpoint(parent_b_native);
    let child = recon_endpoint(child_native);
    insert_thread(pool, child_thread_id - 2, None).await;
    insert_thread(pool, child_thread_id - 1, None).await;
    insert_thread(pool, child_thread_id, None).await;
    reconcile
        .reconcile_subject(child_thread_id - 2, &parent_a, &[], "op", 5_000)
        .await
        .unwrap();
    reconcile
        .reconcile_subject(child_thread_id - 1, &parent_b, &[], "op", 5_000)
        .await
        .unwrap();
    reconcile
        .reconcile_subject(
            child_thread_id,
            &child,
            &[
                observation(child.clone(), Some(parent_a)),
                observation(child.clone(), Some(parent_b)),
            ],
            "op",
            5_001,
        )
        .await
        .unwrap();
    ThreadGroupCandidateAssociationRepositoryImpl::new(
        infra::test_helper::shared_id_generator(),
        pool,
    )
    .list_by_subject_identity("user:1", "codex", true, "", child_native)
    .await
    .unwrap()
    .into_iter()
    .filter(|row| row.state == values::candidate_state::CONFLICT)
    .map(|row| row.id)
    .collect()
}

#[test]
fn operator_confirmation_adopts_and_retraction_revokes() {
    run(async {
        let pool = setup_thread_group_pool().await;
        let reconcile = ThreadGroupReconciliationService::with_id_generator(
            pool,
            infra::test_helper::shared_id_generator(),
        );
        let candidates = setup_conflict_candidates(
            pool,
            &reconcile,
            "op-child",
            20_603,
            "op-parent-a",
            "op-parent-b",
        )
        .await;
        assert_eq!(candidates.len(), 2);

        let outcome = reconcile
            .record_operator_decision(candidates[0], "confirm", "operator-a", "reviewed", 5_100)
            .await
            .unwrap();
        let relation_id = outcome.relation_id.expect("relation adopted");
        assert_eq!(outcome.state, values::candidate_state::CANDIDATE);

        let child_key = key_of(&recon_endpoint("op-child"));
        let relation =
            ThreadRelationRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool)
                .find_by_id(relation_id)
                .await
                .unwrap()
                .unwrap();
        assert_eq!(
            relation.selection_basis,
            values::selection_basis::OPERATOR_CONFIRMATION
        );
        assert!(relation.selected_operator_decision_id.is_some());

        // Identical replay does not append a second decision.
        let replay = reconcile
            .record_operator_decision(candidates[0], "confirm", "operator-a", "reviewed", 5_101)
            .await
            .unwrap();
        assert_eq!(replay.decision_id, outcome.decision_id);
        let decisions =
            OperatorDecisionRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool)
                .list_by_candidate_association_id(candidates[0])
                .await
                .unwrap();
        assert_eq!(decisions.len(), 1);

        // Retraction revokes the operator-adopted relation.
        reconcile
            .record_operator_decision(candidates[0], "retract", "operator-a", "wrong", 5_102)
            .await
            .unwrap();
        let retracted =
            ThreadRelationRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool)
                .find_by_id(relation_id)
                .await
                .unwrap()
                .unwrap();
        assert_eq!(retracted.state, values::relation_state::RETRACTED);
        let _ = child_key;
    });
}

#[test]
fn operator_rejection_supersedes_the_candidate() {
    run(async {
        let pool = setup_thread_group_pool().await;
        let reconcile = ThreadGroupReconciliationService::with_id_generator(
            pool,
            infra::test_helper::shared_id_generator(),
        );
        let candidates = setup_conflict_candidates(
            pool,
            &reconcile,
            "op-rej-child",
            20_613,
            "op-rej-parent-a",
            "op-rej-parent-b",
        )
        .await;
        let outcome = reconcile
            .record_operator_decision(candidates[1], "reject", "operator-a", "not lineage", 5_200)
            .await
            .unwrap();
        assert_eq!(outcome.state, values::candidate_state::SUPERSEDED);
    });
}

#[test]
fn root_member_is_the_parentless_group_member() {
    run(async {
        let pool = setup_thread_group_pool().await;
        let reconcile = ThreadGroupReconciliationService::with_id_generator(
            pool,
            infra::test_helper::shared_id_generator(),
        );
        let parent = recon_endpoint("root-parent");
        let child = recon_endpoint("root-child");
        let (group, parent_key, child_key) =
            setup_pair(pool, &reconcile, &parent, &child, 20_701, 20_702).await;
        let read = ThreadGroupReadService::new(pool);
        let root = read.root_member(group).await.unwrap().expect("root");
        assert_eq!(root.thread_canonical_key, parent_key);
        assert_ne!(root.thread_canonical_key, child_key);
        let view = read.get_lineage(group).await.unwrap().unwrap().group;
        assert_eq!(
            view.root_thread_canonical_key.as_deref(),
            Some(parent_key.as_str())
        );
    });
}

#[test]
fn group_read_model_contains_live_and_deleted_display_information() {
    run(async {
        let pool = setup_thread_group_pool().await;
        let group_repo =
            ThreadGroupRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool);
        let group_id = group_repo
            .create_tx(pool, &new_group(90_001))
            .await
            .unwrap();
        insert_thread(pool, 90_001, Some(1_234)).await;
        sqlx::query(
            "UPDATE thread SET description = ?, updated_at = ?, last_message_at = ? WHERE id = ?",
        )
        .bind("active description")
        .bind(1_235_i64)
        .bind(1_234_i64)
        .bind(90_001_i64)
        .execute(pool)
        .await
        .unwrap();

        let members = ThreadGroupMemberRepositoryImpl::new(pool);
        members
            .insert_tx(pool, &new_member(group_id, Some(90_001), key(90_001)))
            .await
            .unwrap();
        let mut deleted = new_member(group_id, None, key(90_002));
        deleted.state = values::member_state::DELETED.to_string();
        deleted.deleted_at = Some(1_236);
        members.insert_tx(pool, &deleted).await.unwrap();

        let read = ThreadGroupReadService::new(pool);
        let view = read
            .list_groups(false, None, None)
            .await
            .unwrap()
            .into_iter()
            .find(|group| group.id == group_id)
            .expect("active group");
        assert_eq!(view.active_member_count, 1);
        assert_eq!(view.deleted_member_count, 1);
        assert_eq!(view.root_thread_id, Some(90_001));
        assert_eq!(
            view.root_display
                .as_ref()
                .and_then(|display| display.description.as_deref()),
            Some("active description")
        );
        assert!(!view.membership_snapshot_digest.is_empty());

        let lineage = read.get_lineage(group_id).await.unwrap().unwrap();
        let deleted_display = lineage
            .member_displays
            .iter()
            .find(|display| display.thread_canonical_key == key(90_002))
            .expect("deleted display");
        assert_eq!(deleted_display.thread_id, None);
        assert_eq!(deleted_display.deleted_at, Some(1_236));
        assert!(
            lineage
                .member_displays
                .iter()
                .any(|display| display.description.as_deref() == Some("active description"))
        );
    });
}

#[test]
fn lineage_and_group_view_count_the_same_unresolved_candidates() {
    run(async {
        let pool = setup_thread_group_pool().await;
        let group_repo =
            ThreadGroupRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool);
        let group_id = group_repo
            .create_tx(pool, &new_group(98_763_001))
            .await
            .unwrap();
        let other_group_id = group_repo
            .create_tx(pool, &new_group(98_763_002))
            .await
            .unwrap();
        insert_thread(pool, 98_763_001, None).await;
        ThreadGroupMemberRepositoryImpl::new(pool)
            .insert_tx(
                pool,
                &new_member(group_id, Some(98_763_001), key(98_763_001)),
            )
            .await
            .unwrap();
        let candidates = ThreadGroupCandidateAssociationRepositoryImpl::new(
            infra::test_helper::shared_id_generator(),
            pool,
        );
        for (index, state) in [
            values::candidate_state::PENDING,
            values::candidate_state::AMBIGUOUS,
            values::candidate_state::CONFLICT,
        ]
        .into_iter()
        .enumerate()
        {
            let mut by_group =
                infra::infra::thread_group::test_support::new_candidate(92_010 + index as u64);
            by_group.state = state.to_string();
            by_group.candidate_group_id = Some(group_id);
            by_group.subject_thread_id = Some(98_763_001); // Both paths still count this row once.
            candidates.insert_tx(pool, &by_group).await.unwrap();

            let mut by_thread =
                infra::infra::thread_group::test_support::new_candidate(92_020 + index as u64);
            by_thread.state = state.to_string();
            by_thread.candidate_group_id = Some(other_group_id);
            by_thread.subject_thread_id = Some(98_763_001);
            candidates.insert_tx(pool, &by_thread).await.unwrap();

            let mut outside =
                infra::infra::thread_group::test_support::new_candidate(92_030 + index as u64);
            outside.state = state.to_string();
            candidates.insert_tx(pool, &outside).await.unwrap();
        }
        let read = ThreadGroupReadService::new(pool);
        let lineage = read.get_lineage(group_id).await.unwrap().unwrap();
        assert_eq!(lineage.unresolved.len(), 6);
        assert_eq!(lineage.group.unresolved_count, 6);
    });
}

#[test]
fn lineage_returns_only_active_relations_for_tree_data() {
    run(async {
        let pool = setup_thread_group_pool().await;
        let group_repo =
            ThreadGroupRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool);
        let group_id = group_repo
            .create_tx(pool, &new_group(91_001))
            .await
            .unwrap();
        insert_thread(pool, 91_001, None).await;
        insert_thread(pool, 91_002, None).await;
        let members = ThreadGroupMemberRepositoryImpl::new(pool);
        members
            .insert_tx(pool, &new_member(group_id, Some(91_001), key(91_001)))
            .await
            .unwrap();
        members
            .insert_tx(pool, &new_member(group_id, Some(91_002), key(91_002)))
            .await
            .unwrap();

        let relations =
            ThreadRelationRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool);
        let mut historical = infra::infra::thread_group::test_support::new_relation(91_001, 91_002);
        historical.parent_thread_id = Some(91_001);
        historical.child_thread_id = Some(91_002);
        let historical_id = relations.insert_tx(pool, &historical).await.unwrap();
        let mut tx = pool.begin().await.unwrap();
        assert!(
            relations
                .set_state_tx(
                    &mut *tx,
                    historical_id,
                    values::relation_state::ACTIVE,
                    values::relation_state::RETRACTED,
                    2_000,
                )
                .await
                .unwrap()
        );
        tx.commit().await.unwrap();
        let active_id = relations.insert_tx(pool, &historical).await.unwrap();

        let lineage = ThreadGroupReadService::new(pool)
            .get_lineage(group_id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(lineage.relations.len(), 1);
        assert_eq!(lineage.relations[0].id, active_id);
        assert_eq!(lineage.relations[0].state, values::relation_state::ACTIVE);
    });
}

#[test]
fn group_summary_lookup_and_count_use_the_group_summary_kind() {
    run(async {
        let pool = setup_thread_group_pool().await;
        let group_repo =
            ThreadGroupRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool);
        let group_id = group_repo
            .create_tx(pool, &new_group(92_001))
            .await
            .unwrap();
        insert_thread(pool, 92_001, None).await;
        let members = ThreadGroupMemberRepositoryImpl::new(pool);
        members
            .insert_tx(pool, &new_member(group_id, Some(92_001), key(92_001)))
            .await
            .unwrap();
        let read = ThreadGroupReadService::new(pool);
        let before = read.count_group_summaries().await.unwrap();

        let memories = MemoryRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool);
        memories
            .create(
                pool,
                &protobuf::llm_memory::data::MemoryData {
                    user_id: Some(protobuf::llm_memory::data::UserId { value: 1 }),
                    content: "group summary".into(),
                    content_type: protobuf::llm_memory::data::ContentType::Text as i32,
                    external_id: Some(format!("thread-group-summary:{group_id}")),
                    memory_kind: protobuf::llm_memory::data::MemoryKind::DerivedSummary as i32,
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let summary = read
            .find_group_summary(group_id)
            .await
            .unwrap()
            .expect("group summary");
        assert_eq!(
            summary.data.unwrap().memory_kind,
            protobuf::llm_memory::data::MemoryKind::DerivedSummary as i32
        );
        assert_eq!(read.count_group_summaries().await.unwrap(), before + 1);
    });
}

#[test]
fn thread_group_search_filter_resolves_current_members() {
    run(async {
        let pool = setup_thread_group_pool().await;
        insert_thread(pool, 30_001, None).await;
        insert_thread(pool, 30_002, None).await;
        let generator = infra::test_helper::shared_id_generator();
        let groups = ThreadGroupRepositoryImpl::new(generator.clone(), pool);
        let group_id = groups.create_tx(pool, &new_group(9_001)).await.unwrap();
        let members = ThreadGroupMemberRepositoryImpl::new(pool);
        members
            .insert_tx(pool, &new_member(group_id, Some(30_001), key(1)))
            .await
            .unwrap();
        // A deleted placeholder has no live thread_id (design 5.2), so it
        // never contributes a search hit.
        let mut deleted = new_member(group_id, None, key(2));
        deleted.state = values::member_state::DELETED.to_string();
        deleted.deleted_at = Some(1);
        members.insert_tx(pool, &deleted).await.unwrap();

        let thread_repo = ThreadRepositoryImpl::new(generator, pool);
        let ids = thread_repo
            .find_thread_ids_by_group_id(group_id, 10)
            .await
            .unwrap();
        assert_eq!(
            ids,
            vec![30_001],
            "only live members are searchable; placeholders are not"
        );
    });
}

#[test]
fn purge_preview_rejects_wrong_kind_at_group_summary_external_id() {
    run(async {
        let pool = setup_thread_group_pool().await;
        let groups =
            ThreadGroupRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool);
        let group_id = groups.create_tx(pool, &new_group(92_110)).await.unwrap();
        let memories = MemoryRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool);
        memories
            .create(
                pool,
                &protobuf::llm_memory::data::MemoryData {
                    user_id: Some(protobuf::llm_memory::data::UserId { value: 1 }),
                    content: "unrelated".into(),
                    external_id: Some(format!("thread-group-summary:{group_id}")),
                    memory_kind: protobuf::llm_memory::data::MemoryKind::Raw as i32,
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let error = ThreadGroupPurgeService::new(pool)
            .preview(group_id)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("different kind"));
        assert!(groups.find_by_id(group_id).await.unwrap().is_some());
    });
}

#[test]
fn purge_preview_is_stale_after_summary_is_created() {
    run(async {
        let pool = setup_thread_group_pool().await;
        let groups =
            ThreadGroupRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool);
        let group_id = groups.create_tx(pool, &new_group(92_120)).await.unwrap();
        let purge = ThreadGroupPurgeService::new(pool);
        let before = purge.preview(group_id).await.unwrap().unwrap();
        assert!(before.summary_memory_id.is_none());
        let memories = MemoryRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool);
        let summary_id = memories
            .create(
                pool,
                &protobuf::llm_memory::data::MemoryData {
                    user_id: Some(protobuf::llm_memory::data::UserId { value: 1 }),
                    content: "group summary".into(),
                    external_id: Some(format!("thread-group-summary:{group_id}")),
                    memory_kind: protobuf::llm_memory::data::MemoryKind::DerivedSummary as i32,
                    ..Default::default()
                },
            )
            .await
            .unwrap();

        let after = purge.preview(group_id).await.unwrap().unwrap();
        assert_eq!(after.summary_memory_id, Some(summary_id.value));
        assert_ne!(before.digest, after.digest);
        let stale = purge.delete(group_id, &before.digest).await.unwrap_err();
        assert!(stale.to_string().contains("stale"));
        let blocked = purge.delete(group_id, &after.digest).await.unwrap_err();
        assert!(
            blocked
                .to_string()
                .contains("no explicit delete relationship")
        );
        assert!(groups.find_by_id(group_id).await.unwrap().is_some());
        assert!(
            memories
                .find_by_external_id(&format!("thread-group-summary:{group_id}"))
                .await
                .unwrap()
                .is_some()
        );
    });
}

#[test]
fn purge_preview_is_stale_after_summary_content_changes_without_identity_change() {
    run(async {
        let pool = setup_thread_group_pool().await;
        let groups =
            ThreadGroupRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool);
        let group_id = groups.create_tx(pool, &new_group(92_125)).await.unwrap();
        let memories = MemoryRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool);
        let id = memories
            .create(
                pool,
                &protobuf::llm_memory::data::MemoryData {
                    user_id: Some(protobuf::llm_memory::data::UserId { value: 1 }),
                    external_id: Some(format!("thread-group-summary:{group_id}")),
                    memory_kind: protobuf::llm_memory::data::MemoryKind::DerivedSummary as i32,
                    content: "original".into(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let purge = ThreadGroupPurgeService::new(pool);
        let before = purge.preview(group_id).await.unwrap().unwrap();
        assert_eq!(before.summary_memory_id, Some(id.value));
        let mut tx = pool.begin().await.unwrap();
        assert!(
            memories
                .update_content_only(&mut *tx, &id, "changed")
                .await
                .unwrap()
        );
        tx.commit().await.unwrap();
        let after = purge.preview(group_id).await.unwrap().unwrap();
        assert_eq!(after.summary_memory_id, before.summary_memory_id);
        assert_ne!(after.digest, before.digest);
        assert!(
            purge
                .delete(group_id, &before.digest)
                .await
                .unwrap_err()
                .to_string()
                .contains("stale")
        );
        assert!(groups.find_by_id(group_id).await.unwrap().is_some());
    });
}

#[test]
fn group_memory_link_rejects_missing_group_and_shared_owned_memory() {
    run(async {
        let pool = setup_thread_group_pool().await;
        let groups =
            ThreadGroupRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool);
        let first = groups.create_tx(pool, &new_group(92_140)).await.unwrap();
        let second = groups.create_tx(pool, &new_group(92_141)).await.unwrap();
        let third = groups.create_tx(pool, &new_group(92_142)).await.unwrap();
        insert_thread(pool, 92_143, None).await;
        infra::infra::thread_label::rdb::ThreadLabelRepository::add_labels(
            &ThreadLabelRepositoryImpl::new(pool),
            92_143,
            &["thread_group_summary".into(), format!("group_{first}")],
            1,
        )
        .await
        .unwrap();
        let memories = MemoryRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool);
        let id = memories
            .create(
                pool,
                &protobuf::llm_memory::data::MemoryData {
                    user_id: Some(protobuf::llm_memory::data::UserId { value: 1 }),
                    content: "generated".into(),
                    external_id: Some(format!("thread-group-summary:{first}")),
                    memory_kind: protobuf::llm_memory::data::MemoryKind::DerivedSummary as i32,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        ThreadMemoryRepositoryImpl::new(pool)
            .insert_auto_position_tx(pool, 92_143, id.value, 1)
            .await
            .unwrap();
        let relation = ThreadGroupMemoryRelationService::new(pool);
        assert!(
            relation
                .link_existing(
                    9_999_999,
                    id.value,
                    "summary",
                    GroupMemoryDeletePolicy::Delete
                )
                .await
                .is_err()
        );
        relation
            .link_existing(first, id.value, "summary", GroupMemoryDeletePolicy::Delete)
            .await
            .unwrap();
        relation
            .link_existing(first, id.value, "summary", GroupMemoryDeletePolicy::Delete)
            .await
            .unwrap();
        assert!(
            relation
                .link_existing(
                    first,
                    id.value,
                    "reference",
                    GroupMemoryDeletePolicy::Retain
                )
                .await
                .is_err()
        );
        assert!(
            relation
                .link_existing(
                    second,
                    id.value,
                    "reference",
                    GroupMemoryDeletePolicy::Retain
                )
                .await
                .is_err()
        );
        let other = memories
            .create(
                pool,
                &protobuf::llm_memory::data::MemoryData {
                    user_id: Some(protobuf::llm_memory::data::UserId { value: 1 }),
                    content: "shared".into(),
                    memory_kind: protobuf::llm_memory::data::MemoryKind::DerivedSummary as i32,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        relation
            .link_existing(
                first,
                other.value,
                "reference",
                GroupMemoryDeletePolicy::Retain,
            )
            .await
            .unwrap();
        relation
            .link_existing(
                second,
                other.value,
                "reference",
                GroupMemoryDeletePolicy::Retain,
            )
            .await
            .unwrap();
        relation
            .link_existing(
                second,
                other.value,
                "reference",
                GroupMemoryDeletePolicy::Retain,
            )
            .await
            .unwrap();
        assert!(
            relation
                .link_existing(
                    third,
                    other.value,
                    "summary",
                    GroupMemoryDeletePolicy::Delete
                )
                .await
                .is_err()
        );
        let mismatched = memories
            .create(
                pool,
                &protobuf::llm_memory::data::MemoryData {
                    user_id: Some(protobuf::llm_memory::data::UserId { value: 1 }),
                    content: "group summary".into(),
                    memory_kind: protobuf::llm_memory::data::MemoryKind::DerivedSummary as i32,
                    external_id: Some(format!("thread-group-summary:{second}")),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert!(
            relation
                .link_existing(
                    third,
                    mismatched.value,
                    "summary",
                    GroupMemoryDeletePolicy::Delete
                )
                .await
                .is_err()
        );
    });
}

#[test]
fn purge_does_not_leave_group_memory_relations_orphaned() {
    run(async {
        let pool = setup_thread_group_pool().await;
        let groups =
            ThreadGroupRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool);
        let group_id = groups.create_tx(pool, &new_group(92_145)).await.unwrap();
        let purge = ThreadGroupPurgeService::new(pool);
        let before = purge.preview(group_id).await.unwrap().unwrap();
        let memories = MemoryRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool);
        let id = memories
            .create(
                pool,
                &protobuf::llm_memory::data::MemoryData {
                    user_id: Some(protobuf::llm_memory::data::UserId { value: 1 }),
                    content: "linked".into(),
                    memory_kind: protobuf::llm_memory::data::MemoryKind::DerivedSummary as i32,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        ThreadGroupMemoryRelationService::new(pool)
            .link_existing(
                group_id,
                id.value,
                "reference",
                GroupMemoryDeletePolicy::Retain,
            )
            .await
            .unwrap();
        let after = purge.preview(group_id).await.unwrap().unwrap();
        assert_ne!(before.digest, after.digest);
        assert!(
            purge
                .delete(group_id, &before.digest)
                .await
                .unwrap_err()
                .to_string()
                .contains("stale")
        );
        assert!(after.blocked_reason.is_empty());
        assert!(
            purge
                .delete(group_id, &after.digest)
                .await
                .unwrap()
                .group_deleted
        );
        assert!(groups.find_by_id(group_id).await.unwrap().is_none());
        assert!(memories.find(&id, false).await.unwrap().is_some());
    });
}

#[test]
fn purge_preview_tracks_parent_references_across_multiple_retained_memories() {
    run(async {
        let pool = setup_thread_group_pool().await;
        let ids = infra::test_helper::shared_id_generator();
        let group = ThreadGroupRepositoryImpl::new(ids.clone(), pool)
            .create_tx(pool, &new_group(92_166))
            .await
            .unwrap();
        let memories = MemoryRepositoryImpl::new(ids, pool);
        let data = protobuf::llm_memory::data::MemoryData {
            user_id: Some(protobuf::llm_memory::data::UserId { value: 1 }),
            content: "reference".into(),
            ..Default::default()
        };
        let first = memories.create(pool, &data).await.unwrap();
        let second = memories.create(pool, &data).await.unwrap();
        let links = ThreadGroupMemoryRelationService::new(pool);
        for id in [&first, &second] {
            links
                .link_existing(
                    group,
                    id.value,
                    "reference",
                    GroupMemoryDeletePolicy::Retain,
                )
                .await
                .unwrap();
        }
        let purge = ThreadGroupPurgeService::new(pool);
        let before = purge.preview(group).await.unwrap().unwrap();
        let child = memories
            .create(
                pool,
                &protobuf::llm_memory::data::MemoryData {
                    parent_ids: vec![second],
                    ..data
                },
            )
            .await
            .unwrap();
        let after = purge.preview(group).await.unwrap().unwrap();
        assert_ne!(before.digest, after.digest);
        assert!(
            purge
                .delete(group, &before.digest)
                .await
                .unwrap_err()
                .to_string()
                .contains("stale")
        );
        memories.delete(&child).await.unwrap();
        assert_eq!(
            before.digest,
            purge.preview(group).await.unwrap().unwrap().digest
        );
    });
}

#[test]
fn sqlite_purge_reserves_the_writer_before_checking_memory_relationships() {
    run(async {
        let pool = setup_thread_group_pool().await;
        let ids = infra::test_helper::shared_id_generator();
        let groups = ThreadGroupRepositoryImpl::new(ids.clone(), pool);
        let group_id = groups.create_tx(pool, &new_group(92_155)).await.unwrap();
        let id = MemoryRepositoryImpl::new(ids, pool)
            .create(
                pool,
                &protobuf::llm_memory::data::MemoryData {
                    user_id: Some(protobuf::llm_memory::data::UserId { value: 1 }),
                    content: "guard".into(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let mut tx = pool.begin().await.unwrap();
        groups
            .reserve_purge_write_tx(&mut *tx, group_id)
            .await
            .unwrap();
        let mut link = tokio::spawn(async move {
            ThreadGroupMemoryRelationService::new(pool)
                .link_existing(
                    group_id,
                    id.value,
                    "reference",
                    GroupMemoryDeletePolicy::Retain,
                )
                .await
        });
        let outcome = tokio::time::timeout(std::time::Duration::from_millis(100), &mut link).await;
        let pending = outcome.is_err();
        if let Ok(result) = outcome {
            assert!(
                result.unwrap().is_err(),
                "the concurrent relationship write must not commit"
            );
        }
        groups.delete_tx(&mut *tx, group_id).await.unwrap();
        tx.commit().await.unwrap();
        if pending {
            assert!(link.await.unwrap().is_err());
        }
        assert!(
            ThreadGroupMemoryRelationRepositoryImpl::new(pool)
                .list_by_group_id(group_id)
                .await
                .unwrap()
                .is_empty()
        );
    });
}

#[test]
fn global_orphan_cleanup_requires_preview_and_keeps_unproven_memory() {
    run(async {
        let pool = setup_thread_group_pool().await;
        let groups =
            ThreadGroupRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool);
        let removed_group = groups.create_tx(pool, &new_group(92_158)).await.unwrap();
        let surviving_group = groups.create_tx(pool, &new_group(92_159)).await.unwrap();
        let repo = MemoryRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool);
        let old = repo
            .create(
                pool,
                &protobuf::llm_memory::data::MemoryData {
                    user_id: Some(protobuf::llm_memory::data::UserId { value: 1 }),
                    content: "unknown provenance".into(),
                    external_id: Some(format!("thread-group-summary:{removed_group}")),
                    memory_kind: protobuf::llm_memory::data::MemoryKind::DerivedSummary as i32,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let retained = repo
            .create(
                pool,
                &protobuf::llm_memory::data::MemoryData {
                    user_id: Some(protobuf::llm_memory::data::UserId { value: 1 }),
                    content: "retained".into(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let surviving_summary = repo
            .create(
                pool,
                &protobuf::llm_memory::data::MemoryData {
                    user_id: Some(protobuf::llm_memory::data::UserId { value: 1 }),
                    content: "existing group".into(),
                    external_id: Some(format!("thread-group-summary:{surviving_group}")),
                    memory_kind: protobuf::llm_memory::data::MemoryKind::DerivedSummary as i32,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        ThreadGroupMemoryRelationRepositoryImpl::new(pool)
            .insert_tx(
                pool,
                removed_group,
                retained.value,
                "reference",
                "retain",
                1,
            )
            .await
            .unwrap();
        ThreadGroupMemoryRelationRepositoryImpl::new(pool)
            .insert_tx(pool, removed_group, 9_999_991, "summary", "delete", 2)
            .await
            .unwrap();
        groups.delete_tx(pool, removed_group).await.unwrap();
        let config = memory_utils::cache::stretto::MemoryCacheConfig::default();
        let ids = infra::test_helper::shared_id_generator();
        let memory_app = MemoryAppImpl::new(
            MemoryRepositoryImpl::new(ids.clone(), pool),
            MemoryRatingRepositoryImpl::new(ids.clone(), pool),
            ThreadRepositoryImpl::new(ids, pool),
            ThreadMemoryRepositoryImpl::new(pool),
            ThreadLabelRepositoryImpl::new(pool),
            memory_utils::cache::stretto::new_memory_cache(&config),
            memory_utils::cache::stretto::new_memory_cache(&config),
            None,
        );
        let cleanup = ThreadGroupOrphanCleanupService::new(pool, std::sync::Arc::new(memory_app));
        let preview = cleanup.preview().await.unwrap();
        assert_eq!(preview.digest, cleanup.preview().await.unwrap().digest);
        assert_eq!(preview.orphan_relation_count, 2);
        assert!(!preview.memory_index_available);
        assert!(!preview.thread_index_available);
        assert!(preview.unsafe_memory_ids.contains(&old.value));
        assert!(!preview.unsafe_memory_ids.contains(&surviving_summary.value));
        assert!(cleanup.execute("incorrect").await.is_err());
        let outcome = cleanup.execute(&preview.digest).await.unwrap();
        assert_eq!(outcome.deleted_relation_count, 1);
        assert_eq!(cleanup.preview().await.unwrap().orphan_relation_count, 1);
        assert!(!outcome.memory_index_available);
        assert!(!outcome.thread_index_available);
        assert!(repo.find(&old, false).await.unwrap().is_some());
        assert!(repo.find(&retained, false).await.unwrap().is_some());
        assert!(
            repo.find(&surviving_summary, false)
                .await
                .unwrap()
                .is_some()
        );
        assert!(groups.find_by_id(surviving_group).await.unwrap().is_some());
    });
}

#[test]
fn global_orphan_cleanup_removes_verified_summary_but_keeps_storage_thread() {
    run(async {
        let pool = setup_thread_group_pool().await;
        let ids = infra::test_helper::shared_id_generator();
        let groups = ThreadGroupRepositoryImpl::new(ids.clone(), pool);
        let group = groups.create_tx(pool, &new_group(92_160)).await.unwrap();
        insert_thread(pool, 92_161, None).await;
        infra::infra::thread_label::rdb::ThreadLabelRepository::add_labels(
            &ThreadLabelRepositoryImpl::new(pool),
            92_161,
            &["thread_group_summary".into(), format!("group_{group}")],
            1,
        )
        .await
        .unwrap();
        let repo = MemoryRepositoryImpl::new(ids.clone(), pool);
        let id = repo
            .create(
                pool,
                &protobuf::llm_memory::data::MemoryData {
                    user_id: Some(protobuf::llm_memory::data::UserId { value: 1 }),
                    content: "orphaned".into(),
                    external_id: Some(format!("thread-group-summary:{group}")),
                    memory_kind: protobuf::llm_memory::data::MemoryKind::DerivedSummary as i32,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        ThreadMemoryRepositoryImpl::new(pool)
            .insert_auto_position_tx(pool, 92_161, id.value, 1)
            .await
            .unwrap();
        ThreadGroupMemoryRelationService::new(pool)
            .link_existing(group, id.value, "summary", GroupMemoryDeletePolicy::Delete)
            .await
            .unwrap();
        groups.delete_tx(pool, group).await.unwrap();
        let config = memory_utils::cache::stretto::MemoryCacheConfig::default();
        let memory_app = MemoryAppImpl::new(
            repo,
            MemoryRatingRepositoryImpl::new(ids.clone(), pool),
            ThreadRepositoryImpl::new(ids, pool),
            ThreadMemoryRepositoryImpl::new(pool),
            ThreadLabelRepositoryImpl::new(pool),
            memory_utils::cache::stretto::new_memory_cache(&config),
            memory_utils::cache::stretto::new_memory_cache(&config),
            None,
        );
        let cleanup = ThreadGroupOrphanCleanupService::new(pool, std::sync::Arc::new(memory_app));
        let before = cleanup.preview().await.unwrap();
        assert_eq!(before.safe_memory_ids, vec![id.value]);
        let mut tx = pool.begin().await.unwrap();
        MemoryRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool)
            .update_content_only(&mut *tx, &id, "changed")
            .await
            .unwrap();
        tx.commit().await.unwrap();
        assert!(
            cleanup
                .execute(&before.digest)
                .await
                .unwrap_err()
                .to_string()
                .contains("stale")
        );
        let after = cleanup.preview().await.unwrap();
        let outcome = cleanup.execute(&after.digest).await.unwrap();
        assert_eq!(outcome.deleted_memory_ids, vec![id.value]);
        assert!(
            MemoryRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool)
                .find(&id, false)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            ThreadRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool)
                .find(&protobuf::llm_memory::data::ThreadId { value: 92_161 })
                .await
                .unwrap()
                .is_some()
        );
    });
}

#[test]
fn purge_deletes_owned_memory_but_retains_reference_and_storage_thread() {
    run(async {
        let pool = setup_thread_group_pool().await;
        let ids = infra::test_helper::shared_id_generator();
        let group = ThreadGroupRepositoryImpl::new(ids.clone(), pool)
            .create_tx(pool, &new_group(92_156))
            .await
            .unwrap();
        insert_thread(pool, 92_157, None).await;
        let repo = MemoryRepositoryImpl::new(ids.clone(), pool);
        let make = |text: &str| protobuf::llm_memory::data::MemoryData {
            user_id: Some(protobuf::llm_memory::data::UserId { value: 1 }),
            content: text.into(),
            memory_kind: protobuf::llm_memory::data::MemoryKind::DerivedSummary as i32,
            ..Default::default()
        };
        let owned = repo
            .create(
                pool,
                &protobuf::llm_memory::data::MemoryData {
                    external_id: Some(format!("thread-group-summary:{group}")),
                    ..make("owned")
                },
            )
            .await
            .unwrap();
        let retained = repo.create(pool, &make("reference")).await.unwrap();
        infra::infra::thread_label::rdb::ThreadLabelRepository::add_labels(
            &ThreadLabelRepositoryImpl::new(pool),
            92_157,
            &[format!("group_{group}"), "thread_group_summary".into()],
            1,
        )
        .await
        .unwrap();
        ThreadMemoryRepositoryImpl::new(pool)
            .insert_auto_position_tx(pool, 92_157, owned.value, 1)
            .await
            .unwrap();
        let relations = ThreadGroupMemoryRelationService::new(pool);
        relations
            .link_existing(
                group,
                owned.value,
                "summary",
                GroupMemoryDeletePolicy::Delete,
            )
            .await
            .unwrap();
        relations
            .link_existing(
                group,
                retained.value,
                "reference",
                GroupMemoryDeletePolicy::Retain,
            )
            .await
            .unwrap();
        let config = memory_utils::cache::stretto::MemoryCacheConfig::default();
        let app = MemoryAppImpl::new(
            repo,
            MemoryRatingRepositoryImpl::new(ids.clone(), pool),
            ThreadRepositoryImpl::new(ids, pool),
            ThreadMemoryRepositoryImpl::new(pool),
            ThreadLabelRepositoryImpl::new(pool),
            memory_utils::cache::stretto::new_memory_cache(&config),
            memory_utils::cache::stretto::new_memory_cache(&config),
            None,
        );
        let purge = ThreadGroupPurgeService::new(pool).with_memory_app(std::sync::Arc::new(app));
        let preview = purge.preview(group).await.unwrap().unwrap();
        let mut tx = pool.begin().await.unwrap();
        assert!(
            MemoryRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool)
                .update_content_only(&mut *tx, &retained, "changed")
                .await
                .unwrap()
        );
        tx.commit().await.unwrap();
        let changed = purge.preview(group).await.unwrap().unwrap();
        assert_ne!(preview.digest, changed.digest);
        assert!(
            purge
                .delete(group, &preview.digest)
                .await
                .unwrap_err()
                .to_string()
                .contains("stale")
        );
        sqlx::query("INSERT INTO memory (id, content, user_id, parent_ids, created_at, updated_at, role, content_type, memory_kind) VALUES (929901, 'legacy', 1, NULL, 0, 0, 0, 0, 0)")
            .execute(pool).await.unwrap();
        assert!(purge.preview(group).await.is_ok());
        let child = MemoryRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool)
            .create(
                pool,
                &protobuf::llm_memory::data::MemoryData {
                    user_id: Some(protobuf::llm_memory::data::UserId { value: 1 }),
                    content: "references the summary".into(),
                    parent_ids: vec![owned],
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let referenced = purge.preview(group).await.unwrap().unwrap();
        assert!(referenced.blocked_reason.contains("parent"));
        assert_ne!(changed.digest, referenced.digest);
        assert!(
            purge
                .delete(group, &referenced.digest)
                .await
                .unwrap_err()
                .to_string()
                .contains("parent")
        );
        MemoryRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool)
            .delete(&child)
            .await
            .unwrap();
        infra::infra::thread_label::rdb::ThreadLabelRepository::remove_labels(
            &ThreadLabelRepositoryImpl::new(pool),
            92_157,
            &[format!("group_{group}")],
        )
        .await
        .unwrap();
        let invalid = purge.preview(group).await.unwrap().unwrap();
        assert!(invalid.blocked_reason.contains("storage Thread"));
        assert!(purge.delete(group, &invalid.digest).await.is_err());
        assert!(
            MemoryRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool)
                .find(&owned, false)
                .await
                .unwrap()
                .is_some()
        );
        infra::infra::thread_label::rdb::ThreadLabelRepository::add_labels(
            &ThreadLabelRepositoryImpl::new(pool),
            92_157,
            &[format!("group_{group}")],
            2,
        )
        .await
        .unwrap();
        let changed = purge.preview(group).await.unwrap().unwrap();
        let outcome = purge.delete(group, &changed.digest).await.unwrap();
        assert!(outcome.group_deleted);
        assert!(
            MemoryRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool)
                .find(&owned, false)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            MemoryRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool)
                .find(&retained, false)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            ThreadRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool)
                .find(&protobuf::llm_memory::data::ThreadId { value: 92_157 })
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            ThreadGroupMemoryRelationRepositoryImpl::new(pool)
                .list_by_group_id(group)
                .await
                .unwrap()
                .is_empty()
        );
    });
}

#[test]
fn delete_relationship_requires_owned_dedicated_summary_thread() {
    run(async {
        let pool = setup_thread_group_pool().await;
        let group = ThreadGroupRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool)
            .create_tx(pool, &new_group(92_162))
            .await
            .unwrap();
        insert_thread(pool, 92_163, None).await;
        infra::infra::thread_label::rdb::ThreadLabelRepository::add_labels(
            &ThreadLabelRepositoryImpl::new(pool),
            92_163,
            &["thread_group_summary".into(), format!("group_{group}")],
            1,
        )
        .await
        .unwrap();
        let repo = MemoryRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool);
        let id = repo
            .create(
                pool,
                &protobuf::llm_memory::data::MemoryData {
                    user_id: Some(protobuf::llm_memory::data::UserId { value: 2 }),
                    content: "someone else's summary".into(),
                    external_id: Some(format!("thread-group-summary:{group}")),
                    memory_kind: protobuf::llm_memory::data::MemoryKind::DerivedSummary as i32,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        ThreadMemoryRepositoryImpl::new(pool)
            .insert_auto_position_tx(pool, 92_163, id.value, 1)
            .await
            .unwrap();
        let links = ThreadGroupMemoryRelationService::new(pool);
        assert!(
            links
                .link_existing(group, id.value, "summary", GroupMemoryDeletePolicy::Delete)
                .await
                .is_err()
        );
        assert!(
            links
                .link_existing(
                    group,
                    id.value,
                    "reference",
                    GroupMemoryDeletePolicy::Retain
                )
                .await
                .is_ok()
        );
    });
}

#[test]
fn purge_rolls_back_first_owned_memory_when_later_delete_link_is_unsafe() {
    run(async {
        let pool = setup_thread_group_pool().await;
        let ids = infra::test_helper::shared_id_generator();
        let group = ThreadGroupRepositoryImpl::new(ids.clone(), pool)
            .create_tx(pool, &new_group(92_164))
            .await
            .unwrap();
        insert_thread(pool, 92_165, None).await;
        infra::infra::thread_label::rdb::ThreadLabelRepository::add_labels(
            &ThreadLabelRepositoryImpl::new(pool),
            92_165,
            &["thread_group_summary".into(), format!("group_{group}")],
            1,
        )
        .await
        .unwrap();
        let repo = MemoryRepositoryImpl::new(ids.clone(), pool);
        let owned = repo
            .create(
                pool,
                &protobuf::llm_memory::data::MemoryData {
                    user_id: Some(protobuf::llm_memory::data::UserId { value: 1 }),
                    content: "valid".into(),
                    external_id: Some(format!("thread-group-summary:{group}")),
                    memory_kind: protobuf::llm_memory::data::MemoryKind::DerivedSummary as i32,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        ThreadMemoryRepositoryImpl::new(pool)
            .insert_auto_position_tx(pool, 92_165, owned.value, 1)
            .await
            .unwrap();
        ThreadGroupMemoryRelationService::new(pool)
            .link_existing(
                group,
                owned.value,
                "summary",
                GroupMemoryDeletePolicy::Delete,
            )
            .await
            .unwrap();
        let unsafe_memory = repo
            .create(
                pool,
                &protobuf::llm_memory::data::MemoryData {
                    user_id: Some(protobuf::llm_memory::data::UserId { value: 1 }),
                    content: "unverified".into(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        ThreadGroupMemoryRelationRepositoryImpl::new(pool)
            .insert_tx(pool, group, unsafe_memory.value, "other", "delete", 1)
            .await
            .unwrap();
        let config = memory_utils::cache::stretto::MemoryCacheConfig::default();
        let app = MemoryAppImpl::new(
            repo,
            MemoryRatingRepositoryImpl::new(ids.clone(), pool),
            ThreadRepositoryImpl::new(ids, pool),
            ThreadMemoryRepositoryImpl::new(pool),
            ThreadLabelRepositoryImpl::new(pool),
            memory_utils::cache::stretto::new_memory_cache(&config),
            memory_utils::cache::stretto::new_memory_cache(&config),
            None,
        );
        let purge = ThreadGroupPurgeService::new(pool).with_memory_app(std::sync::Arc::new(app));
        let preview = purge.preview(group).await.unwrap().unwrap();
        assert!(purge.delete(group, &preview.digest).await.is_err());
        assert!(
            MemoryRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool)
                .find(&owned, false)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            ThreadGroupRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool)
                .find_by_id(group)
                .await
                .unwrap()
                .is_some()
        );
    });
}

#[test]
fn deleting_memory_detaches_group_relationship_in_the_same_transaction() {
    run(async {
        let pool = setup_thread_group_pool().await;
        let ids = infra::test_helper::shared_id_generator();
        let groups = ThreadGroupRepositoryImpl::new(ids.clone(), pool);
        let group_id = groups.create_tx(pool, &new_group(92_146)).await.unwrap();
        let memories = MemoryRepositoryImpl::new(ids.clone(), pool);
        let id = memories
            .create(
                pool,
                &protobuf::llm_memory::data::MemoryData {
                    user_id: Some(protobuf::llm_memory::data::UserId { value: 1 }),
                    content: "linked".into(),
                    memory_kind: protobuf::llm_memory::data::MemoryKind::DerivedSummary as i32,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        ThreadGroupMemoryRelationService::new(pool)
            .link_existing(
                group_id,
                id.value,
                "reference",
                GroupMemoryDeletePolicy::Retain,
            )
            .await
            .unwrap();
        let config = memory_utils::cache::stretto::MemoryCacheConfig::default();
        let app = MemoryAppImpl::new(
            memories,
            MemoryRatingRepositoryImpl::new(ids.clone(), pool),
            ThreadRepositoryImpl::new(ids, pool),
            ThreadMemoryRepositoryImpl::new(pool),
            ThreadLabelRepositoryImpl::new(pool),
            memory_utils::cache::stretto::new_memory_cache(&config),
            memory_utils::cache::stretto::new_memory_cache(&config),
            None,
        );
        assert!(app.delete_memory(&id).await.unwrap());
        assert!(
            ThreadGroupMemoryRelationRepositoryImpl::new(pool)
                .list_by_memory_id(id.value)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(groups.find_by_id(group_id).await.unwrap().is_some());
    });
}

#[test]
fn memory_service_rejects_summary_for_missing_group_without_blocking_raw_memory() {
    run(async {
        let pool = setup_thread_group_pool().await;
        let ids = infra::test_helper::shared_id_generator();
        let config = memory_utils::cache::stretto::MemoryCacheConfig::default();
        let app = MemoryAppImpl::new(
            MemoryRepositoryImpl::new(ids.clone(), pool),
            MemoryRatingRepositoryImpl::new(ids.clone(), pool),
            ThreadRepositoryImpl::new(ids, pool),
            ThreadMemoryRepositoryImpl::new(pool),
            ThreadLabelRepositoryImpl::new(pool),
            memory_utils::cache::stretto::new_memory_cache(&config),
            memory_utils::cache::stretto::new_memory_cache(&config),
            None,
        );
        let mut memory = protobuf::llm_memory::data::MemoryData {
            user_id: Some(protobuf::llm_memory::data::UserId { value: 1 }),
            content: "group body".into(),
            memory_kind: protobuf::llm_memory::data::MemoryKind::DerivedSummary as i32,
            external_id: Some("thread-group-summary:99999999".into()),
            ..Default::default()
        };
        assert!(app.create_memory(&memory).await.is_err());
        memory.external_id = None;
        let id = app.create_memory(&memory).await.unwrap();
        assert!(
            MemoryRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool)
                .find(&id, false)
                .await
                .unwrap()
                .is_some()
        );
        memory.external_id = Some("thread-group-summary:99999999".into());
        assert!(app.update_memory(&id, &Some(memory.clone())).await.is_err());
        let groups =
            ThreadGroupRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool);
        let group_id = groups.create_tx(pool, &new_group(92_149)).await.unwrap();
        memory.external_id = Some(format!("thread-group-summary:{group_id}"));
        assert!(app.update_memory(&id, &Some(memory)).await.unwrap().updated);
    });
}

#[test]
fn deleting_thread_detaches_relationship_for_its_exclusive_memory() {
    run(async {
        let pool = setup_thread_group_pool().await;
        let ids = infra::test_helper::shared_id_generator();
        let groups = ThreadGroupRepositoryImpl::new(ids.clone(), pool);
        let group_id = groups.create_tx(pool, &new_group(92_147)).await.unwrap();
        insert_thread(pool, 92_148, None).await;
        let memories = MemoryRepositoryImpl::new(ids.clone(), pool);
        let id = memories
            .create(
                pool,
                &protobuf::llm_memory::data::MemoryData {
                    user_id: Some(protobuf::llm_memory::data::UserId { value: 1 }),
                    content: "thread body".into(),
                    memory_kind: protobuf::llm_memory::data::MemoryKind::Raw as i32,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        ThreadMemoryRepositoryImpl::new(pool)
            .insert_auto_position_tx(pool, 92_148, id.value, 1)
            .await
            .unwrap();
        ThreadGroupMemoryRelationService::new(pool)
            .link_existing(
                group_id,
                id.value,
                "attachment",
                GroupMemoryDeletePolicy::Retain,
            )
            .await
            .unwrap();
        let config = memory_utils::cache::stretto::MemoryCacheConfig::default();
        let app = ThreadAppImpl::new(
            ThreadRepositoryImpl::new(ids.clone(), pool),
            ThreadMemoryRepositoryImpl::new(pool),
            ThreadLabelRepositoryImpl::new(pool),
            memories,
            MemoryRatingRepositoryImpl::new(ids, pool),
            memory_utils::cache::stretto::new_memory_cache(&config),
            memory_utils::cache::stretto::new_memory_cache(&config),
            None,
            None,
            None,
        );
        let (deleted, exclusive_ids) = app
            .delete_thread(&protobuf::llm_memory::data::ThreadId { value: 92_148 })
            .await
            .unwrap();
        assert!(deleted);
        assert_eq!(exclusive_ids, vec![id.value]);
        assert!(
            ThreadGroupMemoryRelationRepositoryImpl::new(pool)
                .list_by_memory_id(id.value)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(groups.find_by_id(group_id).await.unwrap().is_some());
    });
}

#[test]
fn thread_add_memory_rejects_legacy_summary_for_missing_group() {
    run(async {
        let pool = setup_thread_group_pool().await;
        insert_thread(pool, 92_150, None).await;
        sqlx::query("UPDATE thread SET memory_kind = 8 WHERE id = ?")
            .bind(92_150_i64)
            .execute(pool)
            .await
            .unwrap();
        let ids = infra::test_helper::shared_id_generator();
        let config = memory_utils::cache::stretto::MemoryCacheConfig::default();
        let app = ThreadAppImpl::new(
            ThreadRepositoryImpl::new(ids.clone(), pool),
            ThreadMemoryRepositoryImpl::new(pool),
            ThreadLabelRepositoryImpl::new(pool),
            MemoryRepositoryImpl::new(ids.clone(), pool),
            MemoryRatingRepositoryImpl::new(ids, pool),
            memory_utils::cache::stretto::new_memory_cache(&config),
            memory_utils::cache::stretto::new_memory_cache(&config),
            None,
            None,
            None,
        );
        let thread = protobuf::llm_memory::data::ThreadId { value: 92_150 };
        let mut memory = protobuf::llm_memory::data::MemoryData {
            user_id: Some(protobuf::llm_memory::data::UserId { value: 1 }),
            content: "generated".into(),
            external_id: Some("thread-group-summary:99999999".into()),
            memory_kind: protobuf::llm_memory::data::MemoryKind::DerivedSummary as i32,
            ..Default::default()
        };
        assert!(app.add_memory(&thread, &memory).await.is_err());
        let groups =
            ThreadGroupRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool);
        let group_id = groups.create_tx(pool, &new_group(92_151)).await.unwrap();
        let other_group = groups.create_tx(pool, &new_group(92_152)).await.unwrap();
        let mut invalid = memory.clone();
        invalid.external_id = Some(format!("thread-group-summary:{other_group}"));
        invalid.memory_kind = protobuf::llm_memory::data::MemoryKind::Raw as i32;
        assert!(
            app.add_memory(&thread, &invalid)
                .await
                .unwrap_err()
                .to_string()
                .contains("kind")
        );
        memory.external_id = Some(format!("thread-group-summary:{group_id}"));
        assert!(app.add_memory(&thread, &memory).await.is_ok());
        let before = ThreadMemoryRepositoryImpl::new(pool)
            .find_memory_ids_by_thread_tx(pool, thread.value)
            .await
            .unwrap();
        memory.external_id = None;
        assert!(
            app.add_memory_with_group_relation(
                &thread,
                &memory,
                9_999_999,
                "summary",
                GroupMemoryDeletePolicy::Delete
            )
            .await
            .is_err()
        );
        assert_eq!(
            ThreadMemoryRepositoryImpl::new(pool)
                .find_memory_ids_by_thread_tx(pool, thread.value)
                .await
                .unwrap()
                .len(),
            before.len()
        );
        let linked_id = app
            .add_memory_with_group_relation(
                &thread,
                &memory,
                group_id,
                "reference",
                GroupMemoryDeletePolicy::Retain,
            )
            .await
            .unwrap();
        let links = ThreadGroupMemoryRelationRepositoryImpl::new(pool)
            .list_by_group_id(group_id)
            .await
            .unwrap();
        assert_eq!(links.len(), 1);
        assert_eq!(links[0].memory_id, linked_id.value);
    });
}

#[test]
fn memory_writes_reject_wrong_kind_at_reserved_summary_external_id() {
    run(async {
        let pool = setup_thread_group_pool().await;
        let ids = infra::test_helper::shared_id_generator();
        let groups = ThreadGroupRepositoryImpl::new(ids.clone(), pool);
        let group = groups.create_tx(pool, &new_group(92_153)).await.unwrap();
        let other_group = groups.create_tx(pool, &new_group(92_154)).await.unwrap();
        let config = memory_utils::cache::stretto::MemoryCacheConfig::default();
        let app = MemoryAppImpl::new(
            MemoryRepositoryImpl::new(ids.clone(), pool),
            MemoryRatingRepositoryImpl::new(ids.clone(), pool),
            ThreadRepositoryImpl::new(ids, pool),
            ThreadMemoryRepositoryImpl::new(pool),
            ThreadLabelRepositoryImpl::new(pool),
            memory_utils::cache::stretto::new_memory_cache(&config),
            memory_utils::cache::stretto::new_memory_cache(&config),
            None,
        );
        let raw = protobuf::llm_memory::data::MemoryData {
            user_id: Some(protobuf::llm_memory::data::UserId { value: 1 }),
            content: "raw".into(),
            memory_kind: protobuf::llm_memory::data::MemoryKind::Raw as i32,
            ..Default::default()
        };
        let invalid = protobuf::llm_memory::data::MemoryData {
            external_id: Some(format!("thread-group-summary:{group}")),
            ..raw.clone()
        };
        assert!(
            app.create_memory(&invalid)
                .await
                .unwrap_err()
                .to_string()
                .contains("kind")
        );
        let summary = app
            .create_memory(&protobuf::llm_memory::data::MemoryData {
                memory_kind: protobuf::llm_memory::data::MemoryKind::DerivedSummary as i32,
                ..invalid
            })
            .await
            .unwrap();
        assert!(
            app.update_memory(
                &summary,
                &Some(protobuf::llm_memory::data::MemoryData {
                    user_id: Some(protobuf::llm_memory::data::UserId { value: 1 }),
                    content: "revised summary".into(),
                    external_id: Some(format!("thread-group-summary:{group}")),
                    memory_kind: protobuf::llm_memory::data::MemoryKind::Unspecified as i32,
                    ..Default::default()
                })
            )
            .await
            .is_ok()
        );
        let raw_id = app.create_memory(&raw).await.unwrap();
        let attempted = protobuf::llm_memory::data::MemoryData {
            external_id: Some(format!("thread-group-summary:{other_group}")),
            ..raw
        };
        assert!(
            app.update_memory(&raw_id, &Some(attempted))
                .await
                .unwrap_err()
                .to_string()
                .contains("kind")
        );
        let repo = MemoryRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool);
        assert!(
            repo.find(&raw_id, false)
                .await
                .unwrap()
                .unwrap()
                .data
                .unwrap()
                .external_id
                .is_none()
        );
        assert!(repo.find(&summary, false).await.unwrap().is_some());
    });
}

#[test]
fn purge_rejects_replaced_membership_even_when_counts_match() {
    run(async {
        let pool = setup_thread_group_pool().await;
        let groups =
            ThreadGroupRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool);
        let group_id = groups.create_tx(pool, &new_group(92_100)).await.unwrap();
        let members = ThreadGroupMemberRepositoryImpl::new(pool);
        let mut old = new_member(group_id, None, key(92_101));
        old.state = values::member_state::DELETED.to_string();
        old.deleted_at = Some(1);
        members.insert_tx(pool, &old).await.unwrap();

        let purge = ThreadGroupPurgeService::new(pool);
        let preview = purge.preview(group_id).await.unwrap().unwrap();
        assert_eq!(preview.inactive_memberships, 1);

        members.delete_by_group_id_tx(pool, group_id).await.unwrap();
        let mut replacement = new_member(group_id, None, key(92_102));
        replacement.state = values::member_state::DELETED.to_string();
        replacement.deleted_at = Some(1);
        members.insert_tx(pool, &replacement).await.unwrap();
        let current = purge.preview(group_id).await.unwrap().unwrap();
        assert_eq!(current.inactive_memberships, preview.inactive_memberships);
        assert_ne!(current.digest, preview.digest);
        let error = purge.delete(group_id, &preview.digest).await.unwrap_err();
        assert!(error.to_string().contains("stale"));
        assert!(groups.find_by_id(group_id).await.unwrap().is_some());
    });
}

#[test]
fn purge_rejects_replaced_relation_even_when_counts_match() {
    run(async {
        let pool = setup_thread_group_pool().await;
        let groups =
            ThreadGroupRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool);
        let group_id = groups.create_tx(pool, &new_group(92_200)).await.unwrap();
        let members = ThreadGroupMemberRepositoryImpl::new(pool);
        for id in [92_201, 92_202] {
            let mut member = new_member(group_id, None, key(id));
            member.state = values::member_state::DELETED.to_string();
            member.deleted_at = Some(1);
            members.insert_tx(pool, &member).await.unwrap();
        }
        let relations =
            ThreadRelationRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool);
        let relation = infra::infra::thread_group::test_support::new_relation(92_201, 92_202);
        let original_id = relations.insert_tx(pool, &relation).await.unwrap();
        let mut tx = pool.begin().await.unwrap();
        relations
            .set_state_tx(
                &mut *tx,
                original_id,
                values::relation_state::ACTIVE,
                values::relation_state::RETRACTED,
                2_000,
            )
            .await
            .unwrap();
        tx.commit().await.unwrap();

        let purge = ThreadGroupPurgeService::new(pool);
        let preview = purge.preview(group_id).await.unwrap().unwrap();
        assert_eq!(preview.inactive_relations, 1);
        relations.delete_by_id_tx(pool, original_id).await.unwrap();
        let replacement_id = relations.insert_tx(pool, &relation).await.unwrap();
        let mut tx = pool.begin().await.unwrap();
        relations
            .set_state_tx(
                &mut *tx,
                replacement_id,
                values::relation_state::ACTIVE,
                values::relation_state::RETRACTED,
                2_000,
            )
            .await
            .unwrap();
        tx.commit().await.unwrap();

        let current = purge.preview(group_id).await.unwrap().unwrap();
        assert_eq!(current.inactive_relations, preview.inactive_relations);
        assert_ne!(current.digest, preview.digest);
        assert!(
            purge
                .delete(group_id, &preview.digest)
                .await
                .unwrap_err()
                .to_string()
                .contains("stale")
        );
    });
}

#[test]
fn purge_rejects_replaced_audit_even_when_counts_match() {
    run(async {
        let pool = setup_thread_group_pool().await;
        let groups =
            ThreadGroupRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool);
        let group_id = groups.create_tx(pool, &new_group(92_300)).await.unwrap();
        let target_id = groups.create_tx(pool, &new_group(92_301)).await.unwrap();
        let audit =
            ThreadGroupAuditRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool);
        let merge = NewGroupAuditMerge {
            source_group_id: group_id,
            target_group_id: target_id,
            actor_id: "operator".into(),
            reason: "test".into(),
            created_at: 1,
        };
        let first_id = audit.append_merge_tx(pool, &merge).await.unwrap();
        let purge = ThreadGroupPurgeService::new(pool);
        let preview = purge.preview(group_id).await.unwrap().unwrap();
        assert_eq!(preview.audit_rows, 1);
        audit.delete_by_id_tx(pool, first_id).await.unwrap();
        audit.append_merge_tx(pool, &merge).await.unwrap();

        let current = purge.preview(group_id).await.unwrap().unwrap();
        assert_eq!(current.audit_rows, preview.audit_rows);
        assert_ne!(current.digest, preview.digest);
        assert!(
            purge
                .delete(group_id, &preview.digest)
                .await
                .unwrap_err()
                .to_string()
                .contains("stale")
        );
    });
}

#[test]
fn purge_preview_and_delete_removes_redirected_history() {
    run(async {
        let pool = setup_thread_group_pool().await;
        let reconcile = ThreadGroupReconciliationService::with_id_generator(
            pool,
            infra::test_helper::shared_id_generator(),
        );
        let operator = ThreadGroupOperatorService::with_id_generator(
            pool,
            infra::test_helper::shared_id_generator(),
        );
        let purge = ThreadGroupPurgeService::new(pool);

        let (group_a, _, _) = setup_pair(
            pool,
            &reconcile,
            &recon_endpoint("purge-parent-a"),
            &recon_endpoint("purge-child-a"),
            20_801,
            20_802,
        )
        .await;
        let (group_b, _, _) = setup_pair(
            pool,
            &reconcile,
            &recon_endpoint("purge-parent-b"),
            &recon_endpoint("purge-child-b"),
            20_811,
            20_812,
        )
        .await;
        operator
            .merge_groups(group_b, group_a, "operator", "merge", 6_000)
            .await
            .unwrap();

        let preview = purge.preview(group_b).await.unwrap().expect("preview");
        assert_eq!(preview.active_memberships, 0);
        assert!(preview.inactive_memberships >= 2);
        // Merge redirects memberships but leaves the canonical relation
        // active (now cross-group), so it must not be purged.
        assert!(preview.active_relations >= 1);

        // A stale digest is rejected.
        assert!(
            purge.delete(group_b, "stale-digest").await.is_err(),
            "stale preview must be rejected"
        );

        let outcome = purge.delete(group_b, &preview.digest).await.unwrap();
        assert!(outcome.group_deleted);
        assert!(outcome.deleted_memberships >= 2);
        assert!(purge.preview(group_b).await.unwrap().is_none());

        // An active group with active members is not purgeable.
        let (group_c, _, _) = setup_pair(
            pool,
            &reconcile,
            &recon_endpoint("purge-parent-c"),
            &recon_endpoint("purge-child-c"),
            20_821,
            20_822,
        )
        .await;
        let active_preview = purge.preview(group_c).await.unwrap().unwrap();
        assert!(active_preview.active_memberships >= 1);
        assert!(active_preview.blocked_reason.contains("active members"));
        assert!(
            purge.delete(group_c, &active_preview.digest).await.is_err(),
            "active group must not be purged"
        );
    });
}

#[test]
fn write_gate_blocks_operator_mutations() {
    run(async {
        let pool = setup_thread_group_pool().await;
        let operator = ThreadGroupOperatorService::with_id_generator(
            pool,
            infra::test_helper::shared_id_generator(),
        );
        app::app::thread_group::set_thread_group_writes_enabled_override(Some(false));
        let blocked = operator
            .create_manual_collection("user:1", "blocked", 1)
            .await;
        app::app::thread_group::set_thread_group_writes_enabled_override(None);
        assert!(
            blocked.is_err(),
            "operator mutation must fail while writes are disabled"
        );
    });
}

#[test]
fn purge_flattens_dangling_redirects() {
    run(async {
        let pool = setup_thread_group_pool().await;
        let reconcile = ThreadGroupReconciliationService::with_id_generator(
            pool,
            infra::test_helper::shared_id_generator(),
        );
        let operator = ThreadGroupOperatorService::with_id_generator(
            pool,
            infra::test_helper::shared_id_generator(),
        );
        let purge = ThreadGroupPurgeService::new(pool);
        let generator = infra::test_helper::shared_id_generator();
        let groups = ThreadGroupRepositoryImpl::new(generator, pool);

        let (group_a, _, _) = setup_pair(
            pool,
            &reconcile,
            &recon_endpoint("pr-a-parent"),
            &recon_endpoint("pr-a-child"),
            21_001,
            21_002,
        )
        .await;
        let (group_b, _, _) = setup_pair(
            pool,
            &reconcile,
            &recon_endpoint("pr-b-parent"),
            &recon_endpoint("pr-b-child"),
            21_011,
            21_012,
        )
        .await;
        let (group_c, _, _) = setup_pair(
            pool,
            &reconcile,
            &recon_endpoint("pr-c-parent"),
            &recon_endpoint("pr-c-child"),
            21_021,
            21_022,
        )
        .await;

        // B -> A, then C -> B. Purging B must repoint C to A.
        operator
            .merge_groups(group_b, group_a, "operator", "merge", 8_000)
            .await
            .unwrap();
        groups
            .redirect_tx(pool, group_c, group_b, 8_001)
            .await
            .unwrap();

        let preview = purge.preview(group_b).await.unwrap().expect("preview");
        assert_eq!(preview.dangling_redirects, 1);
        let outcome = purge.delete(group_b, &preview.digest).await.unwrap();
        assert_eq!(outcome.repointed_redirects, 1);
        assert_eq!(
            groups
                .find_by_id(group_c)
                .await
                .unwrap()
                .unwrap()
                .redirect_to_group_id,
            Some(group_a)
        );
    });
}

#[test]
fn purge_removes_eligible_deletion_markers() {
    run(async {
        let pool = setup_thread_group_pool().await;
        let reconcile = ThreadGroupReconciliationService::with_id_generator(
            pool,
            infra::test_helper::shared_id_generator(),
        );
        let purge = ThreadGroupPurgeService::new(pool);
        let (group, _, _) = setup_pair(
            pool,
            &reconcile,
            &recon_endpoint("pm-parent"),
            &recon_endpoint("pm-child"),
            21_101,
            21_102,
        )
        .await;

        let member_repo = ThreadGroupMemberRepositoryImpl::new(pool);
        let marker_repo = ThreadDeletionMarkerRepositoryImpl::new(pool);
        let members = member_repo.list_current_by_group_id(group).await.unwrap();
        assert!(!members.is_empty());
        let mut tx = pool.begin().await.unwrap();
        for member in &members {
            member_repo
                .mark_current_deleted_tx(&mut *tx, &member.thread_canonical_key, 9_000, 9_000)
                .await
                .unwrap();
            let key = SourceIdentityKey {
                owner_scope: &member.owner_scope,
                source: member.source.as_deref().unwrap_or(""),
                identity_scope: member.identity_scope.as_deref().unwrap_or(""),
                native_id: member.native_id.as_deref().unwrap_or(""),
            };
            marker_repo
                .put_tx(
                    &mut *tx,
                    &NewThreadDeletionMarker {
                        identity: key,
                        forbid_reimport: true,
                        recursive: false,
                        actor_id: "operator".to_string(),
                        reason: Some("privacy".to_string()),
                        deleted_at: 9_000,
                    },
                )
                .await
                .unwrap();
        }
        tx.commit().await.unwrap();

        let preview = purge.preview(group).await.unwrap().expect("preview");
        assert_eq!(preview.active_memberships, 0);
        assert!(preview.purgeable_deletion_markers >= 2);
        assert_eq!(preview.retained_deletion_markers, 0);
        let outcome = purge.delete(group, &preview.digest).await.unwrap();
        assert!(outcome.deleted_deletion_markers >= 2);
        assert!(purge.preview(group).await.unwrap().is_none());
        for member in &members {
            let key = SourceIdentityKey {
                owner_scope: &member.owner_scope,
                source: member.source.as_deref().unwrap_or(""),
                identity_scope: member.identity_scope.as_deref().unwrap_or(""),
                native_id: member.native_id.as_deref().unwrap_or(""),
            };
            assert!(marker_repo.find(&key).await.unwrap().is_none());
        }
    });
}

#[test]
fn preview_import_plans_without_writing() {
    run(async {
        let pool = setup_thread_group_pool().await;
        let reconcile = ThreadGroupReconciliationService::with_id_generator(
            pool,
            infra::test_helper::shared_id_generator(),
        );
        let parent = recon_endpoint("preview-parent");
        let child = recon_endpoint("preview-child");
        insert_thread(pool, 22_001, None).await;
        insert_thread(pool, 22_002, None).await;
        reconcile
            .reconcile_subject(22_001, &parent, &[], "op", 1_000)
            .await
            .unwrap();

        let evidence = vec![observation(child.clone(), Some(parent.clone()))];
        let preview = reconcile
            .preview_import(&child, &evidence, false)
            .await
            .unwrap();
        assert!(!preview.suppressed);
        assert_eq!(preview.planned_relations, 1);
        assert_eq!(preview.pending, 0);
        assert!(!preview.conflict);

        // No writes: the child has no relation yet.
        let child_key = key_of(&child);
        assert!(
            ThreadRelationRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool,)
                .find_active_by_child_canonical_key(&child_key)
                .await
                .unwrap()
                .is_none()
        );

        // A forbidden marker makes the preview report suppression.
        let marker_repo = ThreadDeletionMarkerRepositoryImpl::new(pool);
        let child_identity = SourceIdentityInput {
            owner_scope: "user:1".into(),
            source: "codex".into(),
            identity_scope: IdentityScope::known(String::new()),
            native_id: "preview-child".into(),
        };
        let key = child_identity.key().unwrap();
        marker_repo
            .put_tx(
                pool,
                &NewThreadDeletionMarker {
                    identity: key,
                    forbid_reimport: true,
                    recursive: false,
                    actor_id: "operator".into(),
                    reason: Some("privacy".into()),
                    deleted_at: 2_000,
                },
            )
            .await
            .unwrap();
        let suppressed = reconcile
            .preview_import(&child, &evidence, false)
            .await
            .unwrap();
        assert!(suppressed.suppressed);
        assert_eq!(suppressed.planned_relations, 0);
    });
}

#[test]
fn concurrent_same_parent_reconcile_converges_to_one_edge() {
    run(async {
        use std::sync::Arc;
        let pool = setup_thread_group_pool().await;
        let reconcile = Arc::new(ThreadGroupReconciliationService::with_id_generator(
            pool,
            infra::test_helper::shared_id_generator(),
        ));
        let parent = recon_endpoint("cc-parent");
        let child = recon_endpoint("cc-child");
        insert_thread(pool, 23_001, None).await;
        insert_thread(pool, 23_002, None).await;
        reconcile
            .reconcile_subject(23_001, &parent, &[], "op", 1_000)
            .await
            .unwrap();

        let evidence = observation(child.clone(), Some(parent.clone()));
        let first = reconcile.clone();
        let second = reconcile.clone();
        let (child_a, child_b) = (child.clone(), child.clone());
        let (evidence_a, evidence_b) = (evidence.clone(), evidence.clone());
        let (a, b) = tokio::join!(
            async move {
                first
                    .reconcile_subject(23_002, &child_a, &[evidence_a], "op", 1_001)
                    .await
            },
            async move {
                second
                    .reconcile_subject(23_002, &child_b, &[evidence_b], "op", 1_002)
                    .await
            },
        );
        assert!(
            a.is_ok() || b.is_ok(),
            "at least one concurrent reconcile must succeed"
        );

        let child_key = key_of(&child);
        let active =
            ThreadRelationRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool)
                .list_by_child_canonical_key(&child_key, Some(values::relation_state::ACTIVE))
                .await
                .unwrap();
        assert_eq!(
            active.len(),
            1,
            "concurrent same-parent reconcile must converge to one active edge"
        );
    });
}

#[test]
fn concurrent_distinct_parents_never_leave_two_active_parents() {
    run(async {
        use std::sync::Arc;
        let pool = setup_thread_group_pool().await;
        let reconcile = Arc::new(ThreadGroupReconciliationService::with_id_generator(
            pool,
            infra::test_helper::shared_id_generator(),
        ));
        let parent_a = recon_endpoint("cd-parent-a");
        let parent_b = recon_endpoint("cd-parent-b");
        let child = recon_endpoint("cd-child");
        insert_thread(pool, 23_101, None).await;
        insert_thread(pool, 23_102, None).await;
        insert_thread(pool, 23_103, None).await;
        reconcile
            .reconcile_subject(23_101, &parent_a, &[], "op", 1_000)
            .await
            .unwrap();
        reconcile
            .reconcile_subject(23_102, &parent_b, &[], "op", 1_000)
            .await
            .unwrap();

        let evidence_a = observation(child.clone(), Some(parent_a));
        let evidence_b = observation(child.clone(), Some(parent_b));
        let first = reconcile.clone();
        let second = reconcile.clone();
        let (child_a, child_b) = (child.clone(), child.clone());
        let (_a, _b) = tokio::join!(
            async move {
                first
                    .reconcile_subject(23_103, &child_a, &[evidence_a], "op", 1_001)
                    .await
            },
            async move {
                second
                    .reconcile_subject(23_103, &child_b, &[evidence_b], "op", 1_002)
                    .await
            },
        );

        let child_key = key_of(&child);
        let active =
            ThreadRelationRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool)
                .list_by_child_canonical_key(&child_key, Some(values::relation_state::ACTIVE))
                .await
                .unwrap();
        assert!(
            active.len() <= 1,
            "a child must never have two active canonical parents"
        );
    });
}

#[test]
fn conflicting_later_parent_retracts_without_superseding() {
    run(async {
        let pool = setup_thread_group_pool().await;
        let generator = infra::test_helper::shared_id_generator();
        let reconcile =
            ThreadGroupReconciliationService::with_id_generator(pool, generator.clone());
        let relations = ThreadRelationRepositoryImpl::new(generator, pool);
        let p1 = recon_endpoint("cf-p1");
        let p2 = recon_endpoint("cf-p2");
        let child = recon_endpoint("cf-child");
        insert_thread(pool, 24_001, None).await;
        insert_thread(pool, 24_002, None).await;
        insert_thread(pool, 24_003, None).await;
        reconcile
            .reconcile_subject(24_001, &p1, &[], "op", 1_000)
            .await
            .unwrap();
        reconcile
            .reconcile_subject(24_002, &p2, &[], "op", 1_000)
            .await
            .unwrap();
        reconcile
            .reconcile_subject(
                24_003,
                &child,
                &[observation(child.clone(), Some(p1.clone()))],
                "op",
                1_001,
            )
            .await
            .unwrap();

        let child_key = key_of(&child);
        let before = relations
            .list_by_child_canonical_key(&child_key, None)
            .await
            .unwrap();
        assert_eq!(before.len(), 1);
        assert_eq!(before[0].state, values::relation_state::ACTIVE);
        let adopted_id = before[0].id;

        // Later conflicting exact parent evidence must not silently
        // replace the adopted relation.
        let outcome = reconcile
            .reconcile_subject(
                24_003,
                &child,
                &[observation(child.clone(), Some(p2.clone()))],
                "op",
                1_002,
            )
            .await
            .unwrap();
        assert!(outcome.conflict);
        assert!(outcome.relation_selected.is_none());

        let after = relations
            .list_by_child_canonical_key(&child_key, None)
            .await
            .unwrap();
        assert_eq!(after.len(), 1, "no new relation may be inserted");
        assert_eq!(after[0].id, adopted_id);
        assert_eq!(
            after[0].state,
            values::relation_state::RETRACTED,
            "the adopted relation must be retracted, not superseded"
        );
        assert_eq!(after[0].parent_thread_canonical_key, key_of(&p1));
        assert!(
            relations
                .find_active_by_child_canonical_key(&child_key)
                .await
                .unwrap()
                .is_none(),
            "no active parent may remain after a conflict"
        );

        let observations =
            ThreadObservationRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool)
                .list_by_subject("user:1", "codex", true, "", "cf-child")
                .await
                .unwrap();
        assert!(
            observations
                .iter()
                .any(|o| o.evidence_kind == values::evidence_kind::DERIVED_CONFLICT),
            "a derived conflict observation must be recorded"
        );
    });
}

#[test]
fn membership_converges_regardless_of_arrival_order() {
    run(async {
        let pool = setup_thread_group_pool().await;
        let generator = infra::test_helper::shared_id_generator();
        let reconcile = ThreadGroupReconciliationService::with_id_generator(pool, generator);
        let member_repo = ThreadGroupMemberRepositoryImpl::new(pool);
        let a = recon_endpoint("ord-a");
        let b = recon_endpoint("ord-b");
        let c = recon_endpoint("ord-c");
        let d = recon_endpoint("ord-d");
        for (id, endpoint) in [(24_101, &a), (24_102, &b), (24_103, &c), (24_104, &d)] {
            insert_thread(pool, id, None).await;
            reconcile
                .reconcile_subject(id, endpoint, &[], "op", 1_000)
                .await
                .unwrap();
        }

        // Arrival order: C->D first, then C->B, then B->A.
        reconcile
            .reconcile_subject(
                24_104,
                &d,
                &[observation(d.clone(), Some(c.clone()))],
                "op",
                1_001,
            )
            .await
            .unwrap();
        reconcile
            .reconcile_subject(
                24_103,
                &c,
                &[observation(c.clone(), Some(b.clone()))],
                "op",
                1_002,
            )
            .await
            .unwrap();
        reconcile
            .reconcile_subject(
                24_102,
                &b,
                &[observation(b.clone(), Some(a.clone()))],
                "op",
                1_003,
            )
            .await
            .unwrap();

        let group_of = async |endpoint: &ObservedEndpoint| {
            member_repo
                .find_current_by_thread_canonical_key(&key_of(endpoint))
                .await
                .unwrap()
                .expect("membership")
                .group_id
        };
        let group_a = group_of(&a).await;
        assert_eq!(group_of(&b).await, group_a, "B must follow A");
        assert_eq!(group_of(&c).await, group_a, "C must follow B");
        assert_eq!(group_of(&d).await, group_a, "D must follow C");
    });
}

#[test]
fn purge_preserves_cross_group_inactive_relation() {
    run(async {
        let pool = setup_thread_group_pool().await;
        let generator = infra::test_helper::shared_id_generator();
        let reconcile =
            ThreadGroupReconciliationService::with_id_generator(pool, generator.clone());
        let operator = ThreadGroupOperatorService::with_id_generator(pool, generator.clone());
        let purge = ThreadGroupPurgeService::new(pool);
        let relations = ThreadRelationRepositoryImpl::new(generator, pool);

        let (group_a, a_parent, a_child) = setup_pair(
            pool,
            &reconcile,
            &recon_endpoint("px-a-parent"),
            &recon_endpoint("px-a-child"),
            24_201,
            24_202,
        )
        .await;
        let (group_b, b_parent, b_child) = setup_pair(
            pool,
            &reconcile,
            &recon_endpoint("px-b-parent"),
            &recon_endpoint("px-b-child"),
            24_211,
            24_212,
        )
        .await;

        // Inactive relation fully inside group A (purgeable) and one
        // crossing into group B (must be retained).
        let inside = NewThreadRelation {
            parent_thread_id: Some(24_201),
            child_thread_id: Some(24_202),
            parent_thread_canonical_key: a_parent.clone(),
            child_thread_canonical_key: a_child.clone(),
            parent_owner_scope: "user:1".into(),
            parent_source: Some("codex".into()),
            parent_identity_scope: Some(String::new()),
            parent_native_id: Some("px-a-parent".into()),
            child_owner_scope: "user:1".into(),
            child_source: Some("codex".into()),
            child_identity_scope: Some(String::new()),
            child_native_id: Some("px-a-child".into()),
            relation_type: values::relation_type::DELEGATED.into(),
            state: values::relation_state::RETRACTED.into(),
            selection_basis: values::selection_basis::SOURCE_EXACT.into(),
            source_confidence: Some(values::source_confidence::EXACT.into()),
            selected_observation_id: Some(1),
            selected_operator_decision_id: None,
            created_at: 1,
            updated_at: 1,
        };
        let crossing = NewThreadRelation {
            parent_thread_canonical_key: a_child.clone(),
            child_thread_canonical_key: b_parent.clone(),
            parent_native_id: Some("px-a-child".into()),
            child_native_id: Some("px-b-parent".into()),
            child_thread_id: Some(24_211),
            ..inside.clone()
        };
        relations
            .insert_tx(pool, &inside)
            .await
            .expect("inside relation");
        let crossing_id = relations
            .insert_tx(pool, &crossing)
            .await
            .expect("crossing relation");

        // Make group A purgeable by merging it into group B.
        operator
            .merge_groups(group_a, group_b, "operator", "merge", 2_000)
            .await
            .unwrap();

        let preview = purge.preview(group_a).await.unwrap().expect("preview");
        assert_eq!(preview.inactive_relations, 1);
        let outcome = purge.delete(group_a, &preview.digest).await.unwrap();
        assert!(outcome.group_deleted);
        assert_eq!(outcome.deleted_relations, 1);

        assert!(
            relations.find_by_id(crossing_id).await.unwrap().is_some(),
            "cross-group inactive relation must be preserved"
        );
        let remaining_inside = relations
            .list_by_parent_canonical_key(&a_parent, None)
            .await
            .unwrap()
            .into_iter()
            .filter(|relation| {
                relation.child_thread_canonical_key == a_child
                    && relation.state == values::relation_state::RETRACTED
            })
            .count();
        assert_eq!(remaining_inside, 0, "in-group inactive relation purged");
        let _ = group_b;
        let _ = b_child;
    });
}

#[test]
fn manual_anchor_boundary_holds_across_arrival_orders() {
    run(async {
        let pool = setup_thread_group_pool().await;
        let generator = infra::test_helper::shared_id_generator();
        let reconcile = ThreadGroupReconciliationService::with_id_generator(pool, generator);
        let member_repo = ThreadGroupMemberRepositoryImpl::new(pool);
        let relations =
            ThreadRelationRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool);

        async fn pin_operator(
            member_repo: &ThreadGroupMemberRepositoryImpl,
            pool: &'static infra_utils::infra::rdb::RdbPool,
            key: &str,
        ) {
            let mut tx = pool.begin().await.unwrap();
            let member = member_repo
                .find_current_by_thread_canonical_key_tx(&mut *tx, key)
                .await
                .unwrap()
                .expect("membership to pin");
            member_repo
                .set_role_provenance_tx(
                    &mut *tx,
                    key,
                    &member.role,
                    values::grouping_authority::OPERATOR,
                    1_000,
                )
                .await
                .unwrap();
            tx.commit().await.unwrap();
        }

        // A and C are operator-fixed in different groups. B is A's
        // automatic child and C is B's child; D is C's child. B must stay
        // with A, D must stay with C, and B->C must remain cross-group.
        let orders: [[usize; 3]; 2] = [[0, 1, 2], [2, 1, 0]];
        for (index, order) in orders.into_iter().enumerate() {
            let base = 25_000 + (index as i64) * 100;
            let a = recon_endpoint(&format!("mb{index}-a"));
            let b = recon_endpoint(&format!("mb{index}-b"));
            let c = recon_endpoint(&format!("mb{index}-c"));
            let d = recon_endpoint(&format!("mb{index}-d"));
            for (offset, _) in [&a, &b, &c, &d].into_iter().enumerate() {
                insert_thread(pool, base + 1 + offset as i64, None).await;
            }
            let a_id = base + 1;
            let b_id = base + 2;
            let c_id = base + 3;
            let d_id = base + 4;

            // All four are imported (key + identity mapping + singleton
            // membership) before the relation evidence is replayed, so
            // the child-first order can still resolve its parent mapping.
            for (id, endpoint) in [(a_id, &a), (b_id, &b), (c_id, &c), (d_id, &d)] {
                reconcile
                    .reconcile_subject(id, endpoint, &[], "op", 1_000)
                    .await
                    .unwrap();
            }
            pin_operator(&member_repo, pool, &key_of(&a)).await;
            pin_operator(&member_repo, pool, &key_of(&c)).await;

            let evidence_b = observation(b.clone(), Some(a.clone()));
            let evidence_c = observation(c.clone(), Some(b.clone()));
            let evidence_d = observation(d.clone(), Some(c.clone()));
            for step in order {
                match step {
                    0 => {
                        reconcile
                            .reconcile_subject(
                                b_id,
                                &b,
                                std::slice::from_ref(&evidence_b),
                                "op",
                                1_001,
                            )
                            .await
                            .unwrap();
                    }
                    1 => {
                        reconcile
                            .reconcile_subject(
                                c_id,
                                &c,
                                std::slice::from_ref(&evidence_c),
                                "op",
                                1_002,
                            )
                            .await
                            .unwrap();
                    }
                    2 => {
                        reconcile
                            .reconcile_subject(
                                d_id,
                                &d,
                                std::slice::from_ref(&evidence_d),
                                "op",
                                1_003,
                            )
                            .await
                            .unwrap();
                    }
                    _ => unreachable!(),
                }
            }

            let group_of = async |endpoint: &ObservedEndpoint| {
                member_repo
                    .find_current_by_thread_canonical_key(&key_of(endpoint))
                    .await
                    .unwrap()
                    .expect("membership")
                    .group_id
            };
            let group_a = group_of(&a).await;
            let group_c = group_of(&c).await;
            assert_ne!(group_a, group_c, "A and C are in different manual groups");
            assert_eq!(
                group_of(&b).await,
                group_a,
                "order {order:?}: B must stay on A's side"
            );
            assert_eq!(group_of(&c).await, group_c);
            assert_eq!(
                group_of(&d).await,
                group_c,
                "order {order:?}: D must stay on C's side"
            );

            let cross = relations
                .find_active_by_child_canonical_key(&key_of(&c))
                .await
                .unwrap()
                .expect("B->C relation");
            assert_eq!(cross.parent_thread_canonical_key, key_of(&b));
            assert_eq!(cross.child_thread_canonical_key, key_of(&c));
        }
    });
}

#[test]
fn read_model_revision_tracks_group_and_thread_changes() {
    run(async {
        let pool = setup_thread_group_pool().await;
        let read = ThreadGroupReadService::new(pool);
        let before = read.read_model_revision().await.unwrap();

        // A thread row changes the thread aggregate, not the group one.
        insert_thread(pool, 27_001, None).await;
        let after_thread = read.read_model_revision().await.unwrap();
        assert_eq!(after_thread.threads.row_count, before.threads.row_count + 1);
        assert_eq!(after_thread.groups.row_count, before.groups.row_count);

        // A group row changes the group aggregate.
        let generator = infra::test_helper::shared_id_generator();
        ThreadGroupRepositoryImpl::new(generator, pool)
            .create_tx(pool, &new_group(7_100))
            .await
            .unwrap();
        let after_group = read.read_model_revision().await.unwrap();
        assert_eq!(
            after_group.groups.row_count,
            after_thread.groups.row_count + 1
        );

        // Repeated reads with no mutation are stable (the snapshot digest
        // is derived from exactly these aggregate values).
        let repeat = read.read_model_revision().await.unwrap();
        assert_eq!(after_group, repeat);
    });
}
