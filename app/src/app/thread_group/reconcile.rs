//! Adapter reconciliation, dry-run, and operator-decision application.

use super::domain::{
    Candidate, CandidateEvidence, CandidateSelection, RelationType, SourceConfidence,
    select_parent_candidate,
};
use super::observation::{
    ObservationInput, ObservationRecordRequest, ObservedEndpoint, SuppressionDecision,
    ThreadGroupEventType, decide_suppression, event_id_for, new_observation, observation_identity,
    record_observation_and_event,
};
use super::prelude::*;
use super::{
    THREAD_GROUP_POLICY_VERSION, ensure_thread_group_writes, known_scope_value,
    new_manual_thread_canonical_key,
};

/// Result of reconciling one imported source session against its stored
/// evidence.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReconciliationOutcome {
    pub observation_ids: Vec<i64>,
    /// Internal id of the canonical relation adopted for this subject.
    pub relation_selected: Option<i64>,
    /// Group the subject's current membership landed in.
    pub group_id: Option<i64>,
    /// True when equal-ranked parent candidates left the child with no
    /// active parent.
    pub conflict: bool,
    /// Number of parent candidates still unresolved (late discovery).
    pub pending: usize,
}

/// App-layer reconciliation service for one imported subject session.
///
/// It records the adapter observations and their events, resolves the
/// owner-local identity mappings, selects a canonical parent with the
/// frozen policy, and materialises the relation plus reconciler
/// membership — all inside the caller-visible transaction boundary.
/// Heuristic / unsupported evidence never becomes a canonical relation.
pub struct ThreadGroupReconciliationService {
    pool: &'static RdbPool,
    observations: ThreadObservationRepositoryImpl,
    candidates: ThreadGroupCandidateAssociationRepositoryImpl,
    canonical_keys: ThreadCanonicalKeyRepositoryImpl,
    groups: ThreadGroupRepositoryImpl,
    members: ThreadGroupMemberRepositoryImpl,
    relations: ThreadRelationRepositoryImpl,
    source_identities: SourceThreadIdentityRepositoryImpl,
    locks: ThreadGroupLockRepositoryImpl,
    outbox: ThreadGroupEventOutboxRepositoryImpl,
    decisions: OperatorDecisionRepositoryImpl,
    markers: ThreadDeletionMarkerRepositoryImpl,
}

impl ThreadGroupReconciliationService {
    pub fn new(pool: &'static RdbPool) -> Self {
        Self::with_id_generator(pool, IdGeneratorWrapper::new())
    }

    /// Injection point for tests that share one process-wide Snowflake
    /// bucket (`infra::test_helper::shared_id_generator`).
    pub fn with_id_generator(pool: &'static RdbPool, id_generator: IdGeneratorWrapper) -> Self {
        Self {
            pool,
            observations: ThreadObservationRepositoryImpl::new(id_generator.clone(), pool),
            candidates: ThreadGroupCandidateAssociationRepositoryImpl::new(
                id_generator.clone(),
                pool,
            ),
            canonical_keys: ThreadCanonicalKeyRepositoryImpl::new(pool),
            groups: ThreadGroupRepositoryImpl::new(id_generator.clone(), pool),
            members: ThreadGroupMemberRepositoryImpl::new(pool),
            relations: ThreadRelationRepositoryImpl::new(id_generator.clone(), pool),
            source_identities: SourceThreadIdentityRepositoryImpl::new(pool),
            locks: ThreadGroupLockRepositoryImpl::new(pool),
            outbox: ThreadGroupEventOutboxRepositoryImpl::new(pool),
            decisions: OperatorDecisionRepositoryImpl::new(id_generator, pool),
            markers: ThreadDeletionMarkerRepositoryImpl::new(pool),
        }
    }

    /// Record `observations` for a subject already imported as
    /// `subject_thread_id`, then reconcile its canonical parent and
    /// reconciler membership. Idempotent: replaying the same evidence
    /// leaves the graph unchanged.
    #[allow(clippy::too_many_arguments)]
    pub async fn reconcile_subject(
        &self,
        subject_thread_id: i64,
        subject: &ObservedEndpoint,
        observations: &[ObservationInput],
        operation_id: &str,
        now: i64,
    ) -> anyhow::Result<ReconciliationOutcome> {
        ensure_thread_group_writes()?;
        let mut outcome = ReconciliationOutcome::default();
        let subject_scope = known_scope_value(&subject.identity_scope).unwrap_or_default();
        let subject_key = source_thread_canonical_key(&subject.to_source_identity());

        let lock_key = SourceIdentityKey {
            owner_scope: &subject.owner_scope,
            source: &subject.source,
            identity_scope: &subject_scope,
            native_id: &subject.native_id,
        };

        let mut tx = self.pool.begin().await?;
        self.locks
            .lock_source_identity_tx(&mut *tx, &lock_key)
            .await?;

        if let Some(key) = subject_key.as_deref()
            && self
                .canonical_keys
                .find_by_thread_id_tx(&mut *tx, subject_thread_id)
                .await?
                .is_none()
        {
            self.canonical_keys
                .assign_tx(
                    &mut *tx,
                    subject_thread_id,
                    &subject.owner_scope,
                    key,
                    values::canonical_key_origin::SOURCE_IDENTITY,
                    now,
                )
                .await?;
        }
        self.source_identities
            .upsert_resolved_tx(&mut *tx, &lock_key, subject_thread_id, now)
            .await?;

        for input in observations {
            // Heuristic / unsupported evidence is retained as
            // `unsupported`, never as a promotable candidate.
            let state = match input.source_confidence.as_deref() {
                Some(values::source_confidence::UNSUPPORTED)
                | Some(values::source_confidence::HEURISTIC) => {
                    values::observation_state::UNSUPPORTED
                }
                _ => values::observation_state::CANDIDATE,
            };
            let recorded = record_observation_and_event(
                &self.observations,
                &self.outbox,
                &mut tx,
                ObservationRecordRequest {
                    input,
                    state,
                    event_state: values::observation_state::CANDIDATE,
                    thread_id: Some(subject_thread_id),
                    operation_id,
                    now,
                },
            )
            .await?;
            outcome.observation_ids.push(recorded.observation_id);
        }

        let Some(subject_key) = subject_key else {
            // Unknown identity scope: evidence is retained but never
            // promoted to a mapping, relation, or membership.
            tx.commit().await?;
            return Ok(outcome);
        };

        // Build candidates only from exact / strong supporting source
        // evidence whose parent identity resolves to a live thread.
        let mut candidates: Vec<(Candidate, ObservedEndpoint, i64)> = Vec::new();
        for (input, observation_id) in observations.iter().zip(&outcome.observation_ids) {
            let Some(parent) = input.candidate_parent.clone() else {
                continue;
            };
            let Some(confidence) = supported_confidence(input.source_confidence.as_deref()) else {
                continue;
            };
            let Some(relation_type) = relation_type_of(input.relation_kind.as_deref()) else {
                continue;
            };
            if input.polarity != values::polarity::SUPPORTS {
                continue;
            }
            let parent_scope = known_scope_value(&parent.identity_scope).unwrap_or_default();
            let parent_lock = SourceIdentityKey {
                owner_scope: &parent.owner_scope,
                source: &parent.source,
                identity_scope: &parent_scope,
                native_id: &parent.native_id,
            };
            let Some(resolved) = self
                .source_identities
                .find_resolved_tx(&mut *tx, &parent_lock)
                .await?
            else {
                self.record_candidate(
                    &mut tx,
                    subject_thread_id,
                    subject,
                    None,
                    values::candidate_state::PENDING,
                    *observation_id,
                    now,
                )
                .await?;
                outcome.pending += 1;
                continue;
            };
            let Some(parent_key_row) = self
                .canonical_keys
                .find_by_thread_id_tx(&mut *tx, resolved.thread_id)
                .await?
            else {
                continue;
            };
            candidates.push((
                Candidate {
                    parent_key: parent_key_row.key,
                    relation_type,
                    evidence: CandidateEvidence::Source { confidence },
                },
                parent,
                *observation_id,
            ));
        }

        match select_parent_candidate(
            &candidates
                .iter()
                .map(|(candidate, _, _)| candidate.clone())
                .collect::<Vec<_>>(),
        ) {
            CandidateSelection::NoSelection => {}
            CandidateSelection::Conflict(winners) => {
                outcome.conflict = true;
                self.retract_active_relation(&mut tx, &subject_key, now)
                    .await?;
                for winner in winners {
                    if let Some((_, parent, observation_id)) = candidates
                        .iter()
                        .find(|(candidate, _, _)| *candidate == winner)
                    {
                        let parent_scope =
                            known_scope_value(&parent.identity_scope).unwrap_or_default();
                        let parent_thread_id = self
                            .source_identities
                            .find_resolved_tx(
                                &mut *tx,
                                &SourceIdentityKey {
                                    owner_scope: &parent.owner_scope,
                                    source: &parent.source,
                                    identity_scope: &parent_scope,
                                    native_id: &parent.native_id,
                                },
                            )
                            .await?
                            .map(|row| row.thread_id);
                        self.record_candidate(
                            &mut tx,
                            subject_thread_id,
                            subject,
                            parent_thread_id,
                            values::candidate_state::CONFLICT,
                            *observation_id,
                            now,
                        )
                        .await?;
                    }
                }
                self.append_relation_event(
                    &mut tx,
                    values::event_type::CONFLICT_DETECTED,
                    &subject_key,
                    operation_id,
                    now,
                    serde_json::json!({"reason": "equal_ranked_parents"}),
                )
                .await?;
            }
            CandidateSelection::Selected(winner) => {
                if winner.parent_key == subject_key {
                    // Self edge: evidence retained, canonical edge rejected.
                    self.record_candidate(
                        &mut tx,
                        subject_thread_id,
                        subject,
                        None,
                        values::candidate_state::CONFLICT,
                        winner_observation_id(&candidates, &winner),
                        now,
                    )
                    .await?;
                } else if self
                    .ancestor_contains(&mut tx, &winner.parent_key, &subject_key)
                    .await?
                {
                    self.record_candidate(
                        &mut tx,
                        subject_thread_id,
                        subject,
                        None,
                        values::candidate_state::CONFLICT,
                        winner_observation_id(&candidates, &winner),
                        now,
                    )
                    .await?;
                } else {
                    let (_, parent, observation_id) = candidates
                        .iter()
                        .find(|(candidate, _, _)| *candidate == winner)
                        .expect("winner is from candidates");
                    let relation_id = self
                        .apply_selected_relation(
                            &mut tx,
                            subject_thread_id,
                            &subject_key,
                            subject,
                            parent,
                            &winner,
                            *observation_id,
                            operation_id,
                            now,
                        )
                        .await?;
                    outcome.relation_selected = relation_id;
                    if relation_id.is_none() {
                        // Conflicting later evidence retracted the adopted
                        // relation; no active parent remains.
                        outcome.conflict = true;
                    }
                    outcome.group_id = self
                        .members
                        .find_current_by_thread_canonical_key_tx(&mut *tx, &subject_key)
                        .await?
                        .map(|member| member.group_id);
                }
            }
        }

        // A subject with no resolved parent is still represented as a
        // singleton reconciler group (orphan), so lineage / root /
        // placeholder semantics have a stable home.
        if outcome.group_id.is_none()
            && self
                .members
                .find_current_by_thread_canonical_key_tx(&mut *tx, &subject_key)
                .await?
                .is_none()
        {
            let group_key = reconciler_group_canonical_key(&subject_key);
            let group_id = match self
                .groups
                .find_active_by_canonical_key_tx(&mut *tx, &group_key)
                .await?
            {
                Some(group) => group.id,
                None => {
                    self.groups
                        .create_tx(
                            &mut *tx,
                            &NewThreadGroup {
                                group_canonical_key: group_key,
                                title: None,
                                status: values::group_status::ACTIVE.to_string(),
                                grouping_authority: values::grouping_authority::RECONCILER
                                    .to_string(),
                                redirect_to_group_id: None,
                                created_at: now,
                                updated_at: now,
                            },
                        )
                        .await?
                }
            };
            self.members
                .insert_tx(
                    &mut *tx,
                    &NewThreadGroupMember {
                        group_id,
                        thread_id: Some(subject_thread_id),
                        thread_canonical_key: subject_key.clone(),
                        owner_scope: subject.owner_scope.clone(),
                        source: Some(subject.source.clone()),
                        identity_scope: known_scope_value(&subject.identity_scope),
                        native_id: Some(subject.native_id.clone()),
                        role: values::member_role::ROOT.to_string(),
                        state: values::member_state::ACTIVE.to_string(),
                        provenance: values::grouping_authority::RECONCILER.to_string(),
                        deleted_at: None,
                        created_at: now,
                        updated_at: now,
                    },
                )
                .await?;
            outcome.group_id = Some(group_id);
        }

        tx.commit().await?;
        Ok(outcome)
    }

    #[allow(clippy::too_many_arguments)]
    async fn apply_selected_relation(
        &self,
        tx: &mut RdbTransaction<'_>,
        subject_thread_id: i64,
        subject_key: &str,
        subject: &ObservedEndpoint,
        parent: &ObservedEndpoint,
        winner: &Candidate,
        observation_id: i64,
        operation_id: &str,
        now: i64,
    ) -> anyhow::Result<Option<i64>> {
        let parent_scope = known_scope_value(&parent.identity_scope).unwrap_or_default();
        let parent_lock = SourceIdentityKey {
            owner_scope: &parent.owner_scope,
            source: &parent.source,
            identity_scope: &parent_scope,
            native_id: &parent.native_id,
        };
        let parent_thread_id = self
            .source_identities
            .find_resolved_tx(&mut **tx, &parent_lock)
            .await?
            .map(|row| row.thread_id);

        // Existing active parent: idempotent when identical, otherwise
        // retract the incumbent (no fallback / silent replacement).
        if let Some(existing) = self
            .relations
            .find_active_by_child_canonical_key_tx(&mut **tx, subject_key)
            .await?
        {
            let same = existing.parent_thread_canonical_key == winner.parent_key
                && existing.relation_type == relation_type_str(winner.relation_type.clone());
            if same {
                self.ensure_membership(
                    tx,
                    subject_thread_id,
                    subject_key,
                    subject,
                    parent_thread_id,
                    &winner.parent_key,
                    parent,
                    now,
                )
                .await?;
                return Ok(Some(existing.id));
            }
            // Conflicting later parent evidence: never silently replace
            // the adopted relation. Retract the incumbent, leave the
            // child with no active parent, and record the conflict as
            // derived evidence + audit (spec 3.3 / 6.2).
            self.relations
                .set_state_tx(
                    &mut **tx,
                    existing.id,
                    values::relation_state::ACTIVE,
                    values::relation_state::RETRACTED,
                    now,
                )
                .await?;
            self.record_derived_conflict(tx, subject, "conflicting_parent", now)
                .await?;
            self.record_candidate(
                tx,
                subject_thread_id,
                subject,
                existing.parent_thread_id,
                values::candidate_state::CONFLICT,
                observation_id,
                now,
            )
            .await?;
            self.append_relation_event(
                tx,
                values::event_type::CONFLICT_DETECTED,
                subject_key,
                operation_id,
                now,
                serde_json::json!({
                    "reason": "conflicting_parent",
                    "retracted_relation_id": existing.id,
                }),
            )
            .await?;
            return Ok(None);
        }

        let selection_basis = match &winner.evidence {
            CandidateEvidence::Source {
                confidence: SourceConfidence::Exact,
            } => values::selection_basis::SOURCE_EXACT,
            _ => values::selection_basis::SOURCE_STRONG,
        };
        let source_confidence = match &winner.evidence {
            CandidateEvidence::Source { confidence } => {
                Some(confidence_str(*confidence).to_string())
            }
            CandidateEvidence::OperatorConfirmation => None,
        };
        let relation = NewThreadRelation {
            parent_thread_id,
            child_thread_id: Some(subject_thread_id),
            parent_thread_canonical_key: winner.parent_key.clone(),
            child_thread_canonical_key: subject_key.to_string(),
            parent_owner_scope: parent.owner_scope.clone(),
            parent_source: Some(parent.source.clone()),
            parent_identity_scope: known_scope_value(&parent.identity_scope),
            parent_native_id: Some(parent.native_id.clone()),
            child_owner_scope: subject.owner_scope.clone(),
            child_source: Some(subject.source.clone()),
            child_identity_scope: known_scope_value(&subject.identity_scope),
            child_native_id: Some(subject.native_id.clone()),
            relation_type: relation_type_str(winner.relation_type.clone()).to_string(),
            state: values::relation_state::ACTIVE.to_string(),
            selection_basis: selection_basis.to_string(),
            source_confidence,
            selected_observation_id: Some(observation_id),
            selected_operator_decision_id: None,
            created_at: now,
            updated_at: now,
        };
        let relation_id = self.relations.insert_tx(&mut **tx, &relation).await?;

        self.ensure_membership(
            tx,
            subject_thread_id,
            subject_key,
            subject,
            parent_thread_id,
            &winner.parent_key,
            parent,
            now,
        )
        .await?;

        self.append_relation_event(
            tx,
            values::event_type::RELATION_SELECTED,
            subject_key,
            operation_id,
            now,
            serde_json::json!({
                "parent_thread_canonical_key": winner.parent_key,
                "relation_type": relation_type_str(winner.relation_type.clone()),
                "selection_basis": selection_basis,
            }),
        )
        .await?;
        Ok(Some(relation_id))
    }

    /// Record a domain-derived conflict observation (origin reconciler,
    /// distinct from adapter source evidence). Idempotent by fingerprint.
    async fn record_derived_conflict(
        &self,
        tx: &mut RdbTransaction<'_>,
        subject: &ObservedEndpoint,
        reason: &str,
        now: i64,
    ) -> anyhow::Result<()> {
        let input = ObservationInput {
            subject: subject.clone(),
            candidate_parent: None,
            relation_kind: None,
            evidence_kind: values::evidence_kind::DERIVED_CONFLICT.to_string(),
            polarity: values::polarity::CONFLICTS.to_string(),
            source_confidence: None,
            adapter_version: "reconciler@1".to_string(),
            source_record_ref: format!("reconciler:conflict:{reason}"),
            import_run_id: None,
            observed_at: now,
        };
        let row = new_observation(
            &input,
            values::observation_origin::RECONCILER,
            values::observation_state::CONFLICT,
            now,
        );
        let identity = observation_identity(&row);
        if self
            .observations
            .find_by_identity_tx(&mut **tx, &identity)
            .await?
            .is_none()
        {
            self.observations.insert_tx(&mut **tx, &row).await?;
        }
        Ok(())
    }

    /// Place the child under the nearest manual / existing ancestor's
    /// group, moving automatic memberships when a later parent changes
    /// the assignment. Operator-fixed memberships are never moved, and
    /// automatic descendants follow their ancestor (design 4.6).
    #[allow(clippy::too_many_arguments)]
    async fn ensure_membership(
        &self,
        tx: &mut RdbTransaction<'_>,
        subject_thread_id: i64,
        subject_key: &str,
        subject: &ObservedEndpoint,
        parent_thread_id: Option<i64>,
        parent_key: &str,
        parent: &ObservedEndpoint,
        now: i64,
    ) -> anyhow::Result<()> {
        if let Some(member) = self
            .members
            .find_current_by_thread_canonical_key_tx(&mut **tx, subject_key)
            .await?
            && member.provenance == values::grouping_authority::OPERATOR
        {
            return Ok(());
        }
        let (target_group_id, anchor_key) = self
            .resolve_anchor_group(tx, parent_thread_id, parent_key, parent, now)
            .await?;
        self.place_member(
            tx,
            Some(subject_thread_id),
            subject_key,
            subject,
            target_group_id,
            &anchor_key,
            now,
        )
        .await?;
        self.reassign_automatic_descendants(tx, subject_key, target_group_id, now)
            .await?;
        Ok(())
    }

    /// Walk canonical parents upward to the nearest member with a
    /// current membership (manual or automatic). With no such ancestor,
    /// anchor a new reconciler group at the topmost ancestor.
    #[allow(clippy::too_many_arguments)]
    async fn resolve_anchor_group(
        &self,
        tx: &mut RdbTransaction<'_>,
        parent_thread_id: Option<i64>,
        parent_key: &str,
        parent: &ObservedEndpoint,
        now: i64,
    ) -> anyhow::Result<(i64, String)> {
        let mut current_key = parent_key.to_string();
        let mut current_thread_id = parent_thread_id;
        let mut current_endpoint = parent.clone();
        let mut visited = HashSet::new();
        loop {
            if !visited.insert(current_key.clone()) {
                break;
            }
            if let Some(member) = self
                .members
                .find_current_by_thread_canonical_key_tx(&mut **tx, &current_key)
                .await?
            {
                return Ok((member.group_id, current_key));
            }
            let Some(relation) = self
                .relations
                .find_active_by_child_canonical_key_tx(&mut **tx, &current_key)
                .await?
            else {
                break;
            };
            current_endpoint = ObservedEndpoint {
                source: relation.parent_source.clone().unwrap_or_default(),
                identity_scope: relation
                    .parent_identity_scope
                    .as_deref()
                    .map(IdentityScope::known)
                    .unwrap_or_else(IdentityScope::unknown),
                owner_scope: relation.parent_owner_scope.clone(),
                native_id: relation.parent_native_id.clone().unwrap_or_default(),
            };
            current_thread_id = relation.parent_thread_id;
            current_key = relation.parent_thread_canonical_key;
        }

        let group_key = reconciler_group_canonical_key(&current_key);
        let group_id = match self
            .groups
            .find_active_by_canonical_key_tx(&mut **tx, &group_key)
            .await?
        {
            Some(group) => group.id,
            None => {
                self.groups
                    .create_tx(
                        &mut **tx,
                        &NewThreadGroup {
                            group_canonical_key: group_key,
                            title: None,
                            status: values::group_status::ACTIVE.to_string(),
                            grouping_authority: values::grouping_authority::RECONCILER.to_string(),
                            redirect_to_group_id: None,
                            created_at: now,
                            updated_at: now,
                        },
                    )
                    .await?
            }
        };
        if self
            .members
            .find_current_by_thread_canonical_key_tx(&mut **tx, &current_key)
            .await?
            .is_none()
        {
            self.place_member(
                tx,
                current_thread_id,
                &current_key,
                &current_endpoint,
                group_id,
                &current_key,
                now,
            )
            .await?;
        }
        Ok((group_id, current_key))
    }

    /// Insert or move one automatic membership into `target_group_id`.
    /// Operator-fixed rows are never touched; a moved group that becomes
    /// empty is redirected to the target.
    #[allow(clippy::too_many_arguments)]
    async fn place_member(
        &self,
        tx: &mut RdbTransaction<'_>,
        thread_id: Option<i64>,
        key: &str,
        endpoint: &ObservedEndpoint,
        target_group_id: i64,
        anchor_key: &str,
        now: i64,
    ) -> anyhow::Result<()> {
        let mut state = values::member_state::ACTIVE.to_string();
        let mut deleted_at = None;
        if let Some(existing) = self
            .members
            .find_current_by_thread_canonical_key_tx(&mut **tx, key)
            .await?
        {
            if existing.provenance == values::grouping_authority::OPERATOR {
                return Ok(());
            }
            if existing.group_id == target_group_id {
                return Ok(());
            }
            state = existing.state.clone();
            deleted_at = existing.deleted_at;
            let old_group = existing.group_id;
            self.members
                .redirect_current_tx(&mut **tx, key, old_group, now)
                .await?;
            if self
                .members
                .count_current_by_group_id_tx(&mut **tx, old_group)
                .await?
                == 0
            {
                self.groups
                    .repoint_redirect_tx(&mut **tx, old_group, target_group_id, now)
                    .await?;
            }
        }
        let (stored_thread_id, stored_state) = if state == values::member_state::DELETED {
            (None, values::member_state::DELETED)
        } else {
            let Some(thread_id) = thread_id else {
                // A live automatic member without a resolvable thread row
                // cannot be stored (active members require a thread id).
                return Ok(());
            };
            (Some(thread_id), values::member_state::ACTIVE)
        };
        let role = if key == anchor_key {
            values::member_role::ROOT
        } else {
            values::member_role::MEMBER
        };
        self.members
            .insert_tx(
                &mut **tx,
                &NewThreadGroupMember {
                    group_id: target_group_id,
                    thread_id: stored_thread_id,
                    thread_canonical_key: key.to_string(),
                    owner_scope: endpoint.owner_scope.clone(),
                    source: Some(endpoint.source.clone()),
                    identity_scope: known_scope_value(&endpoint.identity_scope),
                    native_id: Some(endpoint.native_id.clone()),
                    role: role.to_string(),
                    state: stored_state.to_string(),
                    provenance: values::grouping_authority::RECONCILER.to_string(),
                    deleted_at,
                    created_at: now,
                    updated_at: now,
                },
            )
            .await?;
        Ok(())
    }

    /// Move every automatic descendant of `start_key` to the same target
    /// group, so a later-arriving parent reassigns the whole subtree.
    async fn reassign_automatic_descendants(
        &self,
        tx: &mut RdbTransaction<'_>,
        start_key: &str,
        target_group_id: i64,
        now: i64,
    ) -> anyhow::Result<()> {
        let mut queue = vec![start_key.to_string()];
        let mut enqueued: HashSet<String> = HashSet::from([start_key.to_string()]);
        while let Some(parent_key) = queue.pop() {
            for relation in self
                .relations
                .list_by_parent_canonical_key_tx(
                    &mut **tx,
                    &parent_key,
                    Some(values::relation_state::ACTIVE),
                )
                .await?
            {
                let child_key = relation.child_thread_canonical_key;
                if !enqueued.insert(child_key.clone()) {
                    continue;
                }
                // An operator-fixed member is a hard boundary: never move
                // it and never traverse past it, so its automatic
                // descendants stay on the manual group's side (design
                // 4.6). A later-arriving ancestor must not pull them
                // across.
                if let Some(member) = self
                    .members
                    .find_current_by_thread_canonical_key_tx(&mut **tx, &child_key)
                    .await?
                    && member.provenance == values::grouping_authority::OPERATOR
                {
                    continue;
                }
                let endpoint = ObservedEndpoint {
                    source: relation.child_source.clone().unwrap_or_default(),
                    identity_scope: relation
                        .child_identity_scope
                        .as_deref()
                        .map(IdentityScope::known)
                        .unwrap_or_else(IdentityScope::unknown),
                    owner_scope: relation.child_owner_scope.clone(),
                    native_id: relation.child_native_id.clone().unwrap_or_default(),
                };
                self.place_member(
                    tx,
                    relation.child_thread_id,
                    &child_key,
                    &endpoint,
                    target_group_id,
                    "",
                    now,
                )
                .await?;
                queue.push(child_key);
            }
        }
        Ok(())
    }

    async fn retract_active_relation(
        &self,
        tx: &mut RdbTransaction<'_>,
        subject_key: &str,
        now: i64,
    ) -> anyhow::Result<()> {
        if let Some(existing) = self
            .relations
            .find_active_by_child_canonical_key_tx(&mut **tx, subject_key)
            .await?
        {
            self.relations
                .set_state_tx(
                    &mut **tx,
                    existing.id,
                    values::relation_state::ACTIVE,
                    values::relation_state::RETRACTED,
                    now,
                )
                .await?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn record_candidate(
        &self,
        tx: &mut RdbTransaction<'_>,
        subject_thread_id: i64,
        subject: &ObservedEndpoint,
        candidate_parent_thread_id: Option<i64>,
        state: &str,
        observation_id: i64,
        now: i64,
    ) -> anyhow::Result<()> {
        let existing = self
            .candidates
            .list_by_subject_identity_tx(
                &mut **tx,
                &subject.owner_scope,
                &subject.source,
                matches!(subject.identity_scope, IdentityScope::Known(_)),
                &known_scope_value(&subject.identity_scope).unwrap_or_default(),
                &subject.native_id,
            )
            .await?;
        let already = existing.iter().any(|row| {
            row.candidate_parent_thread_id == candidate_parent_thread_id && row.state == state
        });
        if already {
            return Ok(());
        }
        self.candidates
            .insert_tx(
                &mut **tx,
                &NewThreadGroupCandidateAssociation {
                    subject_thread_id: Some(subject_thread_id),
                    subject_source: subject.source.clone(),
                    subject_identity_scope_known: matches!(
                        subject.identity_scope,
                        IdentityScope::Known(_)
                    ),
                    subject_identity_scope_value: known_scope_value(&subject.identity_scope)
                        .unwrap_or_default(),
                    subject_owner_scope: subject.owner_scope.clone(),
                    subject_native_id: subject.native_id.clone(),
                    candidate_group_id: None,
                    candidate_parent_thread_id,
                    state: state.to_string(),
                    selected_observation_id: Some(observation_id),
                    created_at: now,
                    updated_at: now,
                },
            )
            .await?;
        Ok(())
    }

    async fn append_relation_event(
        &self,
        tx: &mut RdbTransaction<'_>,
        event_type: &str,
        subject_key: &str,
        operation_id: &str,
        now: i64,
        payload: serde_json::Value,
    ) -> anyhow::Result<()> {
        let event = NewThreadGroupEvent {
            event_id: event_id_for(
                if event_type == values::event_type::CONFLICT_DETECTED {
                    ThreadGroupEventType::ConflictDetected
                } else {
                    ThreadGroupEventType::RelationSelected
                },
                subject_key,
            ),
            event_type: event_type.to_string(),
            operation_id: operation_id.to_string(),
            policy_version: THREAD_GROUP_POLICY_VERSION.to_string(),
            source: None,
            identity_scope: None,
            owner_scope: None,
            native_id_ref: None,
            group_id: None,
            thread_id: None,
            source_confidence: None,
            selection_basis: None,
            operator_decision_id: None,
            polarity: None,
            payload: payload.to_string(),
            created_at: now,
        };
        self.outbox.append_idempotent_tx(&mut **tx, &event).await?;
        Ok(())
    }

    /// Walk active parents upward from `start_key` looking for
    /// `target_key` (cycle / self rejection without recursive SQL).
    /// Resolve a Thread's canonical key, generating and persisting a
    /// `creation_uuid` key for a manual / legacy thread that has none.
    /// Used by the attach path so callers never have to invent one.
    pub async fn ensure_thread_canonical_key(
        &self,
        thread_id: i64,
        owner_scope: &str,
        now: i64,
    ) -> anyhow::Result<String> {
        ensure_thread_group_writes()?;
        if let Some(row) = self.canonical_keys.find_by_thread_id(thread_id).await? {
            return Ok(row.key);
        }
        let mut tx = self.pool.begin().await?;
        if let Some(row) = self
            .canonical_keys
            .find_by_thread_id_tx(&mut *tx, thread_id)
            .await?
        {
            return Ok(row.key);
        }
        let key = new_manual_thread_canonical_key();
        self.canonical_keys
            .assign_tx(
                &mut *tx,
                thread_id,
                owner_scope,
                &key,
                values::canonical_key_origin::CREATION_UUID,
                now,
            )
            .await?;
        tx.commit().await?;
        Ok(key)
    }

    /// Read-only preview of a session import: suppression decision,
    /// revival, and planned relation / pending / conflict counts. Never
    /// writes.
    pub async fn preview_import(
        &self,
        subject: &ObservedEndpoint,
        observations: &[ObservationInput],
        explicit_override: bool,
    ) -> anyhow::Result<ImportPreview> {
        let marker = match observed_endpoint_key(subject) {
            Some(key) => self.markers.find(&key).await?,
            None => None,
        };
        let decision = decide_suppression(
            marker.as_ref().map(|row| row.forbid_reimport),
            explicit_override,
        );
        let suppressed = decision == SuppressionDecision::Suppress;

        let subject_canonical = source_thread_canonical_key(&subject.to_source_identity());
        let would_revive = match subject_canonical {
            Some(key) => self
                .members
                .find_current_by_thread_canonical_key(&key)
                .await?
                .is_some_and(|member| member.state == values::member_state::DELETED),
            None => false,
        };

        let mut pending = 0i64;
        let mut candidates: Vec<Candidate> = Vec::new();
        if !suppressed {
            for input in observations {
                let Some(parent) = input.candidate_parent.as_ref() else {
                    continue;
                };
                let Some(confidence) = supported_confidence(input.source_confidence.as_deref())
                else {
                    continue;
                };
                let Some(relation_type) = relation_type_of(input.relation_kind.as_deref()) else {
                    continue;
                };
                if input.polarity != values::polarity::SUPPORTS {
                    continue;
                }
                let Some(parent_key) = observed_endpoint_key(parent) else {
                    pending += 1;
                    continue;
                };
                let Some(resolved) = self.source_identities.find_resolved(&parent_key).await?
                else {
                    pending += 1;
                    continue;
                };
                let Some(parent_key_row) = self
                    .canonical_keys
                    .find_by_thread_id(resolved.thread_id)
                    .await?
                else {
                    continue;
                };
                candidates.push(Candidate {
                    parent_key: parent_key_row.key,
                    relation_type,
                    evidence: CandidateEvidence::Source { confidence },
                });
            }
        }
        let (planned_relations, conflict) = match select_parent_candidate(&candidates) {
            CandidateSelection::Selected(_) => (1, false),
            CandidateSelection::Conflict(_) => (0, true),
            CandidateSelection::NoSelection => (0, false),
        };
        Ok(ImportPreview {
            suppressed,
            would_revive,
            planned_observations: observations.len() as i64,
            planned_relations,
            pending,
            conflict,
        })
    }

    /// Owner scope (`user:{thread.user_id}`) of a thread. Used when the
    /// attach path must generate a canonical key for a manual thread.
    pub async fn thread_owner_scope(&self, thread_id: i64) -> anyhow::Result<Option<String>> {
        let repo = ThreadRepositoryImpl::new(IdGeneratorWrapper::new(), self.pool);
        let thread = repo
            .find(&protobuf::llm_memory::data::ThreadId { value: thread_id })
            .await?;
        Ok(thread
            .and_then(|thread| thread.data)
            .and_then(|data| data.user_id)
            .map(|user_id| format!("user:{}", user_id.value)))
    }

    /// Source identity endpoint for a thread, when it is source-backed.
    pub async fn source_endpoint_for_thread(
        &self,
        thread_id: i64,
    ) -> anyhow::Result<Option<ObservedEndpoint>> {
        Ok(self
            .source_identities
            .list_by_thread_id(thread_id)
            .await?
            .into_iter()
            .next()
            .map(|row| ObservedEndpoint {
                source: row.source,
                identity_scope: IdentityScope::known(row.identity_scope),
                owner_scope: row.owner_scope,
                native_id: row.native_id,
            }))
    }

    async fn ancestor_contains(
        &self,
        tx: &mut RdbTransaction<'_>,
        start_key: &str,
        target_key: &str,
    ) -> anyhow::Result<bool> {
        let mut current = start_key.to_string();
        let mut visited = HashSet::new();
        loop {
            if current == target_key {
                return Ok(true);
            }
            if !visited.insert(current.clone()) {
                return Ok(false);
            }
            let Some(edge) = self
                .relations
                .find_active_by_child_canonical_key_tx(&mut **tx, &current)
                .await?
            else {
                return Ok(false);
            };
            current = edge.parent_thread_canonical_key;
        }
    }

    /// Record and apply an explicit operator decision on one candidate
    /// association. The decision audit is separate from source
    /// confidence; `confirm` adopts the candidate parent with
    /// `operator_confirmation`, `reject` supersedes the candidate, and
    /// `retract` retracts an operator-adopted relation. Idempotent for
    /// an identical replay.
    pub async fn record_operator_decision(
        &self,
        candidate_association_id: i64,
        decision: &str,
        actor_id: &str,
        reason: &str,
        now: i64,
    ) -> anyhow::Result<OperatorDecisionOutcome> {
        ensure_thread_group_writes()?;
        if !matches!(
            decision,
            values::operator_decision::CONFIRM
                | values::operator_decision::REJECT
                | values::operator_decision::RETRACT
        ) {
            anyhow::bail!("unknown operator decision {decision}");
        }
        if reason.trim().is_empty() {
            anyhow::bail!("operator decision requires a reason");
        }

        let mut tx = self.pool.begin().await?;
        let candidate = self
            .candidates
            .find_by_id_tx(&mut *tx, candidate_association_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("candidate association not found"))?;

        if let Some(latest) = self
            .decisions
            .find_latest_by_candidate_association_id(candidate_association_id)
            .await?
            && latest.decision == decision
            && latest.reason == reason
        {
            return Ok(OperatorDecisionOutcome {
                decision_id: latest.id,
                relation_id: None,
                state: candidate.state,
            });
        }

        let input_fingerprint = match candidate.selected_observation_id {
            Some(observation_id) => self
                .observations
                .find_by_id(observation_id)
                .await?
                .map(|row| row.evidence_fingerprint)
                .unwrap_or_default(),
            None => String::new(),
        };
        let decision_id = self
            .decisions
            .insert_tx(
                &mut *tx,
                &NewOperatorDecision {
                    owner_scope: candidate.subject_owner_scope.clone(),
                    candidate_association_id,
                    actor_id: actor_id.to_string(),
                    decision: decision.to_string(),
                    reason: reason.to_string(),
                    input_evidence_fingerprint: input_fingerprint,
                    policy_version: THREAD_GROUP_POLICY_VERSION.to_string(),
                    created_at: now,
                },
            )
            .await?;

        let subject_key = candidate_subject_key(&candidate);
        let mut relation_id = None;
        let new_state = match decision {
            values::operator_decision::CONFIRM => {
                let subject_key = subject_key
                    .clone()
                    .ok_or_else(|| anyhow::anyhow!("subject identity scope unknown"))?;
                let subject_thread_id = candidate
                    .subject_thread_id
                    .ok_or_else(|| anyhow::anyhow!("subject thread unresolved"))?;
                let parent_thread_id = candidate
                    .candidate_parent_thread_id
                    .ok_or_else(|| anyhow::anyhow!("candidate parent unresolved"))?;
                let parent_key = self
                    .canonical_keys
                    .find_by_thread_id_tx(&mut *tx, parent_thread_id)
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("parent canonical key missing"))?
                    .key;
                if parent_key == subject_key
                    || self
                        .ancestor_contains(&mut tx, &parent_key, &subject_key)
                        .await?
                {
                    anyhow::bail!("operator confirmation would create a cycle");
                }

                let subject_endpoint = candidate_endpoint(
                    &candidate.subject_source,
                    candidate.subject_identity_scope_known,
                    &candidate.subject_identity_scope_value,
                    &candidate.subject_owner_scope,
                    &candidate.subject_native_id,
                );
                let parent_endpoint = match self
                    .source_identities
                    .list_by_thread_id(parent_thread_id)
                    .await?
                    .into_iter()
                    .next()
                {
                    Some(row) => candidate_endpoint(
                        &row.source,
                        true,
                        &row.identity_scope,
                        &row.owner_scope,
                        &row.native_id,
                    ),
                    None => candidate_endpoint("", false, "", "", ""),
                };

                if let Some(existing) = self
                    .relations
                    .find_active_by_child_canonical_key_tx(&mut *tx, &subject_key)
                    .await?
                {
                    if existing.parent_thread_canonical_key == parent_key {
                        relation_id = Some(existing.id);
                    } else {
                        self.relations
                            .set_state_tx(
                                &mut *tx,
                                existing.id,
                                values::relation_state::ACTIVE,
                                values::relation_state::SUPERSEDED,
                                now,
                            )
                            .await?;
                    }
                }
                if relation_id.is_none() {
                    relation_id = Some(
                        self.relations
                            .insert_tx(
                                &mut *tx,
                                &NewThreadRelation {
                                    parent_thread_id: Some(parent_thread_id),
                                    child_thread_id: Some(subject_thread_id),
                                    parent_thread_canonical_key: parent_key.clone(),
                                    child_thread_canonical_key: subject_key.clone(),
                                    parent_owner_scope: parent_endpoint.owner_scope.clone(),
                                    parent_source: Some(parent_endpoint.source.clone()),
                                    parent_identity_scope: known_scope_value(
                                        &parent_endpoint.identity_scope,
                                    ),
                                    parent_native_id: Some(parent_endpoint.native_id.clone()),
                                    child_owner_scope: subject_endpoint.owner_scope.clone(),
                                    child_source: Some(subject_endpoint.source.clone()),
                                    child_identity_scope: known_scope_value(
                                        &subject_endpoint.identity_scope,
                                    ),
                                    child_native_id: Some(subject_endpoint.native_id.clone()),
                                    relation_type: values::relation_type::DELEGATED.to_string(),
                                    state: values::relation_state::ACTIVE.to_string(),
                                    selection_basis: values::selection_basis::OPERATOR_CONFIRMATION
                                        .to_string(),
                                    source_confidence: None,
                                    selected_observation_id: None,
                                    selected_operator_decision_id: Some(decision_id),
                                    created_at: now,
                                    updated_at: now,
                                },
                            )
                            .await?,
                    );
                    self.ensure_membership(
                        &mut tx,
                        subject_thread_id,
                        &subject_key,
                        &subject_endpoint,
                        Some(parent_thread_id),
                        &parent_key,
                        &parent_endpoint,
                        now,
                    )
                    .await?;
                }
                values::candidate_state::CANDIDATE
            }
            values::operator_decision::REJECT => values::candidate_state::SUPERSEDED,
            values::operator_decision::RETRACT => {
                if let Some(subject_key) = &subject_key
                    && let Some(existing) = self
                        .relations
                        .find_active_by_child_canonical_key_tx(&mut *tx, subject_key)
                        .await?
                    && existing.selected_operator_decision_id.is_some()
                {
                    self.relations
                        .set_state_tx(
                            &mut *tx,
                            existing.id,
                            values::relation_state::ACTIVE,
                            values::relation_state::RETRACTED,
                            now,
                        )
                        .await?;
                }
                values::candidate_state::PENDING
            }
            _ => unreachable!(),
        };

        self.candidates
            .update_selection_tx(
                &mut *tx,
                candidate_association_id,
                &candidate.state,
                new_state,
                candidate.selected_observation_id,
                now,
            )
            .await?;
        tx.commit().await?;
        Ok(OperatorDecisionOutcome {
            decision_id,
            relation_id,
            state: new_state.to_string(),
        })
    }
}

/// Read-only preview of what a session import would do, produced by the
/// connected dry-run without writing anything.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ImportPreview {
    pub suppressed: bool,
    pub would_revive: bool,
    pub planned_observations: i64,
    pub planned_relations: i64,
    pub pending: i64,
    pub conflict: bool,
}

/// Outcome of one operator decision application.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OperatorDecisionOutcome {
    pub decision_id: i64,
    pub relation_id: Option<i64>,
    pub state: String,
}

fn candidate_subject_key(candidate: &ThreadGroupCandidateAssociationRow) -> Option<String> {
    source_thread_canonical_key(&SourceIdentity::new(
        candidate.subject_owner_scope.clone(),
        candidate.subject_source.clone(),
        if candidate.subject_identity_scope_known {
            IdentityScope::known(candidate.subject_identity_scope_value.clone())
        } else {
            IdentityScope::unknown()
        },
        candidate.subject_native_id.clone(),
    ))
}

fn observed_endpoint_key(endpoint: &ObservedEndpoint) -> Option<SourceIdentityKey<'_>> {
    let IdentityScope::Known(scope) = &endpoint.identity_scope else {
        return None;
    };
    Some(SourceIdentityKey {
        owner_scope: &endpoint.owner_scope,
        source: &endpoint.source,
        identity_scope: scope,
        native_id: &endpoint.native_id,
    })
}

fn candidate_endpoint(
    source: &str,
    identity_scope_known: bool,
    identity_scope_value: &str,
    owner_scope: &str,
    native_id: &str,
) -> ObservedEndpoint {
    ObservedEndpoint {
        source: source.to_string(),
        identity_scope: if identity_scope_known {
            IdentityScope::known(identity_scope_value.to_string())
        } else {
            IdentityScope::unknown()
        },
        owner_scope: owner_scope.to_string(),
        native_id: native_id.to_string(),
    }
}

fn winner_observation_id(
    candidates: &[(Candidate, ObservedEndpoint, i64)],
    winner: &Candidate,
) -> i64 {
    candidates
        .iter()
        .find(|(candidate, _, _)| candidate == winner)
        .map(|(_, _, id)| *id)
        .unwrap_or_default()
}

fn supported_confidence(confidence: Option<&str>) -> Option<SourceConfidence> {
    match confidence {
        Some(values::source_confidence::EXACT) => Some(SourceConfidence::Exact),
        Some(values::source_confidence::STRONG) => Some(SourceConfidence::Strong),
        _ => None,
    }
}

fn relation_type_of(kind: Option<&str>) -> Option<RelationType> {
    match kind {
        Some(values::relation_type::DELEGATED) => Some(RelationType::Delegated),
        Some(values::relation_type::FORK) => Some(RelationType::Fork),
        Some(values::relation_type::CONTINUATION) => Some(RelationType::Continuation),
        _ => None,
    }
}

fn relation_type_str(kind: RelationType) -> &'static str {
    match kind {
        RelationType::Delegated => values::relation_type::DELEGATED,
        RelationType::Fork => values::relation_type::FORK,
        RelationType::Continuation => values::relation_type::CONTINUATION,
    }
}

fn confidence_str(confidence: SourceConfidence) -> &'static str {
    match confidence {
        SourceConfidence::Exact => values::source_confidence::EXACT,
        SourceConfidence::Strong => values::source_confidence::STRONG,
        SourceConfidence::Heuristic => values::source_confidence::HEURISTIC,
        SourceConfidence::Unsupported => values::source_confidence::UNSUPPORTED,
    }
}

#[cfg(all(test, not(feature = "postgres")))]
mod tests {
    use super::*;
    use infra::infra::thread_group::observation::{
        ThreadObservationRepository, ThreadObservationRepositoryImpl,
    };
    use infra::infra::thread_group::outbox::{
        ThreadGroupEventOutboxRepository, ThreadGroupEventOutboxRepositoryImpl,
    };
    use infra::infra::thread_group::test_support::{insert_thread, setup_thread_group_pool};

    fn run<F: std::future::Future>(future: F) -> F::Output {
        tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("current-thread runtime")
            .block_on(future)
    }

    fn endpoint(native_id: &str) -> ObservedEndpoint {
        ObservedEndpoint {
            source: "codex".into(),
            identity_scope: IdentityScope::known(""),
            owner_scope: "user:pass2-reconcile".into(),
            native_id: native_id.into(),
        }
    }

    fn evidence(child: ObservedEndpoint, parent: ObservedEndpoint) -> ObservationInput {
        ObservationInput {
            subject: child,
            candidate_parent: Some(parent),
            relation_kind: Some(values::relation_type::DELEGATED.into()),
            evidence_kind: values::evidence_kind::SOURCE_EVENT.into(),
            polarity: values::polarity::SUPPORTS.into(),
            source_confidence: Some(values::source_confidence::EXACT.into()),
            adapter_version: "test@1".into(),
            source_record_ref: "reconcile-replay-record".into(),
            import_run_id: None,
            observed_at: 100,
        }
    }

    #[test]
    fn reconcile_replay_does_not_duplicate_observation_or_event() {
        run(async {
            let pool = setup_thread_group_pool().await;
            let service = ThreadGroupReconciliationService::with_id_generator(
                pool,
                infra::test_helper::shared_id_generator(),
            );
            let parent = endpoint("reconcile-parent");
            let child = endpoint("reconcile-child");
            insert_thread(pool, 29_001, None).await;
            insert_thread(pool, 29_002, None).await;
            service
                .reconcile_subject(29_001, &parent, &[], "pass2", 1_000)
                .await
                .expect("parent reconcile");
            let input = evidence(child.clone(), parent);

            let first = service
                .reconcile_subject(29_002, &child, std::slice::from_ref(&input), "pass2", 1_001)
                .await
                .expect("first reconcile");
            let second = service
                .reconcile_subject(29_002, &child, std::slice::from_ref(&input), "pass2", 1_002)
                .await
                .expect("replayed reconcile");

            assert_eq!(first.observation_ids, second.observation_ids);
            let observations = ThreadObservationRepositoryImpl::new(
                infra::test_helper::shared_id_generator(),
                pool,
            )
            .list_by_subject("user:pass2-reconcile", "codex", true, "", "reconcile-child")
            .await
            .expect("observation lookup");
            assert_eq!(observations.len(), 1);

            let event_id = event_id_for(
                ThreadGroupEventType::ObservationRecorded,
                &super::super::observation::observation_fingerprint(
                    &input,
                    values::observation_origin::ADAPTER,
                ),
            );
            let event = ThreadGroupEventOutboxRepositoryImpl::new(pool)
                .find_by_event_id(&event_id)
                .await
                .expect("event lookup")
                .expect("event exists");
            assert_eq!(event.event_type, values::event_type::OBSERVATION_RECORDED);
            assert_eq!(event.thread_id, Some(29_002));
        });
    }
}
