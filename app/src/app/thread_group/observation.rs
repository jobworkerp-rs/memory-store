//! Observation contracts and transactional observation recording.

use super::prelude::*;
use super::{EVIDENCE_FINGERPRINT_VERSION, THREAD_GROUP_POLICY_VERSION, known_scope_value};

/// One endpoint of an observed relation, already mapped to the
/// owner-local identity key. The importer assigns the typed user id;
/// adapters never choose an owner.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObservedEndpoint {
    pub source: String,
    pub identity_scope: IdentityScope,
    pub user_id: i64,
    pub native_id: String,
}

impl ObservedEndpoint {
    pub(crate) fn to_source_identity(&self) -> SourceIdentity {
        SourceIdentity::new(
            self.user_id,
            self.source.clone(),
            self.identity_scope.clone(),
            self.native_id.clone(),
        )
    }
}

/// Owner-local source identity supplied by the importer for the
/// pre-write suppression gate and revival. Distinct from the observation
/// contract because the import path needs the canonical key and the
/// identity lock key directly.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceIdentityInput {
    pub user_id: i64,
    pub source: String,
    pub identity_scope: IdentityScope,
    pub native_id: String,
}

impl SourceIdentityInput {
    pub fn as_source_identity(&self) -> SourceIdentity {
        SourceIdentity::new(
            self.user_id,
            self.source.clone(),
            self.identity_scope.clone(),
            self.native_id.clone(),
        )
    }

    /// Canonical key, or `None` when the identity scope is unknown
    /// (unknown identities are never mapped).
    pub fn canonical_key(&self) -> Option<String> {
        source_thread_canonical_key(&self.as_source_identity())
    }

    /// Identity lock / marker key, or `None` when the scope is unknown.
    pub fn key(&self) -> Option<SourceIdentityKey<'_>> {
        let IdentityScope::Known(scope) = &self.identity_scope else {
            return None;
        };
        Some(SourceIdentityKey {
            user_id: self.user_id,
            source: &self.source,
            identity_scope: scope,
            native_id: &self.native_id,
        })
    }
}

/// App-layer form of one adapter observation. The raw payload is never
/// carried here; `source_record_ref` is a redacted locator.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObservationInput {
    pub subject: ObservedEndpoint,
    pub candidate_parent: Option<ObservedEndpoint>,
    /// `delegated` / `fork` / `continuation`, or `None` for no-parent
    /// evidence such as an unsupported subagent meta.
    pub relation_kind: Option<String>,
    pub evidence_kind: String,
    pub polarity: String,
    pub source_confidence: Option<String>,
    pub adapter_version: String,
    pub source_record_ref: String,
    pub import_run_id: Option<String>,
    pub observed_at: i64,
}

/// Deterministic evidence fingerprint over the spec 3.4 attribute tuple.
/// Mutable state, import run, DB ids, and observed_at are excluded.
pub fn observation_fingerprint(input: &ObservationInput, origin: &str) -> String {
    let subject = input.subject.to_source_identity();
    let candidate_parent = input
        .candidate_parent
        .as_ref()
        .map(|p| p.to_source_identity());
    evidence_fingerprint(
        EVIDENCE_FINGERPRINT_VERSION,
        origin,
        &input.adapter_version,
        &subject,
        candidate_parent.as_ref(),
        input.relation_kind.as_deref().unwrap_or(""),
        &input.evidence_kind,
        &input.polarity,
        input.source_confidence.as_deref().unwrap_or(""),
        &input.source_record_ref,
    )
}

/// Build the repository insert row for an observation. `known("")` and
/// `unknown` scopes stay distinct (`known` flag + value column), and an
/// absent candidate parent is recorded through `candidate_parent_present`
/// rather than an empty string.
pub fn new_observation(
    input: &ObservationInput,
    origin: &str,
    state: &str,
    now: i64,
) -> NewThreadObservation {
    let fingerprint = observation_fingerprint(input, origin);
    let (subject_identity_scope_known, subject_identity_scope_value) =
        split_scope(&input.subject.identity_scope);
    let (parent_known, parent_value, parent_source, parent_user_id, parent_native) =
        match input.candidate_parent.as_ref() {
            Some(parent) => {
                let (known, value) = split_scope(&parent.identity_scope);
                (
                    known,
                    value,
                    parent.source.clone(),
                    Some(parent.user_id),
                    parent.native_id.clone(),
                )
            }
            None => (false, String::new(), String::new(), None, String::new()),
        };
    NewThreadObservation {
        subject_source: input.subject.source.clone(),
        subject_identity_scope_known,
        subject_identity_scope_value,
        subject_user_id: input.subject.user_id,
        subject_native_id: input.subject.native_id.clone(),
        candidate_parent_present: input.candidate_parent.is_some(),
        candidate_parent_source: parent_source,
        candidate_parent_identity_scope_known: parent_known,
        candidate_parent_identity_scope_value: parent_value,
        candidate_parent_user_id: parent_user_id,
        candidate_parent_native_id: parent_native,
        relation_kind: input.relation_kind.clone(),
        origin: origin.to_string(),
        evidence_kind: input.evidence_kind.clone(),
        polarity: input.polarity.clone(),
        source_confidence: input.source_confidence.clone(),
        evidence_fingerprint: fingerprint,
        source_record_ref: Some(input.source_record_ref.clone()),
        state: state.to_string(),
        import_run_id: input.import_run_id.clone(),
        observed_at: input.observed_at,
        created_at: now,
        updated_at: now,
    }
}

fn split_scope(scope: &IdentityScope) -> (bool, String) {
    match scope {
        IdentityScope::Known(value) => (true, value.clone()),
        IdentityScope::Unknown => (false, String::new()),
    }
}

/// ThreadGroup state-transition event types (design 8.3). The wire
/// strings are frozen; readers must ignore unknown values.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ThreadGroupEventType {
    ObservationRecorded,
    RelationSelected,
    ReconciliationCompleted,
    ConflictDetected,
    Redirected,
}

impl ThreadGroupEventType {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ObservationRecorded => values::event_type::OBSERVATION_RECORDED,
            Self::RelationSelected => values::event_type::RELATION_SELECTED,
            Self::ReconciliationCompleted => values::event_type::RECONCILIATION_COMPLETED,
            Self::ConflictDetected => values::event_type::CONFLICT_DETECTED,
            Self::Redirected => values::event_type::REDIRECTED,
        }
    }
}

/// Deterministic `event_id` for a state transition. Re-deriving the same
/// transition (retry / replay) yields the same id, so at-least-once
/// delivery cannot create a second event row (design 8.3).
pub fn event_id_for(event_type: ThreadGroupEventType, dedup_key: &str) -> String {
    sha256_hex(&common::thread_group_key::canonical_serialize_v2(&[
        Some(event_type.as_str()),
        Some(dedup_key),
    ]))
}

/// Outcome of the design 8.2 pre-write deletion-marker check. `Allow`
/// covers both "no marker" and a revivable (`forbid_reimport=false`)
/// marker; `Suppress` blocks content writes for a forbidden identity;
/// `Override` is the explicit operator re-import that consumes the
/// marker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SuppressionDecision {
    Allow,
    Suppress,
    Override,
}

/// Decide whether content may be written for an identity, given the
/// matching deletion marker (if any) and whether the caller supplied an
/// explicit re-import override. A marker that cannot be read is the
/// caller's responsibility: it must fail closed before reaching here.
pub fn decide_suppression(
    marker_forbid_reimport: Option<bool>,
    explicit_override: bool,
) -> SuppressionDecision {
    match (marker_forbid_reimport, explicit_override) {
        (None, _) => SuppressionDecision::Allow,
        (Some(false), _) => SuppressionDecision::Allow,
        (Some(true), true) => SuppressionDecision::Override,
        (Some(true), false) => SuppressionDecision::Suppress,
    }
}

/// Result of atomically recording one adapter observation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecordObservationOutcome {
    pub observation_id: i64,
    /// True when this call created the observation row; false when an
    /// identical evidence fingerprint already existed.
    pub inserted: bool,
    /// True when this call wrote the outbox event; false when the
    /// deterministic `event_id` already existed.
    pub event_written: bool,
}

/// Record one adapter observation and its deterministic ObservationRecorded
/// event inside the caller-owned transaction.
pub(crate) struct ObservationRecordRequest<'a> {
    pub(crate) input: &'a ObservationInput,
    pub(crate) state: &'a str,
    pub(crate) event_state: &'a str,
    pub(crate) thread_id: Option<i64>,
    pub(crate) operation_id: &'a str,
    pub(crate) now: i64,
}

pub(crate) async fn record_observation_and_event(
    observations: &ThreadObservationRepositoryImpl,
    outbox: &ThreadGroupEventOutboxRepositoryImpl,
    tx: &mut RdbTransaction<'_>,
    request: ObservationRecordRequest<'_>,
) -> anyhow::Result<RecordObservationOutcome> {
    let ObservationRecordRequest {
        input,
        state,
        event_state,
        thread_id,
        operation_id,
        now,
    } = request;
    let row = new_observation(input, values::observation_origin::ADAPTER, state, now);
    let identity = observation_identity(&row);
    let (observation_id, inserted) = match observations
        .find_by_identity_tx(&mut **tx, &identity)
        .await?
    {
        Some(existing) => (existing.id, false),
        None => (observations.insert_tx(&mut **tx, &row).await?, true),
    };

    let event = NewThreadGroupEvent {
        event_id: event_id_for(
            ThreadGroupEventType::ObservationRecorded,
            &row.evidence_fingerprint,
        ),
        event_type: values::event_type::OBSERVATION_RECORDED.to_string(),
        operation_id: operation_id.to_string(),
        policy_version: THREAD_GROUP_POLICY_VERSION.to_string(),
        source: Some(input.subject.source.clone()),
        identity_scope: known_scope_value(&input.subject.identity_scope),
        user_id: Some(input.subject.user_id),
        native_id_ref: Some(input.subject.native_id.clone()),
        group_id: None,
        thread_id,
        source_confidence: input.source_confidence.clone(),
        selection_basis: None,
        operator_decision_id: None,
        polarity: Some(input.polarity.clone()),
        payload: serde_json::json!({
            "state": event_state,
            "adapter_version": input.adapter_version.as_str(),
            "evidence_kind": input.evidence_kind.as_str(),
        })
        .to_string(),
        created_at: now,
    };
    let event_written = outbox.append_idempotent_tx(&mut **tx, &event).await?;

    Ok(RecordObservationOutcome {
        observation_id,
        inserted,
        event_written,
    })
}

/// App-layer service that records adapter observations and their
/// state-transition event in one transaction. The transaction boundary
/// lives here (design 3.2): repositories never begin transactions.
pub struct ThreadGroupObservationService {
    pool: &'static RdbPool,
    observations: ThreadObservationRepositoryImpl,
    locks: ThreadGroupLockRepositoryImpl,
    outbox: ThreadGroupEventOutboxRepositoryImpl,
}

impl ThreadGroupObservationService {
    pub fn new(pool: &'static RdbPool) -> Self {
        Self::with_id_generator(pool, IdGeneratorWrapper::new())
    }

    /// Injection point for tests that share one process-wide Snowflake
    /// bucket (`infra::test_helper::shared_id_generator`).
    pub fn with_id_generator(pool: &'static RdbPool, id_generator: IdGeneratorWrapper) -> Self {
        Self {
            pool,
            observations: ThreadObservationRepositoryImpl::new(id_generator, pool),
            locks: ThreadGroupLockRepositoryImpl::new(pool),
            outbox: ThreadGroupEventOutboxRepositoryImpl::new(pool),
        }
    }

    /// Record one observation idempotently and append its
    /// `thread_group_observation_recorded` event in the same
    /// transaction. Re-running with the same evidence fingerprint
    /// returns the existing row and writes no second event.
    pub async fn record_observation(
        &self,
        input: &ObservationInput,
        state: &str,
        operation_id: &str,
        now: i64,
    ) -> anyhow::Result<RecordObservationOutcome> {
        let subject_scope = known_scope_value(&input.subject.identity_scope).unwrap_or_default();
        let lock_key = SourceIdentityKey {
            user_id: input.subject.user_id,
            source: &input.subject.source,
            identity_scope: &subject_scope,
            native_id: &input.subject.native_id,
        };
        let mut tx = self.pool.begin().await?;
        self.locks
            .lock_source_identity_tx(&mut *tx, &lock_key)
            .await?;

        let outcome = record_observation_and_event(
            &self.observations,
            &self.outbox,
            &mut tx,
            ObservationRecordRequest {
                input,
                state,
                event_state: state,
                thread_id: None,
                operation_id,
                now,
            },
        )
        .await?;
        tx.commit().await?;

        Ok(outcome)
    }
}

pub(crate) fn observation_identity(row: &NewThreadObservation) -> ObservationIdentity<'_> {
    ObservationIdentity {
        subject_source: &row.subject_source,
        subject_identity_scope_known: row.subject_identity_scope_known,
        subject_identity_scope_value: &row.subject_identity_scope_value,
        subject_user_id: row.subject_user_id,
        subject_native_id: &row.subject_native_id,
        candidate_parent_present: row.candidate_parent_present,
        candidate_parent_source: &row.candidate_parent_source,
        candidate_parent_identity_scope_known: row.candidate_parent_identity_scope_known,
        candidate_parent_identity_scope_value: &row.candidate_parent_identity_scope_value,
        candidate_parent_user_id: row.candidate_parent_user_id,
        candidate_parent_native_id: &row.candidate_parent_native_id,
        evidence_kind: &row.evidence_kind,
        evidence_fingerprint: &row.evidence_fingerprint,
    }
}

#[cfg(all(test, not(feature = "postgres")))]
mod tests {
    use super::super::reconcile::ThreadGroupReconciliationService;
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

    fn input() -> ObservationInput {
        ObservationInput {
            subject: ObservedEndpoint {
                source: "codex".into(),
                identity_scope: IdentityScope::known(""),
                user_id: 1,
                native_id: "direct-replay".into(),
            },
            candidate_parent: None,
            relation_kind: None,
            evidence_kind: values::evidence_kind::SOURCE_FIELD.into(),
            polarity: values::polarity::SUPPORTS.into(),
            source_confidence: None,
            adapter_version: "test@1".into(),
            source_record_ref: "direct-replay-record".into(),
            import_run_id: None,
            observed_at: 100,
        }
    }

    fn endpoint(native_id: &str) -> ObservedEndpoint {
        ObservedEndpoint {
            source: "codex".into(),
            identity_scope: IdentityScope::known(""),
            user_id: 1,
            native_id: native_id.into(),
        }
    }

    #[test]
    fn direct_record_replay_does_not_duplicate_observation_or_event() {
        run(async {
            let pool = setup_thread_group_pool().await;
            let service = ThreadGroupObservationService::with_id_generator(
                pool,
                infra::test_helper::shared_id_generator(),
            );
            let input = input();

            let first = service
                .record_observation(&input, values::observation_state::UNSUPPORTED, "pass2", 100)
                .await
                .expect("first record");
            let second = service
                .record_observation(&input, values::observation_state::UNSUPPORTED, "pass2", 200)
                .await
                .expect("replayed record");

            assert!(first.inserted);
            assert!(first.event_written);
            assert_eq!(second.observation_id, first.observation_id);
            assert!(!second.inserted);
            assert!(!second.event_written);

            let observations = ThreadObservationRepositoryImpl::new(
                infra::test_helper::shared_id_generator(),
                pool,
            )
            .list_by_subject(1, "codex", true, "", "direct-replay")
            .await
            .expect("observation lookup");
            assert_eq!(observations.len(), 1);
            assert_eq!(
                observations[0].state,
                values::observation_state::UNSUPPORTED
            );

            let event_id = event_id_for(
                ThreadGroupEventType::ObservationRecorded,
                &observation_fingerprint(&input, values::observation_origin::ADAPTER),
            );
            let event = ThreadGroupEventOutboxRepositoryImpl::new(pool)
                .find_by_event_id(&event_id)
                .await
                .expect("event lookup")
                .expect("event exists");
            assert_eq!(event.event_type, values::event_type::OBSERVATION_RECORDED);
            assert_eq!(event.thread_id, None);
            let payload: serde_json::Value =
                serde_json::from_str(&event.payload).expect("event payload");
            assert_eq!(payload["state"], values::observation_state::UNSUPPORTED);
        });
    }

    #[test]
    fn reconcile_keeps_unsupported_state_but_uses_candidate_event_payload() {
        run(async {
            let pool = setup_thread_group_pool().await;
            let service = ThreadGroupReconciliationService::with_id_generator(
                pool,
                infra::test_helper::shared_id_generator(),
            );
            let parent = endpoint("reconcile-parent");
            let child = endpoint("reconcile-child");
            insert_thread(pool, 39_001, None).await;
            insert_thread(pool, 39_002, None).await;
            service
                .reconcile_subject(39_001, &parent, &[], "pass4", 1_000)
                .await
                .expect("parent reconcile");

            let input = ObservationInput {
                subject: child.clone(),
                candidate_parent: Some(parent),
                relation_kind: Some(values::relation_type::DELEGATED.into()),
                evidence_kind: values::evidence_kind::SOURCE_EVENT.into(),
                polarity: values::polarity::SUPPORTS.into(),
                source_confidence: Some(values::source_confidence::UNSUPPORTED.into()),
                adapter_version: "test@1".into(),
                source_record_ref: "reconcile-state-payload".into(),
                import_run_id: None,
                observed_at: 100,
            };
            service
                .reconcile_subject(39_002, &child, std::slice::from_ref(&input), "pass4", 1_001)
                .await
                .expect("reconcile");

            let observations = ThreadObservationRepositoryImpl::new(
                infra::test_helper::shared_id_generator(),
                pool,
            )
            .list_by_subject(1, "codex", true, "", "reconcile-child")
            .await
            .expect("observation lookup");
            assert_eq!(observations.len(), 1);
            assert_eq!(
                observations[0].state,
                values::observation_state::UNSUPPORTED
            );

            let event_id = event_id_for(
                ThreadGroupEventType::ObservationRecorded,
                &observation_fingerprint(&input, values::observation_origin::ADAPTER),
            );
            let event = ThreadGroupEventOutboxRepositoryImpl::new(pool)
                .find_by_event_id(&event_id)
                .await
                .expect("event lookup")
                .expect("event exists");
            assert_eq!(event.thread_id, Some(39_002));
            let payload: serde_json::Value =
                serde_json::from_str(&event.payload).expect("event payload");
            // Reconciliation preserves its existing candidate event payload
            // even when the stored observation is unsupported.
            assert_eq!(payload["state"], values::observation_state::CANDIDATE);
        });
    }
}
