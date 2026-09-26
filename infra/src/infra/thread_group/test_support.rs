//! Shared fixtures for the ThreadGroup repository tests.
//!
//! The schema under test is the *committed* SQLite migration
//! (`infra/atlas/sqlite/migrations/20260920000001_thread_group_schema.sql`),
//! applied verbatim onto a throwaway temp-file database so these tests
//! never touch the shared `test_db.sqlite3` used by the other repos. The trailing
//! `UPDATE memories_schema_contract ...` statement is the only thing
//! stripped: the contract table is not part of the base test schema,
//! and updating it is the migrate binary's job, not a repository
//! behavior. The base `001_schema.sql` is applied first because the
//! derived `latest_activity_at` read-model helper joins `thread`.
//!
//! Compiled for in-crate tests and for downstream crates that enable
//! the `test-helper` feature; not every fixture is used by every
//! consumer.
#![allow(dead_code)]

use super::rows::{
    NewGroupAuditMerge, NewGroupAuditSplit, NewManualCollection, NewOperatorDecision,
    NewThreadDeletionMarker, NewThreadGroup, NewThreadGroupCandidateAssociation,
    NewThreadGroupEvent, NewThreadGroupMember, NewThreadObservation, NewThreadRelation,
    ObservationIdentity, SourceIdentityKey,
};
use crate::infra::thread_group::rows::values;
use infra_utils::infra::rdb::RdbPool;

const BASE_SCHEMA: &str = include_str!("../../../sql/sqlite/001_schema.sql");
const THREAD_GROUP_SCHEMA: &str =
    include_str!("../../../atlas/sqlite/migrations/20260920000001_thread_group_schema.sql");
const MEMORY_RELATION_SCHEMA: &str = include_str!(
    "../../../atlas/sqlite/migrations/20260926000001_thread_group_memory_relation.sql"
);

/// Deterministic base timestamp; individual tests offset from it.
pub const T0: i64 = 1_700_000_000_000;

/// 64-char hex-shaped canonical key derived from a small number, so
/// tests can distinguish keys without colliding.
pub fn key(n: u64) -> String {
    format!("{n:0>64}")
}

pub async fn setup_thread_group_pool() -> &'static RdbPool {
    static INIT: tokio::sync::OnceCell<RdbPool> = tokio::sync::OnceCell::const_new();
    INIT.get_or_init(|| async {
        let dir = std::env::temp_dir();
        let file = tempfile::NamedTempFile::new_in(dir).expect("temp db file");
        // Leak so the file survives for the whole test binary; the
        // pool must be able to open additional connections, which a
        // private `:memory:` database would not share.
        let file = Box::leak(Box::new(file));
        let url = format!("sqlite://{}", file.path().display());
        let options = <sqlx::sqlite::SqliteConnectOptions as std::str::FromStr>::from_str(&url)
            .expect("sqlite url");
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(4)
            .acquire_timeout(std::time::Duration::from_secs(30))
            .connect_with(options)
            .await
            .expect("connect temp sqlite");
        sqlx::raw_sql(sqlx::AssertSqlSafe(BASE_SCHEMA.to_owned()))
            .execute(&pool)
            .await
            .expect("base schema");
        let ddl = THREAD_GROUP_SCHEMA
            .split("UPDATE memories_schema_contract")
            .next()
            .expect("migration has statements before the contract update");
        sqlx::raw_sql(sqlx::AssertSqlSafe(ddl.to_owned()))
            .execute(&pool)
            .await
            .expect("thread group schema");
        let relation_ddl = MEMORY_RELATION_SCHEMA
            .split("UPDATE memories_schema_contract")
            .next()
            .expect("relationship migration has statements before the contract update");
        sqlx::raw_sql(sqlx::AssertSqlSafe(relation_ddl.to_owned()))
            .execute(&pool)
            .await
            .expect("thread group memory relationship schema");
        pool
    })
    .await
}

/// Insert a bare raw thread row (the ThreadGroup schema itself has no
/// FKs; only the `latest_activity_at` join needs this table).
pub async fn insert_thread(pool: &RdbPool, id: i64, last_message_at: Option<i64>) {
    sqlx::query(
        "INSERT INTO thread (id, user_id, created_at, updated_at, memory_kind, last_message_at) \
         VALUES (?, 1, ?, ?, 1, ?)",
    )
    .bind(id)
    .bind(T0)
    .bind(T0)
    .bind(last_message_at)
    .execute(pool)
    .await
    .expect("insert thread");
}

pub fn new_group(n: u64) -> NewThreadGroup {
    NewThreadGroup {
        group_canonical_key: key(n),
        title: Some(format!("group-{n}")),
        status: values::group_status::ACTIVE.to_string(),
        grouping_authority: values::grouping_authority::RECONCILER.to_string(),
        redirect_to_group_id: None,
        created_at: T0,
        updated_at: T0,
    }
}

pub fn new_member(
    group_id: i64,
    thread_id: Option<i64>,
    thread_key: String,
) -> NewThreadGroupMember {
    NewThreadGroupMember {
        group_id,
        thread_id,
        thread_canonical_key: thread_key,
        owner_scope: "user:1".to_string(),
        source: Some("codex".to_string()),
        identity_scope: Some(String::new()),
        native_id: Some("session-1".to_string()),
        role: values::member_role::MEMBER.to_string(),
        state: values::member_state::ACTIVE.to_string(),
        provenance: values::grouping_authority::RECONCILER.to_string(),
        deleted_at: None,
        created_at: T0,
        updated_at: T0,
    }
}

pub fn new_relation(parent_key: u64, child_key: u64) -> NewThreadRelation {
    NewThreadRelation {
        parent_thread_id: Some(parent_key as i64 * 100),
        child_thread_id: Some(child_key as i64 * 100),
        parent_thread_canonical_key: key(parent_key),
        child_thread_canonical_key: key(child_key),
        parent_owner_scope: "user:1".to_string(),
        parent_source: Some("codex".to_string()),
        parent_identity_scope: Some(String::new()),
        parent_native_id: Some(format!("parent-{parent_key}")),
        child_owner_scope: "user:1".to_string(),
        child_source: Some("codex".to_string()),
        child_identity_scope: Some(String::new()),
        child_native_id: Some(format!("child-{child_key}")),
        relation_type: values::relation_type::DELEGATED.to_string(),
        state: values::relation_state::ACTIVE.to_string(),
        selection_basis: values::selection_basis::SOURCE_EXACT.to_string(),
        source_confidence: Some(values::source_confidence::EXACT.to_string()),
        selected_observation_id: Some(900_000 + parent_key as i64),
        selected_operator_decision_id: None,
        created_at: T0,
        updated_at: T0,
    }
}

pub fn new_observation(n: u64) -> NewThreadObservation {
    NewThreadObservation {
        subject_source: "codex".to_string(),
        subject_identity_scope_known: true,
        subject_identity_scope_value: String::new(),
        subject_owner_scope: "user:1".to_string(),
        subject_native_id: format!("subject-{n}"),
        candidate_parent_present: true,
        candidate_parent_source: "codex".to_string(),
        candidate_parent_identity_scope_known: true,
        candidate_parent_identity_scope_value: String::new(),
        candidate_parent_owner_scope: "user:1".to_string(),
        candidate_parent_native_id: format!("parent-{n}"),
        relation_kind: Some(values::relation_type::DELEGATED.to_string()),
        origin: values::observation_origin::ADAPTER.to_string(),
        evidence_kind: values::evidence_kind::SOURCE_FIELD.to_string(),
        polarity: values::polarity::SUPPORTS.to_string(),
        source_confidence: Some(values::source_confidence::EXACT.to_string()),
        evidence_fingerprint: key(n),
        source_record_ref: Some("rollout.jsonl#line=1".to_string()),
        state: values::observation_state::PENDING.to_string(),
        import_run_id: Some("run-1".to_string()),
        observed_at: T0,
        created_at: T0,
        updated_at: T0,
    }
}

/// Borrowed UNIQUE-key view over an observation parameter struct.
pub fn observation_identity(o: &NewThreadObservation) -> ObservationIdentity<'_> {
    ObservationIdentity {
        subject_source: &o.subject_source,
        subject_identity_scope_known: o.subject_identity_scope_known,
        subject_identity_scope_value: &o.subject_identity_scope_value,
        subject_owner_scope: &o.subject_owner_scope,
        subject_native_id: &o.subject_native_id,
        candidate_parent_present: o.candidate_parent_present,
        candidate_parent_source: &o.candidate_parent_source,
        candidate_parent_identity_scope_known: o.candidate_parent_identity_scope_known,
        candidate_parent_identity_scope_value: &o.candidate_parent_identity_scope_value,
        candidate_parent_owner_scope: &o.candidate_parent_owner_scope,
        candidate_parent_native_id: &o.candidate_parent_native_id,
        evidence_kind: &o.evidence_kind,
        evidence_fingerprint: &o.evidence_fingerprint,
    }
}

pub fn new_candidate(n: u64) -> NewThreadGroupCandidateAssociation {
    NewThreadGroupCandidateAssociation {
        subject_thread_id: None,
        subject_source: "codex".to_string(),
        subject_identity_scope_known: true,
        subject_identity_scope_value: String::new(),
        subject_owner_scope: "user:1".to_string(),
        subject_native_id: format!("subject-{n}"),
        candidate_group_id: None,
        candidate_parent_thread_id: None,
        state: values::candidate_state::PENDING.to_string(),
        selected_observation_id: None,
        created_at: T0,
        updated_at: T0,
    }
}

pub fn identity(n: u64) -> SourceIdentityKey<'static> {
    // Leaked static strings keep the borrowed key trivially constructible
    // in tests without lifetime plumbing.
    SourceIdentityKey {
        owner_scope: Box::leak(format!("user:{}", n % 3).into_boxed_str()),
        source: Box::leak("codex".to_string().into_boxed_str()),
        identity_scope: Box::leak(String::new().into_boxed_str()),
        native_id: Box::leak(format!("native-{n}").into_boxed_str()),
    }
}

pub fn new_deletion_marker(n: u64) -> NewThreadDeletionMarker<'static> {
    NewThreadDeletionMarker {
        identity: identity(n),
        forbid_reimport: true,
        recursive: false,
        actor_id: "operator-a".to_string(),
        reason: Some("privacy".to_string()),
        deleted_at: T0,
    }
}

pub fn new_operator_decision(association_id: i64) -> NewOperatorDecision {
    NewOperatorDecision {
        owner_scope: "user:1".to_string(),
        candidate_association_id: association_id,
        actor_id: "operator-a".to_string(),
        decision: values::operator_decision::CONFIRM.to_string(),
        reason: "confirmed lineage".to_string(),
        input_evidence_fingerprint: key(4242),
        policy_version: "thread-group-policy-v1".to_string(),
        created_at: T0,
    }
}

pub fn new_collection(n: u64) -> NewManualCollection {
    NewManualCollection {
        owner_scope: "user:1".to_string(),
        title: format!("collection-{n}"),
        created_at: T0,
        updated_at: T0,
    }
}

pub fn new_event(n: u64, event_id: String) -> NewThreadGroupEvent {
    NewThreadGroupEvent {
        event_id,
        event_type: values::event_type::OBSERVATION_RECORDED.to_string(),
        operation_id: format!("op-{n}"),
        policy_version: "thread-group-policy-v1".to_string(),
        source: Some("codex".to_string()),
        identity_scope: Some(String::new()),
        owner_scope: Some("user:1".to_string()),
        native_id_ref: Some(format!("native-{n}")),
        group_id: None,
        thread_id: Some(n as i64 * 100),
        source_confidence: Some(values::source_confidence::EXACT.to_string()),
        selection_basis: None,
        operator_decision_id: None,
        polarity: Some(values::polarity::SUPPORTS.to_string()),
        payload: format!("{{\"seq\":{n}}}"),
        created_at: T0 + n as i64,
    }
}

pub fn new_audit_merge(source: i64, target: i64) -> NewGroupAuditMerge {
    NewGroupAuditMerge {
        source_group_id: source,
        target_group_id: target,
        actor_id: "operator-a".to_string(),
        reason: "duplicate roots".to_string(),
        created_at: T0,
    }
}

pub fn new_audit_split(source: i64) -> NewGroupAuditSplit {
    NewGroupAuditSplit {
        source_group_id: source,
        actor_id: "operator-a".to_string(),
        reason: "cross-project mix".to_string(),
        canonical_partition: "[[\"k1\"],[\"k2\"]]".to_string(),
        successor_group_ids: "[11,12]".to_string(),
        created_at: T0,
    }
}
