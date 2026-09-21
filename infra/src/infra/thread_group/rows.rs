//! Row / parameter types for the ThreadGroup tables.
//!
//! Every column of the committed schema is mirrored verbatim (`CHAR(64)`
//! hex keys are plain `String`s; the CHECK constraints in the DDL are the
//! authority on allowed token values, mirrored here as `values` tokens
//! for callers — the repository itself never validates them, matching
//! the "business rules stay in the app layer" contract).
//!
//! Backend notes:
//! - `*_identity_scope_known` / `candidate_parent_present` are separate
//!   boolean columns, and the value columns are NOT NULL with `''`
//!   defaults. `known("")` vs `unknown`, and present vs no-parent, must
//!   never be encoded via NULL / empty-string alone (design 5.4); the
//!   repository stores exactly what the caller supplies.
//! - JSON columns (`payload`, `canonical_partition`,
//!   `successor_group_ids`) are transported as canonical JSON text;
//!   PostgreSQL writes bind through `p_jsonb!` and reads use the
//!   `col::text` column lists below (same convention as `memory` /
//!   `thread` metadata).

/// Storage value tokens for `thread_group.status`.
pub mod values {
    pub mod group_status {
        pub const ACTIVE: &str = "active";
        pub const REDIRECTED: &str = "redirected";
        pub const SPLIT: &str = "split";
    }

    /// Shared by `thread_group.grouping_authority` and
    /// `thread_group_member.provenance`.
    pub mod grouping_authority {
        pub const RECONCILER: &str = "reconciler";
        pub const OPERATOR: &str = "operator";
    }

    pub mod member_role {
        pub const ROOT: &str = "root";
        pub const MEMBER: &str = "member";
    }

    pub mod member_state {
        pub const ACTIVE: &str = "active";
        pub const DELETED: &str = "deleted";
        pub const REDIRECTED: &str = "redirected";
    }

    pub mod relation_type {
        pub const DELEGATED: &str = "delegated";
        pub const FORK: &str = "fork";
        pub const CONTINUATION: &str = "continuation";
    }

    pub mod relation_state {
        pub const ACTIVE: &str = "active";
        pub const RETRACTED: &str = "retracted";
        pub const SUPERSEDED: &str = "superseded";
    }

    pub mod selection_basis {
        pub const SOURCE_EXACT: &str = "source_exact";
        pub const SOURCE_STRONG: &str = "source_strong";
        pub const OPERATOR_CONFIRMATION: &str = "operator_confirmation";
    }

    /// `source_confidence` on relations allows only exact / strong; the
    /// observation column additionally allows heuristic / unsupported.
    pub mod source_confidence {
        pub const EXACT: &str = "exact";
        pub const STRONG: &str = "strong";
        pub const HEURISTIC: &str = "heuristic";
        pub const UNSUPPORTED: &str = "unsupported";
    }

    pub mod observation_origin {
        pub const ADAPTER: &str = "adapter";
        pub const RECONCILER: &str = "reconciler";
    }

    pub mod evidence_kind {
        pub const SOURCE_EVENT: &str = "source_event";
        pub const SOURCE_FIELD: &str = "source_field";
        pub const NEGATIVE_OR_CONFLICT: &str = "negative_or_conflict";
        pub const DERIVED_REJECTION: &str = "derived_rejection";
        pub const DERIVED_CONFLICT: &str = "derived_conflict";
    }

    pub mod polarity {
        pub const SUPPORTS: &str = "supports";
        pub const NEGATES: &str = "negates";
        pub const CONFLICTS: &str = "conflicts";
    }

    pub mod observation_state {
        pub const PENDING: &str = "pending";
        pub const CANDIDATE: &str = "candidate";
        pub const SELECTED: &str = "selected";
        pub const CONFLICT: &str = "conflict";
        pub const SUPERSEDED: &str = "superseded";
        pub const UNSUPPORTED: &str = "unsupported";
    }

    pub mod candidate_state {
        pub const PENDING: &str = "pending";
        pub const CANDIDATE: &str = "candidate";
        pub const AMBIGUOUS: &str = "ambiguous";
        pub const CONFLICT: &str = "conflict";
        pub const UNSUPPORTED: &str = "unsupported";
        pub const SUPERSEDED: &str = "superseded";
    }

    pub mod canonical_key_origin {
        pub const SOURCE_IDENTITY: &str = "source_identity";
        pub const CREATION_UUID: &str = "creation_uuid";
        pub const BACKFILL_MAPPING: &str = "backfill_mapping";
    }

    pub mod operator_decision {
        pub const CONFIRM: &str = "confirm";
        pub const REJECT: &str = "reject";
        pub const RETRACT: &str = "retract";
    }

    /// Only `resolved` rows exist in `source_thread_identity` (design
    /// 5.6); pending / ambiguous stay in observations and candidate
    /// associations.
    pub mod resolution_state {
        pub const RESOLVED: &str = "resolved";
    }

    pub mod audit_type {
        pub const MERGE: &str = "merge";
        pub const SPLIT: &str = "split";
    }

    pub mod event_type {
        pub const OBSERVATION_RECORDED: &str = "thread_group_observation_recorded";
        pub const RELATION_SELECTED: &str = "thread_group_relation_selected";
        pub const RECONCILIATION_COMPLETED: &str = "thread_group_reconciliation_completed";
        pub const CONFLICT_DETECTED: &str = "thread_group_conflict_detected";
        pub const REDIRECTED: &str = "thread_group_redirected";
    }
}

/// Owner-local source identity tuple: the PRIMARY KEY of
/// `source_thread_identity` and `thread_deletion_marker`. Borrowed form
/// for lookup / delete / lock calls.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceIdentityKey<'a> {
    pub owner_scope: &'a str,
    pub source: &'a str,
    pub identity_scope: &'a str,
    pub native_id: &'a str,
}

#[derive(sqlx::FromRow, Debug, Clone)]
pub struct ThreadGroupRow {
    pub id: i64,
    pub group_canonical_key: String,
    pub title: Option<String>,
    pub status: String,
    pub grouping_authority: String,
    pub redirect_to_group_id: Option<i64>,
    pub created_at: i64,
    pub updated_at: i64,
}

/// Insert parameter for `thread_group`. `id` is generated by the
/// repository; timestamps pass through `fill_timestamps` (0 → server
/// now).
#[derive(Debug, Clone)]
pub struct NewThreadGroup {
    pub group_canonical_key: String,
    pub title: Option<String>,
    pub status: String,
    pub grouping_authority: String,
    pub redirect_to_group_id: Option<i64>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(sqlx::FromRow, Debug, Clone)]
pub struct ThreadGroupMemberRow {
    pub group_id: i64,
    pub thread_id: Option<i64>,
    pub thread_canonical_key: String,
    pub owner_scope: String,
    pub source: Option<String>,
    pub identity_scope: Option<String>,
    pub native_id: Option<String>,
    pub role: String,
    pub state: String,
    pub provenance: String,
    pub deleted_at: Option<i64>,
    pub created_at: i64,
    pub updated_at: i64,
}

/// Insert parameter for `thread_group_member`. The table has no id
/// column; current-membership uniqueness per `thread_canonical_key` is
/// enforced by the partial UNIQUE index, so re-inserting a current row
/// for the same key fails and callers must use the state transitions
/// instead.
#[derive(Debug, Clone)]
pub struct NewThreadGroupMember {
    pub group_id: i64,
    pub thread_id: Option<i64>,
    pub thread_canonical_key: String,
    pub owner_scope: String,
    pub source: Option<String>,
    pub identity_scope: Option<String>,
    pub native_id: Option<String>,
    pub role: String,
    pub state: String,
    pub provenance: String,
    pub deleted_at: Option<i64>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(sqlx::FromRow, Debug, Clone)]
pub struct ThreadRelationRow {
    pub id: i64,
    pub parent_thread_id: Option<i64>,
    pub child_thread_id: Option<i64>,
    pub parent_thread_canonical_key: String,
    pub child_thread_canonical_key: String,
    pub parent_owner_scope: String,
    pub parent_source: Option<String>,
    pub parent_identity_scope: Option<String>,
    pub parent_native_id: Option<String>,
    pub child_owner_scope: String,
    pub child_source: Option<String>,
    pub child_identity_scope: Option<String>,
    pub child_native_id: Option<String>,
    pub relation_type: String,
    pub state: String,
    pub selection_basis: String,
    pub source_confidence: Option<String>,
    pub selected_observation_id: Option<i64>,
    pub selected_operator_decision_id: Option<i64>,
    pub created_at: i64,
    pub updated_at: i64,
}

/// Insert parameter for `thread_relation`. Endpoint thread ids may be
/// NULL (deleted node); canonical keys are mandatory and the DDL CHECKs
/// reject self edges, basis/confidence mismatches, and evidence-reference
/// shape errors.
#[derive(Debug, Clone)]
pub struct NewThreadRelation {
    pub parent_thread_id: Option<i64>,
    pub child_thread_id: Option<i64>,
    pub parent_thread_canonical_key: String,
    pub child_thread_canonical_key: String,
    pub parent_owner_scope: String,
    pub parent_source: Option<String>,
    pub parent_identity_scope: Option<String>,
    pub parent_native_id: Option<String>,
    pub child_owner_scope: String,
    pub child_source: Option<String>,
    pub child_identity_scope: Option<String>,
    pub child_native_id: Option<String>,
    pub relation_type: String,
    pub state: String,
    pub selection_basis: String,
    pub source_confidence: Option<String>,
    pub selected_observation_id: Option<i64>,
    pub selected_operator_decision_id: Option<i64>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(sqlx::FromRow, Debug, Clone)]
pub struct ThreadObservationRow {
    pub id: i64,
    pub subject_source: String,
    pub subject_identity_scope_known: bool,
    pub subject_identity_scope_value: String,
    pub subject_owner_scope: String,
    pub subject_native_id: String,
    pub candidate_parent_present: bool,
    pub candidate_parent_source: String,
    pub candidate_parent_identity_scope_known: bool,
    pub candidate_parent_identity_scope_value: String,
    pub candidate_parent_owner_scope: String,
    pub candidate_parent_native_id: String,
    pub relation_kind: Option<String>,
    pub origin: String,
    pub evidence_kind: String,
    pub polarity: String,
    pub source_confidence: Option<String>,
    pub evidence_fingerprint: String,
    pub source_record_ref: Option<String>,
    pub state: String,
    pub import_run_id: Option<String>,
    pub observed_at: i64,
    pub created_at: i64,
    pub updated_at: i64,
}

/// Insert parameter for `thread_observation`. The `''`-sentinel columns
/// (`candidate_parent_*` when absent, identity-scope values when the
/// known flag is false) are filled by the caller per design 5.4 — the
/// repository never substitutes values.
#[derive(Debug, Clone)]
pub struct NewThreadObservation {
    pub subject_source: String,
    pub subject_identity_scope_known: bool,
    pub subject_identity_scope_value: String,
    pub subject_owner_scope: String,
    pub subject_native_id: String,
    pub candidate_parent_present: bool,
    pub candidate_parent_source: String,
    pub candidate_parent_identity_scope_known: bool,
    pub candidate_parent_identity_scope_value: String,
    pub candidate_parent_owner_scope: String,
    pub candidate_parent_native_id: String,
    pub relation_kind: Option<String>,
    pub origin: String,
    pub evidence_kind: String,
    pub polarity: String,
    pub source_confidence: Option<String>,
    pub evidence_fingerprint: String,
    pub source_record_ref: Option<String>,
    pub state: String,
    pub import_run_id: Option<String>,
    pub observed_at: i64,
    pub created_at: i64,
    pub updated_at: i64,
}

/// Borrowed view over the full UNIQUE key of `thread_observation`
/// (identity + presence states + evidence kind + fingerprint; mutable
/// `state` is deliberately excluded). Used by the idempotent lookup
/// helpers.
#[derive(Debug, Clone, Copy)]
pub struct ObservationIdentity<'a> {
    pub subject_source: &'a str,
    pub subject_identity_scope_known: bool,
    pub subject_identity_scope_value: &'a str,
    pub subject_owner_scope: &'a str,
    pub subject_native_id: &'a str,
    pub candidate_parent_present: bool,
    pub candidate_parent_source: &'a str,
    pub candidate_parent_identity_scope_known: bool,
    pub candidate_parent_identity_scope_value: &'a str,
    pub candidate_parent_owner_scope: &'a str,
    pub candidate_parent_native_id: &'a str,
    pub evidence_kind: &'a str,
    pub evidence_fingerprint: &'a str,
}

#[derive(sqlx::FromRow, Debug, Clone)]
pub struct ThreadGroupCandidateAssociationRow {
    pub id: i64,
    pub subject_thread_id: Option<i64>,
    pub subject_source: String,
    pub subject_identity_scope_known: bool,
    pub subject_identity_scope_value: String,
    pub subject_owner_scope: String,
    pub subject_native_id: String,
    pub candidate_group_id: Option<i64>,
    pub candidate_parent_thread_id: Option<i64>,
    pub state: String,
    pub selected_observation_id: Option<i64>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone)]
pub struct NewThreadGroupCandidateAssociation {
    pub subject_thread_id: Option<i64>,
    pub subject_source: String,
    pub subject_identity_scope_known: bool,
    pub subject_identity_scope_value: String,
    pub subject_owner_scope: String,
    pub subject_native_id: String,
    pub candidate_group_id: Option<i64>,
    pub candidate_parent_thread_id: Option<i64>,
    pub state: String,
    pub selected_observation_id: Option<i64>,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(sqlx::FromRow, Debug, Clone)]
pub struct SourceThreadIdentityRow {
    pub owner_scope: String,
    pub source: String,
    pub identity_scope: String,
    pub native_id: String,
    pub thread_id: i64,
    pub resolution_state: String,
    pub first_seen_at: i64,
    pub last_seen_at: i64,
}

#[derive(sqlx::FromRow, Debug, Clone)]
pub struct ThreadCanonicalKeyRow {
    pub thread_id: i64,
    pub owner_scope: String,
    pub key: String,
    pub origin: String,
    pub assigned_at: i64,
}

#[derive(sqlx::FromRow, Debug, Clone)]
pub struct ThreadDeletionMarkerRow {
    pub owner_scope: String,
    pub source: String,
    pub identity_scope: String,
    pub native_id: String,
    pub forbid_reimport: bool,
    pub recursive: bool,
    pub actor_id: String,
    pub reason: Option<String>,
    pub deleted_at: i64,
}

#[derive(Debug, Clone)]
pub struct NewThreadDeletionMarker<'a> {
    pub identity: SourceIdentityKey<'a>,
    pub forbid_reimport: bool,
    pub recursive: bool,
    pub actor_id: String,
    pub reason: Option<String>,
    pub deleted_at: i64,
}

#[derive(sqlx::FromRow, Debug, Clone)]
pub struct OperatorDecisionRow {
    pub id: i64,
    pub owner_scope: String,
    pub candidate_association_id: i64,
    pub actor_id: String,
    pub decision: String,
    pub reason: String,
    pub input_evidence_fingerprint: String,
    pub policy_version: String,
    pub created_at: i64,
}

#[derive(Debug, Clone)]
pub struct NewOperatorDecision {
    pub owner_scope: String,
    pub candidate_association_id: i64,
    pub actor_id: String,
    pub decision: String,
    pub reason: String,
    pub input_evidence_fingerprint: String,
    pub policy_version: String,
    pub created_at: i64,
}

#[derive(sqlx::FromRow, Debug, Clone)]
pub struct ManualCollectionRow {
    pub id: i64,
    pub owner_scope: String,
    pub title: String,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone)]
pub struct NewManualCollection {
    pub owner_scope: String,
    pub title: String,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(sqlx::FromRow, Debug, Clone)]
pub struct ManualCollectionMemberRow {
    pub collection_id: i64,
    pub thread_id: i64,
    pub owner_scope: String,
}

#[derive(sqlx::FromRow, Debug, Clone)]
pub struct ThreadGroupEventOutboxRow {
    pub event_id: String,
    pub event_type: String,
    pub operation_id: String,
    pub policy_version: String,
    pub source: Option<String>,
    pub identity_scope: Option<String>,
    pub owner_scope: Option<String>,
    pub native_id_ref: Option<String>,
    pub group_id: Option<i64>,
    pub thread_id: Option<i64>,
    pub source_confidence: Option<String>,
    pub selection_basis: Option<String>,
    pub operator_decision_id: Option<i64>,
    pub polarity: Option<String>,
    /// Canonical JSON text (`payload::text` on PostgreSQL).
    pub payload: String,
    pub created_at: i64,
}

/// Insert parameter for `thread_group_event_outbox`. `event_id` is
/// supplied by the app layer (it doubles as the jobworkerp `uniq_key`
/// for at-least-once delivery, so retries must reuse it).
#[derive(Debug, Clone)]
pub struct NewThreadGroupEvent {
    pub event_id: String,
    pub event_type: String,
    pub operation_id: String,
    pub policy_version: String,
    pub source: Option<String>,
    pub identity_scope: Option<String>,
    pub owner_scope: Option<String>,
    pub native_id_ref: Option<String>,
    pub group_id: Option<i64>,
    pub thread_id: Option<i64>,
    pub source_confidence: Option<String>,
    pub selection_basis: Option<String>,
    pub operator_decision_id: Option<i64>,
    pub polarity: Option<String>,
    /// Canonical JSON text. SQLite infers NUMERIC affinity for the
    /// `JSON` type name, so a scalar-number payload would round-trip as
    /// an integer; event payloads are contractually JSON objects
    /// (design 8.3), which stay TEXT.
    pub payload: String,
    pub created_at: i64,
}

#[derive(sqlx::FromRow, Debug, Clone)]
pub struct ThreadGroupAuditRow {
    pub id: i64,
    pub audit_type: String,
    pub source_group_id: i64,
    pub target_group_id: Option<i64>,
    pub actor_id: String,
    pub reason: String,
    /// Canonical JSON text; merge rows keep it NULL (DDL CHECK).
    pub canonical_partition: Option<String>,
    /// Canonical JSON text; merge rows keep it NULL (DDL CHECK).
    pub successor_group_ids: Option<String>,
    pub created_at: i64,
}

#[derive(Debug, Clone)]
pub struct NewGroupAuditMerge {
    pub source_group_id: i64,
    pub target_group_id: i64,
    pub actor_id: String,
    pub reason: String,
    pub created_at: i64,
}

#[derive(Debug, Clone)]
pub struct NewGroupAuditSplit {
    pub source_group_id: i64,
    pub actor_id: String,
    pub reason: String,
    pub canonical_partition: String,
    pub successor_group_ids: String,
    pub created_at: i64,
}

// ---------------------------------------------------------------------------
// Backend-safe column select lists (JSON columns need `::text` on PG so
// sqlx can decode them into `String`, same convention as
// `thread_columns!` / `memory_columns!`).
// ---------------------------------------------------------------------------

// Each list becomes a function-like macro so the static SQL constants
// can splice it through `concat!` (a `pub const` cannot be used inside
// `concat!`); `pub(crate) use` then makes the macro importable by path
// exactly like `crate::sql::memory_columns`.
macro_rules! define_columns {
    ($name:ident, $pg:literal, $sqlite:literal) => {
        #[cfg(feature = "postgres")]
        macro_rules! $name {
            () => {
                $pg
            };
        }
        #[cfg(not(feature = "postgres"))]
        macro_rules! $name {
            () => {
                $sqlite
            };
        }
        pub(crate) use $name;
    };
}

define_columns!(
    THREAD_GROUP_COLUMNS,
    "id, group_canonical_key, title, status, grouping_authority, redirect_to_group_id, created_at, updated_at",
    "id, group_canonical_key, title, status, grouping_authority, redirect_to_group_id, created_at, updated_at"
);

define_columns!(
    THREAD_GROUP_MEMBER_COLUMNS,
    "group_id, thread_id, thread_canonical_key, owner_scope, source, identity_scope, native_id, role, state, provenance, deleted_at, created_at, updated_at",
    "group_id, thread_id, thread_canonical_key, owner_scope, source, identity_scope, native_id, role, state, provenance, deleted_at, created_at, updated_at"
);

define_columns!(
    THREAD_RELATION_COLUMNS,
    "id, parent_thread_id, child_thread_id, parent_thread_canonical_key, child_thread_canonical_key, \
     parent_owner_scope, parent_source, parent_identity_scope, parent_native_id, \
     child_owner_scope, child_source, child_identity_scope, child_native_id, \
     relation_type, state, selection_basis, source_confidence, \
     selected_observation_id, selected_operator_decision_id, created_at, updated_at",
    "id, parent_thread_id, child_thread_id, parent_thread_canonical_key, child_thread_canonical_key, \
     parent_owner_scope, parent_source, parent_identity_scope, parent_native_id, \
     child_owner_scope, child_source, child_identity_scope, child_native_id, \
     relation_type, state, selection_basis, source_confidence, \
     selected_observation_id, selected_operator_decision_id, created_at, updated_at"
);

define_columns!(
    THREAD_OBSERVATION_COLUMNS,
    "id, subject_source, subject_identity_scope_known, subject_identity_scope_value, \
     subject_owner_scope, subject_native_id, \
     candidate_parent_present, candidate_parent_source, \
     candidate_parent_identity_scope_known, candidate_parent_identity_scope_value, \
     candidate_parent_owner_scope, candidate_parent_native_id, \
     relation_kind, origin, evidence_kind, polarity, source_confidence, \
     evidence_fingerprint, source_record_ref, state, import_run_id, \
     observed_at, created_at, updated_at",
    "id, subject_source, subject_identity_scope_known, subject_identity_scope_value, \
     subject_owner_scope, subject_native_id, \
     candidate_parent_present, candidate_parent_source, \
     candidate_parent_identity_scope_known, candidate_parent_identity_scope_value, \
     candidate_parent_owner_scope, candidate_parent_native_id, \
     relation_kind, origin, evidence_kind, polarity, source_confidence, \
     evidence_fingerprint, source_record_ref, state, import_run_id, \
     observed_at, created_at, updated_at"
);

define_columns!(
    CANDIDATE_ASSOCIATION_COLUMNS,
    "id, subject_thread_id, subject_source, subject_identity_scope_known, subject_identity_scope_value, \
     subject_owner_scope, subject_native_id, candidate_group_id, candidate_parent_thread_id, \
     state, selected_observation_id, created_at, updated_at",
    "id, subject_thread_id, subject_source, subject_identity_scope_known, subject_identity_scope_value, \
     subject_owner_scope, subject_native_id, candidate_group_id, candidate_parent_thread_id, \
     state, selected_observation_id, created_at, updated_at"
);

define_columns!(
    SOURCE_THREAD_IDENTITY_COLUMNS,
    "owner_scope, source, identity_scope, native_id, thread_id, resolution_state, first_seen_at, last_seen_at",
    "owner_scope, source, identity_scope, native_id, thread_id, resolution_state, first_seen_at, last_seen_at"
);

define_columns!(
    THREAD_CANONICAL_KEY_COLUMNS,
    "thread_id, owner_scope, key, origin, assigned_at",
    "thread_id, owner_scope, key, origin, assigned_at"
);

define_columns!(
    THREAD_DELETION_MARKER_COLUMNS,
    "owner_scope, source, identity_scope, native_id, forbid_reimport, recursive, actor_id, reason, deleted_at",
    "owner_scope, source, identity_scope, native_id, forbid_reimport, recursive, actor_id, reason, deleted_at"
);

define_columns!(
    OPERATOR_DECISION_COLUMNS,
    "id, owner_scope, candidate_association_id, actor_id, decision, reason, input_evidence_fingerprint, policy_version, created_at",
    "id, owner_scope, candidate_association_id, actor_id, decision, reason, input_evidence_fingerprint, policy_version, created_at"
);

define_columns!(
    MANUAL_COLLECTION_COLUMNS,
    "id, owner_scope, title, created_at, updated_at",
    "id, owner_scope, title, created_at, updated_at"
);

define_columns!(
    MANUAL_COLLECTION_MEMBER_COLUMNS,
    "collection_id, thread_id, owner_scope",
    "collection_id, thread_id, owner_scope"
);

define_columns!(
    EVENT_OUTBOX_COLUMNS,
    "event_id, event_type, operation_id, policy_version, source, identity_scope, owner_scope, native_id_ref, \
     group_id, thread_id, source_confidence, selection_basis, operator_decision_id, polarity, \
     payload::text AS payload, created_at",
    "event_id, event_type, operation_id, policy_version, source, identity_scope, owner_scope, native_id_ref, \
     group_id, thread_id, source_confidence, selection_basis, operator_decision_id, polarity, \
     payload, created_at"
);

define_columns!(
    GROUP_AUDIT_COLUMNS,
    "id, audit_type, source_group_id, target_group_id, actor_id, reason, \
     canonical_partition::text AS canonical_partition, successor_group_ids::text AS successor_group_ids, created_at",
    "id, audit_type, source_group_id, target_group_id, actor_id, reason, \
     canonical_partition, successor_group_ids, created_at"
);
