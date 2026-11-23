-- ThreadGroup additive schema for PostgreSQL (docs/thread-groups-spec_ja.md,
-- docs/thread-groups-design_ja.md section 5 / 8.3).
-- Forward-only and additive: existing thread / memory objects are untouched.
-- No FK cascades, triggers, procedures, or DO blocks: deletion order, cycle
-- checks, identity resolution, and grouping policy stay in the app layer.

CREATE TABLE thread_group (
    id BIGINT NOT NULL PRIMARY KEY,
    group_canonical_key CHAR(64) NOT NULL,
    title TEXT,
    status TEXT NOT NULL CHECK (status IN ('active', 'redirected', 'split')),
    grouping_authority TEXT NOT NULL CHECK (grouping_authority IN ('reconciler', 'operator')),
    redirect_to_group_id BIGINT,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    -- A redirect target exists exactly while the group is redirected, and a
    -- group never redirects to itself (chains are flattened in the app layer).
    CHECK ((status = 'redirected' AND redirect_to_group_id IS NOT NULL)
        OR (status <> 'redirected' AND redirect_to_group_id IS NULL)),
    CHECK (redirect_to_group_id <> id)
);
-- group_canonical_key is unique only among active groups; redirected / split
-- history may keep the same key.
CREATE UNIQUE INDEX thread_group_active_canonical_key
    ON thread_group (group_canonical_key) WHERE status = 'active';
CREATE INDEX thread_group_status ON thread_group (status);
CREATE INDEX thread_group_redirect_to_group_id ON thread_group (redirect_to_group_id);

CREATE TABLE thread_group_member (
    group_id BIGINT NOT NULL,
    thread_id BIGINT,
    thread_canonical_key CHAR(64) NOT NULL,
    owner_scope TEXT NOT NULL,
    source TEXT,
    identity_scope TEXT,
    native_id TEXT,
    role TEXT NOT NULL CHECK (role IN ('root', 'member')),
    state TEXT NOT NULL CHECK (state IN ('active', 'deleted', 'redirected')),
    provenance TEXT NOT NULL CHECK (provenance IN ('reconciler', 'operator')),
    deleted_at BIGINT,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    -- state='deleted' is a placeholder member with no thread row; 'active'
    -- always references a live thread row; 'redirected' is history and may
    -- keep or lose either marker (spec 3.2 / 4.5 / 4.6).
    CHECK ((state = 'deleted' AND thread_id IS NULL AND deleted_at IS NOT NULL)
        OR (state = 'active' AND thread_id IS NOT NULL)
        OR state = 'redirected'),
    -- Source identity is an all-or-nothing triple for non-source members;
    -- owner_scope is kept for every member.
    CHECK ((source IS NULL AND identity_scope IS NULL AND native_id IS NULL)
        OR (source IS NOT NULL AND identity_scope IS NOT NULL AND native_id IS NOT NULL))
);
-- Current membership of one canonical key is at most one row ('active' or
-- 'deleted'); 'redirected' rows are past memberships.
CREATE UNIQUE INDEX thread_group_member_current_canonical_key
    ON thread_group_member (thread_canonical_key) WHERE state IN ('active', 'deleted');
CREATE INDEX thread_group_member_group_id_state ON thread_group_member (group_id, state);
CREATE INDEX thread_group_member_thread_id ON thread_group_member (thread_id);
CREATE INDEX thread_group_member_owner_scope ON thread_group_member (owner_scope);

CREATE TABLE thread_relation (
    id BIGINT NOT NULL PRIMARY KEY,
    parent_thread_id BIGINT,
    child_thread_id BIGINT,
    parent_thread_canonical_key CHAR(64) NOT NULL,
    child_thread_canonical_key CHAR(64) NOT NULL,
    parent_owner_scope TEXT NOT NULL,
    parent_source TEXT,
    parent_identity_scope TEXT,
    parent_native_id TEXT,
    child_owner_scope TEXT NOT NULL,
    child_source TEXT,
    child_identity_scope TEXT,
    child_native_id TEXT,
    relation_type TEXT NOT NULL CHECK (relation_type IN ('delegated', 'fork', 'continuation')),
    state TEXT NOT NULL CHECK (state IN ('active', 'retracted', 'superseded')),
    selection_basis TEXT NOT NULL CHECK (selection_basis IN ('source_exact', 'source_strong', 'operator_confirmation')),
    source_confidence TEXT,
    selected_observation_id BIGINT,
    selected_operator_decision_id BIGINT,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    -- self relations must not exist; endpoint thread_id may go NULL after
    -- deletion while canonical keys stay non-null.
    CHECK (parent_thread_canonical_key <> child_thread_canonical_key),
    -- source confidence belongs to source evidence only.
    CHECK (source_confidence IS NULL OR source_confidence IN ('exact', 'strong')),
    CHECK ((selection_basis = 'operator_confirmation' AND source_confidence IS NULL)
        OR (selection_basis <> 'operator_confirmation' AND source_confidence IS NOT NULL)),
    -- The selected evidence reference follows the selection basis.
    CHECK ((selection_basis = 'operator_confirmation'
            AND selected_operator_decision_id IS NOT NULL
            AND selected_observation_id IS NULL)
        OR (selection_basis <> 'operator_confirmation'
            AND selected_observation_id IS NOT NULL
            AND selected_operator_decision_id IS NULL)),
    -- Endpoint source identity is an all-or-nothing triple per side.
    CHECK ((parent_source IS NULL AND parent_identity_scope IS NULL AND parent_native_id IS NULL)
        OR (parent_source IS NOT NULL AND parent_identity_scope IS NOT NULL AND parent_native_id IS NOT NULL)),
    CHECK ((child_source IS NULL AND child_identity_scope IS NULL AND child_native_id IS NULL)
        OR (child_source IS NOT NULL AND child_identity_scope IS NOT NULL AND child_native_id IS NOT NULL))
);
-- At most one active canonical parent per child thread.
CREATE UNIQUE INDEX thread_relation_active_child_canonical_key
    ON thread_relation (child_thread_canonical_key) WHERE state = 'active';
CREATE INDEX thread_relation_parent_canonical_key_state ON thread_relation (parent_thread_canonical_key, state);
CREATE INDEX thread_relation_child_canonical_key_state ON thread_relation (child_thread_canonical_key, state);
CREATE INDEX thread_relation_parent_thread_id ON thread_relation (parent_thread_id);
CREATE INDEX thread_relation_child_thread_id ON thread_relation (child_thread_id);
CREATE INDEX thread_relation_selected_observation_id ON thread_relation (selected_observation_id);
CREATE INDEX thread_relation_selected_operator_decision_id ON thread_relation (selected_operator_decision_id);

CREATE TABLE thread_observation (
    id BIGINT NOT NULL PRIMARY KEY,
    subject_source TEXT NOT NULL,
    subject_identity_scope_known BOOLEAN NOT NULL,
    subject_identity_scope_value TEXT NOT NULL DEFAULT '',
    subject_owner_scope TEXT NOT NULL,
    subject_native_id TEXT NOT NULL,
    candidate_parent_present BOOLEAN NOT NULL,
    candidate_parent_source TEXT NOT NULL DEFAULT '',
    candidate_parent_identity_scope_known BOOLEAN NOT NULL DEFAULT FALSE,
    candidate_parent_identity_scope_value TEXT NOT NULL DEFAULT '',
    candidate_parent_owner_scope TEXT NOT NULL DEFAULT '',
    candidate_parent_native_id TEXT NOT NULL DEFAULT '',
    relation_kind TEXT,
    origin TEXT NOT NULL CHECK (origin IN ('adapter', 'reconciler')),
    evidence_kind TEXT NOT NULL CHECK (evidence_kind IN ('source_event', 'source_field', 'negative_or_conflict', 'derived_rejection', 'derived_conflict')),
    polarity TEXT NOT NULL CHECK (polarity IN ('supports', 'negates', 'conflicts')),
    source_confidence TEXT,
    evidence_fingerprint CHAR(64) NOT NULL,
    source_record_ref TEXT,
    state TEXT NOT NULL CHECK (state IN ('pending', 'candidate', 'selected', 'conflict', 'superseded', 'unsupported')),
    import_run_id TEXT,
    observed_at BIGINT NOT NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    -- Adapter rows carry source evidence with confidence; reconciler rows
    -- carry derived states without source confidence.
    CHECK (source_confidence IS NULL OR source_confidence IN ('exact', 'strong', 'heuristic', 'unsupported')),
    CHECK ((origin = 'adapter'
            AND evidence_kind IN ('source_event', 'source_field', 'negative_or_conflict')
            AND source_confidence IN ('exact', 'strong', 'heuristic', 'unsupported'))
        OR (origin = 'reconciler'
            AND evidence_kind IN ('derived_rejection', 'derived_conflict')
            AND source_confidence IS NULL)),
    CHECK (relation_kind IS NULL OR relation_kind IN ('delegated', 'fork', 'continuation'))
);
-- Idempotent evidence: uniqueness over the full subject / candidate-parent
-- identity, identity_scope known state, parent presence, evidence kind and
-- fingerprint; mutable state is excluded from the key.
CREATE UNIQUE INDEX thread_observation_identity_evidence_key ON thread_observation (
    subject_source, subject_identity_scope_known, subject_identity_scope_value,
    subject_owner_scope, subject_native_id, candidate_parent_present,
    candidate_parent_source, candidate_parent_identity_scope_known,
    candidate_parent_identity_scope_value, candidate_parent_owner_scope,
    candidate_parent_native_id, evidence_kind, evidence_fingerprint
);
CREATE INDEX thread_observation_state ON thread_observation (state);

CREATE TABLE thread_group_candidate_association (
    id BIGINT NOT NULL PRIMARY KEY,
    subject_thread_id BIGINT,
    subject_source TEXT NOT NULL,
    subject_identity_scope_known BOOLEAN NOT NULL,
    subject_identity_scope_value TEXT NOT NULL DEFAULT '',
    subject_owner_scope TEXT NOT NULL,
    subject_native_id TEXT NOT NULL,
    candidate_group_id BIGINT,
    candidate_parent_thread_id BIGINT,
    state TEXT NOT NULL CHECK (state IN ('pending', 'candidate', 'ambiguous', 'conflict', 'unsupported', 'superseded')),
    selected_observation_id BIGINT,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL
);
CREATE INDEX thread_group_candidate_association_subject_identity
    ON thread_group_candidate_association (subject_owner_scope, subject_source, subject_identity_scope_value, subject_native_id);
CREATE INDEX thread_group_candidate_association_subject_thread_id ON thread_group_candidate_association (subject_thread_id);
CREATE INDEX thread_group_candidate_association_candidate_group_id ON thread_group_candidate_association (candidate_group_id);
CREATE INDEX thread_group_candidate_association_parent_thread_id ON thread_group_candidate_association (candidate_parent_thread_id);
CREATE INDEX thread_group_candidate_association_observation_id ON thread_group_candidate_association (selected_observation_id);
CREATE INDEX thread_group_candidate_association_state ON thread_group_candidate_association (state);

-- Resolved owner-local mapping only: pending / ambiguous / unresolved
-- identities stay in observations and candidate associations, never here.
CREATE TABLE source_thread_identity (
    owner_scope TEXT NOT NULL,
    source TEXT NOT NULL,
    identity_scope TEXT NOT NULL,
    native_id TEXT NOT NULL,
    thread_id BIGINT NOT NULL,
    resolution_state TEXT NOT NULL CHECK (resolution_state = 'resolved'),
    first_seen_at BIGINT NOT NULL,
    last_seen_at BIGINT NOT NULL,
    PRIMARY KEY (owner_scope, source, identity_scope, native_id)
);
CREATE INDEX source_thread_identity_thread_id ON source_thread_identity (thread_id);

CREATE TABLE thread_canonical_key (
    thread_id BIGINT NOT NULL PRIMARY KEY,
    owner_scope TEXT NOT NULL,
    key CHAR(64) NOT NULL,
    origin TEXT NOT NULL CHECK (origin IN ('source_identity', 'creation_uuid', 'backfill_mapping')),
    assigned_at BIGINT NOT NULL
);
CREATE UNIQUE INDEX thread_canonical_key_key ON thread_canonical_key (key);

CREATE TABLE thread_deletion_marker (
    owner_scope TEXT NOT NULL,
    source TEXT NOT NULL,
    identity_scope TEXT NOT NULL,
    native_id TEXT NOT NULL,
    forbid_reimport BOOLEAN NOT NULL,
    recursive BOOLEAN NOT NULL,
    actor_id TEXT NOT NULL,
    reason TEXT,
    deleted_at BIGINT NOT NULL,
    PRIMARY KEY (owner_scope, source, identity_scope, native_id)
);

CREATE TABLE operator_decision (
    id BIGINT NOT NULL PRIMARY KEY,
    owner_scope TEXT NOT NULL,
    candidate_association_id BIGINT NOT NULL,
    actor_id TEXT NOT NULL,
    decision TEXT NOT NULL CHECK (decision IN ('confirm', 'reject', 'retract')),
    reason TEXT NOT NULL,
    input_evidence_fingerprint CHAR(64) NOT NULL,
    policy_version TEXT NOT NULL,
    created_at BIGINT NOT NULL
);
CREATE INDEX operator_decision_candidate_association_id ON operator_decision (candidate_association_id);

CREATE TABLE manual_collection (
    id BIGINT NOT NULL PRIMARY KEY,
    owner_scope TEXT NOT NULL,
    title TEXT NOT NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL
);
CREATE INDEX manual_collection_owner_updated ON manual_collection (owner_scope, updated_at DESC, id);

CREATE TABLE manual_collection_member (
    collection_id BIGINT NOT NULL,
    thread_id BIGINT NOT NULL,
    owner_scope TEXT NOT NULL,
    PRIMARY KEY (collection_id, thread_id)
);
CREATE INDEX manual_collection_member_thread_id ON manual_collection_member (thread_id);

-- Transactional ThreadGroup domain event outbox: rows are immutable and are
-- written with the state transition they describe; event_id doubles as the
-- jobworkerp uniq_key for at-least-once delivery.
CREATE TABLE thread_group_event_outbox (
    event_id TEXT NOT NULL PRIMARY KEY,
    event_type TEXT NOT NULL CHECK (event_type IN (
        'thread_group_observation_recorded',
        'thread_group_relation_selected',
        'thread_group_reconciliation_completed',
        'thread_group_conflict_detected',
        'thread_group_redirected'
    )),
    operation_id TEXT NOT NULL,
    policy_version TEXT NOT NULL,
    source TEXT,
    identity_scope TEXT,
    owner_scope TEXT,
    native_id_ref TEXT,
    group_id BIGINT,
    thread_id BIGINT,
    source_confidence TEXT CHECK (source_confidence IS NULL OR source_confidence IN ('exact', 'strong', 'heuristic', 'unsupported')),
    selection_basis TEXT CHECK (selection_basis IS NULL OR selection_basis IN ('source_exact', 'source_strong', 'operator_confirmation')),
    operator_decision_id BIGINT,
    polarity TEXT CHECK (polarity IS NULL OR polarity IN ('supports', 'negates', 'conflicts')),
    payload JSONB NOT NULL,
    created_at BIGINT NOT NULL
);
CREATE INDEX thread_group_event_outbox_operation_id ON thread_group_event_outbox (operation_id);
CREATE INDEX thread_group_event_outbox_event_type_created_at ON thread_group_event_outbox (event_type, created_at);
CREATE INDEX thread_group_event_outbox_group_id ON thread_group_event_outbox (group_id);
CREATE INDEX thread_group_event_outbox_thread_id ON thread_group_event_outbox (thread_id);

-- Group operation audit: merge rows record the redirect with actor, reason
-- and time; split rows record the canonicalized partition and successor
-- groups that make a split retry idempotent. Rows are append-only history.
CREATE TABLE thread_group_audit (
    id BIGINT NOT NULL PRIMARY KEY,
    audit_type TEXT NOT NULL CHECK (audit_type IN ('merge', 'split')),
    source_group_id BIGINT NOT NULL,
    target_group_id BIGINT,
    actor_id TEXT NOT NULL,
    reason TEXT NOT NULL,
    canonical_partition JSONB,
    successor_group_ids JSONB,
    created_at BIGINT NOT NULL,
    CHECK ((audit_type = 'merge' AND target_group_id IS NOT NULL
            AND canonical_partition IS NULL AND successor_group_ids IS NULL)
        OR (audit_type = 'split' AND target_group_id IS NULL
            AND canonical_partition IS NOT NULL AND successor_group_ids IS NOT NULL))
);
-- A group is consumed once by each operation kind: redirected groups are
-- never reactivated and split consumes the active source group exactly once.
CREATE UNIQUE INDEX thread_group_audit_merge_source_group
    ON thread_group_audit (source_group_id) WHERE audit_type = 'merge';
CREATE UNIQUE INDEX thread_group_audit_split_source_group
    ON thread_group_audit (source_group_id) WHERE audit_type = 'split';
CREATE INDEX thread_group_audit_target_group_id ON thread_group_audit (target_group_id);

-- The schema contract row must name the tip of the applied migration prefix,
-- so this transactional migration updates it together with the schema change.
UPDATE memories_schema_contract SET version = '20260920000001' WHERE contract_key = 'rdb_schema';
