-- Introduce typed owner ids for ThreadGroup data. Existing string columns are
-- retained as a compatibility ledger until every old writer is retired.
-- Group ownership is different: all deployed legacy groups are owned by user 1.

ALTER TABLE thread_group ADD COLUMN user_id BIGINT NOT NULL DEFAULT 1;
ALTER TABLE thread_group ALTER COLUMN user_id DROP DEFAULT;
CREATE INDEX thread_group_user_status_created
    ON thread_group (user_id, status, created_at DESC, id ASC);

ALTER TABLE thread_group_member ADD COLUMN user_id BIGINT;
CREATE INDEX thread_group_member_user_id ON thread_group_member (user_id);

ALTER TABLE thread_relation ADD COLUMN parent_user_id BIGINT;
ALTER TABLE thread_relation ADD COLUMN child_user_id BIGINT;

ALTER TABLE thread_observation ADD COLUMN subject_user_id BIGINT;
ALTER TABLE thread_observation ADD COLUMN candidate_parent_user_id BIGINT;
CREATE UNIQUE INDEX thread_observation_typed_identity_evidence_key
    ON thread_observation (
        subject_source, subject_identity_scope_known, subject_identity_scope_value,
        subject_user_id, subject_native_id, candidate_parent_present,
        candidate_parent_source, candidate_parent_identity_scope_known,
        candidate_parent_identity_scope_value, candidate_parent_user_id,
        candidate_parent_native_id, evidence_kind, evidence_fingerprint
    );

ALTER TABLE thread_group_candidate_association ADD COLUMN subject_user_id BIGINT;
CREATE INDEX thread_group_candidate_association_typed_subject_identity
    ON thread_group_candidate_association
       (subject_user_id, subject_source, subject_identity_scope_value, subject_native_id);

ALTER TABLE source_thread_identity ADD COLUMN user_id BIGINT;
CREATE UNIQUE INDEX source_thread_identity_typed_key
    ON source_thread_identity (user_id, source, identity_scope, native_id);

ALTER TABLE thread_canonical_key ADD COLUMN user_id BIGINT;
ALTER TABLE thread_deletion_marker ADD COLUMN user_id BIGINT;
ALTER TABLE thread_deletion_marker ADD COLUMN thread_canonical_key CHAR(64);
CREATE UNIQUE INDEX thread_deletion_marker_typed_key
    ON thread_deletion_marker (user_id, source, identity_scope, native_id);

ALTER TABLE operator_decision ADD COLUMN user_id BIGINT;
ALTER TABLE manual_collection ADD COLUMN user_id BIGINT;
CREATE INDEX manual_collection_user_updated
    ON manual_collection (user_id, updated_at DESC, id);
ALTER TABLE manual_collection_member ADD COLUMN user_id BIGINT;
ALTER TABLE thread_group_event_outbox ADD COLUMN user_id BIGINT;

UPDATE memories_schema_contract
SET version = '20260930000001'
WHERE contract_key = 'rdb_schema';
