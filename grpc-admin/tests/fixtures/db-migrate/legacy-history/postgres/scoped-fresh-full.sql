--
-- PostgreSQL database dump
--


-- Dumped from database version 16.15 (Debian 16.15-1.pgdg13+2)
-- Dumped by pg_dump version 16.15 (Debian 16.15-1.pgdg13+2)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: memories; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA memories;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: atlas_schema_revisions; Type: TABLE; Schema: memories; Owner: -
--

CREATE TABLE memories.atlas_schema_revisions (
    version character varying NOT NULL,
    description character varying NOT NULL,
    type bigint DEFAULT 2 NOT NULL,
    applied bigint DEFAULT 0 NOT NULL,
    total bigint DEFAULT 0 NOT NULL,
    executed_at timestamp with time zone NOT NULL,
    execution_time bigint NOT NULL,
    error text,
    error_stmt text,
    hash character varying NOT NULL,
    partial_hashes jsonb,
    operator_version character varying NOT NULL
);


--
-- Name: failure_mode_dictionary; Type: TABLE; Schema: memories; Owner: -
--

CREATE TABLE memories.failure_mode_dictionary (
    mode character varying(64) NOT NULL,
    description text NOT NULL,
    severity integer NOT NULL,
    category integer NOT NULL,
    default_mitigation text NOT NULL
);


--
-- Name: failure_signature_indicator_norm; Type: TABLE; Schema: memories; Owner: -
--

CREATE TABLE memories.failure_signature_indicator_norm (
    indicator_name character varying(64) NOT NULL,
    max_value double precision NOT NULL,
    weight double precision DEFAULT 1.0 NOT NULL
);


--
-- Name: manual_collection; Type: TABLE; Schema: memories; Owner: -
--

CREATE TABLE memories.manual_collection (
    id bigint NOT NULL,
    owner_scope text NOT NULL,
    title text NOT NULL,
    created_at bigint NOT NULL,
    updated_at bigint NOT NULL,
    user_id bigint
);


--
-- Name: manual_collection_member; Type: TABLE; Schema: memories; Owner: -
--

CREATE TABLE memories.manual_collection_member (
    collection_id bigint NOT NULL,
    thread_id bigint NOT NULL,
    owner_scope text NOT NULL,
    user_id bigint
);


--
-- Name: media_object; Type: TABLE; Schema: memories; Owner: -
--

CREATE TABLE memories.media_object (
    id bigint NOT NULL,
    kind integer NOT NULL,
    media_type character varying(64) NOT NULL,
    byte_size bigint,
    sha256 character(64),
    width integer,
    height integer,
    duration_ms bigint,
    storage_backend character varying(16) NOT NULL,
    storage_uri text,
    alt text,
    ref_count bigint DEFAULT 0 NOT NULL,
    gc_state integer DEFAULT 0 NOT NULL,
    metadata jsonb,
    created_at bigint NOT NULL,
    updated_at bigint NOT NULL
);


--
-- Name: memories_data_migration_task_state; Type: TABLE; Schema: memories; Owner: -
--

CREATE TABLE memories.memories_data_migration_task_state (
    task_identity text NOT NULL,
    canonical_definition_digest text NOT NULL,
    state text NOT NULL,
    execution_id text,
    holder_id text,
    fencing_token bigint DEFAULT 0 NOT NULL,
    heartbeat_at bigint,
    lease_expires_at bigint,
    attempt_count bigint DEFAULT 0 NOT NULL,
    checkpoint text,
    failure_classification text,
    started_at bigint,
    updated_at bigint NOT NULL,
    completed_at bigint,
    CONSTRAINT memories_data_migration_task_state_state_check CHECK ((state = ANY (ARRAY['pending'::text, 'running'::text, 'completed'::text, 'failed'::text])))
);


--
-- Name: memories_schema_contract; Type: TABLE; Schema: memories; Owner: -
--

CREATE TABLE memories.memories_schema_contract (
    contract_key text NOT NULL,
    version text NOT NULL,
    CONSTRAINT memories_schema_contract_contract_key_check CHECK ((contract_key = 'rdb_schema'::text)),
    CONSTRAINT memories_schema_contract_version_check CHECK ((length(version) = 14))
);


--
-- Name: memories_storage_identity; Type: TABLE; Schema: memories; Owner: -
--

CREATE TABLE memories.memories_storage_identity (
    identity_key text NOT NULL,
    storage_id text NOT NULL,
    CONSTRAINT memories_storage_identity_identity_key_check CHECK ((identity_key = 'rdb'::text)),
    CONSTRAINT memories_storage_identity_storage_id_check CHECK ((length(storage_id) = 32))
);


--
-- Name: memory; Type: TABLE; Schema: memories; Owner: -
--

CREATE TABLE memories.memory (
    id bigint NOT NULL,
    parent_ids jsonb,
    user_id bigint NOT NULL,
    content text NOT NULL,
    content_type integer NOT NULL,
    params jsonb,
    metadata jsonb,
    created_at bigint NOT NULL,
    updated_at bigint NOT NULL,
    role integer DEFAULT 0 NOT NULL,
    external_id character varying(512),
    media_object_id bigint,
    memory_kind integer NOT NULL
);


--
-- Name: memory_rating; Type: TABLE; Schema: memories; Owner: -
--

CREATE TABLE memories.memory_rating (
    id bigint NOT NULL,
    memory_id bigint NOT NULL,
    user_id bigint NOT NULL,
    rating double precision NOT NULL,
    metadata jsonb,
    created_at bigint NOT NULL,
    updated_at bigint NOT NULL
);


--
-- Name: operator_decision; Type: TABLE; Schema: memories; Owner: -
--

CREATE TABLE memories.operator_decision (
    id bigint NOT NULL,
    owner_scope text NOT NULL,
    candidate_association_id bigint NOT NULL,
    actor_id text NOT NULL,
    decision text NOT NULL,
    reason text NOT NULL,
    input_evidence_fingerprint character(64) NOT NULL,
    policy_version text NOT NULL,
    created_at bigint NOT NULL,
    user_id bigint,
    CONSTRAINT operator_decision_decision_check CHECK ((decision = ANY (ARRAY['confirm'::text, 'reject'::text, 'retract'::text])))
);


--
-- Name: reflection_applied_target; Type: TABLE; Schema: memories; Owner: -
--

CREATE TABLE memories.reflection_applied_target (
    memory_id bigint NOT NULL,
    target character varying(256) NOT NULL,
    mitigation_fingerprint character varying(64),
    applied_at bigint NOT NULL
);


--
-- Name: reflection_fact; Type: TABLE; Schema: memories; Owner: -
--

CREATE TABLE memories.reflection_fact (
    memory_id bigint NOT NULL,
    fact_memory_id bigint NOT NULL,
    fact_kind integer NOT NULL,
    turn_index integer DEFAULT 0 NOT NULL,
    weight double precision,
    note text,
    links_json jsonb
);


--
-- Name: reflection_failure_mode; Type: TABLE; Schema: memories; Owner: -
--

CREATE TABLE memories.reflection_failure_mode (
    memory_id bigint NOT NULL,
    mode character varying(64) NOT NULL
);


--
-- Name: reflection_few_shot_usage; Type: TABLE; Schema: memories; Owner: -
--

CREATE TABLE memories.reflection_few_shot_usage (
    memory_id bigint NOT NULL,
    used_in_thread_id bigint NOT NULL,
    used_at bigint NOT NULL
);


--
-- Name: reflection_tool; Type: TABLE; Schema: memories; Owner: -
--

CREATE TABLE memories.reflection_tool (
    memory_id bigint NOT NULL,
    tool character varying(128) NOT NULL
);


--
-- Name: reflection_tool_outcome; Type: TABLE; Schema: memories; Owner: -
--

CREATE TABLE memories.reflection_tool_outcome (
    memory_id bigint NOT NULL,
    tool character varying(128) NOT NULL,
    contribution integer NOT NULL,
    error_kind character varying(128) DEFAULT ''::character varying NOT NULL
);


--
-- Name: source_thread_identity; Type: TABLE; Schema: memories; Owner: -
--

CREATE TABLE memories.source_thread_identity (
    owner_scope text NOT NULL,
    source text NOT NULL,
    identity_scope text NOT NULL,
    native_id text NOT NULL,
    thread_id bigint NOT NULL,
    resolution_state text NOT NULL,
    first_seen_at bigint NOT NULL,
    last_seen_at bigint NOT NULL,
    user_id bigint,
    CONSTRAINT source_thread_identity_resolution_state_check CHECK ((resolution_state = 'resolved'::text))
);


--
-- Name: thread; Type: TABLE; Schema: memories; Owner: -
--

CREATE TABLE memories.thread (
    id bigint NOT NULL,
    default_system_memory_id bigint,
    user_id bigint NOT NULL,
    description text,
    channel text,
    embedding bytea,
    embedding_dim integer,
    created_at bigint NOT NULL,
    updated_at bigint NOT NULL,
    metadata jsonb,
    memory_kind integer NOT NULL,
    first_message_at bigint,
    last_message_at bigint
);


--
-- Name: thread_aggregate_key; Type: TABLE; Schema: memories; Owner: -
--

CREATE TABLE memories.thread_aggregate_key (
    user_id bigint NOT NULL,
    labels_hash character(64) NOT NULL,
    thread_id bigint NOT NULL,
    created_at bigint NOT NULL
);


--
-- Name: thread_canonical_key; Type: TABLE; Schema: memories; Owner: -
--

CREATE TABLE memories.thread_canonical_key (
    thread_id bigint NOT NULL,
    owner_scope text NOT NULL,
    key character(64) NOT NULL,
    origin text NOT NULL,
    assigned_at bigint NOT NULL,
    user_id bigint,
    CONSTRAINT thread_canonical_key_origin_check CHECK ((origin = ANY (ARRAY['source_identity'::text, 'creation_uuid'::text, 'backfill_mapping'::text])))
);


--
-- Name: thread_deletion_marker; Type: TABLE; Schema: memories; Owner: -
--

CREATE TABLE memories.thread_deletion_marker (
    owner_scope text NOT NULL,
    source text NOT NULL,
    identity_scope text NOT NULL,
    native_id text NOT NULL,
    forbid_reimport boolean NOT NULL,
    recursive boolean NOT NULL,
    actor_id text NOT NULL,
    reason text,
    deleted_at bigint NOT NULL,
    user_id bigint,
    thread_canonical_key character(64)
);


--
-- Name: thread_group; Type: TABLE; Schema: memories; Owner: -
--

CREATE TABLE memories.thread_group (
    id bigint NOT NULL,
    group_canonical_key character(64) NOT NULL,
    title text,
    status text NOT NULL,
    grouping_authority text NOT NULL,
    redirect_to_group_id bigint,
    created_at bigint NOT NULL,
    updated_at bigint NOT NULL,
    user_id bigint NOT NULL,
    CONSTRAINT thread_group_check CHECK ((((status = 'redirected'::text) AND (redirect_to_group_id IS NOT NULL)) OR ((status <> 'redirected'::text) AND (redirect_to_group_id IS NULL)))),
    CONSTRAINT thread_group_check1 CHECK ((redirect_to_group_id <> id)),
    CONSTRAINT thread_group_grouping_authority_check CHECK ((grouping_authority = ANY (ARRAY['reconciler'::text, 'operator'::text]))),
    CONSTRAINT thread_group_status_check CHECK ((status = ANY (ARRAY['active'::text, 'redirected'::text, 'split'::text])))
);


--
-- Name: thread_group_audit; Type: TABLE; Schema: memories; Owner: -
--

CREATE TABLE memories.thread_group_audit (
    id bigint NOT NULL,
    audit_type text NOT NULL,
    source_group_id bigint NOT NULL,
    target_group_id bigint,
    actor_id text NOT NULL,
    reason text NOT NULL,
    canonical_partition jsonb,
    successor_group_ids jsonb,
    created_at bigint NOT NULL,
    CONSTRAINT thread_group_audit_audit_type_check CHECK ((audit_type = ANY (ARRAY['merge'::text, 'split'::text]))),
    CONSTRAINT thread_group_audit_check CHECK ((((audit_type = 'merge'::text) AND (target_group_id IS NOT NULL) AND (canonical_partition IS NULL) AND (successor_group_ids IS NULL)) OR ((audit_type = 'split'::text) AND (target_group_id IS NULL) AND (canonical_partition IS NOT NULL) AND (successor_group_ids IS NOT NULL))))
);


--
-- Name: thread_group_candidate_association; Type: TABLE; Schema: memories; Owner: -
--

CREATE TABLE memories.thread_group_candidate_association (
    id bigint NOT NULL,
    subject_thread_id bigint,
    subject_source text NOT NULL,
    subject_identity_scope_known boolean NOT NULL,
    subject_identity_scope_value text DEFAULT ''::text NOT NULL,
    subject_owner_scope text NOT NULL,
    subject_native_id text NOT NULL,
    candidate_group_id bigint,
    candidate_parent_thread_id bigint,
    state text NOT NULL,
    selected_observation_id bigint,
    created_at bigint NOT NULL,
    updated_at bigint NOT NULL,
    subject_user_id bigint,
    CONSTRAINT thread_group_candidate_association_state_check CHECK ((state = ANY (ARRAY['pending'::text, 'candidate'::text, 'ambiguous'::text, 'conflict'::text, 'unsupported'::text, 'superseded'::text])))
);


--
-- Name: thread_group_event_outbox; Type: TABLE; Schema: memories; Owner: -
--

CREATE TABLE memories.thread_group_event_outbox (
    event_id text NOT NULL,
    event_type text NOT NULL,
    operation_id text NOT NULL,
    policy_version text NOT NULL,
    source text,
    identity_scope text,
    owner_scope text,
    native_id_ref text,
    group_id bigint,
    thread_id bigint,
    source_confidence text,
    selection_basis text,
    operator_decision_id bigint,
    polarity text,
    payload jsonb NOT NULL,
    created_at bigint NOT NULL,
    user_id bigint,
    CONSTRAINT thread_group_event_outbox_event_type_check CHECK ((event_type = ANY (ARRAY['thread_group_observation_recorded'::text, 'thread_group_relation_selected'::text, 'thread_group_reconciliation_completed'::text, 'thread_group_conflict_detected'::text, 'thread_group_redirected'::text]))),
    CONSTRAINT thread_group_event_outbox_polarity_check CHECK (((polarity IS NULL) OR (polarity = ANY (ARRAY['supports'::text, 'negates'::text, 'conflicts'::text])))),
    CONSTRAINT thread_group_event_outbox_selection_basis_check CHECK (((selection_basis IS NULL) OR (selection_basis = ANY (ARRAY['source_exact'::text, 'source_strong'::text, 'operator_confirmation'::text])))),
    CONSTRAINT thread_group_event_outbox_source_confidence_check CHECK (((source_confidence IS NULL) OR (source_confidence = ANY (ARRAY['exact'::text, 'strong'::text, 'heuristic'::text, 'unsupported'::text]))))
);


--
-- Name: thread_group_member; Type: TABLE; Schema: memories; Owner: -
--

CREATE TABLE memories.thread_group_member (
    group_id bigint NOT NULL,
    thread_id bigint,
    thread_canonical_key character(64) NOT NULL,
    owner_scope text NOT NULL,
    source text,
    identity_scope text,
    native_id text,
    role text NOT NULL,
    state text NOT NULL,
    provenance text NOT NULL,
    deleted_at bigint,
    created_at bigint NOT NULL,
    updated_at bigint NOT NULL,
    user_id bigint,
    CONSTRAINT thread_group_member_check CHECK ((((state = 'deleted'::text) AND (thread_id IS NULL) AND (deleted_at IS NOT NULL)) OR ((state = 'active'::text) AND (thread_id IS NOT NULL)) OR (state = 'redirected'::text))),
    CONSTRAINT thread_group_member_check1 CHECK ((((source IS NULL) AND (identity_scope IS NULL) AND (native_id IS NULL)) OR ((source IS NOT NULL) AND (identity_scope IS NOT NULL) AND (native_id IS NOT NULL)))),
    CONSTRAINT thread_group_member_provenance_check CHECK ((provenance = ANY (ARRAY['reconciler'::text, 'operator'::text]))),
    CONSTRAINT thread_group_member_role_check CHECK ((role = ANY (ARRAY['root'::text, 'member'::text]))),
    CONSTRAINT thread_group_member_state_check CHECK ((state = ANY (ARRAY['active'::text, 'deleted'::text, 'redirected'::text])))
);


--
-- Name: thread_group_memory_relation; Type: TABLE; Schema: memories; Owner: -
--

CREATE TABLE memories.thread_group_memory_relation (
    group_id bigint NOT NULL,
    memory_id bigint NOT NULL,
    purpose text NOT NULL,
    on_group_delete text NOT NULL,
    created_at bigint NOT NULL,
    CONSTRAINT thread_group_memory_relation_on_group_delete_check CHECK ((on_group_delete = ANY (ARRAY['retain'::text, 'delete'::text]))),
    CONSTRAINT thread_group_memory_relation_purpose_check CHECK ((length(purpose) > 0))
);


--
-- Name: thread_label; Type: TABLE; Schema: memories; Owner: -
--

CREATE TABLE memories.thread_label (
    thread_id bigint NOT NULL,
    label character varying(512) NOT NULL,
    created_at bigint NOT NULL
);


--
-- Name: thread_memory; Type: TABLE; Schema: memories; Owner: -
--

CREATE TABLE memories.thread_memory (
    thread_id bigint NOT NULL,
    memory_id bigint NOT NULL,
    "position" integer NOT NULL,
    created_at bigint NOT NULL
);


--
-- Name: thread_observation; Type: TABLE; Schema: memories; Owner: -
--

CREATE TABLE memories.thread_observation (
    id bigint NOT NULL,
    subject_source text NOT NULL,
    subject_identity_scope_known boolean NOT NULL,
    subject_identity_scope_value text DEFAULT ''::text NOT NULL,
    subject_owner_scope text NOT NULL,
    subject_native_id text NOT NULL,
    candidate_parent_present boolean NOT NULL,
    candidate_parent_source text DEFAULT ''::text NOT NULL,
    candidate_parent_identity_scope_known boolean DEFAULT false NOT NULL,
    candidate_parent_identity_scope_value text DEFAULT ''::text NOT NULL,
    candidate_parent_owner_scope text DEFAULT ''::text NOT NULL,
    candidate_parent_native_id text DEFAULT ''::text NOT NULL,
    relation_kind text,
    origin text NOT NULL,
    evidence_kind text NOT NULL,
    polarity text NOT NULL,
    source_confidence text,
    evidence_fingerprint character(64) NOT NULL,
    source_record_ref text,
    state text NOT NULL,
    import_run_id text,
    observed_at bigint NOT NULL,
    created_at bigint NOT NULL,
    updated_at bigint NOT NULL,
    subject_user_id bigint,
    candidate_parent_user_id bigint,
    CONSTRAINT thread_observation_check CHECK ((((origin = 'adapter'::text) AND (evidence_kind = ANY (ARRAY['source_event'::text, 'source_field'::text, 'negative_or_conflict'::text])) AND (source_confidence = ANY (ARRAY['exact'::text, 'strong'::text, 'heuristic'::text, 'unsupported'::text]))) OR ((origin = 'reconciler'::text) AND (evidence_kind = ANY (ARRAY['derived_rejection'::text, 'derived_conflict'::text])) AND (source_confidence IS NULL)))),
    CONSTRAINT thread_observation_evidence_kind_check CHECK ((evidence_kind = ANY (ARRAY['source_event'::text, 'source_field'::text, 'negative_or_conflict'::text, 'derived_rejection'::text, 'derived_conflict'::text]))),
    CONSTRAINT thread_observation_origin_check CHECK ((origin = ANY (ARRAY['adapter'::text, 'reconciler'::text]))),
    CONSTRAINT thread_observation_polarity_check CHECK ((polarity = ANY (ARRAY['supports'::text, 'negates'::text, 'conflicts'::text]))),
    CONSTRAINT thread_observation_relation_kind_check CHECK (((relation_kind IS NULL) OR (relation_kind = ANY (ARRAY['delegated'::text, 'fork'::text, 'continuation'::text])))),
    CONSTRAINT thread_observation_source_confidence_check CHECK (((source_confidence IS NULL) OR (source_confidence = ANY (ARRAY['exact'::text, 'strong'::text, 'heuristic'::text, 'unsupported'::text])))),
    CONSTRAINT thread_observation_state_check CHECK ((state = ANY (ARRAY['pending'::text, 'candidate'::text, 'selected'::text, 'conflict'::text, 'superseded'::text, 'unsupported'::text])))
);


--
-- Name: thread_reflection_index; Type: TABLE; Schema: memories; Owner: -
--

CREATE TABLE memories.thread_reflection_index (
    memory_id bigint NOT NULL,
    thread_id bigint NOT NULL,
    origin_thread_id bigint NOT NULL,
    origin_user_id bigint NOT NULL,
    origin_channel text,
    outcome integer NOT NULL,
    score double precision NOT NULL,
    score_self double precision NOT NULL,
    score_heuristic double precision NOT NULL,
    task_category integer NOT NULL,
    reflection_aspect integer NOT NULL,
    dataset_quality integer DEFAULT 1 NOT NULL,
    summary_embedding_status integer DEFAULT 1 NOT NULL,
    summary_embedding_error text,
    intent_embedding_status integer DEFAULT 1 NOT NULL,
    intent_embedding_error text,
    prompt_version character varying(32) NOT NULL,
    target_model_version character varying(128),
    experiment_id character varying(128),
    experiment_variant character varying(128),
    previous_reflection_id bigint,
    pinned boolean DEFAULT false NOT NULL,
    is_recurrence boolean DEFAULT false NOT NULL,
    mitigation_fingerprint character varying(64),
    created_at bigint NOT NULL,
    updated_at bigint NOT NULL
);


--
-- Name: thread_relation; Type: TABLE; Schema: memories; Owner: -
--

CREATE TABLE memories.thread_relation (
    id bigint NOT NULL,
    parent_thread_id bigint,
    child_thread_id bigint,
    parent_thread_canonical_key character(64) NOT NULL,
    child_thread_canonical_key character(64) NOT NULL,
    parent_owner_scope text NOT NULL,
    parent_source text,
    parent_identity_scope text,
    parent_native_id text,
    child_owner_scope text NOT NULL,
    child_source text,
    child_identity_scope text,
    child_native_id text,
    relation_type text NOT NULL,
    state text NOT NULL,
    selection_basis text NOT NULL,
    source_confidence text,
    selected_observation_id bigint,
    selected_operator_decision_id bigint,
    created_at bigint NOT NULL,
    updated_at bigint NOT NULL,
    parent_user_id bigint,
    child_user_id bigint,
    CONSTRAINT thread_relation_check CHECK ((parent_thread_canonical_key <> child_thread_canonical_key)),
    CONSTRAINT thread_relation_check1 CHECK ((((selection_basis = 'operator_confirmation'::text) AND (source_confidence IS NULL)) OR ((selection_basis <> 'operator_confirmation'::text) AND (source_confidence IS NOT NULL)))),
    CONSTRAINT thread_relation_check2 CHECK ((((selection_basis = 'operator_confirmation'::text) AND (selected_operator_decision_id IS NOT NULL) AND (selected_observation_id IS NULL)) OR ((selection_basis <> 'operator_confirmation'::text) AND (selected_observation_id IS NOT NULL) AND (selected_operator_decision_id IS NULL)))),
    CONSTRAINT thread_relation_check3 CHECK ((((parent_source IS NULL) AND (parent_identity_scope IS NULL) AND (parent_native_id IS NULL)) OR ((parent_source IS NOT NULL) AND (parent_identity_scope IS NOT NULL) AND (parent_native_id IS NOT NULL)))),
    CONSTRAINT thread_relation_check4 CHECK ((((child_source IS NULL) AND (child_identity_scope IS NULL) AND (child_native_id IS NULL)) OR ((child_source IS NOT NULL) AND (child_identity_scope IS NOT NULL) AND (child_native_id IS NOT NULL)))),
    CONSTRAINT thread_relation_relation_type_check CHECK ((relation_type = ANY (ARRAY['delegated'::text, 'fork'::text, 'continuation'::text]))),
    CONSTRAINT thread_relation_selection_basis_check CHECK ((selection_basis = ANY (ARRAY['source_exact'::text, 'source_strong'::text, 'operator_confirmation'::text]))),
    CONSTRAINT thread_relation_source_confidence_check CHECK (((source_confidence IS NULL) OR (source_confidence = ANY (ARRAY['exact'::text, 'strong'::text])))),
    CONSTRAINT thread_relation_state_check CHECK ((state = ANY (ARRAY['active'::text, 'retracted'::text, 'superseded'::text])))
);


--
-- Name: thread_time_migration_state; Type: TABLE; Schema: memories; Owner: -
--

CREATE TABLE memories.thread_time_migration_state (
    migration_key text NOT NULL,
    rdb_completed_at bigint,
    vector_status text NOT NULL,
    staging_table_name text,
    vector_completed_at bigint,
    CONSTRAINT thread_time_migration_state_migration_key_check CHECK ((migration_key = 'thread-time-fields-v1'::text)),
    CONSTRAINT thread_time_migration_state_vector_status_check CHECK ((vector_status = ANY (ARRAY['PENDING'::text, 'STAGED'::text, 'SWITCHING'::text, 'NOT_REQUIRED'::text, 'COMPLETED'::text])))
);


--
-- Name: tool_contribution_stats; Type: TABLE; Schema: memories; Owner: -
--

CREATE TABLE memories.tool_contribution_stats (
    origin_user_id bigint NOT NULL,
    tool character varying(128) NOT NULL,
    contribution integer NOT NULL,
    error_kind character varying(128) DEFAULT ''::character varying NOT NULL,
    count bigint DEFAULT 0 NOT NULL,
    last_updated_at bigint NOT NULL
);


--
-- Name: tool_outcome_stats; Type: TABLE; Schema: memories; Owner: -
--

CREATE TABLE memories.tool_outcome_stats (
    origin_user_id bigint NOT NULL,
    tool character varying(128) NOT NULL,
    outcome integer NOT NULL,
    count bigint DEFAULT 0 NOT NULL,
    last_updated_at bigint NOT NULL
);


--
-- Data for Name: atlas_schema_revisions; Type: TABLE DATA; Schema: memories; Owner: -
--

INSERT INTO memories.atlas_schema_revisions VALUES ('20260803000001', 'adoption_baseline', 2, 60, 60, '2026-11-22 13:00:09.537034+00', 167507, '', '', 'tVQsyaFRd3aGrgmp258Fo0zxHzIisbk7wq16apQMcvg=', 'null', 'Atlas CLI v1.3.0');
INSERT INTO memories.atlas_schema_revisions VALUES ('20260803000002', 'thread_message_times_schema', 2, 9, 9, '2026-11-22 13:00:09.944683+00', 758586, '', '', 'gPfxfZ8dp+gXmpHJE+7rHbxHkMuMCZ6f8JCOHgQyFfo=', 'null', 'Atlas CLI v1.3.0');
INSERT INTO memories.atlas_schema_revisions VALUES ('20260803000003', 'schema_contract', 2, 3, 3, '2026-11-22 13:00:09.997796+00', 267734, '', '', 'TyUDS2LzmlI8zqCkYtlJPm7gtMvaqQn4cE9OexVS1yQ=', 'null', 'Atlas CLI v1.3.0');
INSERT INTO memories.atlas_schema_revisions VALUES ('20260926000001', 'thread_group_memory_relation', 2, 3, 3, '2026-11-22 13:00:10.429488+00', 383562, '', '', '7LwRzaXxaEEeWszOKPstwq5wei4jUYgo8EoJyJdZJOg=', 'null', 'Atlas CLI v1.3.0');
INSERT INTO memories.atlas_schema_revisions VALUES ('20260920000001', 'thread_group_schema', 2, 48, 48, '2026-11-22 13:00:10.030624+00', 352488, '', '', 'sLgDuvjPScqYsnfoCCXcl6eUpI6Khdv1B4B9CP7JtNg=', 'null', 'Atlas CLI v1.3.0');
INSERT INTO memories.atlas_schema_revisions VALUES ('20260930000001', 'thread_group_user_ids', 2, 24, 24, '2026-11-22 13:00:10.453671+00', 668828, '', '', '5eb28KKC7vAoDVC3CFoqTnWAIFKhDEQj4ig0xeU+pac=', 'null', 'Atlas CLI v1.3.0');
INSERT INTO memories.atlas_schema_revisions VALUES ('20261009000001', 'embedding_storage_identity', 2, 3, 3, '2026-11-22 13:00:10.52261+00', 99016, '', '', 'VsSVVFzfzNNSOTwpS7pthtpDM2GC2TUL8VH4NjhKk0k=', 'null', 'Atlas CLI v1.3.0');


--
-- Data for Name: failure_mode_dictionary; Type: TABLE DATA; Schema: memories; Owner: -
--

INSERT INTO memories.failure_mode_dictionary VALUES ('tool_misuse', 'Wrong tool selection or argument shape', 3, 1, 'Consult tool docs/schemas before invocation and validate arguments with a minimal trial run before executing for real.');
INSERT INTO memories.failure_mode_dictionary VALUES ('loop', 'Repeating identical tool calls without progress', 3, 1, 'Self-abort when the same tool fails three times in a row with equivalent arguments and request user guidance.');
INSERT INTO memories.failure_mode_dictionary VALUES ('scope_drift', 'Drifting away from the originally stated goal', 2, 1, 'Restate the task intent at start and self-check against it every five turns, surfacing any drift to the user.');
INSERT INTO memories.failure_mode_dictionary VALUES ('hallucination', 'Asserting unverified factual claims', 3, 1, 'Cross-check verifiable facts (API names, file paths, command syntax) against an external source before stating them.');
INSERT INTO memories.failure_mode_dictionary VALUES ('context_overflow', 'Approaching context-window saturation', 2, 1, 'When context usage exceeds 70% of the limit, summarise older turns proactively and drop irrelevant history.');
INSERT INTO memories.failure_mode_dictionary VALUES ('data_loss', 'Destructive operation may corrupt user assets', 4, 2, 'Always seek explicit confirmation before destructive operations (delete, overwrite, force push) and surface reversibility.');
INSERT INTO memories.failure_mode_dictionary VALUES ('permission_issue', 'Operating without sufficient privileges', 4, 2, 'Run dry-runs or pre-checks for permission-sensitive actions and never silently swallow permission errors.');
INSERT INTO memories.failure_mode_dictionary VALUES ('ambiguous_instruction', 'Ambiguous user instructions', 2, 3, 'Surface up to three plausible interpretations and ask the user to disambiguate before starting work.');
INSERT INTO memories.failure_mode_dictionary VALUES ('conflicting_requirements', 'Mutually conflicting requirements', 2, 3, 'Restate the conflict explicitly and ask the user to set priorities before proceeding.');
INSERT INTO memories.failure_mode_dictionary VALUES ('missing_context', 'Required information missing', 2, 3, 'List the missing pieces as a bullet checklist and confirm with the user before starting.');
INSERT INTO memories.failure_mode_dictionary VALUES ('misleading_premise', 'User instruction starts from a false premise', 3, 3, 'When premise validity is doubtful, verify what is verifiable then ask the user to confirm the rest.');
INSERT INTO memories.failure_mode_dictionary VALUES ('goal_drift_by_user', 'User changes goal mid-task', 1, 3, 'On detecting a mid-task goal change, surface the relationship to the prior goal and confirm whether to replace or stack it.');
INSERT INTO memories.failure_mode_dictionary VALUES ('tool_unavailable', 'Required tool unavailable', 1, 4, 'Offer an alternate tool or manual workaround and confirm with the user whether to proceed.');
INSERT INTO memories.failure_mode_dictionary VALUES ('external_service_failure', 'External service request failure', 1, 4, 'Use exponential backoff up to three retries; on persistent failure, explain the situation and ask the user how to proceed.');
INSERT INTO memories.failure_mode_dictionary VALUES ('rate_limit', 'Hit external rate limit', 1, 4, 'Wait the indicated cooldown then retry, or surface an alternate resource fallback for the user to choose from.');
INSERT INTO memories.failure_mode_dictionary VALUES ('OTHER', 'Unclassified failure mode (use failure_modes_other for free text)', 1, 5, 'No default mitigation; fall back to free-text guidance recorded in failure_modes_other.');


--
-- Data for Name: failure_signature_indicator_norm; Type: TABLE DATA; Schema: memories; Owner: -
--

INSERT INTO memories.failure_signature_indicator_norm VALUES ('same_tool_repeated_count', 20, 1);
INSERT INTO memories.failure_signature_indicator_norm VALUES ('consecutive_errors', 10, 1);
INSERT INTO memories.failure_signature_indicator_norm VALUES ('no_state_change_turns', 20, 1);
INSERT INTO memories.failure_signature_indicator_norm VALUES ('tool_calls_per_turn_ratio', 10, 1);
INSERT INTO memories.failure_signature_indicator_norm VALUES ('compact_boundary_count', 10, 1);
INSERT INTO memories.failure_signature_indicator_norm VALUES ('user_clarification_count', 10, 1);
INSERT INTO memories.failure_signature_indicator_norm VALUES ('turn_count_at_detection', 500, 1);
INSERT INTO memories.failure_signature_indicator_norm VALUES ('elapsed_ms_at_detection', 3600000, 1);


--
-- Data for Name: manual_collection; Type: TABLE DATA; Schema: memories; Owner: -
--



--
-- Data for Name: manual_collection_member; Type: TABLE DATA; Schema: memories; Owner: -
--



--
-- Data for Name: media_object; Type: TABLE DATA; Schema: memories; Owner: -
--



--
-- Data for Name: memories_data_migration_task_state; Type: TABLE DATA; Schema: memories; Owner: -
--

INSERT INTO memories.memories_data_migration_task_state VALUES ('thread-message-times-v1@1', 'd2fe9980cf1048115967a8ab019a53c4422b284f3a8cf185ce2c55988506ed1d', 'completed', '3774391-thread-message-times-v1@1-1795352410653', 'memories-db-migrate', 1, NULL, NULL, 1, '{"format":"thread-message-times-v1@1/checkpoint-v1","task_identity":"thread-message-times-v1@1","canonical_definition_digest":"d2fe9980cf1048115967a8ab019a53c4422b284f3a8cf185ce2c55988506ed1d","payload":{"rdb_phase":"verified","rdb_last_thread_id":null,"vector_phase":"completed","canonical_table_name":null,"staging_table_name":null,"staging_origin":null,"staging_cleanup_started":false,"index_outcomes":null,"vector_outcome":"not_required"}}', NULL, 1795352410655, 1795352410669, 1795352410669);
INSERT INTO memories.memories_data_migration_task_state VALUES ('thread-groups-canonical-keys-v1@2', 'ad2a56fbae48549f6a7af61182b4358eb1e3ece4fb12f4b1da1d0904044312b2', 'completed', '3774391-thread-groups-canonical-keys-v1@2-1795352410628', 'memories-db-migrate', 1, NULL, NULL, 1, '{"format":"thread-groups-canonical-keys-v1@2/checkpoint-v2","task_identity":"thread-groups-canonical-keys-v1@2","canonical_definition_digest":"ad2a56fbae48549f6a7af61182b4358eb1e3ece4fb12f4b1da1d0904044312b2","payload":{"phase":"verified","source_backed_last_thread_id":null,"manual_last_thread_id":null,"migration_version":"20260930000001","adapter_version":"canonical-serialization-v2","policy_version":"thread-group-policy-v1"}}', NULL, 1795352410630, 1795352410641, 1795352410641);
INSERT INTO memories.memories_data_migration_task_state VALUES ('thread-groups-user-ids-v1@4', '744a996decd4a48522168a89aae2c460d2ec522ce9f84ea25abc0951001a3501', 'completed', '3774391-thread-groups-user-ids-v1@4-1795352410643', 'memories-db-migrate', 1, NULL, NULL, 1, NULL, NULL, 1795352410650, 1795352410652, 1795352410652);


--
-- Data for Name: memories_schema_contract; Type: TABLE DATA; Schema: memories; Owner: -
--

INSERT INTO memories.memories_schema_contract VALUES ('rdb_schema', '20261009000001');


--
-- Data for Name: memories_storage_identity; Type: TABLE DATA; Schema: memories; Owner: -
--

INSERT INTO memories.memories_storage_identity VALUES ('rdb', 'f8e57caebdaf4a9abaa62c69d0183594');


--
-- Data for Name: memory; Type: TABLE DATA; Schema: memories; Owner: -
--



--
-- Data for Name: memory_rating; Type: TABLE DATA; Schema: memories; Owner: -
--



--
-- Data for Name: operator_decision; Type: TABLE DATA; Schema: memories; Owner: -
--



--
-- Data for Name: reflection_applied_target; Type: TABLE DATA; Schema: memories; Owner: -
--



--
-- Data for Name: reflection_fact; Type: TABLE DATA; Schema: memories; Owner: -
--



--
-- Data for Name: reflection_failure_mode; Type: TABLE DATA; Schema: memories; Owner: -
--



--
-- Data for Name: reflection_few_shot_usage; Type: TABLE DATA; Schema: memories; Owner: -
--



--
-- Data for Name: reflection_tool; Type: TABLE DATA; Schema: memories; Owner: -
--



--
-- Data for Name: reflection_tool_outcome; Type: TABLE DATA; Schema: memories; Owner: -
--



--
-- Data for Name: source_thread_identity; Type: TABLE DATA; Schema: memories; Owner: -
--



--
-- Data for Name: thread; Type: TABLE DATA; Schema: memories; Owner: -
--



--
-- Data for Name: thread_aggregate_key; Type: TABLE DATA; Schema: memories; Owner: -
--



--
-- Data for Name: thread_canonical_key; Type: TABLE DATA; Schema: memories; Owner: -
--



--
-- Data for Name: thread_deletion_marker; Type: TABLE DATA; Schema: memories; Owner: -
--



--
-- Data for Name: thread_group; Type: TABLE DATA; Schema: memories; Owner: -
--



--
-- Data for Name: thread_group_audit; Type: TABLE DATA; Schema: memories; Owner: -
--



--
-- Data for Name: thread_group_candidate_association; Type: TABLE DATA; Schema: memories; Owner: -
--



--
-- Data for Name: thread_group_event_outbox; Type: TABLE DATA; Schema: memories; Owner: -
--



--
-- Data for Name: thread_group_member; Type: TABLE DATA; Schema: memories; Owner: -
--



--
-- Data for Name: thread_group_memory_relation; Type: TABLE DATA; Schema: memories; Owner: -
--



--
-- Data for Name: thread_label; Type: TABLE DATA; Schema: memories; Owner: -
--



--
-- Data for Name: thread_memory; Type: TABLE DATA; Schema: memories; Owner: -
--



--
-- Data for Name: thread_observation; Type: TABLE DATA; Schema: memories; Owner: -
--



--
-- Data for Name: thread_reflection_index; Type: TABLE DATA; Schema: memories; Owner: -
--



--
-- Data for Name: thread_relation; Type: TABLE DATA; Schema: memories; Owner: -
--



--
-- Data for Name: thread_time_migration_state; Type: TABLE DATA; Schema: memories; Owner: -
--



--
-- Data for Name: tool_contribution_stats; Type: TABLE DATA; Schema: memories; Owner: -
--



--
-- Data for Name: tool_outcome_stats; Type: TABLE DATA; Schema: memories; Owner: -
--



--
-- Name: atlas_schema_revisions atlas_schema_revisions_pkey; Type: CONSTRAINT; Schema: memories; Owner: -
--

ALTER TABLE ONLY memories.atlas_schema_revisions
    ADD CONSTRAINT atlas_schema_revisions_pkey PRIMARY KEY (version);


--
-- Name: failure_mode_dictionary failure_mode_dictionary_pkey; Type: CONSTRAINT; Schema: memories; Owner: -
--

ALTER TABLE ONLY memories.failure_mode_dictionary
    ADD CONSTRAINT failure_mode_dictionary_pkey PRIMARY KEY (mode);


--
-- Name: failure_signature_indicator_norm failure_signature_indicator_norm_pkey; Type: CONSTRAINT; Schema: memories; Owner: -
--

ALTER TABLE ONLY memories.failure_signature_indicator_norm
    ADD CONSTRAINT failure_signature_indicator_norm_pkey PRIMARY KEY (indicator_name);


--
-- Name: manual_collection_member manual_collection_member_pkey; Type: CONSTRAINT; Schema: memories; Owner: -
--

ALTER TABLE ONLY memories.manual_collection_member
    ADD CONSTRAINT manual_collection_member_pkey PRIMARY KEY (collection_id, thread_id);


--
-- Name: manual_collection manual_collection_pkey; Type: CONSTRAINT; Schema: memories; Owner: -
--

ALTER TABLE ONLY memories.manual_collection
    ADD CONSTRAINT manual_collection_pkey PRIMARY KEY (id);


--
-- Name: media_object media_object_pkey; Type: CONSTRAINT; Schema: memories; Owner: -
--

ALTER TABLE ONLY memories.media_object
    ADD CONSTRAINT media_object_pkey PRIMARY KEY (id);


--
-- Name: memories_data_migration_task_state memories_data_migration_task_state_pkey; Type: CONSTRAINT; Schema: memories; Owner: -
--

ALTER TABLE ONLY memories.memories_data_migration_task_state
    ADD CONSTRAINT memories_data_migration_task_state_pkey PRIMARY KEY (task_identity);


--
-- Name: memories_schema_contract memories_schema_contract_pkey; Type: CONSTRAINT; Schema: memories; Owner: -
--

ALTER TABLE ONLY memories.memories_schema_contract
    ADD CONSTRAINT memories_schema_contract_pkey PRIMARY KEY (contract_key);


--
-- Name: memories_storage_identity memories_storage_identity_pkey; Type: CONSTRAINT; Schema: memories; Owner: -
--

ALTER TABLE ONLY memories.memories_storage_identity
    ADD CONSTRAINT memories_storage_identity_pkey PRIMARY KEY (identity_key);


--
-- Name: memory memory_pkey; Type: CONSTRAINT; Schema: memories; Owner: -
--

ALTER TABLE ONLY memories.memory
    ADD CONSTRAINT memory_pkey PRIMARY KEY (id);


--
-- Name: memory_rating memory_rating_pkey; Type: CONSTRAINT; Schema: memories; Owner: -
--

ALTER TABLE ONLY memories.memory_rating
    ADD CONSTRAINT memory_rating_pkey PRIMARY KEY (id);


--
-- Name: operator_decision operator_decision_pkey; Type: CONSTRAINT; Schema: memories; Owner: -
--

ALTER TABLE ONLY memories.operator_decision
    ADD CONSTRAINT operator_decision_pkey PRIMARY KEY (id);


--
-- Name: reflection_applied_target reflection_applied_target_pkey; Type: CONSTRAINT; Schema: memories; Owner: -
--

ALTER TABLE ONLY memories.reflection_applied_target
    ADD CONSTRAINT reflection_applied_target_pkey PRIMARY KEY (memory_id, target);


--
-- Name: reflection_fact reflection_fact_pkey; Type: CONSTRAINT; Schema: memories; Owner: -
--

ALTER TABLE ONLY memories.reflection_fact
    ADD CONSTRAINT reflection_fact_pkey PRIMARY KEY (memory_id, fact_memory_id, fact_kind);


--
-- Name: reflection_failure_mode reflection_failure_mode_pkey; Type: CONSTRAINT; Schema: memories; Owner: -
--

ALTER TABLE ONLY memories.reflection_failure_mode
    ADD CONSTRAINT reflection_failure_mode_pkey PRIMARY KEY (memory_id, mode);


--
-- Name: reflection_few_shot_usage reflection_few_shot_usage_pkey; Type: CONSTRAINT; Schema: memories; Owner: -
--

ALTER TABLE ONLY memories.reflection_few_shot_usage
    ADD CONSTRAINT reflection_few_shot_usage_pkey PRIMARY KEY (memory_id, used_in_thread_id);


--
-- Name: reflection_tool_outcome reflection_tool_outcome_pkey; Type: CONSTRAINT; Schema: memories; Owner: -
--

ALTER TABLE ONLY memories.reflection_tool_outcome
    ADD CONSTRAINT reflection_tool_outcome_pkey PRIMARY KEY (memory_id, tool, contribution, error_kind);


--
-- Name: reflection_tool reflection_tool_pkey; Type: CONSTRAINT; Schema: memories; Owner: -
--

ALTER TABLE ONLY memories.reflection_tool
    ADD CONSTRAINT reflection_tool_pkey PRIMARY KEY (memory_id, tool);


--
-- Name: source_thread_identity source_thread_identity_pkey; Type: CONSTRAINT; Schema: memories; Owner: -
--

ALTER TABLE ONLY memories.source_thread_identity
    ADD CONSTRAINT source_thread_identity_pkey PRIMARY KEY (owner_scope, source, identity_scope, native_id);


--
-- Name: thread_aggregate_key thread_aggregate_key_pkey; Type: CONSTRAINT; Schema: memories; Owner: -
--

ALTER TABLE ONLY memories.thread_aggregate_key
    ADD CONSTRAINT thread_aggregate_key_pkey PRIMARY KEY (user_id, labels_hash);


--
-- Name: thread_canonical_key thread_canonical_key_pkey; Type: CONSTRAINT; Schema: memories; Owner: -
--

ALTER TABLE ONLY memories.thread_canonical_key
    ADD CONSTRAINT thread_canonical_key_pkey PRIMARY KEY (thread_id);


--
-- Name: thread_deletion_marker thread_deletion_marker_pkey; Type: CONSTRAINT; Schema: memories; Owner: -
--

ALTER TABLE ONLY memories.thread_deletion_marker
    ADD CONSTRAINT thread_deletion_marker_pkey PRIMARY KEY (owner_scope, source, identity_scope, native_id);


--
-- Name: thread_group_audit thread_group_audit_pkey; Type: CONSTRAINT; Schema: memories; Owner: -
--

ALTER TABLE ONLY memories.thread_group_audit
    ADD CONSTRAINT thread_group_audit_pkey PRIMARY KEY (id);


--
-- Name: thread_group_candidate_association thread_group_candidate_association_pkey; Type: CONSTRAINT; Schema: memories; Owner: -
--

ALTER TABLE ONLY memories.thread_group_candidate_association
    ADD CONSTRAINT thread_group_candidate_association_pkey PRIMARY KEY (id);


--
-- Name: thread_group_event_outbox thread_group_event_outbox_pkey; Type: CONSTRAINT; Schema: memories; Owner: -
--

ALTER TABLE ONLY memories.thread_group_event_outbox
    ADD CONSTRAINT thread_group_event_outbox_pkey PRIMARY KEY (event_id);


--
-- Name: thread_group_memory_relation thread_group_memory_relation_pkey; Type: CONSTRAINT; Schema: memories; Owner: -
--

ALTER TABLE ONLY memories.thread_group_memory_relation
    ADD CONSTRAINT thread_group_memory_relation_pkey PRIMARY KEY (group_id, memory_id);


--
-- Name: thread_group thread_group_pkey; Type: CONSTRAINT; Schema: memories; Owner: -
--

ALTER TABLE ONLY memories.thread_group
    ADD CONSTRAINT thread_group_pkey PRIMARY KEY (id);


--
-- Name: thread_label thread_label_pkey; Type: CONSTRAINT; Schema: memories; Owner: -
--

ALTER TABLE ONLY memories.thread_label
    ADD CONSTRAINT thread_label_pkey PRIMARY KEY (thread_id, label);


--
-- Name: thread_memory thread_memory_pkey; Type: CONSTRAINT; Schema: memories; Owner: -
--

ALTER TABLE ONLY memories.thread_memory
    ADD CONSTRAINT thread_memory_pkey PRIMARY KEY (thread_id, memory_id);


--
-- Name: thread_observation thread_observation_pkey; Type: CONSTRAINT; Schema: memories; Owner: -
--

ALTER TABLE ONLY memories.thread_observation
    ADD CONSTRAINT thread_observation_pkey PRIMARY KEY (id);


--
-- Name: thread thread_pkey; Type: CONSTRAINT; Schema: memories; Owner: -
--

ALTER TABLE ONLY memories.thread
    ADD CONSTRAINT thread_pkey PRIMARY KEY (id);


--
-- Name: thread_reflection_index thread_reflection_index_pkey; Type: CONSTRAINT; Schema: memories; Owner: -
--

ALTER TABLE ONLY memories.thread_reflection_index
    ADD CONSTRAINT thread_reflection_index_pkey PRIMARY KEY (memory_id);


--
-- Name: thread_relation thread_relation_pkey; Type: CONSTRAINT; Schema: memories; Owner: -
--

ALTER TABLE ONLY memories.thread_relation
    ADD CONSTRAINT thread_relation_pkey PRIMARY KEY (id);


--
-- Name: thread_time_migration_state thread_time_migration_state_pkey; Type: CONSTRAINT; Schema: memories; Owner: -
--

ALTER TABLE ONLY memories.thread_time_migration_state
    ADD CONSTRAINT thread_time_migration_state_pkey PRIMARY KEY (migration_key);


--
-- Name: tool_contribution_stats tool_contribution_stats_pkey; Type: CONSTRAINT; Schema: memories; Owner: -
--

ALTER TABLE ONLY memories.tool_contribution_stats
    ADD CONSTRAINT tool_contribution_stats_pkey PRIMARY KEY (origin_user_id, tool, contribution, error_kind);


--
-- Name: tool_outcome_stats tool_outcome_stats_pkey; Type: CONSTRAINT; Schema: memories; Owner: -
--

ALTER TABLE ONLY memories.tool_outcome_stats
    ADD CONSTRAINT tool_outcome_stats_pkey PRIMARY KEY (origin_user_id, tool, outcome);


--
-- Name: manual_collection_member_thread_id; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX manual_collection_member_thread_id ON memories.manual_collection_member USING btree (thread_id);


--
-- Name: manual_collection_owner_updated; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX manual_collection_owner_updated ON memories.manual_collection USING btree (owner_scope, updated_at DESC, id);


--
-- Name: manual_collection_user_updated; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX manual_collection_user_updated ON memories.manual_collection USING btree (user_id, updated_at DESC, id);


--
-- Name: media_object_gc_state; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX media_object_gc_state ON memories.media_object USING btree (gc_state);


--
-- Name: media_object_kind; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX media_object_kind ON memories.media_object USING btree (kind);


--
-- Name: media_object_sha256; Type: INDEX; Schema: memories; Owner: -
--

CREATE UNIQUE INDEX media_object_sha256 ON memories.media_object USING btree (sha256);


--
-- Name: memory_external_id; Type: INDEX; Schema: memories; Owner: -
--

CREATE UNIQUE INDEX memory_external_id ON memories.memory USING btree (external_id);


--
-- Name: memory_media_object_id; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX memory_media_object_id ON memories.memory USING btree (media_object_id);


--
-- Name: memory_rating_memory_id; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX memory_rating_memory_id ON memories.memory_rating USING btree (memory_id);


--
-- Name: memory_rating_memory_user; Type: INDEX; Schema: memories; Owner: -
--

CREATE UNIQUE INDEX memory_rating_memory_user ON memories.memory_rating USING btree (memory_id, user_id);


--
-- Name: memory_rating_user_id; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX memory_rating_user_id ON memories.memory_rating USING btree (user_id);


--
-- Name: memory_user_id; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX memory_user_id ON memories.memory USING btree (user_id);


--
-- Name: memory_user_memory_kind_updated_at; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX memory_user_memory_kind_updated_at ON memories.memory USING btree (user_id, memory_kind, updated_at);


--
-- Name: operator_decision_candidate_association_id; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX operator_decision_candidate_association_id ON memories.operator_decision USING btree (candidate_association_id);


--
-- Name: rat_fingerprint; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX rat_fingerprint ON memories.reflection_applied_target USING btree (mitigation_fingerprint);


--
-- Name: rf_fact_memory; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX rf_fact_memory ON memories.reflection_fact USING btree (fact_memory_id);


--
-- Name: rf_kind; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX rf_kind ON memories.reflection_fact USING btree (memory_id, fact_kind);


--
-- Name: rfm_mode; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX rfm_mode ON memories.reflection_failure_mode USING btree (mode);


--
-- Name: rfsu_thread_used; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX rfsu_thread_used ON memories.reflection_few_shot_usage USING btree (used_in_thread_id);


--
-- Name: rt_tool; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX rt_tool ON memories.reflection_tool USING btree (tool);


--
-- Name: rto_tool_contrib; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX rto_tool_contrib ON memories.reflection_tool_outcome USING btree (tool, contribution);


--
-- Name: source_thread_identity_thread_id; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX source_thread_identity_thread_id ON memories.source_thread_identity USING btree (thread_id);


--
-- Name: source_thread_identity_typed_key; Type: INDEX; Schema: memories; Owner: -
--

CREATE UNIQUE INDEX source_thread_identity_typed_key ON memories.source_thread_identity USING btree (user_id, source, identity_scope, native_id);


--
-- Name: tak_thread_id; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX tak_thread_id ON memories.thread_aggregate_key USING btree (thread_id);


--
-- Name: tcs_user_tool_contrib; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX tcs_user_tool_contrib ON memories.tool_contribution_stats USING btree (origin_user_id, tool, contribution);


--
-- Name: thread_canonical_key_key; Type: INDEX; Schema: memories; Owner: -
--

CREATE UNIQUE INDEX thread_canonical_key_key ON memories.thread_canonical_key USING btree (key);


--
-- Name: thread_default_system_memory_id; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX thread_default_system_memory_id ON memories.thread USING btree (default_system_memory_id);


--
-- Name: thread_deletion_marker_typed_key; Type: INDEX; Schema: memories; Owner: -
--

CREATE UNIQUE INDEX thread_deletion_marker_typed_key ON memories.thread_deletion_marker USING btree (user_id, source, identity_scope, native_id);


--
-- Name: thread_first_message_at; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX thread_first_message_at ON memories.thread USING btree (first_message_at);


--
-- Name: thread_group_active_canonical_key; Type: INDEX; Schema: memories; Owner: -
--

CREATE UNIQUE INDEX thread_group_active_canonical_key ON memories.thread_group USING btree (group_canonical_key) WHERE (status = 'active'::text);


--
-- Name: thread_group_audit_merge_source_group; Type: INDEX; Schema: memories; Owner: -
--

CREATE UNIQUE INDEX thread_group_audit_merge_source_group ON memories.thread_group_audit USING btree (source_group_id) WHERE (audit_type = 'merge'::text);


--
-- Name: thread_group_audit_split_source_group; Type: INDEX; Schema: memories; Owner: -
--

CREATE UNIQUE INDEX thread_group_audit_split_source_group ON memories.thread_group_audit USING btree (source_group_id) WHERE (audit_type = 'split'::text);


--
-- Name: thread_group_audit_target_group_id; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX thread_group_audit_target_group_id ON memories.thread_group_audit USING btree (target_group_id);


--
-- Name: thread_group_candidate_association_candidate_group_id; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX thread_group_candidate_association_candidate_group_id ON memories.thread_group_candidate_association USING btree (candidate_group_id);


--
-- Name: thread_group_candidate_association_observation_id; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX thread_group_candidate_association_observation_id ON memories.thread_group_candidate_association USING btree (selected_observation_id);


--
-- Name: thread_group_candidate_association_parent_thread_id; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX thread_group_candidate_association_parent_thread_id ON memories.thread_group_candidate_association USING btree (candidate_parent_thread_id);


--
-- Name: thread_group_candidate_association_state; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX thread_group_candidate_association_state ON memories.thread_group_candidate_association USING btree (state);


--
-- Name: thread_group_candidate_association_subject_identity; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX thread_group_candidate_association_subject_identity ON memories.thread_group_candidate_association USING btree (subject_owner_scope, subject_source, subject_identity_scope_value, subject_native_id);


--
-- Name: thread_group_candidate_association_subject_thread_id; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX thread_group_candidate_association_subject_thread_id ON memories.thread_group_candidate_association USING btree (subject_thread_id);


--
-- Name: thread_group_candidate_association_typed_subject_identity; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX thread_group_candidate_association_typed_subject_identity ON memories.thread_group_candidate_association USING btree (subject_user_id, subject_source, subject_identity_scope_value, subject_native_id);


--
-- Name: thread_group_event_outbox_event_type_created_at; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX thread_group_event_outbox_event_type_created_at ON memories.thread_group_event_outbox USING btree (event_type, created_at);


--
-- Name: thread_group_event_outbox_group_id; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX thread_group_event_outbox_group_id ON memories.thread_group_event_outbox USING btree (group_id);


--
-- Name: thread_group_event_outbox_operation_id; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX thread_group_event_outbox_operation_id ON memories.thread_group_event_outbox USING btree (operation_id);


--
-- Name: thread_group_event_outbox_thread_id; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX thread_group_event_outbox_thread_id ON memories.thread_group_event_outbox USING btree (thread_id);


--
-- Name: thread_group_member_current_canonical_key; Type: INDEX; Schema: memories; Owner: -
--

CREATE UNIQUE INDEX thread_group_member_current_canonical_key ON memories.thread_group_member USING btree (thread_canonical_key) WHERE (state = ANY (ARRAY['active'::text, 'deleted'::text]));


--
-- Name: thread_group_member_group_id_state; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX thread_group_member_group_id_state ON memories.thread_group_member USING btree (group_id, state);


--
-- Name: thread_group_member_owner_scope; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX thread_group_member_owner_scope ON memories.thread_group_member USING btree (owner_scope);


--
-- Name: thread_group_member_thread_id; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX thread_group_member_thread_id ON memories.thread_group_member USING btree (thread_id);


--
-- Name: thread_group_member_user_id; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX thread_group_member_user_id ON memories.thread_group_member USING btree (user_id);


--
-- Name: thread_group_memory_relation_memory_id; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX thread_group_memory_relation_memory_id ON memories.thread_group_memory_relation USING btree (memory_id);


--
-- Name: thread_group_redirect_to_group_id; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX thread_group_redirect_to_group_id ON memories.thread_group USING btree (redirect_to_group_id);


--
-- Name: thread_group_status; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX thread_group_status ON memories.thread_group USING btree (status);


--
-- Name: thread_group_user_status_created; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX thread_group_user_status_created ON memories.thread_group USING btree (user_id, status, created_at DESC, id);


--
-- Name: thread_label_label; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX thread_label_label ON memories.thread_label USING btree (label);


--
-- Name: thread_label_thread_id; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX thread_label_thread_id ON memories.thread_label USING btree (thread_id);


--
-- Name: thread_last_message_at_id; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX thread_last_message_at_id ON memories.thread USING btree (last_message_at DESC NULLS LAST, id DESC);


--
-- Name: thread_memory_memory_id; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX thread_memory_memory_id ON memories.thread_memory USING btree (memory_id);


--
-- Name: thread_memory_thread_position; Type: INDEX; Schema: memories; Owner: -
--

CREATE UNIQUE INDEX thread_memory_thread_position ON memories.thread_memory USING btree (thread_id, "position");


--
-- Name: thread_observation_identity_evidence_key; Type: INDEX; Schema: memories; Owner: -
--

CREATE UNIQUE INDEX thread_observation_identity_evidence_key ON memories.thread_observation USING btree (subject_source, subject_identity_scope_known, subject_identity_scope_value, subject_owner_scope, subject_native_id, candidate_parent_present, candidate_parent_source, candidate_parent_identity_scope_known, candidate_parent_identity_scope_value, candidate_parent_owner_scope, candidate_parent_native_id, evidence_kind, evidence_fingerprint);


--
-- Name: thread_observation_state; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX thread_observation_state ON memories.thread_observation USING btree (state);


--
-- Name: thread_observation_typed_identity_evidence_key; Type: INDEX; Schema: memories; Owner: -
--

CREATE UNIQUE INDEX thread_observation_typed_identity_evidence_key ON memories.thread_observation USING btree (subject_source, subject_identity_scope_known, subject_identity_scope_value, subject_user_id, subject_native_id, candidate_parent_present, candidate_parent_source, candidate_parent_identity_scope_known, candidate_parent_identity_scope_value, candidate_parent_user_id, candidate_parent_native_id, evidence_kind, evidence_fingerprint);


--
-- Name: thread_relation_active_child_canonical_key; Type: INDEX; Schema: memories; Owner: -
--

CREATE UNIQUE INDEX thread_relation_active_child_canonical_key ON memories.thread_relation USING btree (child_thread_canonical_key) WHERE (state = 'active'::text);


--
-- Name: thread_relation_child_canonical_key_state; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX thread_relation_child_canonical_key_state ON memories.thread_relation USING btree (child_thread_canonical_key, state);


--
-- Name: thread_relation_child_thread_id; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX thread_relation_child_thread_id ON memories.thread_relation USING btree (child_thread_id);


--
-- Name: thread_relation_parent_canonical_key_state; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX thread_relation_parent_canonical_key_state ON memories.thread_relation USING btree (parent_thread_canonical_key, state);


--
-- Name: thread_relation_parent_thread_id; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX thread_relation_parent_thread_id ON memories.thread_relation USING btree (parent_thread_id);


--
-- Name: thread_relation_selected_observation_id; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX thread_relation_selected_observation_id ON memories.thread_relation USING btree (selected_observation_id);


--
-- Name: thread_relation_selected_operator_decision_id; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX thread_relation_selected_operator_decision_id ON memories.thread_relation USING btree (selected_operator_decision_id);


--
-- Name: thread_updated_at; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX thread_updated_at ON memories.thread USING btree (updated_at);


--
-- Name: thread_user_first_message_at; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX thread_user_first_message_at ON memories.thread USING btree (user_id, first_message_at);


--
-- Name: thread_user_id; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX thread_user_id ON memories.thread USING btree (user_id);


--
-- Name: thread_user_last_message_at_id; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX thread_user_last_message_at_id ON memories.thread USING btree (user_id, last_message_at DESC NULLS LAST, id DESC);


--
-- Name: thread_user_memory_kind_first_message_at; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX thread_user_memory_kind_first_message_at ON memories.thread USING btree (user_id, memory_kind, first_message_at);


--
-- Name: thread_user_memory_kind_last_message_at_id; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX thread_user_memory_kind_last_message_at_id ON memories.thread USING btree (user_id, memory_kind, last_message_at DESC NULLS LAST, id DESC);


--
-- Name: thread_user_memory_kind_updated_at; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX thread_user_memory_kind_updated_at ON memories.thread USING btree (user_id, memory_kind, updated_at);


--
-- Name: tos_user_tool; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX tos_user_tool ON memories.tool_outcome_stats USING btree (origin_user_id, tool);


--
-- Name: tri_experiment; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX tri_experiment ON memories.thread_reflection_index USING btree (experiment_id, experiment_variant);


--
-- Name: tri_origin_thread_created; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX tri_origin_thread_created ON memories.thread_reflection_index USING btree (origin_thread_id, created_at DESC);


--
-- Name: tri_origin_thread_id; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX tri_origin_thread_id ON memories.thread_reflection_index USING btree (origin_thread_id);


--
-- Name: tri_prompt_version; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX tri_prompt_version ON memories.thread_reflection_index USING btree (prompt_version, created_at);


--
-- Name: tri_reflection_aspect; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX tri_reflection_aspect ON memories.thread_reflection_index USING btree (reflection_aspect);


--
-- Name: tri_target_model; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX tri_target_model ON memories.thread_reflection_index USING btree (target_model_version);


--
-- Name: tri_task_category; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX tri_task_category ON memories.thread_reflection_index USING btree (task_category);


--
-- Name: tri_thread_id; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX tri_thread_id ON memories.thread_reflection_index USING btree (thread_id);


--
-- Name: tri_user_channel; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX tri_user_channel ON memories.thread_reflection_index USING btree (origin_user_id, origin_channel);


--
-- Name: tri_user_intent_status; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX tri_user_intent_status ON memories.thread_reflection_index USING btree (origin_user_id, intent_embedding_status);


--
-- Name: tri_user_outcome_score; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX tri_user_outcome_score ON memories.thread_reflection_index USING btree (origin_user_id, outcome, score);


--
-- Name: tri_user_summary_status; Type: INDEX; Schema: memories; Owner: -
--

CREATE INDEX tri_user_summary_status ON memories.thread_reflection_index USING btree (origin_user_id, summary_embedding_status);


--
-- PostgreSQL database dump complete
--


