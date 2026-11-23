PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;
CREATE TABLE thread (
    id BIGINT NOT NULL PRIMARY KEY,
    default_system_memory_id BIGINT,
    user_id BIGINT NOT NULL,
    description TEXT,
    channel TEXT,
    embedding BLOB,
    embedding_dim INT,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    metadata JSON,
    memory_kind INT NOT NULL
, first_message_at BIGINT, last_message_at BIGINT);
CREATE TABLE memory (
    id BIGINT NOT NULL PRIMARY KEY,
    parent_ids JSON,
    user_id BIGINT NOT NULL,
    content TEXT NOT NULL,
    content_type INT NOT NULL,
    params JSON,
    metadata JSON,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    role INT NOT NULL DEFAULT 0,
    external_id VARCHAR(512),
    media_object_id BIGINT,
    memory_kind INT NOT NULL
);
CREATE TABLE media_object (
    id BIGINT NOT NULL PRIMARY KEY,
    kind INT NOT NULL,
    media_type VARCHAR(64) NOT NULL,
    byte_size BIGINT,
    sha256 CHAR(64),
    width INT,
    height INT,
    duration_ms BIGINT,
    storage_backend VARCHAR(16) NOT NULL,
    storage_uri TEXT,
    alt TEXT,
    ref_count BIGINT NOT NULL DEFAULT 0,
    gc_state INT NOT NULL DEFAULT 0,
    metadata JSON,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL
);
CREATE TABLE memory_rating (
    id BIGINT NOT NULL PRIMARY KEY,
    memory_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    rating REAL NOT NULL,
    metadata JSON,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL
);
CREATE TABLE thread_memory (
    thread_id BIGINT NOT NULL,
    memory_id BIGINT NOT NULL,
    position INT NOT NULL,
    created_at BIGINT NOT NULL,
    PRIMARY KEY (thread_id, memory_id)
);
CREATE TABLE thread_label (
    thread_id BIGINT NOT NULL,
    label VARCHAR(512) NOT NULL,
    created_at BIGINT NOT NULL,
    PRIMARY KEY (thread_id, label)
);
CREATE TABLE thread_reflection_index (
    memory_id BIGINT NOT NULL PRIMARY KEY, thread_id BIGINT NOT NULL,
    origin_thread_id BIGINT NOT NULL, origin_user_id BIGINT NOT NULL,
    origin_channel TEXT, outcome INT NOT NULL, score REAL NOT NULL,
    score_self REAL NOT NULL, score_heuristic REAL NOT NULL,
    task_category INT NOT NULL, reflection_aspect INT NOT NULL,
    dataset_quality INT NOT NULL DEFAULT 1,
    summary_embedding_status INT NOT NULL DEFAULT 1,
    summary_embedding_error TEXT, intent_embedding_status INT NOT NULL DEFAULT 1,
    intent_embedding_error TEXT, prompt_version VARCHAR(32) NOT NULL,
    target_model_version VARCHAR(128), experiment_id VARCHAR(128),
    experiment_variant VARCHAR(128), previous_reflection_id BIGINT,
    pinned BOOLEAN NOT NULL DEFAULT 0, is_recurrence BOOLEAN NOT NULL DEFAULT 0,
    mitigation_fingerprint VARCHAR(64), created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL
);
CREATE TABLE reflection_failure_mode (
    memory_id BIGINT NOT NULL, mode VARCHAR(64) NOT NULL,
    PRIMARY KEY (memory_id, mode)
);
CREATE TABLE reflection_tool (
    memory_id BIGINT NOT NULL, tool VARCHAR(128) NOT NULL,
    PRIMARY KEY (memory_id, tool)
);
CREATE TABLE reflection_tool_outcome (
    memory_id BIGINT NOT NULL, tool VARCHAR(128) NOT NULL,
    contribution INT NOT NULL, error_kind VARCHAR(128) NOT NULL DEFAULT '',
    PRIMARY KEY (memory_id, tool, contribution, error_kind)
);
CREATE TABLE reflection_fact (
    memory_id BIGINT NOT NULL, fact_memory_id BIGINT NOT NULL,
    fact_kind INT NOT NULL, turn_index INT NOT NULL DEFAULT 0, weight REAL,
    note TEXT, links_json JSON, PRIMARY KEY (memory_id, fact_memory_id, fact_kind)
);
CREATE TABLE reflection_applied_target (
    memory_id BIGINT NOT NULL, target VARCHAR(256) NOT NULL,
    mitigation_fingerprint VARCHAR(64), applied_at BIGINT NOT NULL,
    PRIMARY KEY (memory_id, target)
);
CREATE TABLE reflection_few_shot_usage (
    memory_id BIGINT NOT NULL, used_in_thread_id BIGINT NOT NULL,
    used_at BIGINT NOT NULL, PRIMARY KEY (memory_id, used_in_thread_id)
);
CREATE TABLE tool_outcome_stats (
    origin_user_id BIGINT NOT NULL, tool VARCHAR(128) NOT NULL,
    outcome INT NOT NULL, count BIGINT NOT NULL DEFAULT 0,
    last_updated_at BIGINT NOT NULL, PRIMARY KEY (origin_user_id, tool, outcome)
);
CREATE TABLE tool_contribution_stats (
    origin_user_id BIGINT NOT NULL, tool VARCHAR(128) NOT NULL,
    contribution INT NOT NULL, error_kind VARCHAR(128) NOT NULL DEFAULT '',
    count BIGINT NOT NULL DEFAULT 0, last_updated_at BIGINT NOT NULL,
    PRIMARY KEY (origin_user_id, tool, contribution, error_kind)
);
CREATE TABLE failure_mode_dictionary (
    mode VARCHAR(64) NOT NULL PRIMARY KEY, description TEXT NOT NULL,
    severity INT NOT NULL, category INT NOT NULL, default_mitigation TEXT NOT NULL
);
INSERT INTO failure_mode_dictionary VALUES('tool_misuse','Wrong tool selection or argument shape',3,1,'Consult tool docs/schemas before invocation and validate arguments with a minimal trial run before executing for real.');
INSERT INTO failure_mode_dictionary VALUES('loop','Repeating identical tool calls without progress',3,1,'Self-abort when the same tool fails three times in a row with equivalent arguments and request user guidance.');
INSERT INTO failure_mode_dictionary VALUES('scope_drift','Drifting away from the originally stated goal',2,1,'Restate the task intent at start and self-check against it every five turns, surfacing any drift to the user.');
INSERT INTO failure_mode_dictionary VALUES('hallucination','Asserting unverified factual claims',3,1,'Cross-check verifiable facts (API names, file paths, command syntax) against an external source before stating them.');
INSERT INTO failure_mode_dictionary VALUES('context_overflow','Approaching context-window saturation',2,1,'When context usage exceeds 70% of the limit, summarise older turns proactively and drop irrelevant history.');
INSERT INTO failure_mode_dictionary VALUES('data_loss','Destructive operation may corrupt user assets',4,2,'Always seek explicit confirmation before destructive operations (delete, overwrite, force push) and surface reversibility.');
INSERT INTO failure_mode_dictionary VALUES('permission_issue','Operating without sufficient privileges',4,2,'Run dry-runs or pre-checks for permission-sensitive actions and never silently swallow permission errors.');
INSERT INTO failure_mode_dictionary VALUES('ambiguous_instruction','Ambiguous user instructions',2,3,'Surface up to three plausible interpretations and ask the user to disambiguate before starting work.');
INSERT INTO failure_mode_dictionary VALUES('conflicting_requirements','Mutually conflicting requirements',2,3,'Restate the conflict explicitly and ask the user to set priorities before proceeding.');
INSERT INTO failure_mode_dictionary VALUES('missing_context','Required information missing',2,3,'List the missing pieces as a bullet checklist and confirm with the user before starting.');
INSERT INTO failure_mode_dictionary VALUES('misleading_premise','User instruction starts from a false premise',3,3,'When premise validity is doubtful, verify what is verifiable then ask the user to confirm the rest.');
INSERT INTO failure_mode_dictionary VALUES('goal_drift_by_user','User changes goal mid-task',1,3,'On detecting a mid-task goal change, surface the relationship to the prior goal and confirm whether to replace or stack it.');
INSERT INTO failure_mode_dictionary VALUES('tool_unavailable','Required tool unavailable',1,4,'Offer an alternate tool or manual workaround and confirm with the user whether to proceed.');
INSERT INTO failure_mode_dictionary VALUES('external_service_failure','External service request failure',1,4,'Use exponential backoff up to three retries; on persistent failure, explain the situation and ask the user how to proceed.');
INSERT INTO failure_mode_dictionary VALUES('rate_limit','Hit external rate limit',1,4,'Wait the indicated cooldown then retry, or surface an alternate resource fallback for the user to choose from.');
INSERT INTO failure_mode_dictionary VALUES('OTHER','Unclassified failure mode (use failure_modes_other for free text)',1,5,'No default mitigation; fall back to free-text guidance recorded in failure_modes_other.');
CREATE TABLE failure_signature_indicator_norm (
    indicator_name VARCHAR(64) NOT NULL PRIMARY KEY, max_value REAL NOT NULL,
    weight REAL NOT NULL DEFAULT 1.0
);
INSERT INTO failure_signature_indicator_norm VALUES('same_tool_repeated_count',20.0,1.0);
INSERT INTO failure_signature_indicator_norm VALUES('consecutive_errors',10.0,1.0);
INSERT INTO failure_signature_indicator_norm VALUES('no_state_change_turns',20.0,1.0);
INSERT INTO failure_signature_indicator_norm VALUES('tool_calls_per_turn_ratio',10.0,1.0);
INSERT INTO failure_signature_indicator_norm VALUES('compact_boundary_count',10.0,1.0);
INSERT INTO failure_signature_indicator_norm VALUES('user_clarification_count',10.0,1.0);
INSERT INTO failure_signature_indicator_norm VALUES('turn_count_at_detection',500.0,1.0);
INSERT INTO failure_signature_indicator_norm VALUES('elapsed_ms_at_detection',3600000.0,1.0);
CREATE TABLE thread_aggregate_key (
    user_id BIGINT NOT NULL, labels_hash CHAR(64) NOT NULL,
    thread_id BIGINT NOT NULL, created_at BIGINT NOT NULL,
    PRIMARY KEY (user_id, labels_hash)
);
CREATE TABLE `atlas_schema_revisions` (`version` text NOT NULL, `description` text NOT NULL, `type` integer NOT NULL DEFAULT (2), `applied` integer NOT NULL DEFAULT (0), `total` integer NOT NULL DEFAULT (0), `executed_at` datetime NOT NULL, `execution_time` integer NOT NULL, `error` text NULL, `error_stmt` text NULL, `hash` text NOT NULL, `partial_hashes` json NULL, `operator_version` text NOT NULL, PRIMARY KEY (`version`));
INSERT INTO atlas_schema_revisions VALUES('20260803000001','adoption_baseline',1,0,0,'2026-11-22 21:40:23.502000142+09:00',0,'','','',x'6e756c6c','Atlas CLI v1.3.0');
INSERT INTO atlas_schema_revisions VALUES('20260803000002','thread_message_times_schema',2,9,9,'2026-11-22 21:40:23.51269071+09:00',86323,'','','ztKNN2k6fe061EkReFmi101OHQdQBFoOJeymhJoRfIY=',x'6e756c6c','Atlas CLI v1.3.0');
INSERT INTO atlas_schema_revisions VALUES('20260803000003','schema_contract',2,3,3,'2026-11-22 21:40:23.525354509+09:00',90406,'','','VR0ZoYAk6dTG3k3V/d6Ie76XEsEIVIVLBV3RMSP1JRs=',x'6e756c6c','Atlas CLI v1.3.0');
INSERT INTO atlas_schema_revisions VALUES('20260920000001','thread_group_schema',2,48,48,'2026-11-22 21:40:23.539251889+09:00',83655,'','','LJrUfL+MHgy8MQxp50pbxTgAAfg/JYDoBgIQPOI00YI=',x'6e756c6c','Atlas CLI v1.3.0');
INSERT INTO atlas_schema_revisions VALUES('20260926000001','thread_group_memory_relation',2,3,3,'2026-11-22 21:40:23.559079568+09:00',73327,'','','xPD577FP/nxlPlwsj1yhV+ZA0IPxC6z+IZz0iNPK7eM=',x'6e756c6c','Atlas CLI v1.3.0');
INSERT INTO atlas_schema_revisions VALUES('20260930000001','thread_group_user_ids',2,29,29,'2026-11-22 21:40:23.570470786+09:00',95540,'','','5mNZbAes+LIoQ+RilKK9l1leMLUllyX0A6Df17yw7kY=',x'6e756c6c','Atlas CLI v1.3.0');
INSERT INTO atlas_schema_revisions VALUES('20261009000001','embedding_storage_identity',2,3,3,'2026-11-22 21:40:23.594674519+09:00',73359,'','','oCuRoWl71QdxQSSB38VopbQ+Ag47UKHaLUiEDZqosvo=',x'6e756c6c','Atlas CLI v1.3.0');
CREATE TABLE thread_time_migration_state (
    migration_key TEXT PRIMARY KEY,
    rdb_completed_at BIGINT NULL,
    vector_status TEXT NOT NULL,
    staging_table_name TEXT NULL,
    vector_completed_at BIGINT NULL,
    CHECK (migration_key = 'thread-time-fields-v1'),
    CHECK (vector_status IN ('PENDING', 'STAGED', 'SWITCHING', 'NOT_REQUIRED', 'COMPLETED'))
);
CREATE TABLE memories_schema_contract (
    contract_key TEXT PRIMARY KEY CHECK (contract_key = 'rdb_schema'),
    version TEXT NOT NULL CHECK (length(version) = 14)
);
INSERT INTO memories_schema_contract VALUES('rdb_schema','20261009000001');
CREATE TABLE memories_data_migration_task_state (
    task_identity TEXT PRIMARY KEY,
    canonical_definition_digest TEXT NOT NULL,
    state TEXT NOT NULL CHECK (state IN ('pending', 'running', 'completed', 'failed')),
    execution_id TEXT,
    holder_id TEXT,
    fencing_token BIGINT NOT NULL DEFAULT 0,
    heartbeat_at BIGINT,
    lease_expires_at BIGINT,
    attempt_count BIGINT NOT NULL DEFAULT 0,
    checkpoint TEXT,
    failure_classification TEXT,
    started_at BIGINT,
    updated_at BIGINT NOT NULL,
    completed_at BIGINT
);
INSERT INTO memories_data_migration_task_state VALUES('thread-groups-canonical-keys-v1@2','ad2a56fbae48549f6a7af61182b4358eb1e3ece4fb12f4b1da1d0904044312b2','completed','3731874-thread-groups-canonical-keys-v1@2-1795351224425','memories-db-migrate',1,NULL,NULL,1,'{"format":"thread-groups-canonical-keys-v1@2/checkpoint-v2","task_identity":"thread-groups-canonical-keys-v1@2","canonical_definition_digest":"ad2a56fbae48549f6a7af61182b4358eb1e3ece4fb12f4b1da1d0904044312b2","payload":{"phase":"verified","source_backed_last_thread_id":null,"manual_last_thread_id":null,"migration_version":"20260930000001","adapter_version":"canonical-serialization-v2","policy_version":"thread-group-policy-v1"}}',NULL,1795351224426,1795351224510,1795351224510);
INSERT INTO memories_data_migration_task_state VALUES('thread-groups-user-ids-v1@4','744a996decd4a48522168a89aae2c460d2ec522ce9f84ea25abc0951001a3501','completed','3731874-thread-groups-user-ids-v1@4-1795351224528','memories-db-migrate',1,NULL,NULL,1,NULL,NULL,1795351224530,1795351224530,1795351224530);
INSERT INTO memories_data_migration_task_state VALUES('thread-message-times-v1@1','d2fe9980cf1048115967a8ab019a53c4422b284f3a8cf185ce2c55988506ed1d','completed','3731874-thread-message-times-v1@1-1795351224547','memories-db-migrate',1,NULL,NULL,1,'{"format":"thread-message-times-v1@1/checkpoint-v1","task_identity":"thread-message-times-v1@1","canonical_definition_digest":"d2fe9980cf1048115967a8ab019a53c4422b284f3a8cf185ce2c55988506ed1d","payload":{"rdb_phase":"verified","rdb_last_thread_id":null,"vector_phase":"completed","canonical_table_name":null,"staging_table_name":null,"staging_origin":null,"staging_cleanup_started":false,"index_outcomes":null,"vector_outcome":"not_required"}}',NULL,1795351224548,1795351224681,1795351224681);
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
    updated_at BIGINT NOT NULL, user_id BIGINT,
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
    updated_at BIGINT NOT NULL, parent_user_id BIGINT, child_user_id BIGINT,
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
CREATE TABLE thread_observation (
    id BIGINT NOT NULL PRIMARY KEY,
    subject_source TEXT NOT NULL,
    subject_identity_scope_known BOOLEAN NOT NULL,
    subject_identity_scope_value TEXT NOT NULL DEFAULT '',
    subject_owner_scope TEXT NOT NULL,
    subject_native_id TEXT NOT NULL,
    candidate_parent_present BOOLEAN NOT NULL,
    candidate_parent_source TEXT NOT NULL DEFAULT '',
    candidate_parent_identity_scope_known BOOLEAN NOT NULL DEFAULT 0,
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
    updated_at BIGINT NOT NULL, subject_user_id BIGINT, candidate_parent_user_id BIGINT,
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
, subject_user_id BIGINT);
CREATE TABLE source_thread_identity (
    owner_scope TEXT NOT NULL,
    source TEXT NOT NULL,
    identity_scope TEXT NOT NULL,
    native_id TEXT NOT NULL,
    thread_id BIGINT NOT NULL,
    resolution_state TEXT NOT NULL CHECK (resolution_state = 'resolved'),
    first_seen_at BIGINT NOT NULL,
    last_seen_at BIGINT NOT NULL, user_id BIGINT,
    PRIMARY KEY (owner_scope, source, identity_scope, native_id)
);
CREATE TABLE thread_canonical_key (
    thread_id BIGINT NOT NULL PRIMARY KEY,
    owner_scope TEXT NOT NULL,
    key CHAR(64) NOT NULL,
    origin TEXT NOT NULL CHECK (origin IN ('source_identity', 'creation_uuid', 'backfill_mapping')),
    assigned_at BIGINT NOT NULL
, user_id BIGINT);
CREATE TABLE thread_deletion_marker (
    owner_scope TEXT NOT NULL,
    source TEXT NOT NULL,
    identity_scope TEXT NOT NULL,
    native_id TEXT NOT NULL,
    forbid_reimport BOOLEAN NOT NULL,
    recursive BOOLEAN NOT NULL,
    actor_id TEXT NOT NULL,
    reason TEXT,
    deleted_at BIGINT NOT NULL, user_id BIGINT, thread_canonical_key CHAR(64),
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
, user_id BIGINT);
CREATE TABLE manual_collection (
    id BIGINT NOT NULL PRIMARY KEY,
    owner_scope TEXT NOT NULL,
    title TEXT NOT NULL,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL
, user_id BIGINT);
CREATE TABLE manual_collection_member (
    collection_id BIGINT NOT NULL,
    thread_id BIGINT NOT NULL,
    owner_scope TEXT NOT NULL, user_id BIGINT,
    PRIMARY KEY (collection_id, thread_id)
);
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
    payload JSON NOT NULL,
    created_at BIGINT NOT NULL
, user_id BIGINT);
CREATE TABLE thread_group_audit (
    id BIGINT NOT NULL PRIMARY KEY,
    audit_type TEXT NOT NULL CHECK (audit_type IN ('merge', 'split')),
    source_group_id BIGINT NOT NULL,
    target_group_id BIGINT,
    actor_id TEXT NOT NULL,
    reason TEXT NOT NULL,
    canonical_partition JSON,
    successor_group_ids JSON,
    created_at BIGINT NOT NULL,
    CHECK ((audit_type = 'merge' AND target_group_id IS NOT NULL
            AND canonical_partition IS NULL AND successor_group_ids IS NULL)
        OR (audit_type = 'split' AND target_group_id IS NULL
            AND canonical_partition IS NOT NULL AND successor_group_ids IS NOT NULL))
);
CREATE TABLE thread_group_memory_relation (
    group_id BIGINT NOT NULL,
    memory_id BIGINT NOT NULL,
    purpose TEXT NOT NULL CHECK (length(purpose) > 0),
    on_group_delete TEXT NOT NULL CHECK (on_group_delete IN ('retain', 'delete')),
    created_at BIGINT NOT NULL,
    PRIMARY KEY (group_id, memory_id)
);
CREATE TABLE IF NOT EXISTS "thread_group" (
    id BIGINT NOT NULL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    group_canonical_key CHAR(64) NOT NULL,
    title TEXT,
    status TEXT NOT NULL CHECK (status IN ('active', 'redirected', 'split')),
    grouping_authority TEXT NOT NULL CHECK (grouping_authority IN ('reconciler', 'operator')),
    redirect_to_group_id BIGINT,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL,
    CHECK ((status = 'redirected' AND redirect_to_group_id IS NOT NULL)
        OR (status <> 'redirected' AND redirect_to_group_id IS NULL)),
    CHECK (redirect_to_group_id <> id)
);
CREATE TABLE memories_storage_identity (
    identity_key TEXT PRIMARY KEY CHECK (identity_key = 'rdb'),
    storage_id TEXT NOT NULL CHECK (length(storage_id) = 32)
);
INSERT INTO memories_storage_identity VALUES('rdb','3c5161b9aabf5d71727728e45c872c1c');
CREATE INDEX thread_default_system_memory_id ON thread (default_system_memory_id);
CREATE INDEX thread_user_id ON thread (user_id);
CREATE INDEX thread_updated_at ON thread (updated_at);
CREATE INDEX thread_user_memory_kind_updated_at ON thread (user_id, memory_kind, updated_at);
CREATE INDEX memory_user_id ON memory (user_id);
CREATE INDEX memory_user_memory_kind_updated_at ON memory (user_id, memory_kind, updated_at);
CREATE UNIQUE INDEX memory_external_id ON memory (external_id);
CREATE INDEX memory_media_object_id ON memory (media_object_id);
CREATE UNIQUE INDEX media_object_sha256 ON media_object (sha256);
CREATE INDEX media_object_kind ON media_object (kind);
CREATE INDEX media_object_gc_state ON media_object (gc_state);
CREATE UNIQUE INDEX memory_rating_memory_user ON memory_rating (memory_id, user_id);
CREATE INDEX memory_rating_memory_id ON memory_rating (memory_id);
CREATE INDEX memory_rating_user_id ON memory_rating (user_id);
CREATE UNIQUE INDEX thread_memory_thread_position ON thread_memory (thread_id, position);
CREATE INDEX thread_memory_memory_id ON thread_memory (memory_id);
CREATE INDEX thread_label_thread_id ON thread_label (thread_id);
CREATE INDEX thread_label_label ON thread_label (label);
CREATE INDEX tri_thread_id ON thread_reflection_index (thread_id);
CREATE INDEX tri_origin_thread_id ON thread_reflection_index (origin_thread_id);
CREATE INDEX tri_user_outcome_score ON thread_reflection_index (origin_user_id, outcome, score);
CREATE INDEX tri_user_channel ON thread_reflection_index (origin_user_id, origin_channel);
CREATE INDEX tri_task_category ON thread_reflection_index (task_category);
CREATE INDEX tri_reflection_aspect ON thread_reflection_index (reflection_aspect);
CREATE INDEX tri_prompt_version ON thread_reflection_index (prompt_version, created_at);
CREATE INDEX tri_target_model ON thread_reflection_index (target_model_version);
CREATE INDEX tri_experiment ON thread_reflection_index (experiment_id, experiment_variant);
CREATE INDEX tri_user_summary_status ON thread_reflection_index (origin_user_id, summary_embedding_status);
CREATE INDEX tri_user_intent_status ON thread_reflection_index (origin_user_id, intent_embedding_status);
CREATE INDEX tri_origin_thread_created ON thread_reflection_index (origin_thread_id, created_at DESC);
CREATE INDEX rfm_mode ON reflection_failure_mode (mode);
CREATE INDEX rt_tool ON reflection_tool (tool);
CREATE INDEX rto_tool_contrib ON reflection_tool_outcome (tool, contribution);
CREATE INDEX rf_kind ON reflection_fact (memory_id, fact_kind);
CREATE INDEX rf_fact_memory ON reflection_fact (fact_memory_id);
CREATE INDEX rat_fingerprint ON reflection_applied_target (mitigation_fingerprint);
CREATE INDEX rfsu_thread_used ON reflection_few_shot_usage (used_in_thread_id);
CREATE INDEX tos_user_tool ON tool_outcome_stats (origin_user_id, tool);
CREATE INDEX tcs_user_tool_contrib ON tool_contribution_stats (origin_user_id, tool, contribution);
CREATE INDEX tak_thread_id ON thread_aggregate_key (thread_id);
CREATE INDEX thread_user_memory_kind_last_message_at_id
    ON thread (user_id, memory_kind, last_message_at DESC, id DESC);
CREATE INDEX thread_user_last_message_at_id
    ON thread (user_id, last_message_at DESC, id DESC);
CREATE INDEX thread_last_message_at_id ON thread (last_message_at DESC, id DESC);
CREATE INDEX thread_user_memory_kind_first_message_at
    ON thread (user_id, memory_kind, first_message_at);
CREATE INDEX thread_user_first_message_at ON thread (user_id, first_message_at);
CREATE INDEX thread_first_message_at ON thread (first_message_at);
CREATE UNIQUE INDEX thread_group_member_current_canonical_key
    ON thread_group_member (thread_canonical_key) WHERE state IN ('active', 'deleted');
CREATE INDEX thread_group_member_group_id_state ON thread_group_member (group_id, state);
CREATE INDEX thread_group_member_thread_id ON thread_group_member (thread_id);
CREATE INDEX thread_group_member_owner_scope ON thread_group_member (owner_scope);
CREATE UNIQUE INDEX thread_relation_active_child_canonical_key
    ON thread_relation (child_thread_canonical_key) WHERE state = 'active';
CREATE INDEX thread_relation_parent_canonical_key_state ON thread_relation (parent_thread_canonical_key, state);
CREATE INDEX thread_relation_child_canonical_key_state ON thread_relation (child_thread_canonical_key, state);
CREATE INDEX thread_relation_parent_thread_id ON thread_relation (parent_thread_id);
CREATE INDEX thread_relation_child_thread_id ON thread_relation (child_thread_id);
CREATE INDEX thread_relation_selected_observation_id ON thread_relation (selected_observation_id);
CREATE INDEX thread_relation_selected_operator_decision_id ON thread_relation (selected_operator_decision_id);
CREATE UNIQUE INDEX thread_observation_identity_evidence_key ON thread_observation (
    subject_source, subject_identity_scope_known, subject_identity_scope_value,
    subject_owner_scope, subject_native_id, candidate_parent_present,
    candidate_parent_source, candidate_parent_identity_scope_known,
    candidate_parent_identity_scope_value, candidate_parent_owner_scope,
    candidate_parent_native_id, evidence_kind, evidence_fingerprint
);
CREATE INDEX thread_observation_state ON thread_observation (state);
CREATE INDEX thread_group_candidate_association_subject_identity
    ON thread_group_candidate_association (subject_owner_scope, subject_source, subject_identity_scope_value, subject_native_id);
CREATE INDEX thread_group_candidate_association_subject_thread_id ON thread_group_candidate_association (subject_thread_id);
CREATE INDEX thread_group_candidate_association_candidate_group_id ON thread_group_candidate_association (candidate_group_id);
CREATE INDEX thread_group_candidate_association_parent_thread_id ON thread_group_candidate_association (candidate_parent_thread_id);
CREATE INDEX thread_group_candidate_association_observation_id ON thread_group_candidate_association (selected_observation_id);
CREATE INDEX thread_group_candidate_association_state ON thread_group_candidate_association (state);
CREATE INDEX source_thread_identity_thread_id ON source_thread_identity (thread_id);
CREATE UNIQUE INDEX thread_canonical_key_key ON thread_canonical_key (key);
CREATE INDEX operator_decision_candidate_association_id ON operator_decision (candidate_association_id);
CREATE INDEX manual_collection_owner_updated ON manual_collection (owner_scope, updated_at DESC, id);
CREATE INDEX manual_collection_member_thread_id ON manual_collection_member (thread_id);
CREATE INDEX thread_group_event_outbox_operation_id ON thread_group_event_outbox (operation_id);
CREATE INDEX thread_group_event_outbox_event_type_created_at ON thread_group_event_outbox (event_type, created_at);
CREATE INDEX thread_group_event_outbox_group_id ON thread_group_event_outbox (group_id);
CREATE INDEX thread_group_event_outbox_thread_id ON thread_group_event_outbox (thread_id);
CREATE UNIQUE INDEX thread_group_audit_merge_source_group
    ON thread_group_audit (source_group_id) WHERE audit_type = 'merge';
CREATE UNIQUE INDEX thread_group_audit_split_source_group
    ON thread_group_audit (source_group_id) WHERE audit_type = 'split';
CREATE INDEX thread_group_audit_target_group_id ON thread_group_audit (target_group_id);
CREATE INDEX thread_group_memory_relation_memory_id
    ON thread_group_memory_relation (memory_id);
CREATE UNIQUE INDEX thread_group_active_canonical_key
    ON thread_group (group_canonical_key) WHERE status = 'active';
CREATE INDEX thread_group_status ON thread_group (status);
CREATE INDEX thread_group_user_status_created
    ON thread_group (user_id, status, created_at DESC, id ASC);
CREATE INDEX thread_group_redirect_to_group_id ON thread_group (redirect_to_group_id);
CREATE INDEX thread_group_member_user_id ON thread_group_member (user_id);
CREATE UNIQUE INDEX thread_observation_typed_identity_evidence_key
    ON thread_observation (
        subject_source, subject_identity_scope_known, subject_identity_scope_value,
        subject_user_id, subject_native_id, candidate_parent_present,
        candidate_parent_source, candidate_parent_identity_scope_known,
        candidate_parent_identity_scope_value, candidate_parent_user_id,
        candidate_parent_native_id, evidence_kind, evidence_fingerprint
    );
CREATE INDEX thread_group_candidate_association_typed_subject_identity
    ON thread_group_candidate_association
       (subject_user_id, subject_source, subject_identity_scope_value, subject_native_id);
CREATE UNIQUE INDEX source_thread_identity_typed_key
    ON source_thread_identity (user_id, source, identity_scope, native_id);
CREATE UNIQUE INDEX thread_deletion_marker_typed_key
    ON thread_deletion_marker (user_id, source, identity_scope, native_id);
CREATE INDEX manual_collection_user_updated
    ON manual_collection (user_id, updated_at DESC, id);
COMMIT;
