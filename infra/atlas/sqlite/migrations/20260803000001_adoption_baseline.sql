-- The adoption baseline is a complete schema snapshot for fresh SQLite databases.
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
);
CREATE INDEX thread_default_system_memory_id ON thread (default_system_memory_id);
CREATE INDEX thread_user_id ON thread (user_id);
CREATE INDEX thread_updated_at ON thread (updated_at);
CREATE INDEX thread_user_memory_kind_updated_at ON thread (user_id, memory_kind, updated_at);

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
CREATE INDEX memory_user_id ON memory (user_id);
CREATE INDEX memory_user_memory_kind_updated_at ON memory (user_id, memory_kind, updated_at);
CREATE UNIQUE INDEX memory_external_id ON memory (external_id);
CREATE INDEX memory_media_object_id ON memory (media_object_id);

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
CREATE UNIQUE INDEX media_object_sha256 ON media_object (sha256);
CREATE INDEX media_object_kind ON media_object (kind);
CREATE INDEX media_object_gc_state ON media_object (gc_state);

CREATE TABLE memory_rating (
    id BIGINT NOT NULL PRIMARY KEY,
    memory_id BIGINT NOT NULL,
    user_id BIGINT NOT NULL,
    rating REAL NOT NULL,
    metadata JSON,
    created_at BIGINT NOT NULL,
    updated_at BIGINT NOT NULL
);
CREATE UNIQUE INDEX memory_rating_memory_user ON memory_rating (memory_id, user_id);
CREATE INDEX memory_rating_memory_id ON memory_rating (memory_id);
CREATE INDEX memory_rating_user_id ON memory_rating (user_id);

CREATE TABLE thread_memory (
    thread_id BIGINT NOT NULL,
    memory_id BIGINT NOT NULL,
    position INT NOT NULL,
    created_at BIGINT NOT NULL,
    PRIMARY KEY (thread_id, memory_id)
);
CREATE UNIQUE INDEX thread_memory_thread_position ON thread_memory (thread_id, position);
CREATE INDEX thread_memory_memory_id ON thread_memory (memory_id);

CREATE TABLE thread_label (
    thread_id BIGINT NOT NULL,
    label VARCHAR(512) NOT NULL,
    created_at BIGINT NOT NULL,
    PRIMARY KEY (thread_id, label)
);
CREATE INDEX thread_label_thread_id ON thread_label (thread_id);
CREATE INDEX thread_label_label ON thread_label (label);

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

CREATE TABLE reflection_failure_mode (
    memory_id BIGINT NOT NULL, mode VARCHAR(64) NOT NULL,
    PRIMARY KEY (memory_id, mode)
);
CREATE INDEX rfm_mode ON reflection_failure_mode (mode);
CREATE TABLE reflection_tool (
    memory_id BIGINT NOT NULL, tool VARCHAR(128) NOT NULL,
    PRIMARY KEY (memory_id, tool)
);
CREATE INDEX rt_tool ON reflection_tool (tool);
CREATE TABLE reflection_tool_outcome (
    memory_id BIGINT NOT NULL, tool VARCHAR(128) NOT NULL,
    contribution INT NOT NULL, error_kind VARCHAR(128) NOT NULL DEFAULT '',
    PRIMARY KEY (memory_id, tool, contribution, error_kind)
);
CREATE INDEX rto_tool_contrib ON reflection_tool_outcome (tool, contribution);
CREATE TABLE reflection_fact (
    memory_id BIGINT NOT NULL, fact_memory_id BIGINT NOT NULL,
    fact_kind INT NOT NULL, turn_index INT NOT NULL DEFAULT 0, weight REAL,
    note TEXT, links_json JSON, PRIMARY KEY (memory_id, fact_memory_id, fact_kind)
);
CREATE INDEX rf_kind ON reflection_fact (memory_id, fact_kind);
CREATE INDEX rf_fact_memory ON reflection_fact (fact_memory_id);
CREATE TABLE reflection_applied_target (
    memory_id BIGINT NOT NULL, target VARCHAR(256) NOT NULL,
    mitigation_fingerprint VARCHAR(64), applied_at BIGINT NOT NULL,
    PRIMARY KEY (memory_id, target)
);
CREATE INDEX rat_fingerprint ON reflection_applied_target (mitigation_fingerprint);
CREATE TABLE reflection_few_shot_usage (
    memory_id BIGINT NOT NULL, used_in_thread_id BIGINT NOT NULL,
    used_at BIGINT NOT NULL, PRIMARY KEY (memory_id, used_in_thread_id)
);
CREATE INDEX rfsu_thread_used ON reflection_few_shot_usage (used_in_thread_id);
CREATE TABLE tool_outcome_stats (
    origin_user_id BIGINT NOT NULL, tool VARCHAR(128) NOT NULL,
    outcome INT NOT NULL, count BIGINT NOT NULL DEFAULT 0,
    last_updated_at BIGINT NOT NULL, PRIMARY KEY (origin_user_id, tool, outcome)
);
CREATE INDEX tos_user_tool ON tool_outcome_stats (origin_user_id, tool);
CREATE TABLE tool_contribution_stats (
    origin_user_id BIGINT NOT NULL, tool VARCHAR(128) NOT NULL,
    contribution INT NOT NULL, error_kind VARCHAR(128) NOT NULL DEFAULT '',
    count BIGINT NOT NULL DEFAULT 0, last_updated_at BIGINT NOT NULL,
    PRIMARY KEY (origin_user_id, tool, contribution, error_kind)
);
CREATE INDEX tcs_user_tool_contrib ON tool_contribution_stats (origin_user_id, tool, contribution);
CREATE TABLE failure_mode_dictionary (
    mode VARCHAR(64) NOT NULL PRIMARY KEY, description TEXT NOT NULL,
    severity INT NOT NULL, category INT NOT NULL, default_mitigation TEXT NOT NULL
);
CREATE TABLE failure_signature_indicator_norm (
    indicator_name VARCHAR(64) NOT NULL PRIMARY KEY, max_value REAL NOT NULL,
    weight REAL NOT NULL DEFAULT 1.0
);
CREATE TABLE thread_aggregate_key (
    user_id BIGINT NOT NULL, labels_hash CHAR(64) NOT NULL,
    thread_id BIGINT NOT NULL, created_at BIGINT NOT NULL,
    PRIMARY KEY (user_id, labels_hash)
);
CREATE INDEX tak_thread_id ON thread_aggregate_key (thread_id);

INSERT INTO failure_mode_dictionary (mode, description, severity, category, default_mitigation) VALUES
    ('tool_misuse', 'Wrong tool selection or argument shape', 3, 1, 'Consult tool docs/schemas before invocation and validate arguments with a minimal trial run before executing for real.'),
    ('loop', 'Repeating identical tool calls without progress', 3, 1, 'Self-abort when the same tool fails three times in a row with equivalent arguments and request user guidance.'),
    ('scope_drift', 'Drifting away from the originally stated goal', 2, 1, 'Restate the task intent at start and self-check against it every five turns, surfacing any drift to the user.'),
    ('hallucination', 'Asserting unverified factual claims', 3, 1, 'Cross-check verifiable facts (API names, file paths, command syntax) against an external source before stating them.'),
    ('context_overflow', 'Approaching context-window saturation', 2, 1, 'When context usage exceeds 70% of the limit, summarise older turns proactively and drop irrelevant history.'),
    ('data_loss', 'Destructive operation may corrupt user assets', 4, 2, 'Always seek explicit confirmation before destructive operations (delete, overwrite, force push) and surface reversibility.'),
    ('permission_issue', 'Operating without sufficient privileges', 4, 2, 'Run dry-runs or pre-checks for permission-sensitive actions and never silently swallow permission errors.'),
    ('ambiguous_instruction', 'Ambiguous user instructions', 2, 3, 'Surface up to three plausible interpretations and ask the user to disambiguate before starting work.'),
    ('conflicting_requirements', 'Mutually conflicting requirements', 2, 3, 'Restate the conflict explicitly and ask the user to set priorities before proceeding.'),
    ('missing_context', 'Required information missing', 2, 3, 'List the missing pieces as a bullet checklist and confirm with the user before starting.'),
    ('misleading_premise', 'User instruction starts from a false premise', 3, 3, 'When premise validity is doubtful, verify what is verifiable then ask the user to confirm the rest.'),
    ('goal_drift_by_user', 'User changes goal mid-task', 1, 3, 'On detecting a mid-task goal change, surface the relationship to the prior goal and confirm whether to replace or stack it.'),
    ('tool_unavailable', 'Required tool unavailable', 1, 4, 'Offer an alternate tool or manual workaround and confirm with the user whether to proceed.'),
    ('external_service_failure', 'External service request failure', 1, 4, 'Use exponential backoff up to three retries; on persistent failure, explain the situation and ask the user how to proceed.'),
    ('rate_limit', 'Hit external rate limit', 1, 4, 'Wait the indicated cooldown then retry, or surface an alternate resource fallback for the user to choose from.'),
    ('OTHER', 'Unclassified failure mode (use failure_modes_other for free text)', 1, 5, 'No default mitigation; fall back to free-text guidance recorded in failure_modes_other.');
INSERT INTO failure_signature_indicator_norm (indicator_name, max_value, weight) VALUES
    ('same_tool_repeated_count', 20.0, 1.0), ('consecutive_errors', 10.0, 1.0),
    ('no_state_change_turns', 20.0, 1.0), ('tool_calls_per_turn_ratio', 10.0, 1.0),
    ('compact_boundary_count', 10.0, 1.0), ('user_clarification_count', 10.0, 1.0),
    ('turn_count_at_detection', 500.0, 1.0), ('elapsed_ms_at_detection', 3600000.0, 1.0);
