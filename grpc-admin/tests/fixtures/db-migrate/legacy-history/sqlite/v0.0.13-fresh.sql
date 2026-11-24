PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;
CREATE TABLE `atlas_schema_revisions` (`version` text NOT NULL, `description` text NOT NULL, `type` integer NOT NULL DEFAULT (2), `applied` integer NOT NULL DEFAULT (0), `total` integer NOT NULL DEFAULT (0), `executed_at` datetime NOT NULL, `execution_time` integer NOT NULL, `error` text NULL, `error_stmt` text NULL, `hash` text NOT NULL, `partial_hashes` json NULL, `operator_version` text NOT NULL, PRIMARY KEY (`version`));
INSERT INTO atlas_schema_revisions VALUES('20260803000001','adoption_baseline',2,60,60,'2026-11-24 07:09:56.11863567+09:00',93932,'','','/v5antcBhqv47/my3h7eqWa3w7afwcTT1FCTQmt8Qa8=',x'6e756c6c','Atlas CLI v1.3.0');
INSERT INTO atlas_schema_revisions VALUES('20260803000002','thread_message_times_schema',2,9,9,'2026-11-24 07:09:56.138847725+09:00',77243,'','','ztKNN2k6fe061EkReFmi101OHQdQBFoOJeymhJoRfIY=',x'6e756c6c','Atlas CLI v1.3.0');
INSERT INTO atlas_schema_revisions VALUES('20260803000003','schema_contract',2,3,3,'2026-11-24 07:09:56.151285484+09:00',75327,'','','VR0ZoYAk6dTG3k3V/d6Ie76XEsEIVIVLBV3RMSP1JRs=',x'6e756c6c','Atlas CLI v1.3.0');
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
INSERT INTO thread VALUES(7530754539293556730,7530754539293556731,1,'週末の買い物リスト',NULL,NULL,NULL,1795471796850,1795471796871,NULL,1,1795136400000,1795136640000);
INSERT INTO thread VALUES(7530754539293556725,NULL,1,'週末の買い物リスト (別案)',NULL,NULL,NULL,1795471796872,1795471796882,NULL,1,1795136520000,1795137120000);
INSERT INTO thread VALUES(7530754539293556722,NULL,1,NULL,NULL,NULL,NULL,1795471796882,1795471796882,NULL,1,NULL,NULL);
INSERT INTO thread VALUES(7530754539293556721,NULL,2,NULL,NULL,NULL,NULL,1795471796887,1795471796893,NULL,1,1795138260000,1795138320000);
INSERT INTO thread VALUES(7530754539293556718,NULL,2,'消したメモ',NULL,NULL,NULL,1795471796898,1795471796909,NULL,1,NULL,NULL);
INSERT INTO thread VALUES(7530754539293556713,NULL,1,'lookback (main)','claude_code:8c1d2e3f-4a5b-4c6d-8e7f-9a0b1c2d3e4f',NULL,NULL,1795471796925,1795471796926,NULL,1,1795140600000,1795140780000);
INSERT INTO thread VALUES(7530754539293556708,NULL,1,'agent:claude_code, branch:main, coding_agent, dir:lookback, path:works/lookback の要約集',NULL,NULL,NULL,1795471796931,1795471796936,NULL,2,1795140600000,1795140600000);
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
INSERT INTO memory VALUES(7530754539293556731,'[]',1,'あなたは Lookback のアシスタントです。簡潔に日本語で答えてください。',0,NULL,NULL,1795136400000,1795136400000,3,NULL,NULL,1);
INSERT INTO memory VALUES(7530754539293556729,'[7530754539293556731]',1,'週末の買い物リストを作って。',0,NULL,NULL,1795136460000,1795136460000,1,NULL,NULL,1);
INSERT INTO memory VALUES(7530754539293556728,'[7530754539293556731,7530754539293556729]',1,'牛乳、卵、食パン、玉ねぎ、鶏もも肉です。',0,NULL,NULL,1795136520000,1795136520000,2,NULL,NULL,1);
INSERT INTO memory VALUES(7530754539293556727,'[7530754539293556731,7530754539293556728]',1,'飲み物も足して。',0,NULL,NULL,1795136580000,1795136580000,1,NULL,NULL,1);
INSERT INTO memory VALUES(7530754539293556726,'[7530754539293556731,7530754539293556727]',1,'麦茶のパックと炭酸水を追加しました。',0,NULL,NULL,1795136640000,1795136640000,2,NULL,NULL,1);
INSERT INTO memory VALUES(7530754539293556724,'[7530754539293556728]',1,'鶏もも肉を豚こまに変えた場合は?',0,NULL,NULL,1795137060000,1795137060000,1,NULL,NULL,1);
INSERT INTO memory VALUES(7530754539293556723,'[7530754539293556724]',1,'豚こまとキャベツで回鍋肉にできます。',0,NULL,NULL,1795137120000,1795137120000,2,NULL,NULL,1);
INSERT INTO memory VALUES(7530754539293556720,'[]',2,'明日の朝 6 時に起きるコツは?',0,NULL,NULL,1795138260000,1795138260000,1,NULL,NULL,1);
INSERT INTO memory VALUES(7530754539293556719,'[7530754539293556720]',2,'寝る 1 時間前に画面を見るのをやめ、起床時刻を毎日そろえることです。',0,NULL,NULL,1795138320000,1795138320000,2,NULL,NULL,1);
INSERT INTO memory VALUES(7530754539293556712,'[]',1,'設定画面の保存ボタンが二重送信される。',0,NULL,'{"kind":"user","session":{"cwd":"/home/user/works/lookback","git_branch":"main"},"source":"claude_code"}',1795140600000,1795140600000,1,'claude_code:1:8c1d2e3f-4a5b-4c6d-8e7f-9a0b1c2d3e4f:a1b2c3d4-0001:b0',NULL,1);
INSERT INTO memory VALUES(7530754539293556711,'[7530754539293556712]',1,'送信中はボタンを無効化します。',0,NULL,'{"kind":"assistant","session":{"cwd":"/home/user/works/lookback","git_branch":"main"},"source":"claude_code"}',1795140660000,1795140660000,2,'claude_code:1:8c1d2e3f-4a5b-4c6d-8e7f-9a0b1c2d3e4f:a1b2c3d4-0002:b0',NULL,1);
INSERT INTO memory VALUES(7530754539293556710,'[7530754539293556711]',1,'{"name":"Edit","input":{"file_path":"src/settings.tsx"}}',1,NULL,'{"kind":"tool_call","session":{"cwd":"/home/user/works/lookback","git_branch":"main"},"source":"claude_code"}',1795140720000,1795140720000,2,'claude_code:1:8c1d2e3f-4a5b-4c6d-8e7f-9a0b1c2d3e4f:a1b2c3d4-0003:b0',NULL,1);
INSERT INTO memory VALUES(7530754539293556709,'[7530754539293556710]',1,'The file src/settings.tsx has been updated.',1,NULL,'{"kind":"tool_output","session":{"cwd":"/home/user/works/lookback","git_branch":"main"},"source":"claude_code"}',1795140780000,1795140780000,4,'claude_code:1:8c1d2e3f-4a5b-4c6d-8e7f-9a0b1c2d3e4f:a1b2c3d4-0004:b0',NULL,1);
INSERT INTO memory VALUES(7530754539293556707,'[]',1,'{"category":"bugfix","key_decisions":["送信中はボタンを無効化"],"status":"completed","summary":"保存ボタンを送信中に無効化して二重送信を防いだ。","title":"設定画面の二重送信修正"}',0,NULL,'{"category":"bugfix","source_thread_id":"7530754539293556713","summary_version":"1.0"}',1795140600000,1795140600000,2,'summary:7530754539293556713',NULL,2);
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
INSERT INTO thread_memory VALUES(7530754539293556730,7530754539293556731,0,1795471796850);
INSERT INTO thread_memory VALUES(7530754539293556730,7530754539293556729,1,1795471796856);
INSERT INTO thread_memory VALUES(7530754539293556730,7530754539293556728,2,1795471796861);
INSERT INTO thread_memory VALUES(7530754539293556730,7530754539293556727,3,1795471796866);
INSERT INTO thread_memory VALUES(7530754539293556730,7530754539293556726,4,1795471796871);
INSERT INTO thread_memory VALUES(7530754539293556725,7530754539293556728,0,1795471796877);
INSERT INTO thread_memory VALUES(7530754539293556725,7530754539293556724,1,1795471796877);
INSERT INTO thread_memory VALUES(7530754539293556725,7530754539293556723,2,1795471796882);
INSERT INTO thread_memory VALUES(7530754539293556721,7530754539293556720,0,1795471796888);
INSERT INTO thread_memory VALUES(7530754539293556721,7530754539293556719,1,1795471796893);
INSERT INTO thread_memory VALUES(7530754539293556713,7530754539293556712,0,1795471796925);
INSERT INTO thread_memory VALUES(7530754539293556713,7530754539293556711,1,1795471796925);
INSERT INTO thread_memory VALUES(7530754539293556713,7530754539293556710,2,1795471796925);
INSERT INTO thread_memory VALUES(7530754539293556713,7530754539293556709,3,1795471796926);
INSERT INTO thread_memory VALUES(7530754539293556708,7530754539293556707,0,1795471796936);
CREATE TABLE thread_label (
    thread_id BIGINT NOT NULL,
    label VARCHAR(512) NOT NULL,
    created_at BIGINT NOT NULL,
    PRIMARY KEY (thread_id, label)
);
INSERT INTO thread_label VALUES(7530754539293556730,'topic:shopping',1795471796850);
INSERT INTO thread_label VALUES(7530754539293556713,'agent:claude_code',1795471796925);
INSERT INTO thread_label VALUES(7530754539293556713,'branch:main',1795471796925);
INSERT INTO thread_label VALUES(7530754539293556713,'coding_agent',1795471796925);
INSERT INTO thread_label VALUES(7530754539293556713,'dir:lookback',1795471796925);
INSERT INTO thread_label VALUES(7530754539293556713,'path:works/lookback',1795471796925);
INSERT INTO thread_label VALUES(7530754539293556708,'agent:claude_code',1795471796931);
INSERT INTO thread_label VALUES(7530754539293556708,'branch:main',1795471796931);
INSERT INTO thread_label VALUES(7530754539293556708,'coding_agent',1795471796931);
INSERT INTO thread_label VALUES(7530754539293556708,'dir:lookback',1795471796931);
INSERT INTO thread_label VALUES(7530754539293556708,'path:works/lookback',1795471796931);
INSERT INTO thread_label VALUES(7530754539293556708,'summary',1795471796931);
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
INSERT INTO memories_schema_contract VALUES('rdb_schema','20260803000003');
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
INSERT INTO memories_data_migration_task_state VALUES('thread-message-times-v1@1','d2fe9980cf1048115967a8ab019a53c4422b284f3a8cf185ce2c55988506ed1d','completed','1558771-thread-message-times-v1@1-1795471796173','memories-db-migrate',1,NULL,NULL,1,'{"format":"thread-message-times-v1@1/checkpoint-v1","task_identity":"thread-message-times-v1@1","canonical_definition_digest":"d2fe9980cf1048115967a8ab019a53c4422b284f3a8cf185ce2c55988506ed1d","payload":{"rdb_phase":"verified","rdb_last_thread_id":null,"vector_phase":"completed","canonical_table_name":null,"staging_table_name":null,"staging_origin":null,"staging_cleanup_started":false,"index_outcomes":null,"vector_outcome":"not_required"}}',NULL,1795471796173,1795471796299,1795471796299);
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
COMMIT;
