PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;
CREATE TABLE `media_object` (
    `id`              BIGINT      NOT NULL PRIMARY KEY,   -- snowflake (IdGeneratorWrapper)
    `kind`            INT         NOT NULL,                -- ContentType (IMAGE=2; AUDIO/VIDEO reserved, embedding unimplemented)
    `media_type`      VARCHAR(64) NOT NULL,                -- "image/png" etc.
    `byte_size`       BIGINT,                              -- NULL allowed (url backend size unknown)
    `sha256`          CHAR(64),                            -- dedup/integrity key. NULL allowed (url backend has no sha256)
    `width`           INT,
    `height`          INT,
    `duration_ms`     BIGINT,                              -- AUDIO/VIDEO (future)
    `storage_backend` VARCHAR(16) NOT NULL,                -- s3|file|url|inline|unresolvable
    `storage_uri`     TEXT,                                -- NULL = reservation/promoting in progress or unresolvable
    `alt`             TEXT,                                -- accessibility / caption
    `ref_count`       BIGINT      NOT NULL DEFAULT 0,      -- number of referencing memories
    -- 0=active / 1=orphan / 2=deleted-failed / 3=unresolvable / 4=promoting / 5=deleting
    -- (6-state machine; see ai-docs/image-memory-design.md 1/3 §3.1.1)
    `gc_state`        INT         NOT NULL DEFAULT 0,
    `metadata`        JSON,                                -- EXIF / inline base64 (inline backend only)
    `created_at`      BIGINT      NOT NULL,
    `updated_at`      BIGINT      NOT NULL
);
CREATE TABLE `memory_rating` (
    `id` BIGINT NOT NULL PRIMARY KEY,
    `memory_id` BIGINT NOT NULL,
    `user_id` BIGINT NOT NULL,
    `rating` REAL NOT NULL,
    `metadata` JSON,
    `created_at` BIGINT NOT NULL,
    `updated_at` BIGINT NOT NULL
);
CREATE TABLE `thread_memory` (
    `thread_id`  BIGINT NOT NULL,
    `memory_id`  BIGINT NOT NULL,
    `position`   INT NOT NULL,
    `created_at` BIGINT NOT NULL,
    PRIMARY KEY (`thread_id`, `memory_id`)
);
INSERT INTO thread_memory VALUES(7380000000000001001,7380000000000002001,0,1784509200000);
INSERT INTO thread_memory VALUES(7380000000000001001,7380000000000002002,1,1784509200000);
INSERT INTO thread_memory VALUES(7380000000000001001,7380000000000002003,2,1784509200000);
INSERT INTO thread_memory VALUES(7380000000000001001,7380000000000002004,3,1784509200000);
INSERT INTO thread_memory VALUES(7380000000000001002,7380000000000002011,0,1783994400000);
INSERT INTO thread_memory VALUES(7380000000000001002,7380000000000002012,1,1783994400000);
INSERT INTO thread_memory VALUES(7380000000000001003,7380000000000002021,0,1783850400000);
INSERT INTO thread_memory VALUES(7380000000000001003,7380000000000002022,1,1783850410000);
INSERT INTO thread_memory VALUES(7380000000000001003,7380000000000002023,2,1783850425000);
INSERT INTO thread_memory VALUES(7380000000000001003,7380000000000002024,3,1783850500000);
INSERT INTO thread_memory VALUES(7380000000000001003,7380000000000002025,4,1783850520000);
INSERT INTO thread_memory VALUES(7380000000000001004,7380000000000002021,0,1783852200000);
INSERT INTO thread_memory VALUES(7380000000000001004,7380000000000002023,1,1783852210000);
INSERT INTO thread_memory VALUES(7380000000000001004,7380000000000002026,2,1783852210000);
INSERT INTO thread_memory VALUES(7380000000000001004,7380000000000002027,3,1783852230000);
INSERT INTO thread_memory VALUES(7380000000000001006,7380000000000002041,0,1784516400000);
INSERT INTO thread_memory VALUES(7380000000000001007,7380000000000002051,0,1784561400000);
INSERT INTO thread_memory VALUES(7380000000000001008,7380000000000002061,0,1784517000000);
INSERT INTO thread_memory VALUES(7530754562135736314,7530754562135736315,0,1795471802279);
INSERT INTO thread_memory VALUES(7530754562135736314,7530754562135736313,1,1795471802285);
INSERT INTO thread_memory VALUES(7530754562135736314,7530754562135736312,2,1795471802291);
INSERT INTO thread_memory VALUES(7530754562135736314,7530754562135736311,3,1795471802291);
INSERT INTO thread_memory VALUES(7530754562135736314,7530754562135736310,4,1795471802297);
INSERT INTO thread_memory VALUES(7530754562135736309,7530754562135736312,0,1795471802302);
INSERT INTO thread_memory VALUES(7530754562135736309,7530754562135736308,1,1795471802302);
INSERT INTO thread_memory VALUES(7530754562135736309,7530754562135736307,2,1795471802308);
INSERT INTO thread_memory VALUES(7530754562135736305,7530754562135736304,0,1795471802313);
INSERT INTO thread_memory VALUES(7530754562135736305,7530754562135736303,1,1795471802319);
INSERT INTO thread_memory VALUES(7530754562135736297,7530754562135736296,0,1795471802353);
INSERT INTO thread_memory VALUES(7530754562135736297,7530754562135736295,1,1795471802353);
INSERT INTO thread_memory VALUES(7530754562135736297,7530754562135736294,2,1795471802353);
INSERT INTO thread_memory VALUES(7530754562135736297,7530754562135736293,3,1795471802359);
INSERT INTO thread_memory VALUES(7530754562135736292,7530754562135736291,0,1795471802364);
CREATE TABLE `thread_label` (
    `thread_id`  BIGINT NOT NULL,
    `label`      VARCHAR(512) NOT NULL,
    `created_at` BIGINT NOT NULL,
    PRIMARY KEY (`thread_id`, `label`)
);
INSERT INTO thread_label VALUES(7380000000000001001,'agent:claude_code',1784509200000);
INSERT INTO thread_label VALUES(7380000000000001001,'branch:main',1784509200000);
INSERT INTO thread_label VALUES(7380000000000001001,'coding_agent',1784509200000);
INSERT INTO thread_label VALUES(7380000000000001001,'dir:lookback',1784509200000);
INSERT INTO thread_label VALUES(7380000000000001001,'path:works/lookback',1784509200000);
INSERT INTO thread_label VALUES(7380000000000001002,'agent:claude_code',1783994400000);
INSERT INTO thread_label VALUES(7380000000000001002,'branch:master',1783994400000);
INSERT INTO thread_label VALUES(7380000000000001002,'coding_agent',1783994400000);
INSERT INTO thread_label VALUES(7380000000000001002,'dir:dotfiles',1783994400000);
INSERT INTO thread_label VALUES(7380000000000001002,'path:works/dotfiles',1783994400000);
INSERT INTO thread_label VALUES(7380000000000001006,'agent:claude_code',1784516400000);
INSERT INTO thread_label VALUES(7380000000000001006,'branch:main',1784516400000);
INSERT INTO thread_label VALUES(7380000000000001006,'coding_agent',1784516400000);
INSERT INTO thread_label VALUES(7380000000000001006,'dir:lookback',1784516400000);
INSERT INTO thread_label VALUES(7380000000000001006,'path:works/lookback',1784516400000);
INSERT INTO thread_label VALUES(7380000000000001006,'summary',1784516400000);
INSERT INTO thread_label VALUES(7380000000000001007,'daily_summary',1784561400000);
INSERT INTO thread_label VALUES(7380000000000001007,'date:2026-07-20',1784561400000);
INSERT INTO thread_label VALUES(7380000000000001007,'scope:_all',1784561400000);
INSERT INTO thread_label VALUES(7380000000000001008,'personality',1784517000000);
INSERT INTO thread_label VALUES(7380000000000001008,'personality_signal',1784517000000);
INSERT INTO thread_label VALUES(7380000000000001008,'thread:7380000000000001001',1784517000000);
INSERT INTO thread_label VALUES(7380000000000001008,'user:1',1784517000000);
INSERT INTO thread_label VALUES(7530754562135736314,'topic:shopping',1795471802280);
INSERT INTO thread_label VALUES(7530754562135736297,'agent:claude_code',1795471802353);
INSERT INTO thread_label VALUES(7530754562135736297,'branch:main',1795471802353);
INSERT INTO thread_label VALUES(7530754562135736297,'coding_agent',1795471802353);
INSERT INTO thread_label VALUES(7530754562135736297,'dir:lookback',1795471802353);
INSERT INTO thread_label VALUES(7530754562135736297,'path:works/lookback',1795471802353);
INSERT INTO thread_label VALUES(7530754562135736292,'agent:claude_code',1795471802359);
INSERT INTO thread_label VALUES(7530754562135736292,'branch:main',1795471802359);
INSERT INTO thread_label VALUES(7530754562135736292,'coding_agent',1795471802359);
INSERT INTO thread_label VALUES(7530754562135736292,'dir:lookback',1795471802359);
INSERT INTO thread_label VALUES(7530754562135736292,'path:works/lookback',1795471802359);
INSERT INTO thread_label VALUES(7530754562135736292,'summary',1795471802359);
CREATE TABLE `thread_reflection_index` (
    `memory_id` BIGINT NOT NULL PRIMARY KEY,
    `thread_id` BIGINT NOT NULL,
    `origin_thread_id` BIGINT NOT NULL,
    `origin_user_id` BIGINT NOT NULL,
    `origin_channel` TEXT,
    `outcome` INT NOT NULL,
    `score` REAL NOT NULL,
    `score_self` REAL NOT NULL,
    `score_heuristic` REAL NOT NULL,
    `task_category` INT NOT NULL,
    `reflection_aspect` INT NOT NULL,
    `dataset_quality` INT NOT NULL DEFAULT 1,
    `summary_embedding_status` INT NOT NULL DEFAULT 1,
    `summary_embedding_error` TEXT,
    `intent_embedding_status` INT NOT NULL DEFAULT 1,
    `intent_embedding_error` TEXT,
    `prompt_version` VARCHAR(32) NOT NULL,
    `target_model_version` VARCHAR(128),
    `experiment_id` VARCHAR(128),
    `experiment_variant` VARCHAR(128),
    `previous_reflection_id` BIGINT,
    `pinned` BOOLEAN NOT NULL DEFAULT 0,
    `is_recurrence` BOOLEAN NOT NULL DEFAULT 0,
    `mitigation_fingerprint` VARCHAR(64),
    `created_at` BIGINT NOT NULL,
    `updated_at` BIGINT NOT NULL
);
CREATE TABLE `reflection_failure_mode` (
    `memory_id` BIGINT NOT NULL,
    `mode` VARCHAR(64) NOT NULL,
    PRIMARY KEY (`memory_id`, `mode`)
);
CREATE TABLE `reflection_tool` (
    `memory_id` BIGINT NOT NULL,
    `tool` VARCHAR(128) NOT NULL,
    PRIMARY KEY (`memory_id`, `tool`)
);
CREATE TABLE `reflection_tool_outcome` (
    `memory_id` BIGINT NOT NULL,
    `tool` VARCHAR(128) NOT NULL,
    `contribution` INT NOT NULL,
    `error_kind` VARCHAR(128) NOT NULL DEFAULT '',
    PRIMARY KEY (`memory_id`, `tool`, `contribution`, `error_kind`)
);
CREATE TABLE `reflection_fact` (
    `memory_id` BIGINT NOT NULL,
    `fact_memory_id` BIGINT NOT NULL,
    `fact_kind` INT NOT NULL,
    `turn_index` INT NOT NULL DEFAULT 0,
    `weight` REAL,
    `note` TEXT,
    `links_json` JSON,
    PRIMARY KEY (`memory_id`, `fact_memory_id`, `fact_kind`)
);
CREATE TABLE `reflection_applied_target` (
    `memory_id` BIGINT NOT NULL,
    `target` VARCHAR(256) NOT NULL,
    `mitigation_fingerprint` VARCHAR(64),
    `applied_at` BIGINT NOT NULL,
    PRIMARY KEY (`memory_id`, `target`)
);
CREATE TABLE `reflection_few_shot_usage` (
    `memory_id` BIGINT NOT NULL,
    `used_in_thread_id` BIGINT NOT NULL,
    `used_at` BIGINT NOT NULL,
    PRIMARY KEY (`memory_id`, `used_in_thread_id`)
);
CREATE TABLE `tool_outcome_stats` (
    `origin_user_id` BIGINT NOT NULL,
    `tool` VARCHAR(128) NOT NULL,
    `outcome` INT NOT NULL,
    `count` BIGINT NOT NULL DEFAULT 0,
    `last_updated_at` BIGINT NOT NULL,
    PRIMARY KEY (`origin_user_id`, `tool`, `outcome`)
);
CREATE TABLE `tool_contribution_stats` (
    `origin_user_id` BIGINT NOT NULL,
    `tool` VARCHAR(128) NOT NULL,
    `contribution` INT NOT NULL,
    `error_kind` VARCHAR(128) NOT NULL DEFAULT '',
    `count` BIGINT NOT NULL DEFAULT 0,
    `last_updated_at` BIGINT NOT NULL,
    PRIMARY KEY (`origin_user_id`, `tool`, `contribution`, `error_kind`)
);
CREATE TABLE `failure_mode_dictionary` (
    `mode` VARCHAR(64) NOT NULL PRIMARY KEY,
    `description` TEXT NOT NULL,
    `severity` INT NOT NULL,
    `category` INT NOT NULL,
    `default_mitigation` TEXT NOT NULL
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
CREATE TABLE `failure_signature_indicator_norm` (
    `indicator_name` VARCHAR(64) NOT NULL PRIMARY KEY,
    `max_value` REAL NOT NULL,
    `weight` REAL NOT NULL DEFAULT 1.0
);
INSERT INTO failure_signature_indicator_norm VALUES('same_tool_repeated_count',20.0,1.0);
INSERT INTO failure_signature_indicator_norm VALUES('consecutive_errors',10.0,1.0);
INSERT INTO failure_signature_indicator_norm VALUES('no_state_change_turns',20.0,1.0);
INSERT INTO failure_signature_indicator_norm VALUES('tool_calls_per_turn_ratio',10.0,1.0);
INSERT INTO failure_signature_indicator_norm VALUES('compact_boundary_count',10.0,1.0);
INSERT INTO failure_signature_indicator_norm VALUES('user_clarification_count',10.0,1.0);
INSERT INTO failure_signature_indicator_norm VALUES('turn_count_at_detection',500.0,1.0);
INSERT INTO failure_signature_indicator_norm VALUES('elapsed_ms_at_detection',3600000.0,1.0);
CREATE TABLE `thread_aggregate_key` (
    `user_id` BIGINT NOT NULL,
    `labels_hash` CHAR(64) NOT NULL,
    `thread_id` BIGINT NOT NULL,
    `created_at` BIGINT NOT NULL,
    PRIMARY KEY (`user_id`, `labels_hash`)
);
CREATE TABLE IF NOT EXISTS "thread" (
    `id` BIGINT NOT NULL PRIMARY KEY,
    `default_system_memory_id` BIGINT,
    `user_id` BIGINT NOT NULL,
    `description` TEXT,
    `channel` TEXT,
    `embedding` BLOB,
    `embedding_dim` INT,
    `created_at` BIGINT NOT NULL,
    `updated_at` BIGINT NOT NULL,
    `metadata` JSON,
    `memory_kind` INT NOT NULL
, first_message_at BIGINT, last_message_at BIGINT);
INSERT INTO thread VALUES(7380000000000001001,NULL,1,'【スレッド一覧のページング修正】 Lookback のスレッド一覧で 2 ページ目以降が重複表示される不具合を、カーソルを updated_at と id の組に変えて修正した。','claude_code:3f2c9a1e-5b7d-4c11-9e2a-8d0f6a4b2c10',NULL,NULL,1784505600000,1784516400000,NULL,1,1784505600000,1784505780000);
INSERT INTO thread VALUES(7380000000000001002,NULL,2,'dotfiles (master)','claude_code:b7e4d2c1-0a9f-4e3d-8c2b-1a0f9e8d7c6b',NULL,NULL,1783990800000,1783990860000,NULL,1,1783990800000,1783990860000);
INSERT INTO thread VALUES(7380000000000001003,7380000000000002021,1,'夏休みの旅行計画',NULL,NULL,NULL,1783850400000,1783850520000,NULL,1,1783850395000,1783850520000);
INSERT INTO thread VALUES(7380000000000001004,7380000000000002021,1,'夏休みの旅行計画 (電車案)',NULL,NULL,NULL,1783852200000,1783852230000,NULL,1,1783850395000,1783852230000);
INSERT INTO thread VALUES(7380000000000001005,NULL,2,NULL,NULL,NULL,NULL,1784102400000,1784102400000,NULL,1,NULL,NULL);
INSERT INTO thread VALUES(7380000000000001006,NULL,1,'agent:claude_code, branch:main, coding_agent, dir:lookback, path:works/lookback の要約集',NULL,NULL,NULL,1784505600000,1784516400000,NULL,2,1784505600000,1784505600000);
INSERT INTO thread VALUES(7380000000000001007,NULL,1,'2026-07-20 の作業要約 (1 スレッド)',NULL,NULL,NULL,1784473200000,1784561400000,NULL,3,1784473200000,1784473200000);
INSERT INTO thread VALUES(7380000000000001008,NULL,1,'personality signals for thread 7380000000000001001 (user 1)',NULL,NULL,NULL,1784505600000,1784517000000,NULL,6,1784505600000,1784505600000);
INSERT INTO thread VALUES(7530754562135736314,7530754562135736315,1,'週末の買い物リスト',NULL,NULL,NULL,1795471802279,1795471802297,NULL,1,1795136400000,1795136640000);
INSERT INTO thread VALUES(7530754562135736309,NULL,1,'週末の買い物リスト (別案)',NULL,NULL,NULL,1795471802302,1795471802308,NULL,1,1795136520000,1795137120000);
INSERT INTO thread VALUES(7530754562135736306,NULL,1,NULL,NULL,NULL,NULL,1795471802308,1795471802308,NULL,1,NULL,NULL);
INSERT INTO thread VALUES(7530754562135736305,NULL,2,NULL,NULL,NULL,NULL,1795471802313,1795471802319,NULL,1,1795138260000,1795138320000);
INSERT INTO thread VALUES(7530754562135736302,NULL,2,'消したメモ',NULL,NULL,NULL,1795471802319,1795471802336,NULL,1,NULL,NULL);
INSERT INTO thread VALUES(7530754562135736297,NULL,1,'lookback (main)','claude_code:8c1d2e3f-4a5b-4c6d-8e7f-9a0b1c2d3e4f',NULL,NULL,1795471802353,1795471802359,NULL,1,1795140600000,1795140780000);
INSERT INTO thread VALUES(7530754562135736292,NULL,1,'agent:claude_code, branch:main, coding_agent, dir:lookback, path:works/lookback の要約集',NULL,NULL,NULL,1795471802359,1795471802364,NULL,2,1795140600000,1795140600000);
CREATE TABLE IF NOT EXISTS "memory" (
    `id` BIGINT NOT NULL PRIMARY KEY,
    `parent_ids` JSON,
    `user_id` BIGINT NOT NULL,
    `content` TEXT NOT NULL,
    `content_type` INT NOT NULL,
    `params` JSON,
    `metadata` JSON,
    `created_at` BIGINT NOT NULL,
    `updated_at` BIGINT NOT NULL,
    `role` INT NOT NULL DEFAULT 0,
    `external_id` VARCHAR(512),
    `media_object_id` BIGINT,
    `memory_kind` INT NOT NULL
);
INSERT INTO memory VALUES(7380000000000002001,'[]',1,'スレッド一覧の 2 ページ目に 1 ページ目と同じスレッドが出る。原因を調べて直して。',0,NULL,'{"source":"claude_code","kind":"user","session":{"cwd":"/home/user/works/lookback","git_branch":"main"}}',1784505600000,1784505600000,1,'claude_code:1:3f2c9a1e-5b7d-4c11-9e2a-8d0f6a4b2c10:0e1d2c3b-4a59-4687-9a1b-2c3d4e5f6a70:b0:4f1c2a9e7d3b8c60',NULL,1);
INSERT INTO memory VALUES(7380000000000002002,'[7380000000000002001]',1,'ページングが updated_at だけをカーソルにしているため、同じ時刻のスレッドが境界で重複しています。id を第 2 キーに加えます。',0,NULL,'{"source":"claude_code","kind":"assistant","session":{"cwd":"/home/user/works/lookback","git_branch":"main"}}',1784505660000,1784505660000,2,'claude_code:1:3f2c9a1e-5b7d-4c11-9e2a-8d0f6a4b2c10:1f2e3d4c-5b6a-4798-8b2c-3d4e5f6a7b81:b0:9a8b7c6d5e4f3a21',NULL,1);
INSERT INTO memory VALUES(7380000000000002003,'[7380000000000002002]',1,'{"name":"Edit","input":{"file_path":"src/thread_list.rs"}}',1,NULL,'{"source":"claude_code","kind":"tool_call","tool":{"name":"Edit"},"session":{"cwd":"/home/user/works/lookback","git_branch":"main"}}',1784505720000,1784505720000,2,'claude_code:1:3f2c9a1e-5b7d-4c11-9e2a-8d0f6a4b2c10:2a3b4c5d-6e7f-48a9-9c3d-4e5f6a7b8c92:b1:c3d2e1f0a9b8c7d6',NULL,1);
INSERT INTO memory VALUES(7380000000000002004,'[7380000000000002003]',1,'The file src/thread_list.rs has been updated.',1,NULL,'{"source":"claude_code","kind":"tool_output","tool":{"name":"Edit"},"session":{"cwd":"/home/user/works/lookback","git_branch":"main"}}',1784505780000,1784505780000,4,'claude_code:1:3f2c9a1e-5b7d-4c11-9e2a-8d0f6a4b2c10:3b4c5d6e-7f80-49ba-8d4e-5f6a7b8c9da3:b0:e5f4d3c2b1a09f8e',NULL,1);
INSERT INTO memory VALUES(7380000000000002011,'[]',2,'zsh の起動が遅いので原因を調べて。',0,NULL,'{"source":"claude_code","kind":"user","session":{"cwd":"/home/user2/works/dotfiles","git_branch":"master"}}',1783990800000,1783990800000,1,'claude_code:2:b7e4d2c1-0a9f-4e3d-8c2b-1a0f9e8d7c6b:4c5d6e7f-8091-4acb-9e5f-6a7b8c9daeb4:b0:0f1e2d3c4b5a6978',NULL,1);
INSERT INTO memory VALUES(7380000000000002012,'[7380000000000002011]',2,'nvm の初期化が毎回走っています。遅延ロードにすると 0.6 秒短縮できます。',0,NULL,'{"source":"claude_code","kind":"assistant","session":{"cwd":"/home/user2/works/dotfiles","git_branch":"master"}}',1783990860000,1783990860000,2,'claude_code:2:b7e4d2c1-0a9f-4e3d-8c2b-1a0f9e8d7c6b:5d6e7f80-91a2-4bdc-8f6a-7b8c9daebfc5:b0:1a2b3c4d5e6f7081',NULL,1);
INSERT INTO memory VALUES(7380000000000002021,'[]',1,'あなたは Lookback のアシスタントです。簡潔に日本語で答えてください。',0,NULL,NULL,1783850395000,1783850395000,3,NULL,NULL,1);
INSERT INTO memory VALUES(7380000000000002022,'[7380000000000002021]',1,'8 月に 3 泊で金沢と富山を回りたい。移動手段の案を出して。',0,NULL,NULL,1783850410000,1783850410000,1,NULL,NULL,1);
INSERT INTO memory VALUES(7380000000000002023,'[7380000000000002021,7380000000000002022]',1,'北陸新幹線で金沢に入り、富山へは在来線で戻る案と、レンタカーで能登を回る案があります。',0,NULL,NULL,1783850425000,1783850425000,2,NULL,NULL,1);
INSERT INTO memory VALUES(7380000000000002024,'[7380000000000002021,7380000000000002023]',1,'レンタカー案で宿の候補を教えて。',0,NULL,NULL,1783850500000,1783850500000,1,NULL,NULL,1);
INSERT INTO memory VALUES(7380000000000002025,'[7380000000000002021,7380000000000002024]',1,'和倉温泉に 1 泊、金沢市内に 2 泊する組み合わせが移動時間を抑えられます。',0,NULL,NULL,1783850520000,1783850520000,2,NULL,NULL,1);
INSERT INTO memory VALUES(7380000000000002026,'[7380000000000002021,7380000000000002023]',1,'電車だけで回る場合の時刻表の目安は?',0,NULL,NULL,1783852210000,1783852210000,1,NULL,NULL,1);
INSERT INTO memory VALUES(7380000000000002027,'[7380000000000002021,7380000000000002026]',1,'東京 8:00 発のかがやきで金沢 10:30 着、富山へはあいの風とやま鉄道で約 1 時間です。',0,NULL,NULL,1783852230000,1783852230000,2,NULL,NULL,1);
INSERT INTO memory VALUES(7380000000000002031,'[]',1,'あなたはコードレビュアーです。指摘は重要度順に並べてください。',0,NULL,NULL,1783760400000,1783760400000,3,NULL,NULL,1);
INSERT INTO memory VALUES(7380000000000002041,'[]',1,'{"category":"bugfix","title":"スレッド一覧のページング修正","summary":"Lookback のスレッド一覧で 2 ページ目以降が重複表示される不具合を、カーソルを updated_at と id の組に変えて修正した。","key_decisions":["カーソルに id を第 2 キーとして追加"],"status":"completed"}',0,NULL,'{"source_thread_id":"7380000000000001001","category":"bugfix","summary_version":"1.0"}',1784505600000,1784505780000,2,'summary:7380000000000001001',NULL,2);
INSERT INTO memory VALUES(7380000000000002051,'[]',1,'{"overall_purpose":"Lookback の一覧表示の不具合修正","items":[{"purpose":"スレッド一覧のページング修正","bullets":["カーソルを updated_at と id の組に変更"],"source_memory_ids":["7380000000000002041"],"status":"completed"}]}',0,NULL,'{"daily_date":"2026-07-20","scope":"_all","extra_labels":[],"source_memory_count":1,"source_memory_ids":["7380000000000002041"],"source_thread_ids":["7380000000000001006"],"summary_version":"1.0"}',1784473200000,1784505780000,2,'daily:1:2026-07-20:_all',NULL,3);
INSERT INTO memory VALUES(7380000000000002061,'[]',1,'{"no_signal":false,"signals":[{"trait":"根本原因を確認してから修正方針を決める","evidence":"原因を調べて直して"}]}',0,NULL,'{"source_user_id":"1","source_thread_id":"7380000000000001001","signal_version":"1.0","no_signal":false,"truncation_level":0}',1784505600000,1784505780000,2,'personality:7380000000000001001',NULL,6);
INSERT INTO memory VALUES(7380000000000002071,'[]',100000,'{"category":"other","title":"引っ越しの手続き","summary":"転居届と電気・ガスの契約変更の順番を整理した。","key_decisions":[],"status":"completed"}',0,NULL,'{"source_thread_id":"7380000000000000999","category":"other","summary_version":"1.0"}',1783641600000,1783645200000,2,'summary:7380000000000000999',NULL,1);
INSERT INTO memory VALUES(7530754562135736315,'[]',1,'あなたは Lookback のアシスタントです。簡潔に日本語で答えてください。',0,NULL,NULL,1795136400000,1795136400000,3,NULL,NULL,1);
INSERT INTO memory VALUES(7530754562135736313,'[7530754562135736315]',1,'週末の買い物リストを作って。',0,NULL,NULL,1795136460000,1795136460000,1,NULL,NULL,1);
INSERT INTO memory VALUES(7530754562135736312,'[7530754562135736315,7530754562135736313]',1,'牛乳、卵、食パン、玉ねぎ、鶏もも肉です。',0,NULL,NULL,1795136520000,1795136520000,2,NULL,NULL,1);
INSERT INTO memory VALUES(7530754562135736311,'[7530754562135736315,7530754562135736312]',1,'飲み物も足して。',0,NULL,NULL,1795136580000,1795136580000,1,NULL,NULL,1);
INSERT INTO memory VALUES(7530754562135736310,'[7530754562135736315,7530754562135736311]',1,'麦茶のパックと炭酸水を追加しました。',0,NULL,NULL,1795136640000,1795136640000,2,NULL,NULL,1);
INSERT INTO memory VALUES(7530754562135736308,'[7530754562135736312]',1,'鶏もも肉を豚こまに変えた場合は?',0,NULL,NULL,1795137060000,1795137060000,1,NULL,NULL,1);
INSERT INTO memory VALUES(7530754562135736307,'[7530754562135736308]',1,'豚こまとキャベツで回鍋肉にできます。',0,NULL,NULL,1795137120000,1795137120000,2,NULL,NULL,1);
INSERT INTO memory VALUES(7530754562135736304,'[]',2,'明日の朝 6 時に起きるコツは?',0,NULL,NULL,1795138260000,1795138260000,1,NULL,NULL,1);
INSERT INTO memory VALUES(7530754562135736303,'[7530754562135736304]',2,'寝る 1 時間前に画面を見るのをやめ、起床時刻を毎日そろえることです。',0,NULL,NULL,1795138320000,1795138320000,2,NULL,NULL,1);
INSERT INTO memory VALUES(7530754562135736296,'[]',1,'設定画面の保存ボタンが二重送信される。',0,NULL,'{"kind":"user","session":{"cwd":"/home/user/works/lookback","git_branch":"main"},"source":"claude_code"}',1795140600000,1795140600000,1,'claude_code:1:8c1d2e3f-4a5b-4c6d-8e7f-9a0b1c2d3e4f:a1b2c3d4-0001:b0',NULL,1);
INSERT INTO memory VALUES(7530754562135736295,'[7530754562135736296]',1,'送信中はボタンを無効化します。',0,NULL,'{"kind":"assistant","session":{"cwd":"/home/user/works/lookback","git_branch":"main"},"source":"claude_code"}',1795140660000,1795140660000,2,'claude_code:1:8c1d2e3f-4a5b-4c6d-8e7f-9a0b1c2d3e4f:a1b2c3d4-0002:b0',NULL,1);
INSERT INTO memory VALUES(7530754562135736294,'[7530754562135736295]',1,'{"name":"Edit","input":{"file_path":"src/settings.tsx"}}',1,NULL,'{"kind":"tool_call","session":{"cwd":"/home/user/works/lookback","git_branch":"main"},"source":"claude_code"}',1795140720000,1795140720000,2,'claude_code:1:8c1d2e3f-4a5b-4c6d-8e7f-9a0b1c2d3e4f:a1b2c3d4-0003:b0',NULL,1);
INSERT INTO memory VALUES(7530754562135736293,'[7530754562135736294]',1,'The file src/settings.tsx has been updated.',1,NULL,'{"kind":"tool_output","session":{"cwd":"/home/user/works/lookback","git_branch":"main"},"source":"claude_code"}',1795140780000,1795140780000,4,'claude_code:1:8c1d2e3f-4a5b-4c6d-8e7f-9a0b1c2d3e4f:a1b2c3d4-0004:b0',NULL,1);
INSERT INTO memory VALUES(7530754562135736291,'[]',1,'{"category":"bugfix","key_decisions":["送信中はボタンを無効化"],"status":"completed","summary":"保存ボタンを送信中に無効化して二重送信を防いだ。","title":"設定画面の二重送信修正"}',0,NULL,'{"category":"bugfix","source_thread_id":"7530754562135736297","summary_version":"1.0"}',1795140600000,1795140600000,2,'summary:7530754562135736297',NULL,2);
CREATE TABLE `atlas_schema_revisions` (`version` text NOT NULL, `description` text NOT NULL, `type` integer NOT NULL DEFAULT (2), `applied` integer NOT NULL DEFAULT (0), `total` integer NOT NULL DEFAULT (0), `executed_at` datetime NOT NULL, `execution_time` integer NOT NULL, `error` text NULL, `error_stmt` text NULL, `hash` text NOT NULL, `partial_hashes` json NULL, `operator_version` text NOT NULL, PRIMARY KEY (`version`));
INSERT INTO atlas_schema_revisions VALUES('20260803000001','adoption_baseline',1,0,0,'2026-11-24 07:10:01.481488394+09:00',0,'','','',x'6e756c6c','Atlas CLI v1.3.0');
INSERT INTO atlas_schema_revisions VALUES('20260803000002','thread_message_times_schema',2,9,9,'2026-11-24 07:10:01.481913876+09:00',94427,'','','ztKNN2k6fe061EkReFmi101OHQdQBFoOJeymhJoRfIY=',x'6e756c6c','Atlas CLI v1.3.0');
INSERT INTO atlas_schema_revisions VALUES('20260803000003','schema_contract',2,3,3,'2026-11-24 07:10:01.484038161+09:00',89711,'','','VR0ZoYAk6dTG3k3V/d6Ie76XEsEIVIVLBV3RMSP1JRs=',x'6e756c6c','Atlas CLI v1.3.0');
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
INSERT INTO memories_data_migration_task_state VALUES('thread-message-times-v1@1','d2fe9980cf1048115967a8ab019a53c4422b284f3a8cf185ce2c55988506ed1d','completed','1560399-thread-message-times-v1@1-1795471801668','memories-db-migrate',1,NULL,NULL,1,'{"format":"thread-message-times-v1@1/checkpoint-v1","task_identity":"thread-message-times-v1@1","canonical_definition_digest":"d2fe9980cf1048115967a8ab019a53c4422b284f3a8cf185ce2c55988506ed1d","payload":{"rdb_phase":"verified","rdb_last_thread_id":null,"vector_phase":"completed","canonical_table_name":null,"staging_table_name":null,"staging_origin":null,"staging_cleanup_started":false,"index_outcomes":null,"vector_outcome":"not_required"}}',NULL,1795471801668,1795471801739,1795471801739);
CREATE UNIQUE INDEX `media_object_sha256`   ON `media_object` (`sha256`);
CREATE INDEX `media_object_kind`            ON `media_object` (`kind`);
CREATE INDEX `media_object_gc_state`        ON `media_object` (`gc_state`);
CREATE UNIQUE INDEX `memory_rating_memory_user` ON `memory_rating` (`memory_id`, `user_id`);
CREATE INDEX `memory_rating_memory_id` ON `memory_rating` (`memory_id`);
CREATE INDEX `memory_rating_user_id` ON `memory_rating` (`user_id`);
CREATE UNIQUE INDEX `thread_memory_thread_position`
    ON `thread_memory` (`thread_id`, `position`);
CREATE INDEX `thread_memory_memory_id`
    ON `thread_memory` (`memory_id`);
CREATE INDEX `thread_label_thread_id`
    ON `thread_label` (`thread_id`);
CREATE INDEX `thread_label_label`
    ON `thread_label` (`label`);
CREATE INDEX `tri_thread_id`
    ON `thread_reflection_index` (`thread_id`);
CREATE INDEX `tri_origin_thread_id`
    ON `thread_reflection_index` (`origin_thread_id`);
CREATE INDEX `tri_user_outcome_score`
    ON `thread_reflection_index` (`origin_user_id`, `outcome`, `score`);
CREATE INDEX `tri_user_channel`
    ON `thread_reflection_index` (`origin_user_id`, `origin_channel`);
CREATE INDEX `tri_task_category`
    ON `thread_reflection_index` (`task_category`);
CREATE INDEX `tri_reflection_aspect`
    ON `thread_reflection_index` (`reflection_aspect`);
CREATE INDEX `tri_prompt_version`
    ON `thread_reflection_index` (`prompt_version`, `created_at`);
CREATE INDEX `tri_target_model`
    ON `thread_reflection_index` (`target_model_version`);
CREATE INDEX `tri_experiment`
    ON `thread_reflection_index` (`experiment_id`, `experiment_variant`);
CREATE INDEX `tri_user_summary_status`
    ON `thread_reflection_index` (`origin_user_id`, `summary_embedding_status`);
CREATE INDEX `tri_user_intent_status`
    ON `thread_reflection_index` (`origin_user_id`, `intent_embedding_status`);
CREATE INDEX `tri_origin_thread_created`
    ON `thread_reflection_index` (`origin_thread_id`, `created_at` DESC);
CREATE INDEX `rfm_mode`
    ON `reflection_failure_mode` (`mode`);
CREATE INDEX `rt_tool`
    ON `reflection_tool` (`tool`);
CREATE INDEX `rto_tool_contrib`
    ON `reflection_tool_outcome` (`tool`, `contribution`);
CREATE INDEX `rf_kind`
    ON `reflection_fact` (`memory_id`, `fact_kind`);
CREATE INDEX `rf_fact_memory`
    ON `reflection_fact` (`fact_memory_id`);
CREATE INDEX `rat_fingerprint`
    ON `reflection_applied_target` (`mitigation_fingerprint`);
CREATE INDEX `rfsu_thread_used`
    ON `reflection_few_shot_usage` (`used_in_thread_id`);
CREATE INDEX `tos_user_tool`
    ON `tool_outcome_stats` (`origin_user_id`, `tool`);
CREATE INDEX `tcs_user_tool_contrib`
    ON `tool_contribution_stats` (`origin_user_id`, `tool`, `contribution`);
CREATE INDEX `tak_thread_id`
    ON `thread_aggregate_key` (`thread_id`);
CREATE INDEX `thread_default_system_memory_id` ON `thread` (`default_system_memory_id`);
CREATE INDEX `thread_user_id` ON `thread` (`user_id`);
CREATE INDEX `thread_updated_at` ON `thread` (`updated_at`);
CREATE INDEX `thread_user_memory_kind_updated_at` ON `thread` (`user_id`, `memory_kind`, `updated_at`);
CREATE INDEX `memory_user_id` ON `memory` (`user_id`);
CREATE INDEX `memory_user_memory_kind_updated_at` ON `memory` (`user_id`, `memory_kind`, `updated_at`);
CREATE UNIQUE INDEX `memory_external_id` ON `memory` (`external_id`);
CREATE INDEX `memory_media_object_id` ON `memory` (`media_object_id`);
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
