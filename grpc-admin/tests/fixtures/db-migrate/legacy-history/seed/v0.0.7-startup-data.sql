-- Data of a Lookback desktop database written by release v0.0.7 (commit
-- 06ce5b4), i.e. after the memory_kind migration and before the
-- migration engine took over the schema.
--
-- Target schema: the v0.0.7 server startup schema, i.e.
-- `infra/sql/sqlite/001_schema.sql` + `003_reflection_schema.sql` at commit
-- 06ce5b4 (`thread.memory_kind` / `memory.memory_kind` are NOT NULL; no
-- message-time columns). Load with `sqlite3 db < file` after those two files.
--
-- memory_kind values: 1 = RAW, 2 = THREAD_SUMMARY (MemoryKind in
-- protobuf/protobuf/llm_memory/data/common.proto at 06ce5b4). Roles:
-- 1 user, 2 assistant, 3 system, 4 tool. content_type 1 = tool.
--
-- Contents (users 1 and 2 are the desktop users):
--   7390000000000001001  user 1, Claude Code session imported by
--                        agent-chat-import (owner-scoped external_id)
--   7390000000000001002  user 1, chat with a default system prompt
--   7390000000000001003  user 2, chat without description
--   7390000000000001004  user 1, THREAD_SUMMARY thread of ...1001 written by
--                        the v0.0.7 thread-summary workflow
--   7390000000000001005  user 2, empty thread
-- Every thread_memory row points at an existing thread and memory, and the
-- memory_kind of each memory equals that of its thread.

BEGIN TRANSACTION;

-- ---------------------------------------------------------------- threads

INSERT INTO thread (id, default_system_memory_id, user_id, description, channel, embedding, embedding_dim, created_at, updated_at, metadata, memory_kind) VALUES
  (7390000000000001001, NULL, 1, '【検索結果のハイライト崩れ修正】 全角記号を含む検索語でハイライト位置がずれる不具合を、バイト位置ではなく文字位置で計算するよう修正した。', 'claude_code:5a6b7c8d-9e0f-4a1b-8c2d-3e4f5a6b7c8d', NULL, NULL, 1787014800000, 1787022000000, NULL, 1),
  (7390000000000001002, 7390000000000002011, 1, 'お盆の帰省の持ち物', NULL, NULL, NULL, 1787029200000, 1787029320000, NULL, 1),
  (7390000000000001003, NULL, 2, NULL, NULL, NULL, NULL, 1787036400000, 1787036430000, NULL, 1),
  (7390000000000001004, NULL, 1, 'agent:claude_code, branch:main, coding_agent, dir:lookback, path:works/lookback の要約集', NULL, NULL, NULL, 1787014800000, 1787022000000, NULL, 2),
  (7390000000000001005, NULL, 2, '新しいチャット', NULL, NULL, NULL, 1787097600000, 1787097600000, NULL, 1);

-- --------------------------------------------------------------- memories

INSERT INTO memory (id, parent_ids, user_id, content, content_type, params, metadata, created_at, updated_at, role, external_id, media_object_id, memory_kind) VALUES
  -- imported session of user 1
  (7390000000000002001, '[]', 1, '検索語に全角の括弧を入れるとハイライトが 1 文字ずれる。直して。', 0, NULL,
   '{"source":"claude_code","kind":"user","session":{"cwd":"/home/user/works/lookback","git_branch":"main"}}',
   1787014800000, 1787014800000, 1, 'claude_code:1:5a6b7c8d-9e0f-4a1b-8c2d-3e4f5a6b7c8d:6e7f8091-a2b3-4ced-9a7b-8c9daebfc0d6:b0:2b3c4d5e6f708192', NULL, 1),
  (7390000000000002002, '[7390000000000002001]', 1, 'ハイライト範囲を UTF-8 のバイト位置で切り出しているのが原因です。char_indices で文字位置に変換します。', 0, NULL,
   '{"source":"claude_code","kind":"assistant","session":{"cwd":"/home/user/works/lookback","git_branch":"main"}}',
   1787014860000, 1787014860000, 2, 'claude_code:1:5a6b7c8d-9e0f-4a1b-8c2d-3e4f5a6b7c8d:7f8091a2-b3c4-4dfe-8b8c-9daebfc0d1e7:b0:3c4d5e6f708192a3', NULL, 1),
  (7390000000000002003, '[7390000000000002002]', 1, '{"name":"Edit","input":{"file_path":"src/highlight.rs"}}', 1, NULL,
   '{"source":"claude_code","kind":"tool_call","tool":{"name":"Edit"},"session":{"cwd":"/home/user/works/lookback","git_branch":"main"}}',
   1787014920000, 1787014920000, 2, 'claude_code:1:5a6b7c8d-9e0f-4a1b-8c2d-3e4f5a6b7c8d:8091a2b3-c4d5-4e0f-9c9d-aebfc0d1e2f8:b1:4d5e6f708192a3b4', NULL, 1),
  (7390000000000002004, '[7390000000000002003]', 1, 'The file src/highlight.rs has been updated.', 1, NULL,
   '{"source":"claude_code","kind":"tool_output","tool":{"name":"Edit"},"session":{"cwd":"/home/user/works/lookback","git_branch":"main"}}',
   1787014980000, 1787014980000, 4, 'claude_code:1:5a6b7c8d-9e0f-4a1b-8c2d-3e4f5a6b7c8d:91a2b3c4-d5e6-4f10-8dae-bfc0d1e2f309:b0:5e6f708192a3b4c5', NULL, 1),

  -- chat of user 1 with a default system prompt; the server injected the
  -- default as parent_ids[0]
  (7390000000000002011, '[]', 1, 'あなたは Lookback のアシスタントです。箇条書きで簡潔に答えてください。', 0, NULL, NULL,
   1787029195000, 1787029195000, 3, NULL, NULL, 1),
  (7390000000000002012, '[7390000000000002011]', 1, '3 泊 4 日で実家に帰る。子ども連れの持ち物リストを作って。', 0, NULL, NULL,
   1787029210000, 1787029210000, 1, NULL, NULL, 1),
  (7390000000000002013, '[7390000000000002011,7390000000000002012]', 1, '着替え 4 日分、常備薬、保険証、モバイルバッテリー、車内で遊べるおもちゃ。', 0, NULL, NULL,
   1787029225000, 1787029225000, 2, NULL, NULL, 1),
  (7390000000000002014, '[7390000000000002011,7390000000000002013]', 1, '手土産も追加して。', 0, NULL, NULL,
   1787029300000, 1787029300000, 1, NULL, NULL, 1),
  (7390000000000002015, '[7390000000000002011,7390000000000002014]', 1, '追加しました: 手土産 (日持ちする焼き菓子)。', 0, NULL, NULL,
   1787029320000, 1787029320000, 2, NULL, NULL, 1),

  -- chat of user 2
  (7390000000000002021, '[]', 2, '冷蔵庫の残り物で作れる夕飯を教えて。卵、キャベツ、豚こま。', 0, NULL, NULL,
   1787036410000, 1787036410000, 1, NULL, NULL, 1),
  (7390000000000002022, '[7390000000000002021]', 2, '豚こまとキャベツの卵とじ丼はいかがでしょう。15 分ほどで作れます。', 0, NULL, NULL,
   1787036430000, 1787036430000, 2, NULL, NULL, 1),

  -- thread-summary of ...1001
  (7390000000000002031, '[]', 1, '{"category":"bugfix","title":"検索結果のハイライト崩れ修正","summary":"全角記号を含む検索語でハイライト位置がずれる不具合を、バイト位置ではなく文字位置で計算するよう修正した。","key_decisions":["char_indices で文字位置に変換"],"status":"completed"}', 0, NULL,
   '{"source_thread_id":"7390000000000001001","category":"bugfix","summary_version":"1.0"}',
   1787014800000, 1787014980000, 2, 'summary:7390000000000001001', NULL, 2);

-- ---------------------------------------------------------- thread_memory

INSERT INTO thread_memory (thread_id, memory_id, position, created_at) VALUES
  (7390000000000001001, 7390000000000002001, 0, 1787018400000),
  (7390000000000001001, 7390000000000002002, 1, 1787018400000),
  (7390000000000001001, 7390000000000002003, 2, 1787018400000),
  (7390000000000001001, 7390000000000002004, 3, 1787018400000),
  -- the default system prompt is anchored at position 0 on thread creation
  (7390000000000001002, 7390000000000002011, 0, 1787029200000),
  (7390000000000001002, 7390000000000002012, 1, 1787029210000),
  (7390000000000001002, 7390000000000002013, 2, 1787029225000),
  (7390000000000001002, 7390000000000002014, 3, 1787029300000),
  (7390000000000001002, 7390000000000002015, 4, 1787029320000),
  (7390000000000001003, 7390000000000002021, 0, 1787036410000),
  (7390000000000001003, 7390000000000002022, 1, 1787036430000),
  (7390000000000001004, 7390000000000002031, 0, 1787022000000);

-- ----------------------------------------------------------- thread_label

INSERT INTO thread_label (thread_id, label, created_at) VALUES
  (7390000000000001001, 'agent:claude_code', 1787018400000),
  (7390000000000001001, 'branch:main', 1787018400000),
  (7390000000000001001, 'coding_agent', 1787018400000),
  (7390000000000001001, 'dir:lookback', 1787018400000),
  (7390000000000001001, 'path:works/lookback', 1787018400000),
  (7390000000000001004, 'agent:claude_code', 1787022000000),
  (7390000000000001004, 'branch:main', 1787022000000),
  (7390000000000001004, 'coding_agent', 1787022000000),
  (7390000000000001004, 'dir:lookback', 1787022000000),
  (7390000000000001004, 'path:works/lookback', 1787022000000),
  (7390000000000001004, 'summary', 1787022000000);

COMMIT;
