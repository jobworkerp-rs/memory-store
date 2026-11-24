-- Data of a Lookback desktop database written by releases v0.0.1..v0.0.6,
-- before `memory_kind` existed.
--
-- Target schema: the v0.0.6 server startup schema, i.e.
-- `infra/sql/sqlite/001_schema.sql` + `003_reflection_schema.sql` at tag
-- v0.0.6 (identical in v0.0.1..v0.0.6). Load with `sqlite3 db < file` after
-- those two files; `manual/011_add_memory_kind.sql` is applied afterwards.
--
-- Contents (users 1 and 2 are the desktop users):
--   7380000000000001001  user 1, Claude Code session imported by
--                        agent-chat-import (source-prefixed external_id,
--                        metadata.source); later summarized
--   7380000000000001002  user 2, imported Claude Code session
--   7380000000000001003  user 1, chat with a default system prompt
--   7380000000000001004  user 1, branch of ...1003; memory ...2023 belongs
--                        to both threads
--   7380000000000001005  user 2, empty thread without description
--   7380000000000001006  summary thread of ...1001 (owner 100000)
--   7380000000000001007  daily summary of 2026-07-20 (owner 100000)
--   7380000000000001008  personality signal of ...1001 (owner 200000)
--   7380000000000001009  summary thread whose source thread
--                        7380000000000000999 was deleted by the user
--   memory 7380000000000002031: saved system prompt not attached to a thread
--
-- Owners 100000 / 200000 are the defaults the v0.0.6 workflows used for
-- generated threads (`summary_user_id`, `personality_user_id`); the shapes
-- of their labels, metadata and external_ids follow
-- agent-chat-import/workers/{thread-summary,daily-work-summary,personality}
-- at v0.0.6.
--
-- Thread ...1009 is the only row `migrate-memory-kind client-apply` with an
-- empty mapping cannot classify (missing_summary_source_thread). Nothing
-- references it or its memory, and that memory carries neither
-- metadata.source nor an aggregate external_id prefix, so
-- `client-prune-unresolved` can drop the thread and a second client-apply
-- finishes without warnings. Keep it that way when editing this file.

BEGIN TRANSACTION;

-- ---------------------------------------------------------------- threads

INSERT INTO thread (id, default_system_memory_id, user_id, description, channel, embedding, embedding_dim, created_at, updated_at, metadata) VALUES
  (7380000000000001001, NULL, 1, '【スレッド一覧のページング修正】 Lookback のスレッド一覧で 2 ページ目以降が重複表示される不具合を、カーソルを updated_at と id の組に変えて修正した。', 'claude_code:3f2c9a1e-5b7d-4c11-9e2a-8d0f6a4b2c10', NULL, NULL, 1784505600000, 1784516400000, NULL),
  (7380000000000001002, NULL, 2, 'dotfiles (master)', 'claude_code:b7e4d2c1-0a9f-4e3d-8c2b-1a0f9e8d7c6b', NULL, NULL, 1783990800000, 1783990860000, NULL),
  (7380000000000001003, 7380000000000002021, 1, '夏休みの旅行計画', NULL, NULL, NULL, 1783850400000, 1783850520000, NULL),
  (7380000000000001004, 7380000000000002021, 1, '夏休みの旅行計画 (電車案)', NULL, NULL, NULL, 1783852200000, 1783852230000, NULL),
  (7380000000000001005, NULL, 2, NULL, NULL, NULL, NULL, 1784102400000, 1784102400000, NULL),
  (7380000000000001006, NULL, 100000, 'agent:claude_code, branch:main, coding_agent, dir:lookback, path:works/lookback の要約集', NULL, NULL, NULL, 1784505600000, 1784516400000, NULL),
  (7380000000000001007, NULL, 100000, '2026-07-20 の作業要約 (1 スレッド)', NULL, NULL, NULL, 1784473200000, 1784561400000, NULL),
  (7380000000000001008, NULL, 200000, 'personality signals for thread 7380000000000001001 (user 1)', NULL, NULL, NULL, 1784505600000, 1784517000000, NULL),
  (7380000000000001009, NULL, 100000, 'thread_7380000000000000999 の要約集', NULL, NULL, NULL, 1783641600000, 1783652400000, NULL);

-- --------------------------------------------------------------- memories

INSERT INTO memory (id, parent_ids, user_id, content, content_type, params, metadata, created_at, updated_at, role, external_id, media_object_id) VALUES
  -- imported session of user 1 (role 1=user, 2=assistant, 4=tool; content_type 1=tool)
  (7380000000000002001, '[]', 1, 'スレッド一覧の 2 ページ目に 1 ページ目と同じスレッドが出る。原因を調べて直して。', 0, NULL,
   '{"source":"claude_code","kind":"user","session":{"cwd":"/home/user/works/lookback","git_branch":"main"}}',
   1784505600000, 1784505600000, 1, 'claude_code:3f2c9a1e-5b7d-4c11-9e2a-8d0f6a4b2c10:0e1d2c3b-4a59-4687-9a1b-2c3d4e5f6a70:b0:4f1c2a9e7d3b8c60', NULL),
  (7380000000000002002, '[7380000000000002001]', 1, 'ページングが updated_at だけをカーソルにしているため、同じ時刻のスレッドが境界で重複しています。id を第 2 キーに加えます。', 0, NULL,
   '{"source":"claude_code","kind":"assistant","session":{"cwd":"/home/user/works/lookback","git_branch":"main"}}',
   1784505660000, 1784505660000, 2, 'claude_code:3f2c9a1e-5b7d-4c11-9e2a-8d0f6a4b2c10:1f2e3d4c-5b6a-4798-8b2c-3d4e5f6a7b81:b0:9a8b7c6d5e4f3a21', NULL),
  (7380000000000002003, '[7380000000000002002]', 1, '{"name":"Edit","input":{"file_path":"src/thread_list.rs"}}', 1, NULL,
   '{"source":"claude_code","kind":"tool_call","tool":{"name":"Edit"},"session":{"cwd":"/home/user/works/lookback","git_branch":"main"}}',
   1784505720000, 1784505720000, 2, 'claude_code:3f2c9a1e-5b7d-4c11-9e2a-8d0f6a4b2c10:2a3b4c5d-6e7f-48a9-9c3d-4e5f6a7b8c92:b1:c3d2e1f0a9b8c7d6', NULL),
  (7380000000000002004, '[7380000000000002003]', 1, 'The file src/thread_list.rs has been updated.', 1, NULL,
   '{"source":"claude_code","kind":"tool_output","tool":{"name":"Edit"},"session":{"cwd":"/home/user/works/lookback","git_branch":"main"}}',
   1784505780000, 1784505780000, 4, 'claude_code:3f2c9a1e-5b7d-4c11-9e2a-8d0f6a4b2c10:3b4c5d6e-7f80-49ba-8d4e-5f6a7b8c9da3:b0:e5f4d3c2b1a09f8e', NULL),

  -- imported session of user 2
  (7380000000000002011, '[]', 2, 'zsh の起動が遅いので原因を調べて。', 0, NULL,
   '{"source":"claude_code","kind":"user","session":{"cwd":"/home/user2/works/dotfiles","git_branch":"master"}}',
   1783990800000, 1783990800000, 1, 'claude_code:b7e4d2c1-0a9f-4e3d-8c2b-1a0f9e8d7c6b:4c5d6e7f-8091-4acb-9e5f-6a7b8c9daeb4:b0:0f1e2d3c4b5a6978', NULL),
  (7380000000000002012, '[7380000000000002011]', 2, 'nvm の初期化が毎回走っています。遅延ロードにすると 0.6 秒短縮できます。', 0, NULL,
   '{"source":"claude_code","kind":"assistant","session":{"cwd":"/home/user2/works/dotfiles","git_branch":"master"}}',
   1783990860000, 1783990860000, 2, 'claude_code:b7e4d2c1-0a9f-4e3d-8c2b-1a0f9e8d7c6b:5d6e7f80-91a2-4bdc-8f6a-7b8c9daebfc5:b0:1a2b3c4d5e6f7081', NULL),

  -- chat of user 1 with a default system prompt (role 3=system); the
  -- server injected the default as parent_ids[0]
  (7380000000000002021, '[]', 1, 'あなたは Lookback のアシスタントです。簡潔に日本語で答えてください。', 0, NULL, NULL,
   1783850395000, 1783850395000, 3, NULL, NULL),
  (7380000000000002022, '[7380000000000002021]', 1, '8 月に 3 泊で金沢と富山を回りたい。移動手段の案を出して。', 0, NULL, NULL,
   1783850410000, 1783850410000, 1, NULL, NULL),
  (7380000000000002023, '[7380000000000002021,7380000000000002022]', 1, '北陸新幹線で金沢に入り、富山へは在来線で戻る案と、レンタカーで能登を回る案があります。', 0, NULL, NULL,
   1783850425000, 1783850425000, 2, NULL, NULL),
  (7380000000000002024, '[7380000000000002021,7380000000000002023]', 1, 'レンタカー案で宿の候補を教えて。', 0, NULL, NULL,
   1783850500000, 1783850500000, 1, NULL, NULL),
  (7380000000000002025, '[7380000000000002021,7380000000000002024]', 1, '和倉温泉に 1 泊、金沢市内に 2 泊する組み合わせが移動時間を抑えられます。', 0, NULL, NULL,
   1783850520000, 1783850520000, 2, NULL, NULL),
  -- branch: the new thread continued from ...2023, which attached it
  (7380000000000002026, '[7380000000000002021,7380000000000002023]', 1, '電車だけで回る場合の時刻表の目安は?', 0, NULL, NULL,
   1783852210000, 1783852210000, 1, NULL, NULL),
  (7380000000000002027, '[7380000000000002021,7380000000000002026]', 1, '東京 8:00 発のかがやきで金沢 10:30 着、富山へはあいの風とやま鉄道で約 1 時間です。', 0, NULL, NULL,
   1783852230000, 1783852230000, 2, NULL, NULL),

  -- saved system prompt that never got a thread
  (7380000000000002031, '[]', 1, 'あなたはコードレビュアーです。指摘は重要度順に並べてください。', 0, NULL, NULL,
   1783760400000, 1783760400000, 3, NULL, NULL),

  -- thread-summary of ...1001 (owner 100000)
  (7380000000000002041, '[]', 100000, '{"category":"bugfix","title":"スレッド一覧のページング修正","summary":"Lookback のスレッド一覧で 2 ページ目以降が重複表示される不具合を、カーソルを updated_at と id の組に変えて修正した。","key_decisions":["カーソルに id を第 2 キーとして追加"],"status":"completed"}', 0, NULL,
   '{"source_thread_id":"7380000000000001001","category":"bugfix","summary_version":"1.0"}',
   1784505600000, 1784505780000, 2, 'summary:7380000000000001001', NULL),

  -- daily-work-summary of 2026-07-20 (JST), built from ...2041
  (7380000000000002051, '[]', 100000, '{"overall_purpose":"Lookback の一覧表示の不具合修正","items":[{"purpose":"スレッド一覧のページング修正","bullets":["カーソルを updated_at と id の組に変更"],"source_memory_ids":["7380000000000002041"],"status":"completed"}]}', 0, NULL,
   '{"daily_date":"2026-07-20","scope":"_all","extra_labels":[],"source_memory_count":1,"source_memory_ids":["7380000000000002041"],"source_thread_ids":["7380000000000001006"],"summary_version":"1.0"}',
   1784473200000, 1784505780000, 2, 'daily:2026-07-20:_all', NULL),

  -- thread-personality signal of ...1001 (owner 200000)
  (7380000000000002061, '[]', 200000, '{"no_signal":false,"signals":[{"trait":"根本原因を確認してから修正方針を決める","evidence":"原因を調べて直して"}]}', 0, NULL,
   '{"source_user_id":"1","source_thread_id":"7380000000000001001","signal_version":"1.0","no_signal":false,"truncation_level":0}',
   1784505600000, 1784505780000, 2, 'personality:7380000000000001001', NULL),

  -- thread-summary of a conversation the user deleted afterwards
  (7380000000000002071, '[]', 100000, '{"category":"other","title":"引っ越しの手続き","summary":"転居届と電気・ガスの契約変更の順番を整理した。","key_decisions":[],"status":"completed"}', 0, NULL,
   '{"source_thread_id":"7380000000000000999","category":"other","summary_version":"1.0"}',
   1783641600000, 1783645200000, 2, 'summary:7380000000000000999', NULL);

-- ---------------------------------------------------------- thread_memory

INSERT INTO thread_memory (thread_id, memory_id, position, created_at) VALUES
  (7380000000000001001, 7380000000000002001, 0, 1784509200000),
  (7380000000000001001, 7380000000000002002, 1, 1784509200000),
  (7380000000000001001, 7380000000000002003, 2, 1784509200000),
  (7380000000000001001, 7380000000000002004, 3, 1784509200000),
  (7380000000000001002, 7380000000000002011, 0, 1783994400000),
  (7380000000000001002, 7380000000000002012, 1, 1783994400000),
  -- the default system prompt is anchored at position 0 on thread creation
  (7380000000000001003, 7380000000000002021, 0, 1783850400000),
  (7380000000000001003, 7380000000000002022, 1, 1783850410000),
  (7380000000000001003, 7380000000000002023, 2, 1783850425000),
  (7380000000000001003, 7380000000000002024, 3, 1783850500000),
  (7380000000000001003, 7380000000000002025, 4, 1783850520000),
  (7380000000000001004, 7380000000000002021, 0, 1783852200000),
  (7380000000000001004, 7380000000000002023, 1, 1783852210000),
  (7380000000000001004, 7380000000000002026, 2, 1783852210000),
  (7380000000000001004, 7380000000000002027, 3, 1783852230000),
  (7380000000000001006, 7380000000000002041, 0, 1784516400000),
  (7380000000000001007, 7380000000000002051, 0, 1784561400000),
  (7380000000000001008, 7380000000000002061, 0, 1784517000000),
  (7380000000000001009, 7380000000000002071, 0, 1783652400000);

-- ----------------------------------------------------------- thread_label

INSERT INTO thread_label (thread_id, label, created_at) VALUES
  (7380000000000001001, 'agent:claude_code', 1784509200000),
  (7380000000000001001, 'branch:main', 1784509200000),
  (7380000000000001001, 'coding_agent', 1784509200000),
  (7380000000000001001, 'dir:lookback', 1784509200000),
  (7380000000000001001, 'path:works/lookback', 1784509200000),
  (7380000000000001002, 'agent:claude_code', 1783994400000),
  (7380000000000001002, 'branch:master', 1783994400000),
  (7380000000000001002, 'coding_agent', 1783994400000),
  (7380000000000001002, 'dir:dotfiles', 1783994400000),
  (7380000000000001002, 'path:works/dotfiles', 1783994400000),
  (7380000000000001006, 'agent:claude_code', 1784516400000),
  (7380000000000001006, 'branch:main', 1784516400000),
  (7380000000000001006, 'coding_agent', 1784516400000),
  (7380000000000001006, 'dir:lookback', 1784516400000),
  (7380000000000001006, 'path:works/lookback', 1784516400000),
  (7380000000000001006, 'summary', 1784516400000),
  (7380000000000001007, 'daily_summary', 1784561400000),
  (7380000000000001007, 'date:2026-07-20', 1784561400000),
  (7380000000000001007, 'scope:_all', 1784561400000),
  (7380000000000001008, 'personality', 1784517000000),
  (7380000000000001008, 'personality_signal', 1784517000000),
  (7380000000000001008, 'thread:7380000000000001001', 1784517000000),
  (7380000000000001008, 'user:1', 1784517000000),
  (7380000000000001009, 'summary', 1783652400000),
  (7380000000000001009, 'thread_7380000000000000999', 1783652400000);

COMMIT;
