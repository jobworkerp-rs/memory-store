# agent-chat-import (`memories-import`)

エージェントとの対話ログを memories に取り込む standalone CLI。crate 名は `agent-chat-import`、binary 名は `memories-import`。

ソース別サブコマンドを取り、現在は以下をサポート:

- `claude-code` — Claude Code JSONL transcript (`~/.claude/projects/<hash>/<session>.jsonl`)
- `codex` — OpenAI Codex CLI rollout (`~/.codex/sessions/<yyyy>/<mm>/<dd>/rollout-*.jsonl`)
- `plain` — Obsidian vault などのプレーンテキストツリー (`.md` / `.txt`)

## Canonical schema

正規化レイヤにより、表示層 / RAG / summary workflow が **source 非依存** で memory metadata を読めるようになった。具体的には:

- `metadata.tool` (tool_call / tool_output 共通): `name` (provider 提供の生 tool 名)、`source`、`category` (importer 共通テーブルで決まる用途分類: `shell_exec` / `file_read` / `file_write` / `file_search` / `web_search` / `web_fetch`)、`call_id`、`arguments` / `output`、`status` (tri-state: `ok` / `error` / null)、`source_event` (codex の `exec_command_end` / `patch_apply_end` 由来か)
- attachment (image / audio / video / url / ref): メディア本体は S3/file/url などサーバ側の media storage に保存または登録され、Memory は `media_object_id` で参照する。
- `metadata.raw.<source>`: 正規化で落ちた provider 拡張フィールドの保全
- `metadata.claude_code.*` (claude-code source 限定): JSONL 行の付帯 top-level scalar を **網羅的に転記** したオブジェクト。表示層 (agent-app 等) が本文を読まずに足場情報を判定できる documented contract。
  - **転記方針**: 除外集合 (本文 = `message` / `content` / `title`、および canonical/block/attachment 経路が既に消費する大 payload = `toolUseResult` / `data` / `snapshot` / `attachment` / `files` / `stdout` 等) **以外** の top-level field を汎用転記する。新しい upstream field はコード変更なしで自動的に拾われる。
  - **サイズガード**: 単一値が serialize 後 2048 bytes を超える場合は当該値のみスキップ (entry 全体ではなく値単位)。`null` 値もスキップ。本文・大 payload は `MemoryData.content` / `canonical.*` 側が保持するため metadata は軽量な属性バッグに保たれる。
  - **安定キー** (snake_case 固定 contract 名): `user_type` / `is_meta` / `entrypoint` / `claude_version` (raw `version`) / `slug` / `prompt_id` / `tool_use_id` / `source_tool_assistant_uuid` / `parent_tool_use_id`。その他の field は camelCase → snake_case の汎用変換。
  - **promoted top-level キー** (`uuid` / `parent_uuid` / `entry_type` / `is_sidechain` / `request_id` / `block_type` / `subtype`) は metadata 直下に置かれ、`claude_code.*` には重複させない。
  - codex source は session_meta 付帯 field を metadata 直下 (`session_source` / `git` / `cli_version` 等) に持つ。`claude_code` は claude 固有名のため衝突しない。

既定の env サイズ閾値:
- `MEMORY_TOOL_OUTPUT_FULL_BYTES = 65536` (`metadata.tool.output` 上限)
- `MEMORY_TOOL_OUTPUT_PREVIEW_BYTES = 4096` (`MemoryData.content` の preview)
- `MEMORY_TOOL_ARG_PREVIEW_BYTES = 512` (tool_call サマリの arguments 部)
- `MEMORY_ATTACHMENT_INLINE_MAX_BYTES = 1048576` (inline base64 入力を受け付ける上限。import 成功時は `media_object` 化される)

メディア登録に失敗した場合や変換できない入力の場合だけ、batch 全体は止めず、後続の再処理で回収できる情報を metadata に保持します。

block 分解により以下の memory が生成される:

- `tool_use` block → `kind=tool_call`, `role=assistant`, `content_type=tool` の 1 memory
- `tool_result` block → `kind=tool_output`, `role=tool`, `content_type=tool` の 1 memory + 内部 image があれば `kind=attachment` の sub-block memory
- 直 `image` block → `kind=attachment` の 1 memory
- `type=attachment` JSONL イベント → `kind=attachment`, `role=meta` の 1 memory (subtype 別本文抽出付き)

global オプション (`-u, -s, -l, -n, -v, -b, --server-url`, summarize 系) は **subcommand の前後どちらにも置ける**:

```bash
memories-import claude-code -u 1 --all-projects   # 後置
memories-import -u 1 claude-code --all-projects   # 前置
```

## ビルド

```bash
# 既定 (sqlite + summarize-after + personality-after)
cargo build --release -p agent-chat-import

# summarize 連携だけ要らないとき (personality 連携のみ残す)
cargo build --release -p agent-chat-import --no-default-features --features personality-after

# personality 連携だけ要らないとき (summarize 連携のみ残す)
cargo build --release -p agent-chat-import --no-default-features --features summarize-after
```

`summarize-after` と `personality-after` は独立した feature。両方とも jobworkerp 経由で対応するワークフロー (`thread-summary-batch.yaml` / `thread-personality-batch.yaml`) を起動する CLI ラッパーで、ワークフロー本体とは無関係に on/off できる。

ビルド成果物は `target/release/memories-import`。

## 必要な環境変数

| 変数 | いつ必要か | 例 |
|---|---|---|
| `--server-url` | import 実行時 (`--dry-run` 以外) | `http://localhost:9010` |
| `JOBWORKERP_ADDR` | `--summarize-after-*` または `--extract-personality-after-*` を指定したとき (URI スキーム必須) | `http://localhost:9000` |

## `claude-code` サブコマンド

### 単一 JSONL セッションを取り込む

```bash
memories-import --user-id 1 --server-url http://localhost:9010 claude-code \
  --session-file ~/.claude/projects/-home-me-app/abc123.jsonl
```

### 1 プロジェクト分まとめて取り込む

```bash
memories-import --user-id 1 --server-url http://localhost:9010 claude-code \
  --project-dir ~/.claude/projects/-home-me-app
```

### `~/.claude/projects/` 配下を全部 + 直近以降のみ + ラベル付与

```bash
memories-import --user-id 1 \
  --server-url http://localhost:9010 \
  --since 2026-04-29T00:00:00Z \
  --labels coding_agent,claude_code \
  claude-code --all-projects
```

`--labels` は **多重指定可** (clap `ArgAction::Append`)。各 occurrence は CSV 分割され、全 occurrences を flatten + 重複除去する: `--labels foo,bar --labels baz` → `[foo, bar, baz]`。1 ラベル 512 byte 上限 (`thread_label.label VARCHAR(512)` と整合) を超えると **CLI parse 段階でエラー終了** (truncate しない)。

### `path:` ラベルからベースパスを剥がして相対化する

`/home/<user>/...` のような環境依存プレフィクスがラベルに混じるのを避けたい場合、
`-P, --strip-path-prefix` でカンマ区切りのベースパスを指定する。`cwd` がいずれかの
ベースパス配下のときだけ、その分を取り除いた相対パスが `path:` ラベルになる
(複数マッチ時は最長を採用、マッチしなければ絶対パスが入る)。

```bash
memories-import --user-id 1 claude-code \
  --server-url http://localhost:9010 \
  --all-projects \
  --strip-path-prefix /home/me,/usr/share
```

例: `cwd=/home/me/work/foo` → ラベルは `path:work/foo`。
`dir:` ラベルや `metadata.project_path` は変化しない (絶対パスのまま)。

### dry-run (DB 接続なしで件数だけ確認)

```bash
memories-import --user-id 1 \
  --dry-run \
  --since 2026-04-29T00:00:00Z \
  claude-code --all-projects
```

dry-run でも `--user-id` と subcommand の input group (`--session-file` / `--project-dir` / `--all-projects` のいずれか 1 つ) は **必須**。

## 生成系 lang_worker の登録

direct enqueue / ローカルアプリ向けに、reflection / summary / personality の言語別 WORKFLOW worker を登録する。
prompt 本文は登録時に worker settings へ焼き込まれるため、実行時に prompt ファイル、HTTP endpoint、
python3 へ依存しない。prompt を変更した場合は再登録する。

```bash
JOBWORKERP_ADDR=http://localhost:9000 \
memories-import upsert-generation-workers \
  --feature all \
  --language all \
  --channel workflow_lang
```

登録される worker 名は `memories-thread-reflection-single-ja/en`,
`memories-thread-summary-single-ja/en`, `memories-daily-work-summary-single-ja/en`,
`memories-weekly-work-summary-single-ja/en`, `memories-monthly-work-summary-single-ja/en`,
`memories-thread-personality-single-ja/en`,
`memories-user-personality-merge-ja/en`。
このサブコマンドは import ではないため `--user-id` は不要。

### `workers/` ディレクトリの場所指定

登録時に各 `workers/<feature>/*-single.yaml` と `workers/<feature>/prompts/*.txt` を読むため、
それらを含む **agent-chat-import crate dir** を解決する必要がある。解決順は次のとおり:

1. `--repo-root <PATH>`（CLI 引数。最優先）
2. `MEMORY_REPO_ROOT` 環境変数
3. ビルド時の `agent-chat-import` crate dir（`CARGO_MANIFEST_DIR`。デフォルト）

`memories-import` バイナリをビルドツリーから離れた場所（コンテナ / 別ホスト等）へ配布した場合、
デフォルト（3）はビルドしたマシンの絶対パスを指すため `workers/` を見つけられない。
その場合は `--repo-root` か `MEMORY_REPO_ROOT` で `agent-chat-import` crate dir を明示する。
`MEMORY_REPO_ROOT` を指定する場合は、workspace root ではなく `agent-chat-import` crate dir を指定する:

```bash
# CLI 引数で指定
memories-import upsert-generation-workers --feature all --repo-root /opt/memories/agent-chat-import

# 環境変数で指定（k8s / コンテナ運用向け）
MEMORY_REPO_ROOT=/opt/memories/agent-chat-import \
  memories-import upsert-generation-workers --feature all
```

## 取り込み + 要約 (`--summarize-after-*`)

import 完了 (および embedding redispatch) のあとに `agent-chat-import/workflows/thread-summary/thread-summary-batch.yaml` を jobworkerp 経由で起動する。`summarize-after` feature が有効 (default) かつ `JOBWORKERP_ADDR` 指定時のみ動作する。

`user_id` / `updated_after_ms` は memories-import の引数 (`--user-id` と `--since`) から自動的に上書きされる。`--since` 未指定なら `updated_after_ms` は touch しない (テンプレ側の値を尊重)。`--since` を絶対値 (epoch ms) として workflow にそのまま渡すため、jobworkerp のキュー待ち時間が長くても要約対象の下限が import 範囲と揃う。

import が `summary.errors > 0` で終わった場合、要約 dispatch は skip される (中途半端なスレッドを要約してしまわないためのガード)。

要約・reflection の prompt は、言語別 worker 登録時に `agent-chat-import/workers/<feature>/prompts/`
から worker settings へ焼き込まれる。出力言語は `--output-language` > `MEMORY_DEFAULT_LANGUAGE` > `ja`
の順で解決し、batch は言語別 worker 名を選択して fan-out する。

### 入力 JSON の準備

```bash
cat > /tmp/summarize.json <<'EOF'
{
  "memories_grpc_host": "localhost",
  "memories_grpc_port": 9010,
  "ollama_base_url": "http://192.168.1.2:11434",
  "summary_model": "qwen3.6:27b",
  "summary_user_id": 100000
}
EOF
```

### import + 要約 (基本)

```bash
JOBWORKERP_ADDR=http://localhost:9000 \
memories-import --user-id 1 \
  --server-url http://localhost:9010 \
  --summarize-after-file /tmp/summarize.json \
  --summarize-workflow /abs/path/to/memories/agent-chat-import/workflows/thread-summary/thread-summary-batch.yaml \
  claude-code --all-projects
```

### import + 要約 (`--since` で `updated_after_ms` 自動算出)

```bash
JOBWORKERP_ADDR=http://localhost:9000 \
memories-import --user-id 1 \
  --server-url http://localhost:9010 \
  --output-language ja \
  --since 2026-04-29T00:00:00Z \
  --summarize-after-file /tmp/summarize.json \
  --summarize-workflow /abs/path/to/memories/agent-chat-import/workflows/thread-summary/thread-summary-batch.yaml \
  claude-code --all-projects
```

`--since` の epoch ms (この例では `1745884800000`) が `updated_after_ms` として batch input にセットされ、workflow 内の `updated_after` フィルタにそのまま使われる。

### インライン JSON + チャンネル + 短い timeout

```bash
JOBWORKERP_ADDR=http://localhost:9000 \
memories-import --user-id 1 \
  --server-url http://localhost:9010 \
  --summarize-after-json '{"memories_grpc_host":"localhost","memories_grpc_port":9010,"summary_user_id":100000,"min_message_count":2}' \
  --summarize-workflow /abs/path/to/thread-summary-batch.yaml \
  --summarize-channel summarize \
  --summarize-timeout-sec 7200 \
  claude-code --all-projects
```

### dry-run + summarize-after (実 dispatch は skip、JSON validation のみ)

```bash
memories-import --user-id 1 \
  --dry-run \
  --summarize-after-file /tmp/summarize.json \
  --summarize-workflow /abs/path/to/thread-summary-batch.yaml \
  claude-code --all-projects
```

末尾に `[dry-run] Skipping thread-summary-batch workflow execution` が出る。

### 短スパン (1時間おき等) 運用での parse skip

`--since` 指定時は session ファイル単位の mtime filter が自動的に有効になり、`since - margin` より古い JSONL は **parse 自体を skip** する (`memories_skipped_filtered` に計上)。

```bash
# 1 時間おき cron での import 例 (jaq / GNU date 想定)
memories-import --user-id 1 -v \
  --server-url http://localhost:9010 \
  --since "$(date -u -d '1 hour ago' +%Y-%m-%dT%H:%M:%SZ)" \
  claude-code --all-projects
```

- `--mtime-margin-seconds 60` (既定): 書き込み中ファイルが誤 skip されるリスクを 60 秒のマージンで吸収。閉じ際の append でも 60 秒以内に since 値を超えていれば parse される
- `--no-mtime-filter`: NFS / クラウドストレージ等で mtime が古いまま固定される / 大幅遅延する環境では disable
- `metadata()` / `modified()` 呼び出しが失敗した場合は **保守的に parse 続行**。skip による取りこぼしより重複処理の方が安全

## 取り込み + 嗜好抽出 (`--extract-personality-after-*`)

import 完了 (および embedding redispatch) のあとに `agent-chat-import/workflows/personality/thread-personality-batch.yaml` を jobworkerp 経由で起動する。`personality-after` feature が有効 (default) かつ `JOBWORKERP_ADDR` 指定時のみ動作する。

`--summarize-after-*` と独立した別系統で、両方を同時に指定すると **summarize と personality を並列に dispatch** する (失敗は片方ずつ独立して扱われる)。`user_id` / `updated_after_ms` の自動上書き、`--since` の絶対 epoch ms 渡し、`summary.errors > 0` 時の skip ガードはすべて summarize 系と同じ挙動。

import 完了後の dispatch 関係:

```
memories-import (import 本体)
        │
        ├── --summarize-after-*           → agent-chat-import/workflows/thread-summary/thread-summary-batch.yaml
        └── --extract-personality-after-* → agent-chat-import/workflows/personality/thread-personality-batch.yaml
                                              (内部で thread-personality-single を fan-out)
```

ワークフロー詳細は `agent-chat-import/workflows/personality/README_ja.md` を参照。

**観測性の注意**: `thread-personality-batch` は per-thread fan-out で `onError: continue` を使うため、個別スレッドの抽出失敗は握り潰され、batch ワークフロー自体は常に `completed: true` で返る (これは既存の `thread-summary-batch` と同じ挙動)。memories-import 側の `dispatch_personality_after` も batch ワークフロー全体が落ちたときだけ warning を吐く。個別スレッドの成否は jobworkerp の per-job ログまたは memories DB の personality memory 件数で確認すること。

なお `agent-chat-import/workflows/agent-chat-pipeline/agent-chat-pipeline.yaml` (および `agent-chat-import/workflows/agent-chat-pipeline/run-pipeline.sh` に personality 系 YAML パスを渡すか `--enable-personality` を指定する) を使えば、import → summary → personality の連携を memories-import の CLI フラグではなくパイプライン YAML 側で完結させられる (こちらは `personality-after` feature 不要)。

### 入力 JSON の準備

```bash
cat > /tmp/personality.json <<'EOF'
{
  "memories_grpc_host": "localhost",
  "memories_grpc_port": 9010,
  "ollama_base_url": "http://192.168.1.2:11434",
  "personality_model": "qwen3.6:27b",
  "personality_user_id": 200000,
  "summary_user_id": 100000,
  "min_user_messages": 2,
  "merge_enabled": true
}
EOF
```

`personality_user_id` は **`user_id` および `summary_user_id` と異なる値** にすること (再帰汚染防止)。`personality_model` を省略するとワークフロー入力スキーマの既定 (`qwen3.6:27b`) が使われる。

### import + 嗜好抽出 (基本)

```bash
JOBWORKERP_ADDR=http://localhost:9000 \
memories-import --user-id 1 \
  --server-url http://localhost:9010 \
  --output-language ja \
  --since 2026-04-29T00:00:00Z \
  --extract-personality-after-file /tmp/personality.json \
  --personality-workflow /abs/path/to/memories/agent-chat-import/workflows/personality/thread-personality-batch.yaml \
  claude-code --all-projects
```

### import + summary + personality (両立)

```bash
JOBWORKERP_ADDR=http://localhost:9000 \
memories-import --user-id 1 \
  --server-url http://localhost:9010 \
  --since 2026-04-29T00:00:00Z \
  --summarize-after-file /tmp/summarize.json \
  --summarize-workflow /abs/path/to/memories/agent-chat-import/workflows/thread-summary/thread-summary-batch.yaml \
  --extract-personality-after-file /tmp/personality.json \
  --personality-workflow /abs/path/to/memories/agent-chat-import/workflows/personality/thread-personality-batch.yaml \
  claude-code --all-projects
```

両ワークフローは **並列に dispatch** される (所有者 `user_id` が完全に分離されているため衝突しない)。片方の失敗が他方をブロックしない。

### 2層 (ユーザ統合プロファイル) も連動させたい場合

`thread-personality-batch.yaml` の入力に `merge_enabled: true` を指定すると、1層の後に2層 (`memories-user-personality-merge-<lang>`) も同じ batch 内で起動する。`--output-language` は personality の1層/2層 worker 選択にも使われ、未指定時は `MEMORY_DEFAULT_LANGUAGE`、さらに未指定なら `ja` になる。

### プロンプト context の供給 (embedded_context 経路)

personality の prompt は、言語別 worker 登録時に
`agent-chat-import/workers/personality/prompts/{thread_system_prompt,merge_system_prompt}.<lang>.txt`
から worker settings へ焼き込まれる。

batch→single / merge の fan-out は言語別 worker (`memories-*-<lang>`) を `workerName` で呼ぶため、
bare なテンプレートでも prompt context は不要。事前に
`memories-import upsert-generation-workers --feature all --language all --channel workflow_lang`
などで言語別 worker を登録しておく。

## `codex` サブコマンド

OpenAI Codex CLI の rollout JSONL を取り込む。1 rollout = 1 thread。`session_meta.payload.id` (UUIDv4) を `session_id` として使用するため、同じ rollout を再 import すると idempotent に skip される (`external_id` UNIQUE)。

```bash
# 単一 rollout
memories-import --user-id 1 --server-url http://localhost:9010 codex \
  --session-file ~/.codex/sessions/2026/05/02/rollout-2026-05-02T10-22-47-...-.jsonl

# 1 日分まとめて
memories-import --user-id 1 --server-url http://localhost:9010 codex \
  --day-dir ~/.codex/sessions/2026/05/02

# 全件 dry-run
memories-import --user-id 1 --dry-run codex --all-sessions
```

### type ごとの取り込み方針

`metadata.kind` フィールドで分類。`-t, --include-types` で抑制可能 (default は全て):

| `metadata.kind` | role | content_type | 由来 |
|---|---|---|---|
| `system` | Meta | Text | `session_meta` (base_instructions を content), `turn_context`, `token_count`, `task_started/complete`, `turn_aborted`, `exec_command_end`, `patch_apply_end`, `entered/exited_review_mode`, その他 event_msg |
| `user` | User | Text | `response_item.message`(role=user), `event_msg.user_message` |
| `assistant` | Assistant | Text | `response_item.message`(role=assistant), `event_msg.agent_message` |
| `reasoning` | Assistant | Text | `response_item.reasoning` (`encrypted_content` は **既定では保存しない**。代わりに `encrypted_content_sha256` と `encrypted_content_size` のみ metadata に残す。`--include-encrypted-reasoning` で本文を保存), `event_msg.agent_reasoning` |
| `tool_call` | Assistant | Tool | `response_item.function_call`, `response_item.custom_tool_call` |
| `tool_output` | Tool | Tool | `response_item.function_call_output`, `response_item.custom_tool_call_output` |

`role=Meta` の memory は `MemoryVectorService.SearchByText` 等の意味検索には載らない (embedding dispatcher が role=USER/ASSISTANT/SYSTEM のみを許可)。直接 `MemoryService.Find` で取得は可能。

### tool_call ↔ tool_output リンク

既定で `function_call_output` (および `custom_tool_call_output`) は同一 `call_id` を持つ `function_call` の memory を `parent_ids` に張る。メインループで前方参照しつつ、ループ後に未解決 output を再走査して埋めるので、output が call より前に現れる reorder/編集済み rollout でも親子リンクが復旧する。

`--no-link-tool-calls` で無効化可 (output の `parent_ids` は空のまま取り込まれる):

```bash
memories-import --user-id 1 codex --all-sessions --no-link-tool-calls
```

### `codex` サブコマンド固有オプション

| オプション | 必須 | デフォルト | 備考 |
|---|---|---|---|
| `-f, --session-file` / `--day-dir` / `--all-sessions` | いずれか 1 つ ○ | — | 排他 |
| `-d, --codex-dir` | — | `~/.codex` | `~` は `HOME` に展開 |
| `-t, --include-types` | — | `user,assistant,tool_call,tool_output,system,reasoning` | カンマ区切り |
| `-P, --strip-path-prefix` | — | (なし) | `path:` ラベルから剥がすベースパス。`session_meta.payload.cwd` に効く |
| `--include-encrypted-reasoning` | — | `false` | `reasoning.encrypted_content` の本文を metadata に保存。**既定では保存しない**(sensitive blob が DB / dump / backup に残らないように)。本文の代わりに `encrypted_content_sha256` と `encrypted_content_size` は常に保存される |
| `--link-tool-calls` / `--no-link-tool-calls` | — | `true` | tool_call ↔ output の親子リンクを張るか |

## `plain` サブコマンド

Obsidian vault などのプレーンテキストツリー (`.md` / `.txt`) を取り込む。`.gitignore` 尊重 walker (`ignore` crate) + glob exclude (`globset`) + frontmatter パース (`serde_yaml`) を備える。

3 つの thread 戦略があり、`--thread-strategy` で切り替える:

- `per-file` (既定): 1 ファイル = 1 thread (`session_id = file:<sha256(rel_path)[:32]>`)
- `per-dir`: 同じ親 dir の直下 file が 1 thread (`session_id = dir:<sha256(rel_dir)[:32]>`)。`rel_dir` は `--root` からの相対パス**全体**をハッシュするので、`2026/05/a.md` と `archive/2026/05/a.md` は別 thread になる。階層を変えたい場合は `--root` を切り替えるか、後述の "ファイル移動・削除の取り扱い" を参照
- `single`: `--root` 全体で 1 thread (`session_id = root:<sha256(basename)[:32]>`)。vault 全体を移動・clone しても basename が同じなら同一 thread に収束する (spec §5.3.4)

#### ファイル移動・削除の取り扱い

##### vault 不変条件

`plain` source の `external_id` / `session_id` / `metadata.path` はすべて `--source-name` と `--root` を入力にして組み立てられる。**同じ `--source-name` では vault `--root` を常に同じパスに固定すること。** 違反時の動作 (重複 memory / 重複 thread / `--prune-missing` の誤判定) は未定義。

vault が複数あるときは `obsidian-private`, `notes-archive` のように distinct な `--source-name` を割り当てる。

##### add-only モード (既定)

`--prune-missing` を付けない場合、 `plain` source は filesystem を walk して **「現在見えているファイル」を memory に変換するだけ**で、前回 import したが今回見えなくなったファイル（削除・改名・移動）を検知して既存 memory を tombstone / 削除する経路は持たない:

- ファイル改名・移動 → `rel_path` が変わるため `entry_uid` が変わり、新 memory として追加される。**移動元の memory は元の `external_id` のまま DB に残る**
- per-file / per-dir では `session_id` も `rel_path` / 親 dir 全体に依存するため、移動先で別 thread が作られる。元 thread は既存メモリを抱えたまま残る
- 削除 → 何も起こらない (memory は残り続ける)

##### `--prune-missing`

削除追跡モード。 `--source-name:` プレフィックス + `user_id` で server 側 memory 集合を取り、 fs に存在しない `metadata.path` を持つ memory を削除する:

```bash
# 削除追跡あり (cron / CI 用には --no-interactive を必須)
memories-import --user-id 1 plain \
  --root ~/Obsidian/Private \
  --source-name obsidian-private \
  --prune-missing
# 候補一覧を表示して "Continue? [y/N]" で確認

# 自動実行
memories-import --user-id 1 plain \
  --root ~/Obsidian/Private \
  --source-name obsidian-private \
  --prune-missing --no-interactive
```

prune は **import が全 session error 無しで終わった後**にだけ実行される。 import エラーが残っているケースで delete を走らせると、半分しか追加できなかった memory を「missing」と誤判定して消す危険があるため。

`--prune-orphan-threads` (既定 ON、無効化は `--no-prune-orphan-threads`) は prune 後に memory が 0 件になった thread を `ThreadService.Delete` で消す。

`--dry-run` と `--prune-missing` を併用すると prune ステップは warning + skip。 dry-run client は RPC を呼ばないので、削除候補の計算自体ができない。 削除候補だけ確認したい場合は live server に接続したうえで `--prune-missing` を付け、 確認プロンプトで `n` を押す。

###### 制約

- rename / move は **delete + create** として扱われる。 元 path の memory は `--prune-missing` で削除され、新 path で別 memory が立つ
- `MemoryListEntry.thread_id` は representative-thread (代表 1 つ) なので、 plain 由来 memory を外部で複数 thread に attach した場合は単一 attach として扱われる。 想定外の運用の責任は呼び出し側

```bash
# per-file (既定)
memories-import --user-id 1 --server-url http://localhost:9010 plain \
  --root ~/Obsidian/Private \
  --source-name obsidian-private

# per-dir で月別 thread
memories-import --user-id 1 --server-url http://localhost:9010 plain \
  --root ~/Obsidian/Private \
  --thread-strategy per-dir \
  --source-name obsidian-private

# .gitignore + 追加 exclude
memories-import --user-id 1 --server-url http://localhost:9010 plain \
  --root ~/notes \
  --exclude-glob '**/*.bak' \
  --exclude-glob '.obsidian/**' \
  --source-name notes
```

### `--source-name` (重要)

channel / `external_id` / `agent:<source-name>` ラベルすべての prefix になる識別子。**vault ごとに一意な名前を付け、 同じ名前では `--root` を変えないこと** (上記 "vault 不変条件" 参照)。`^[a-z0-9_-]{1,32}$` に一致しない値は CLI parse 段階で reject (空文字 / 大文字 / 33 byte 超 / 日本語 / `/` `:` 等)。一度取り込んだ vault の名前を後から変えると別物として再取り込みされるので、命名は慎重に。

### entry_uid (frontmatter のみ変更でも履歴を残す)

`entry_uid = <sha256(rel_path)[:16]>:<sha256(raw_file_bytes)[:16]>` (固定 33 byte)。**frontmatter だけを変更したファイルでも raw bytes hash が変わるため新 memory として取り込まれる** (履歴を残す挙動)。逆に同じ raw bytes なら 2 回目以降 import は idempotent に skip される。

### role / metadata

- 全 memory `role = ROLE_USER` (Obsidian note は人間が書いた一次資料)
- `content_type = TEXT`
- `metadata.path` (rel_path 平文), `metadata.frontmatter` (`--frontmatter` 有効時の object), `metadata.size_bytes`, `metadata.mtime_ms`

### `plain` サブコマンド固有オプション

| オプション | 必須 | デフォルト | 備考 |
|---|---|---|---|
| `-r, --root` | ○ | — | 取り込みルート |
| `--source-name` | — | `plain` | vault 識別子。`^[a-z0-9_-]{1,32}$` |
| `--ext` | — | `md,txt` | 取り込む拡張子 (CSV、大小無視、先頭 `.` 許可) |
| `--exclude-glob` | — | (なし) | glob 除外。多重指定可で OR 合成 |
| `--follow-symlinks` / `--no-follow-symlinks` | — | OFF | symlink を辿るか |
| `--thread-strategy` | — | `per-file` | `per-file` / `per-dir` / `single` |
| `--encoding` | — | `utf8-lossy` | `utf8-strict` で不正 UTF-8 を skip |
| `--frontmatter` / `--no-frontmatter` | — | ON | 先頭 YAML frontmatter を parse (`.md` / `.markdown` のみ。`created` / `updated` キーは file mtime より優先) |
| `--label-from-frontmatter` | — | (なし) | frontmatter キー名の CSV (例: `tags,category`)。**`--thread-strategy=per-file` のみ有効**: 各 thread に `tag:<v>` (`tags` キーのみ慣用 alias)、その他は `<key>:<v>` ラベルを付与。値が string / array of string 以外、または 480 byte 超の値は warn skip |
| `--max-file-size-bytes` | — | `1048576` (1 MiB) | 超過は warn skip |
| `--respect-gitignore` / `--no-respect-gitignore` | — | ON | `.gitignore` を尊重 |
| `-P, --strip-path-prefix` | — | (なし) | `path:` ラベルから剥がすベースパス |
| `--prune-missing` | — | OFF | fs から消えたファイルの memory を削除。 確認プロンプトあり |
| `--prune-orphan-threads` / `--no-prune-orphan-threads` | — | ON | prune 後に memory が 0 件になった thread を削除するか |
| `--no-interactive` | — | OFF | 確認プロンプトを抑止 (cron / CI 必須)。 非 TTY で未指定だと exit 1 |

## グローバルオプション (全サブコマンド共通)

| オプション | 必須 | デフォルト | 備考 |
|---|---|---|---|
| `-u, --user-id` | import 系で ○ | — | i64。`upsert-generation-workers` では不要 |
| `-s, --since` | — | (なし) | ISO 8601。要約時は `updated_after_ms` (絶対 epoch ms) として workflow に渡される |
| `--mtime-margin-seconds` | — | `60` | `--since` 指定時、`since - margin` より古い session ファイルは parse skip する保守側マージン (秒)。書き込み中ファイルの誤 skip を防ぐ |
| `--no-mtime-filter` | — | `false` | `--since` 指定時の session-level mtime filter を完全 disable。NFS / クラウドストレージ等 mtime が信頼できない環境向け |
| `-l, --labels` | — | (なし) | 追加ラベル (カンマ区切り)。多重指定可 (`ArgAction::Append`)、512 byte 超は parse error |
| `-n, --dry-run` | — | `false` | DB 接続なしで件数だけ表示 |
| `-v, --verbose` | — | `false` | `RUST_LOG=debug` 相当 |
| `-b, --batch-size` | — | `100` | 進捗ログ間隔 |
| `--summarize-after-file` / `--summarize-after-json` | — | (なし) | 排他、`--summarize-workflow` 必須 |
| `--summarize-workflow` | summarize 時 ○ | — | thread-summary-batch.yaml の絶対パス |
| `--summarize-channel` | — | (なし) | jobworkerp チャンネル名 |
| `--summarize-timeout-sec` | — | `86400` (24h) | 要約 job の timeout (秒)。jobworkerp 既定値 1200 秒は LLM バッチ要約には短すぎるため長めに設定 |
| `--output-language` | — | `MEMORY_DEFAULT_LANGUAGE` または `ja` | post-import 生成 workflow の出力言語。現在は reflection prompt context と `output_language` 伝搬に使用 (`ja` / `en`) |
| `--extract-personality-after-file` / `--extract-personality-after-json` | — | (なし) | 排他、`--personality-workflow` 必須 |
| `--personality-workflow` | personality 時 ○ | — | thread-personality-batch.yaml の絶対パス |
| `--personality-channel` | — | (なし) | jobworkerp チャンネル名 |
| `--personality-timeout-sec` | — | `86400` (24h) | personality job の timeout (秒)。理由は summarize 側と同じ |
| `--server-retry-max` | — | `3` | 1 RPC あたりの最大試行回数 (初回含む)。 retry 対象は `Unavailable` / `DeadlineExceeded` / `ResourceExhausted` および PostgreSQL `40001` / `40P01` SQLSTATE。`--server-retry-max 1` または `--no-retry` で完全に無効化できる |
| `--server-retry-base-ms` | — | `1000` | 指数バックオフの基底 ms (試行 N の待機は `min(base * 2^(N-1), cap)` を jitter 倍したもの) |
| `--server-retry-cap-ms` | — | `30000` | バックオフの上限 ms |
| `--server-retry-jitter-ratio` | — | `0.25` | jitter 比率 (0.0 で完全停止、`0.25` で `[delay, delay * 1.25)` の一様分布) |
| `--no-retry` | — | `false` | RPC retry を完全無効化 (デバッグ用) |
| `--chunk-max-entries` | — | `200` | 1 AddMemoriesBatch あたりの memory 数上限。cnpg PostgreSQL 上で 1 transaction が長期化することを避けるため小さく取る |
| `--chunk-max-bytes` | — | `4194304` (4 MiB) | 1 AddMemoriesBatch の prost エンコード後上限 bytes。tonic frame 16 MiB の余裕を考慮した既定値 |

### Back-pressure と再試行

memories サーバ側 (cnpg PostgreSQL) で `AddMemoriesBatch` の INSERT が遅延する場合、 client はチャンクごとの逐次 `await` で待機するため自然に律速されます。さらに以下の挙動が組み合わさります:

1. **チャンクサイズ縮小**: 既定 200 件 / 4 MiB に絞ることで 1 transaction を数秒程度に抑え、 cnpg の lock contention が解消する余地を与えます。
2. **指数バックオフ + リトライ**: 上記 SQLSTATE か gRPC `Unavailable` / `DeadlineExceeded` / `ResourceExhausted` が返った場合、 max 3 回まで指数バックオフで再送します。`AddMemoriesBatch` は server 側で `upsert_by_external_id=true` 固定により冪等で、`UpdateMemoryParents` も同じ memory_id への再送は `rewired: false` の no-op になるため、リトライ起因の二重作成は起きません。

### 長時間プロセスでのメモリ管理 (推奨)

29 日以上の連続 import など長時間動作するプロセスは、 Rust の `drop` 後も glibc malloc がメモリを OS に返却しない arena fragmentation の影響を受け、 RSS が累積する傾向があります。 OOM-killer 発動を避けるために以下の環境変数を推奨します:

```bash
MALLOC_ARENA_MAX=2 memories-import claude-code --all-projects ...
```

これは glibc malloc の thread-arena 数を 2 に制限し、 fragmentation による RSS 累積を抑えます。 jobworkerp の COMMAND runner 経由でも env 経由で同様に設定できます。

## `claude-code` サブコマンド固有オプション

| オプション | 必須 | デフォルト | 備考 |
|---|---|---|---|
| `-f, --session-file` / `-p, --project-dir` / `--all-projects` | いずれか 1 つ ○ | — | 排他 |
| `-d, --claude-dir` | — | `~/.claude` | `~` は `HOME` に展開 |
| `-t, --include-types` | — | `user,assistant,tool_call,tool_output,system,reasoning,attachment` | canonical kind ベース。`tool_use` / `tool_result` / `image` block は対応する canonical kind を渡さないと取り込まれない |
| `-P, --strip-path-prefix` | — | (なし) | `path:` ラベルから剥がすベースパス (カンマ区切り、最長マッチ優先)。`cwd` がベースパス配下なら相対パスが入る。マッチしなければ絶対パス |
| `--attachment-subtypes` | — | `default` | `default` (HIGH-VALUE のみ: `task_reminder` / `diagnostics` / `edited_text_file` / `nested_memory`) / `all` / `none` / カンマ区切り whitelist。`type=attachment` JSONL イベント (画面共有・diagnostics 等) の取り込み制御 |

`memories-import --help` で全オプションを確認できる (subcommand 別 help は `memories-import claude-code --help`)。

## 関連ドキュメント

- `agent-chat-import/workflows/thread-summary/README_ja.md` — thread-summary-batch ワークフローの仕様
- `agent-chat-import/workflows/personality/README_ja.md` — thread-personality-batch / user-personality-merge ワークフローの仕様
- `agent-chat-import/workflows/agent-chat-pipeline/README_ja.md` — import → summary → personality を一気通貫で回すパイプライン
