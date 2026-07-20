# personality workflows

ユーザの嗜好・思考傾向・コミュニケーションスタイルを抽出する2層ワークフロー。

## 構成

```
agent-chat-import          (生のチャットログを memory に登録, user_id=N)
        │
        ▼
thread-personality-single  ([1層] スレッド単位の嗜好シグナル / user_id=入力ユーザ, PERSONALITY)
        │  (thread-personality-batch がユーザの全スレッドを順次起動)
        ▼
user-personality-merge     ([2層] ユーザ統合プロファイル / user_id=入力ユーザ, PERSONALITY)
        │
        ▼
パーソナライズエージェント     (FindListByCondition で profile を取得)
```

`thread-summary` 系と直交する別系統で、入力会話は同じだが出力は
`PERSONALITY`種別で分離されている。

## ファイル

single / merge と prompts は `workers/personality/` 配下、batch は本ディレクトリ
(`workflows/personality/`)。batch は single / merge を `workerName` で呼ぶため、
事前に `memories-import upsert-generation-workers` で言語別 worker を登録しておく
（後述）。

| ファイル | 役割 |
|---|---|
| `thread-personality-batch.yaml` | 1層 batch: ユーザの全スレッドを順次処理。本ディレクトリ |
| `../../workers/personality/thread-personality-single.yaml` | 1層: 単一スレッドから嗜好シグナルを抽出。prompt は登録時に worker settings へ焼き込まれる |
| `../../workers/personality/user-personality-merge.yaml` | 2層: ユーザ統合プロファイルを生成。同上 |
| `../../workers/personality/prompts/thread_system_prompt.{ja,en}.txt` / `thread_user_tail.{ja,en}.txt` | 1層の言語別 system prompt / user prompt 末尾指示 |
| `../../workers/personality/prompts/merge_system_prompt.{ja,en}.txt` / `merge_user_tail.{ja,en}.txt` | 2層の言語別 system prompt / user prompt 末尾指示 |

## 不変条件 (要約)

| ID | 内容 |
|---|---|
| I1 | 1層 personality memory thread のラベルは `["personality", "user:<source_user_id>", "thread:<source_thread_id>"]` の3つを LABEL_ALL マッチで一意特定 |
| I2 | 1層 personality memory の metadata は `source_user_id` (string) / `source_thread_id` (string) / `signal_version` / `no_signal` / `truncation_level` の5フィールド必須 |
| I3 | 2層 profile memory thread のラベルは `["personality_profile", "user:<source_user_id>"]` の2つを LABEL_ALL マッチで特定。1ユーザ1スレッド |
| I4 | 2層 profile memory の metadata は `source_user_id` (string) / `signal_count` / `no_signal_count` / `profile_version` を含む。`external_id = "personality_profile:<source_user_id>"` |
| I4b | 2層 profile 配列 (`interests` / `preferences` / `values_and_beliefs` / `anti_preferences`) は各カテゴリ **最大20件**。Schema の `maxItems` で制約し、`confirmEntryDates` 内で weight (high>medium>low) と supporting_source_thread_ids 数の降順で上位20件のみ残す保険を適用する |
| I4c | 2層 profile thread の `updated_at` は memory 更新と同じ `$max_signal_updated_at` に同期する。新規作成パスは `ThreadService/Create` の `updated_at`、既存更新パスは `MemoryService/Update` 直後の `ThreadService/Update` で揃える (MemoryService/Update は親 thread に伝播しないため) |
| I5 | 1層実行時に `thread.data.userId.value == input.user_id` を検証 (不一致は 403) |
| I6 | 2層シグナル収集時は LABEL_ALL `["personality", "user:<input.user_id>"]` でフィルタ + `metadata.source_user_id` の二重防衛フィルタ |

personalityのthread作成者は入力`user_id`であり、出力thread／memoryは`PERSONALITY`種別で分離される。

## パラメータ表

### `thread-personality-single`

| パラメータ | デフォルト | 説明 |
|---|---|---|
| `user_id` (必須) | — | 元会話の所有者 |
| `thread_id` (必須) | — | 抽出対象スレッド |
| `memories_grpc_host` | `localhost` | memories gRPC ホスト |
| `memories_grpc_port` | `9100` | memories gRPC ポート |
| `min_message_count` | `4` | 最小メッセージ件数 |
| `min_user_messages` | `2` | 最小 ROLE_USER 件数 (これ未満で skip) |
| `max_context_chars` | `200000` | LLM コンテキスト上限 |
| `personality_model` | `qwen3.6:27b` | LLM モデル |
| `ollama_base_url` | `http://localhost:11434` | Ollama URL |
| `output_language` | `ja` | 出力言語 (`ja` / `en`) |
| `force_reextract` | `false` | true なら必ず再抽出 |

### `thread-personality-batch`

`thread-personality-single` の入力に加えて:

| パラメータ | デフォルト | 説明 |
|---|---|---|
| `thread_ids` | — | 指定時のみ対象スレッドを絞り込み |
| `labels_filter` | — | 指定時のみ該当ラベルを持つスレッドを処理 |
| `updated_within_hours` | — | 直近 N 時間以内に更新されたスレッドのみ |
| `updated_after_ms` | — | 絶対 epoch ms 下限 (`updated_within_hours` より優先) |
| `output_language` | `ja` | single / merge に伝搬する出力言語 (`ja` / `en`) |
| `merge_enabled` | `false` | true なら1層後に言語別 `user-personality-merge` worker を呼ぶ |

### `user-personality-merge`

| パラメータ | デフォルト | 説明 |
|---|---|---|
| `user_id` (必須) | — | 統合対象ユーザの元 ID |
| `memories_grpc_host` | `localhost` | |
| `memories_grpc_port` | `9100` | |
| `max_context_chars` | `200000` | |
| `merge_model` | `qwen3.6:27b` | |
| `ollama_base_url` | `http://localhost:11434` | |
| `output_language` | `ja` | 出力言語 (`ja` / `en`) |
| `force_remerge` | `false` | true なら必ず再統合 |
| `max_signals` | `200` | LLM 投入する1層シグナルの上限 |

## prompt context と worker 登録

`thread-personality-single.yaml` と `user-personality-merge.yaml` は system prompt と user prompt 末尾指示を YAML に埋め込まず、`workflow_context` の
`thread_personality_system_prompt` / `thread_personality_user_tail` /
`user_personality_merge_system_prompt` / `user_personality_merge_user_tail` を必須入力として使う。空の場合は `prompt_context_missing` で fail closed する。

prompt は `memories-import upsert-generation-workers` が `agent-chat-import/workers/personality/prompts/` の `.txt` を読み、言語別 worker の `settings.workflow_context` に焼き込む。direct enqueue / ローカルアプリ向けの言語別 worker は次で登録する:

```bash
JOBWORKERP_ADDR=http://localhost:9000 \
memories-import upsert-generation-workers \
  --feature personality \
  --language all \
  --channel workflow_lang
```

登録される worker は `memories-thread-personality-single-ja/en` と `memories-user-personality-merge-ja/en`。prompt を変更したら再登録する。
`thread-personality-batch.yaml` は `output_language` に応じてこれらの言語別 worker を `workerName` で呼ぶため、
batch 実行時に single / merge の YAML パスや prompt context を渡す経路は無い。

## dispatch サンプル

通常運用は batch を呼ぶ。batch が `output_language` に応じて登録済みの言語別
single / merge worker (`memories-thread-personality-single-<lang>` /
`memories-user-personality-merge-<lang>`) を `workerName` で fan-out する。

### batch (ユーザの全スレッド)

```bash
jobworkerp-client job enqueue-workflow \
  -i '{
    "user_id": 1,
    "memories_grpc_host": "localhost",
    "memories_grpc_port": 9100,
    "output_language": "ja",
    "merge_enabled": true
  }' \
  -w /abs/path/agent-chat-import/workflows/personality/thread-personality-batch.yaml \
  --format json -t 86400
```

`merge_enabled: true` のとき、1層完了後に 2層 (`user-personality-merge`) まで
続けて実行する。

### 単発 (1スレッド)

特定スレッドだけを処理する場合も batch に `thread_ids` を 1 件だけ渡す
（生の single YAML を `-w` で直接呼ぶと prompt context が無く失敗するため、
登録済みの言語別 worker を経由する batch を使う）:

```bash
jobworkerp-client job enqueue-workflow \
  -i '{
    "user_id": 1,
    "thread_ids": [7453040111820003484],
    "memories_grpc_host": "localhost",
    "memories_grpc_port": 9100,
    "output_language": "ja"
  }' \
  -w /abs/path/agent-chat-import/workflows/personality/thread-personality-batch.yaml \
  --format json -t 1800
```

## memories-import からの自動 dispatch

`agent-chat-import` (バイナリ `memories-import`) を `personality-after`
feature 付きでビルドすると、インポート後に自動で
`thread-personality-batch` を dispatch できる:

```bash
memories-import claude-code -u 1 -f <session.jsonl> \
  --output-language ja \
  --extract-personality-after-file pers-input.json \
  --personality-workflow /abs/path/agent-chat-import/workflows/personality/thread-personality-batch.yaml
```

`pers-input.json` の例:

```json
{
  "memories_grpc_host": "localhost",
  "memories_grpc_port": 9100,
  "merge_enabled": true
}
```

`user_id` と (`--since` 指定時) `updated_after_ms` は memories-import 側で
上書き注入されるので、テンプレートに記述する必要はない。`output_language` も CLI 側で解決して上書きされる。

## プロファイル取得 (パーソナライズエージェント側)

```python
req = FindMemoryListRequest(
    user_id=UserId(value=target_user_id),
    external_id=f"personality_profile:{target_user_id}",
    roles=["ROLE_ASSISTANT"],
    limit=1,
)
async for entry in stub.FindListByCondition(req):
    profile = json.loads(entry.memory.data.content)
    assert json.loads(entry.memory.data.metadata)["source_user_id"] == str(target_user_id)
    break
```

プロファイル未生成のユーザに対しては空ストリームが返る — エージェント側で
固定のデフォルト system prompt にフォールバックすること。

## 削除フロー

特定ユーザの嗜好データはそのユーザが所有するため、`PERSONALITY`種別と
ラベルで絞って削除する:

```python
target = <削除対象ユーザID>

# 1層 personality threads
threads_p = await stub.FindThreadListByLabels(
    labels=["personality", f"user:{target}"],
    match_mode="LABEL_ALL",
    user_id=target_user_id,
)
for t in threads_p:
    await stub.Delete(t.id)  # cascade で配下 memory も削除

# 2層 profile thread
threads_pp = await stub.FindThreadListByLabels(
    labels=["personality_profile", f"user:{target}"],
    match_mode="LABEL_ALL",
    user_id=target_user_id,
)
for t in threads_pp:
    await stub.Delete(t.id)
```
