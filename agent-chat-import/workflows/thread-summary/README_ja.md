# スレッド要約ワークフロー

memoriesサービスに蓄積されたチャットスレッドをLLMで要約し、結果を `ThreadData.description` および専用メモリスレッドに保存するワークフロー。

## ファイル構成

| ファイル | 説明 |
|---------|------|
| `thread-summary-batch.yaml` | バッチオーケストレーション（全スレッドを順次処理）。本ディレクトリ |
| `../../workers/thread-summary/thread-summary-single.yaml` | 単一スレッド要約ワークフロー（1スレッドの要約を実行）。prompt は登録時に worker settings へ焼き込まれる |

batch は single を `workerName` で呼ぶ。`output_language` に応じて
`memories-thread-summary-single-ja` / `memories-thread-summary-single-en` を
選び分けるため、事前に `memories-import upsert-generation-workers` で言語別
worker を登録しておく（後述）。

## 前提条件

- **jobworkerp** サーバが起動していること（デフォルト: `localhost:9000`）
- **memoriesサービス** が起動していること（gRPCポート指定が必要）
- **Ollama** サーバが利用可能であること（`ollama_base_url` で指定）

## 使い方

### 言語別 worker の登録（初回 / prompt 変更時）

single は prompt を YAML に埋め込まず、登録時に worker settings へ焼き込む。
batch を動かす前に、対象言語の worker を登録しておく:

```bash
memories-import upsert-generation-workers \
  --feature thread-summary \
  --language all \
  --repo-root /abs/path/to/memories/agent-chat-import
```

`--channel` を省略すると jobworkerp のデフォルト channel が使われる。特定の
channel が必要な場合だけ指定する。

### 単一スレッド要約

特定のスレッドを1つだけ要約する場合は、batch に `thread_ids` を 1 件だけ渡す
（生の single YAML を `-w` で直接呼ぶと prompt context が無く失敗するため、
登録済みの言語別 worker を経由する batch を使う）:

```bash
jobworkerp-client job enqueue-workflow \
  -i '{
    "user_id": 1,
    "thread_ids": [7453040111820003484],
    "memories_grpc_host": "localhost",
    "memories_grpc_port": 9010,
    "ollama_base_url": "http://192.168.1.2:11434",
    "summary_model": "qwen3.6:27b",
    "output_language": "ja"
  }' \
  -w /absolute/path/to/thread-summary-batch.yaml \
  --format json \
  -t 300
```

### バッチ実行（全スレッド）

ユーザの全スレッドを要約する場合:

```bash
jobworkerp-client job enqueue-workflow \
  -i '{
    "user_id": 1,
    "memories_grpc_host": "localhost",
    "memories_grpc_port": 9010,
    "ollama_base_url": "http://192.168.1.2:11434",
    "summary_model": "qwen3.6:27b"
  }' \
  -w /absolute/path/to/thread-summary-batch.yaml \
  --format json \
  -t 86400
```

### ラベルで絞り込んだバッチ実行

特定のラベルを持つスレッドのみ要約する場合:

```bash
jobworkerp-client job enqueue-workflow \
  -i '{
    "user_id": 1,
    "memories_grpc_host": "localhost",
    "memories_grpc_port": 9010,
    "labels_filter": ["coding_agent", "agent:claude_code"],
    "ollama_base_url": "http://192.168.1.2:11434"
  }' \
  -w /absolute/path/to/thread-summary-batch.yaml \
  --format json \
  -t 86400
```

### 全件強制再要約

既に要約済みのスレッドも含めて再要約する場合:

```bash
# force_resummarize: true を指定
jobworkerp-client job enqueue-workflow \
  -i '{
    "user_id": 1,
    "memories_grpc_host": "localhost",
    "memories_grpc_port": 9010,
    "ollama_base_url": "http://192.168.1.2:11434",
    "force_resummarize": true
  }' \
  -w /absolute/path/to/thread-summary-batch.yaml \
  --format json \
  -t 86400
```

### `memories-import` の後段で自動起動する

`agent-chat-import` クレートのバイナリ `memories-import` には、import 完了直後に本ワークフロー (batch) を jobworkerp 経由で実行する `--summarize-after-*` オプションがある。

```bash
# 入力 JSON を用意 (user_id と last_message_after_ms は memories-import 側で上書きされる)
cat > /tmp/summarize.json <<'EOF'
{
  "memories_grpc_host": "localhost",
  "memories_grpc_port": 9010,
  "ollama_base_url": "http://192.168.1.2:11434",
  "summary_model": "qwen3.6:27b"
}
EOF

# JOBWORKERP_ADDR (URI スキーム必須、http:// または https://) を指定する。
# --since を指定すると last_message_after_ms (epoch ms) が設定される
JOBWORKERP_ADDR=http://localhost:9000 \
  memories-import -u 1 --all-projects \
    --since 2026-04-29T00:00:00Z \
    --summarize-after-file /tmp/summarize.json \
    --summarize-workflow /absolute/path/to/thread-summary-batch.yaml
```

上書きされるフィールド:

- `user_id` — `memories-import --user-id` の値で常に上書き (テンプレ側の値は無視)
- `last_message_after_ms` — `--since` を指定したときのみ、その時刻 (epoch ms) で上書き。jobworkerp のキュー待ちで窓がずれないよう絶対値で渡す。`--since` 未指定なら touch しない

その他のフィールド (`memories_grpc_host`/`_port`、`labels_filter` 等) は JSON の値がそのまま透過される。`user_id` はimportの`--user-id`から上書きされる。

`--summarize-after-json '<JSON>'` で同等のインライン指定も可。

ジョブ timeout は `--summarize-timeout-sec`(秒、デフォルト 86400 = 24h) で調整できる。jobworkerp の既定値 1200 秒は複数スレッドの LLM 要約には短いため長めの default を採用している。

workflow YAML 自体には document / for / try の timeout を設定していない。ハングした LLM 呼び出しや single 呼び出しを打ち切るには、jobworkerp 側で `WORKFLOW_TASK_DEFAULT_TIMEOUT_SEC`（run/call タスクだけに適用される既定 timeout、例: `3600`）を設定する。

## 入力パラメータ

### 共通（single / batch）

| パラメータ | 必須 | デフォルト | 説明 |
|-----------|------|-----------|------|
| `user_id` | ○ | - | 要約対象ユーザのID |
| `memories_grpc_host` | ○ | `localhost` | memoriesサービスのgRPCホスト |
| `memories_grpc_port` | ○ | `9100` | memoriesサービスのgRPCポート |
| `min_message_count` | - | `4` | 要約対象とする最小メッセージ数 |
| `max_context_chars` | - | `200000` | 会話本文の合計文字数カット閾値 |
| `summary_model` | - | `qwen3.6:27b` | 要約に使用するOllamaモデル |
| `ollama_base_url` | - | `http://localhost:11434` | OllamaサーバのベースURL |
| `memory_thread_label_prefix` | - | `summary` | 要約メモリスレッドのマーカーラベル |
| `force_resummarize` | - | `false` | `true` で既存要約も再生成 |
| `output_language` | - | `ja` | 生成言語 `ja` / `en`。batch が呼ぶ言語別 worker の選択に使う |

### single worker 固有

batch が単一スレッドへ fan-out する際に渡すフィールド（single worker は直接呼ばず、
batch 経由で `workerName` 指定の言語別 worker に渡る）:

| パラメータ | 必須 | 説明 |
|-----------|------|------|
| `thread_id` | ○ | 要約対象スレッドのID |

### batch 固有

| パラメータ | 必須 | デフォルト | 説明 |
|-----------|------|-----------|------|
| `thread_ids` | - | (全件) | 指定したスレッドIDのみ要約 |
| `target_thread_ids` | - | (未指定) | 対象スレッドID集合（文字列）。指定時は `thread_ids` より優先され、空配列なら対象 0 件 |
| `labels_filter` | - | (フィルタなし) | 指定ラベルを持つスレッドのみ対象 |
| `labels_filter_match_mode` | - | `LABEL_ALL` | `labels_filter` の一致方式 (`LABEL_ALL` / `LABEL_ANY`) |
| `exclude_thread_labels` | - | `["subagent"]` | これらの label を持つスレッドを期間・label 指定の選択から除外する（`target_thread_ids` / `thread_ids` の明示指定には適用しない）。subagent スレッドは thread-group summary 経由でだけ要約される（group summary を使わない運用では subagent スレッドの要約は作られない）。`labels_filter: ["subagent"]` で subagent スレッドを対象にする場合は `exclude_thread_labels: []` も指定する。空配列で除外なし |
| `summary_thread_ignored_labels` | - | `["subagent"]` | 要約メモリスレッドの label 集合から除く元スレッドの label。subagent スレッドの要約は親と同じ要約メモリスレッドに入る。single worker にも引き渡す |
| `last_message_within_hours` | - | (未指定) | 実行時刻から過去 N 時間以内に最終メッセージがあるスレッドのみ対象 |
| `last_message_after_ms` | - | (未指定) | 最終メッセージ時刻の下限 (epoch ms, `>`)。`last_message_within_hours` より優先 |
| `last_message_before_ms` | - | (未指定) | 最終メッセージ時刻の上限 (epoch ms, `<=`) |
| `thread_created_after_ms` | - | (未指定) | スレッド作成時刻の下限 (epoch ms, `>`) |
| `thread_created_before_ms` | - | (未指定) | スレッド作成時刻の上限 (epoch ms, `<=`) |

旧入力 `updated_within_hours` / `updated_after_ms` / `updated_before_ms` は
`last_message_within_hours` / `last_message_after_ms` / `last_message_before_ms` に改名された。

batch は `output_language` に応じて `memories-thread-summary-single-ja` /
`memories-thread-summary-single-en` を `workerName` で呼ぶ。事前に
`memories-import upsert-generation-workers --feature thread-summary --language all`
などで言語別 single worker を登録しておく。

## 差分実行の仕組み

`force_resummarize: false`（デフォルト）の場合、single worker がメッセージを読む前に以下の条件を判定し、
すべて満たすスレッドをスキップする（batch は対象の全スレッドで single を呼び、判定は single だけが行う）:

1. `description` が設定済み
2. 既存の要約メモリが存在する
3. スレッドの `last_message_at` が既存要約の `updated_at` 以下（新しいメッセージなし）
4. 要約メモリスレッドが元スレッドのラベル + `memory_thread_label_prefix` をすべて持つ（ラベルは `summary_thread_ignored_labels` を除いたもの）

上記すべてを満たす場合はスキップされるため、バッチを繰り返し実行しても処理済みスレッドは再処理されない。

## 要約データの構造

### スレッドのdescription

```
【{タイトル}】 {要約本文}
```

### 要約メモリ

要約は元会話と同じ`user_id`を持つ`THREAD_SUMMARY`メモリスレッドに保存される。同一ラベルのスレッドは同一メモリスレッドに集約される。

- `content`: JSON構造化要約（category, title, summary, key_decisions, status）。LLM 出力が JSON として解釈できない、または title / summary が空の場合は再生成し（最大 2 回）、それでも満たさなければその thread を失敗として扱い保存しない
- `created_at` / `updated_at`: 元スレッドの `first_message_at` / `last_message_at`。スレッド自体の `created_at` / `updated_at` はサーバの監査時刻（import 時刻）なので使わない。日次要約はこの `updated_at` で対象日を決める
- `metadata`: `{"source_thread_id": "<元スレッドID(文字列)>", "category": "...", "summary_version": "1.0"}` （`source_thread_id` はint64の精度欠落を避けるため文字列で格納）
- `external_id`: `summary:<元スレッドID>`（逆引き・重複防止用）
- `role`: `ROLE_ASSISTANT`

### status

構造化要約の `status` は次のいずれかです。

| 値 | 意味 |
|---|---|
| `ongoing` | 主作業を継続中 |
| `deferred` | 作業を意図的に保留した |
| `resolved` | 主目的と残課題がすべて完了した |
| `abandoned` | 作業を明示的に中止した |

`in_review`, `blocked`, `deferred` は下位の日次・週次・月次要約で持ち越し対象として扱う。

### カテゴリ

LLMが会話内容から自動判定:

| カテゴリ | 説明 |
|---------|------|
| `coding` | コーディング・開発作業 |
| `consultation` | 相談・アドバイス |
| `research` | 調査・リサーチ |
| `creative` | 創作・クリエイティブ |
| `general` | その他一般 |

## 注意事項

- 要約の分離は`THREAD_SUMMARY`種別で行う。別のthread作成者を指定するフィールドはない。
- **ワークフローファイルのパスは絶対パスで指定すること**。jobworkerpサーバの作業ディレクトリとワークフローファイルの場所が異なるため
- **バッチ実行は逐次処理**。最新のスレッドは single がメッセージを読む前にスキップするため、短時間で終わる。時間がかかるのは生成対象だけ
- **`max_context_chars`** はモデルのコンテキスト長に対してマージンを取った値を指定する。Qwen3.6:27b（256kトークン）ではデフォルト `200000` を推奨

## バッチ結果

`generated_count` / `skipped_count` / `failed_count` と対応する thread ID 配列を返す。
`skipped` は既存要約が利用可能な正常結果であり、`succeeded_count` に含まれる。
従って上位ワークフローは依存順序を維持したまま日次要約へ進む。
single worker が `completed=false, skipped=false` を返した場合だけ論理失敗として扱う。
結果を返さないまま終わった（timeout・中断）スレッドも `failed_threads` に数え、未完了の実行を
`completed=true` として報告しない。

`insufficient_message_threads` は `min_message_count` 未満のためスキップしたスレッド ID
（`skipped_threads` の部分集合）。スレッドグループ要約
（[`../thread-group-summary/`](../thread-group-summary/README_ja.md)）が「要約不要なほど短い」
メンバーと「要約が欠けている」メンバーを区別するために使う。

各反復は `progress_processed` / `progress_total`（処理中の番号 / 対象件数）を出力コンテキストに
載せるため、ストリーミングで受け取る呼び出し側は `(m/N)` 形式の進捗を表示できる。
