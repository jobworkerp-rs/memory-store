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
  --channel workflow_lang \
  --repo-root /abs/path/to/memories/agent-chat-import
```

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
# 入力 JSON を用意 (user_id と updated_within_hours は memories-import 側で上書きされる)
cat > /tmp/summarize.json <<'EOF'
{
  "memories_grpc_host": "localhost",
  "memories_grpc_port": 9010,
  "ollama_base_url": "http://192.168.1.2:11434",
  "summary_model": "qwen3.6:27b"
}
EOF

# JOBWORKERP_ADDR (URI スキーム必須、http:// または https://) を指定する。
# --since を指定すると updated_within_hours が自動算出される
JOBWORKERP_ADDR=http://localhost:9000 \
  memories-import -u 1 --all-projects \
    --since 2026-04-29T00:00:00Z \
    --summarize-after-file /tmp/summarize.json \
    --summarize-workflow /absolute/path/to/thread-summary-batch.yaml
```

上書きされるフィールド:

- `user_id` — `memories-import --user-id` の値で常に上書き (テンプレ側の値は無視)
- `updated_within_hours` — `--since` を指定したときのみ、`(now - since)` を 1 時間単位に切り上げて上書き。`--since` 未指定なら touch しない

その他のフィールド (`memories_grpc_host`/`_port`、`labels_filter` 等) は JSON の値がそのまま透過される。`user_id` はimportの`--user-id`から上書きされる。

`--summarize-after-json '<JSON>'` で同等のインライン指定も可。

ジョブ timeout は `--summarize-timeout-sec`(秒、デフォルト 86400 = 24h) で調整できる。jobworkerp の既定値 1200 秒は複数スレッドの LLM 要約には短いため、本オプションは workflow YAML 側の上限 24h に合わせて長めの default を採用している。

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
| `labels_filter` | - | (フィルタなし) | 指定ラベルを持つスレッドのみ対象 |

batch は `output_language` に応じて `memories-thread-summary-single-ja` /
`memories-thread-summary-single-en` を `workerName` で呼ぶ。事前に
`memories-import upsert-generation-workers --feature thread-summary --language all --channel workflow_lang`
などで言語別 single worker を登録しておく。

## 差分実行の仕組み

`force_resummarize: false`（デフォルト）の場合、以下の条件で各スレッドをスキップする:

1. `description` が設定済み
2. 既存の要約メモリが存在する
3. スレッドの `updated_at` が既存要約の `updated_at` 以下（更新なし）

上記すべてを満たす場合はスキップされるため、バッチを繰り返し実行しても処理済みスレッドは再処理されない。

## 要約データの構造

### スレッドのdescription

```
【{タイトル}】 {要約本文}
```

### 要約メモリ

要約は元会話と同じ`user_id`を持つ`THREAD_SUMMARY`メモリスレッドに保存される。同一ラベルのスレッドは同一メモリスレッドに集約される。

- `content`: JSON構造化要約（category, title, summary, key_decisions, status）
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
- **バッチ実行は逐次処理**。全スレッド処理には `スレッド数 × 約1分` 程度かかる。タイムアウトは十分な値を設定すること
- **`max_context_chars`** はモデルのコンテキスト長に対してマージンを取った値を指定する。Qwen3.6:27b（256kトークン）ではデフォルト `200000` を推奨
