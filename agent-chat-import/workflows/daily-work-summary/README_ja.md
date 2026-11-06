# 日次作業要約ワークフロー

`thread-summary-single` が生成した「スレッド単位の要約」を1日単位で集約し、ユーザの作業目的別・トピック別にまとめる第3層の要約ワークフロー。

```
[1] agent-chat-import          (生のチャットログを memory に登録)
        ↓
[2] thread-summary-single      (1スレッド単位の要約 / "summary" ラベル)
        ↓
[3] daily-work-summary-single  (1日単位の集約要約 / "daily_summary" ラベル) ← 本ワークフロー
```

## ファイル構成

| ファイル | 説明 |
|---------|------|
| `daily-work-summary-batch.yaml`  | 日付レンジを指定して single を逐次実行するバッチ。本ディレクトリ |
| `../../workers/daily-work-summary/daily-work-summary-single.yaml` | 1日分の集約要約を実行する単発ワークフロー。prompt は登録時に worker settings へ焼き込まれる |
| `run-daily-summary.sh`           | `jobworkerp-client` 経由で batch を起動するヘルパースクリプト |

batch は single を `workerName` で呼ぶ。`output_language` に応じて
`memories-daily-work-summary-single-ja` / `memories-daily-work-summary-single-en`
を選び分けるため、事前に `memories-import upsert-generation-workers` で言語別
worker を登録しておく（後述）。

## 前提条件

- `thread-summary-single` が `summary` ラベル付き `THREAD_SUMMARY` を対象の `user_id` 配下に生成済みであること
- 各要約スレッドの `ThreadData.description` に `【タイトル】 要約本文` 形式のテキストが入っていること（thread-summary-single の Step 9 で書き込まれる）
- jobworkerp / memories / Ollama の起動状態は thread-summary と同じ

## 設計上のキー

| 項目 | 値 | 理由 |
|---|---|---|
| 集約スレッドの所有者 | 入力 `user_id` (= thread 要約と同じ) | `memory_kind` とラベルで要約階層を分離する |
| 集約スレッドのラベル | `daily_summary`, `date:YYYY-MM-DD`, `scope:<scope_key>`, `extra_labels_filter` の各値 (sort 済み) | `daily_summary` で一覧、`date:` で日付絞り込み、`scope:` で同日内の異 scope を分離 |
| 集約メモリの `external_id` | `daily:<user_id>:YYYY-MM-DD:<scope_key>` | `memory.external_id` は DB 全体で UNIQUE。所有者と scope を含め、同日に異なるユーザーまたは `extra_labels_filter` で並列実行しても衝突しないようにする |
| `scope_key` の算出 | `extra_labels_filter` を `sort | join(",")`。空なら `_all` | 呼び出し側のラベル順に依存しない (`["b","a"]` も `["a","b"]` も `scope_key="a,b"`)。external_id・スレッドラベル・filter request すべてが順序非依存 |
| 入力の取得方法 | `MemoryService.FindListByCondition` で **要約メモリ自身**を絞り込む (`memory_kinds=[MEMORY_KIND_THREAD_SUMMARY]` + `roles=[ROLE_ASSISTANT]` + `updated_after/before` + `thread_filter.labels=[summary]+extra` の AND マッチ) | スレッドの `updated_at` はサーバが `AddMemory` 時に `now` で bump されるため、要約 *スレッド* 単位で絞ると元の会話日付ではなく要約実行日でヒットしてしまう。要約 *メモリ* の `updated_at` には `thread-summary-single` が元スレッドの会話の最終メッセージ時刻 (`last_message_at`) を書き込むため、メモリ単位で絞ることで正しく「会話があった日」で絞れる |

| LLM 入力 | `memory.data.content` (構造化要約 JSON: category / title / summary / key_decisions / status) + `thread_description` の組み合わせ | スレッドの一行サマリだけでなく key_decisions など詳細データを LLM に渡すことでグルーピング精度を上げる |
| コンテキスト圧縮 | `token_count` で計測し、収まるまで古い入力から外す。正確に数えられないプロバイダは `summary_input_byte_budget` で updated_at desc 順に詰める | 下記「パス単位集約と生成時の保護」を参照 |

## 上位の目的別整理（このワークフローの価値）

`thread-summary-single` の出力はスレッド単位の事実列挙（category / title / summary / key_decisions / status）に留まる。本ワークフローではそれらを横断して以下を生成する:

- **`overall_purpose`** — 当日の上位目的を 1〜3 文で言語化
- **`purpose_groups`** — 目的が共通する要約をグルーピングし、目的・箇条書き・元 memory_id・状態を整理
- **`by_topic`** — リポジトリ・技術領域などのトピック軸で再整理（purpose_groups と直交する切り口）
- **`carryover`** — 翌日以降への持ち越し
- **`metrics`** — 件数集計

`purpose_groups.status` は thread-summary の [status](../thread-summary/README_ja.md#status)
と同じ値を使う。`in_review`, `blocked`, `deferred` は `carryover` に含める。

system prompt は `agent-chat-import/workers/daily-work-summary/prompts/system_prompt.<lang>.txt`、
user prompt 末尾の言語依存指示は `agent-chat-import/workers/daily-work-summary/prompts/user_tail.<lang>.txt`
に置き、言語別 worker 登録時に `settings.workflow_context` へ焼き込む。

## 使い方

### 言語別 worker の登録（初回 / prompt 変更時）

single は prompt を YAML に埋め込まず、登録時に worker settings へ焼き込む。
batch を動かす前に、対象言語の worker を登録しておく:

```bash
memories-import upsert-generation-workers \
  --feature daily-work-summary \
  --language all \
  --repo-root /abs/path/to/memories/agent-chat-import
```

`--channel` を省略すると jobworkerp のデフォルト channel が使われる。特定の
channel が必要な場合だけ指定する。

### 単発（1日分）

1日だけ生成する場合も batch を `start_date = end_date` で呼ぶ（生の single YAML を
`-w` で直接呼ぶと prompt context が無く失敗するため、登録済みの言語別 worker を
経由する batch を使う）:

```bash
jobworkerp-client job enqueue-workflow \
  -i '{
    "memories_grpc_host": "localhost",
    "memories_grpc_port": 9010,
    "ollama_base_url": "http://192.168.1.2:11434",
    "summary_model": "qwen3.6:27b",
    "start_date": "2026-05-05",
    "end_date": "2026-05-05",
    "output_language": "ja"
  }' \
  -w /absolute/path/to/daily-work-summary-batch.yaml \
  --format json \
  -t 1800
```

`start_date` / `end_date` / `last_n_days` をすべて省略すると「昨日」
（`timezone_offset_hours=9` の JST 基準）を自動選択する。cron での日次運用は
この形が便利。

### バッチ（日付レンジ）

```bash
# 直近 7 日を一括生成
jobworkerp-client job enqueue-workflow \
  -i '{
    "memories_grpc_host": "localhost",
    "memories_grpc_port": 9010,
    "ollama_base_url": "http://192.168.1.2:11434",
    "summary_model": "qwen3.6:27b",
    "last_n_days": 7
  }' \
  -w /absolute/path/to/daily-work-summary-batch.yaml \
  --format json \
  -t 86400

# 明示的な日付レンジ
jobworkerp-client job enqueue-workflow \
  -i '{
    "memories_grpc_host": "localhost",
    "memories_grpc_port": 9010,
    "ollama_base_url": "http://192.168.1.2:11434",
    "start_date": "2026-04-29",
    "end_date": "2026-05-05"
  }' \
  -w /absolute/path/to/daily-work-summary-batch.yaml \
  --format json \
  -t 86400
```

### ヘルパースクリプト経由（推奨）

JSON 入力の組み立てとワークフローパス指定を省略できる:

```bash
# 最近 7 日分
agent-chat-import/workflows/daily-work-summary/run-daily-summary.sh --last-n-days 7

# 指定日のみ
agent-chat-import/workflows/daily-work-summary/run-daily-summary.sh --target-date 2026-05-06

# プロジェクト絞り込み + 強制再生成
agent-chat-import/workflows/daily-work-summary/run-daily-summary.sh \
    --target-date 2026-05-06 \
    --extra-labels "agent:claude_code,coding_agent" \
    --force-resummarize

# k8s 本番環境向け (port-forward 自動起動)
agent-chat-import/workflows/daily-work-summary/run-daily-summary.sh \
    --port-forward \
    --last-n-days 7 \
    --batch-yaml https://raw.githubusercontent.com/jobworkerp-rs/memory-store/main/agent-chat-import/workflows/daily-work-summary/daily-work-summary-batch.yaml
```

実行内容を確認したいだけなら `--print-only` を付ける（JSON とコマンドが標準エラーに表示される）。
全オプションは `run-daily-summary.sh --help` を参照。

### プロジェクト単位で集約したい場合

```bash
# agent:claude_code ラベルが付いたスレッド要約のみを対象に
jobworkerp-client job enqueue-workflow \
  -i '{
    "memories_grpc_host": "localhost",
    "memories_grpc_port": 9010,
    "ollama_base_url": "http://192.168.1.2:11434",
    "extra_labels_filter": ["agent:claude_code"],
    "start_date": "2026-05-05",
    "end_date": "2026-05-05"
  }' \
  -w /absolute/path/to/daily-work-summary-batch.yaml
```

`extra_labels_filter` は `summary` ラベルと AND で評価され、結果として作られる集約スレッドのラベルにも追加される（同日でも別 `extra_labels_filter` の組合せごとに別スレッドが作られる点に注意）。

## 入力パラメータ

### 共通

| パラメータ | 必須 | デフォルト | 説明 |
|-----------|------|-----------|------|
| `user_id` | ○ | - | 集約対象。`THREAD_SUMMARY` と日次出力を同じ実ユーザー所有にする |
| `memories_grpc_host` / `_port` | ○ | `localhost:9100` | memories gRPC エンドポイント |
| `summary_label` | - | `summary` | 入力スレッドのマーカーラベル |
| `daily_label` | - | `daily_summary` | 出力スレッドのマーカーラベル |
| `extra_labels_filter` | - | `[]` | 入力に AND マッチする追加ラベル（全 project 横断が既定） |
| `min_thread_count` | - | `1` | 入力スレッド数がこれ未満なら skip |
| `max_context_chars` | - | `200000` | 互換のため受け付けるのみ（未使用）。入力上限は `summary_input_byte_budget` と `token_count` で決まる |
| `llm_context_window_tokens` | - | `131072` | モデルのコンテキスト長（ハードリミット） |
| `summary_max_completion_tokens` | - | `32768` | 出力トークン上限 |
| `summary_input_byte_budget` | - | `200000` | token_count 不可時の入力バイト上限 |
| `llm_expected_model` | - | `""` | 指定時は token_count の解決済みモデル名と一致を要求 |
| `extra_labels_match_mode` | - | `LABEL_ALL` | `extra_labels_filter` の照合方法 |
| `exclude_thread_labels` | - | `["subagent"]` | これらの label を持つ元スレッドの要約（`summary:<thread_id>`）を集約から除く。subagent の内容は親スレッドの要約と thread-group summary に含まれる。空配列で除外なし |
| `summary_model` / `ollama_base_url` | - | thread-summary と同じ | LLM 設定 |
| `timezone_offset_hours` | - | `9` | 1日の境界を切るタイムゾーン（JST 既定） |
| `force_resummarize` | - | `false` | `true` で既存日次要約も再生成 |
| `output_language` | - | `ja` | 生成言語 `ja` / `en`。batch が呼ぶ言語別 worker の選択に使う |

### single worker 固有

batch が単一日へ fan-out する際に渡すフィールド（single worker は直接呼ばず、
batch 経由で `workerName` 指定の言語別 worker に渡る）:

| パラメータ | 必須 | デフォルト | 説明 |
|-----------|------|-----------|------|
| `target_date` | - | 昨日 | 対象日 `YYYY-MM-DD` |

### batch 固有

| パラメータ | 必須 | デフォルト | 説明 |
|-----------|------|-----------|------|
| `start_date` / `end_date` | - | - | 両方指定で範囲モード（包含） |
| `last_n_days` | - | - | 直近 N 日モード（昨日終わり） |
| `target_dates` | - | `[]` | 対象期間の明示列挙。指定時は範囲・直近 N より優先 |
| `stop_on_error` | - | `false` | `true` で最初の失敗で停止 |
| `source_scope_digest` | - | 絞り込みなし範囲の digest | path 単位集約（v2）の入力範囲識別子 |
| `generation_id` / `generation_started_at` | - | `""` | 生成世代の識別子（空なら実行時刻）。metadata に記録 |

`(start_date, end_date)` と `last_n_days` の両方が省略されたときは「昨日のみ」のフォールバック。両方指定されたときは `(start_date, end_date)` が勝つ。
batch は `output_language` に応じて `memories-daily-work-summary-single-ja` /
`memories-daily-work-summary-single-en` を `workerName` で呼ぶ。事前に
`memories-import upsert-generation-workers --feature daily-work-summary --language all`
などで言語別 single worker を登録しておく。

## パス単位集約と生成時の保護

batch は各日について、まず single を `plan_only: true` で呼び、入力のスレッド要約メモリを
path bucket（入力スレッドの `path:*` ラベル。0 個なら `unassigned`、複数なら `ambiguous`）に分類する。
次に bucket ごとの path 要約
（`daily:v2:<user_id>:YYYY-MM-DD:<source_scope_digest>:path:<digest>`、ラベル
`summary_scope:path` / `source_scope:<digest>`）を書き、最後にそれらを入力とした全体要約
（`...:overall`、ラベル `summary_scope:overall`）を書く。計画から消えた bucket の path 要約は
メモリ行だけ削除する（スレッドは残す）。入力が 1 件だけのときは LLM を呼ばずにそのまま転記する。

- `source_scope_digest` は入力ラベル範囲の識別子。既定値は絞り込みなし範囲の digest。
  `extra_labels_match_mode`（`LABEL_ALL` / `LABEL_ANY`）で `extra_labels_filter` の照合方法を選ぶ。
- `target_dates` は対象期間を明示列挙するモードで、範囲指定・直近 N 指定より優先される。
- LLM リクエストはまず LLM runner の `token_count` メソッドで計測し、
  `llm_context_window_tokens` と `summary_max_completion_tokens` に収まるまで古い入力から外す。
  正確に数えられないプロバイダ（Ollama など）は `summary_input_byte_budget` によるバイト上限に
  フォールバックする。`llm_expected_model` を指定した場合は解決済みモデル名と一致しなければ失敗する。
  `max_context_chars` は互換のために受け付けるだけで使われない。
- JSON として不正、または空欄の LLM 出力は `.../invalid-output` として拒否し、最大 2 回再生成する。
  空の要約は保存しない。
- 再生成は source fingerprint、path 計画の fingerprint、（LLM で生成した要約では）
  モデル契約の fingerprint（prompt・モデル・予算）のいずれかが変わったときに行う。
- `stop_on_error: true` で最初に失敗した path / 期間で停止する。既定はベストエフォート。
  batch 出力の `outcome` は `succeeded`、`completed_with_errors`（path が失敗したが全体要約は書けた。
  `failed_path_buckets` を参照）、`failed` のいずれか。
- batch 全体と期間ループには 31 日の timeout がある。結果を返さずに終わった項目
  （timeout / 中断）は失敗として数える。個々の run/call タスクは worker が
  `WORKFLOW_TASK_DEFAULT_TIMEOUT_SEC` を設定していればそれに従う。

## 差分実行

`force_resummarize: false`（既定）のとき、以下を満たす場合にスキップする:

1. 同 (user_id, date, scope) の `external_id = "daily:<user_id>:YYYY-MM-DD:<scope_key>"` 集約メモリが存在
2. その集約メモリの `updated_at` が、当日の入力スレッド群の最大 `updated_at` 以上
3. 入力要約の ID と `updated_at` から作る source fingerprint が、保存済み metadata の fingerprint と一致する
4. path 計画の fingerprint と、LLM で生成した要約ではモデル契約の fingerprint も一致する

入力の追加・削除を最大 `updated_at` が変わらない場合にも検知するため、2〜4 をすべて満たしたときだけスキップする。

## 出力データの構造

### 集約スレッド (`Thread`)

- `user_id`: 入力 `user_id` と同じ実ユーザー
- `labels`: `["daily_summary", "date:YYYY-MM-DD", "scope:<scope_key>"]` + sorted `extra_labels_filter`
- `description`: `"YYYY-MM-DD — <overall_purpose>"`

### 集約メモリ (`Memory`, role=ASSISTANT)

- `content`: 下記 JSON 構造を `tojson` した文字列
- `external_id`: `daily:<user_id>:YYYY-MM-DD:<scope_key>`
- `metadata`: `{daily_date, scope, extra_labels[], source_memory_count, source_snapshot_version, source_max_updated_at, source_fingerprint_algorithm, source_fingerprint, summary_version}`
  - 上記は `v2_rollup_scope` 未指定（直接呼び出し）時の形式。batch が書く path / overall 要約は `schema_version=2` 形式で、`period_kind`・`period_key`・`rollup_scope`・`source_scope_digest`・`path_bucket_kind`・`aggregation_mode`（`llm` / `passthrough`）・`completion_status`・`source_plan_fingerprint`・`model_contract_fingerprint`・`generation_id` などを持つ
  - `source_fingerprint` は入力要約の ID と `updated_at` を SHA-256 で固定長化した値。`updated_at` だけでは検出できない入力の追加・削除も検知する

```json
{
  "overall_purpose": "...",
  "purpose_groups": [
    {
      "purpose": "...",
      "bullets": ["...", "..."],
      "source_memory_ids": ["7453040111820003484", "7453040111820003520"],
      "status": "resolved"
    }
  ],
  "by_topic": [
    { "topic": "...", "bullets": ["..."] }
  ],
  "carryover": ["..."],
  "metrics": {
    "total_memories": 7,
    "total_resolved": 4,
    "total_ongoing": 2,
    "total_abandoned": 1
  }
}
```

## 注意事項

- **`thread-summary-batch` は `MEMORY_KIND_RAW` のみを取得する**ため、日次要約スレッドは次回のスレッド要約対象に含まれない。
- **`extra_labels_filter` を変えると別スレッドができる**。同一日に「全 project 横断」と「`agent:claude_code` のみ」の両方を実行すると、`labels` が異なる 2 本の集約スレッドが作られる。狙ったとおりなら問題ないが、運用は揃えたほうが listing が綺麗。
- **タイムゾーンの取り扱い**。日界は jq を評価する jobworkerp worker の `TZ` 環境変数（例 `TZ=Asia/Tokyo`）で決まり、夏時間 (DST) と負オフセットに対応する。`TZ` 未設定時のフォールバック `timezone_offset_hours` は時単位（0..23）なので、半端なオフセット（IST 等）や負オフセットには未対応。それらが必要なら worker の `TZ` を設定する。
- **LLM のコンテキスト長**。`llm_context_window_tokens` を使用モデルのコンテキスト長に合わせること（Ollama では token_count が使えないため `summary_input_byte_budget` で入力量を調整する）。

## バッチ結果

`generated_count` / `skipped_count` / `failed_count` と対応する日付配列を返す。
既存・最新または入力不足による `skipped` は正常結果として `succeeded_count` に含まれ、
上位ワークフローは週次要約へ進む。`completed=false, skipped=false` のみ失敗として記録する。
