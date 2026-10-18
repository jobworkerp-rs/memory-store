# 月次作業要約ワークフロー

`weekly-work-summary-single` が生成した週次の作業要約を 1 暦月分集約し、月内の重要事項 (highlights) と達成された節目 (milestones) を抽出する第 5 層の要約ワークフロー。

```
[1] agent-chat-import
       ↓
[2] thread-summary-single
       ↓
[3] daily-work-summary-single
       ↓
[4] weekly-work-summary-single
       ↓
[5] monthly-work-summary-single (1 月単位の集約 / "monthly_summary" ラベル) ← 本ワークフロー
```

## ファイル構成

| ファイル | 説明 |
|---------|------|
| `monthly-work-summary-batch.yaml`  | 月レンジを指定して single を逐次実行するバッチ。本ディレクトリ |
| `../../workers/monthly-work-summary/monthly-work-summary-single.yaml` | 1 月分の集約要約を実行する単発ワークフロー。prompt は登録時に worker settings へ焼き込まれる |
| `run-monthly-summary.sh`           | `jobworkerp-client` 経由で batch を起動するヘルパースクリプト |

batch は single を `workerName` で呼ぶ。`output_language` に応じて
`memories-monthly-work-summary-single-ja` / `memories-monthly-work-summary-single-en`
を選び分けるため、事前に `memories-import upsert-generation-workers` で言語別
worker を登録しておく（後述）。

## 前提条件

- `weekly-work-summary-single` が `weekly_summary` ラベル付き `WEEKLY_SUMMARY` を対象の `user_id` 配下に生成済みであること
- 各週次要約スレッドの `ThreadData.description` に `<YYYY-Www> — <overall_purpose>` 形式のテキストが入っていること（weekly-work-summary Step 14 で書き込まれる）
- jobworkerp / memories / Ollama の起動状態は daily/weekly と同じ

## 設計上のキー

| 項目 | 値 | 理由 |
|---|---|---|
| 集約スレッドの所有者 | 入力 `user_id` (= 上層と同じ) | `memory_kind` とラベルで要約階層を分離する |
| 集約スレッドのラベル | `monthly_summary`, `month:YYYY-MM`, `scope:<scope_key>`, `extra_labels_filter` の各値 (sort 済み) | `monthly_summary` で一覧、`month:` で月絞り込み、`scope:` で同月内の異 scope を分離 |
| 集約メモリの `external_id` | `monthly:<user_id>:YYYY-MM:<scope_key>` | `memory.external_id` は DB 全体で UNIQUE。所有者と scope を含め、同月に異なるユーザーまたは `extra_labels_filter` で並列実行しても衝突しないようにする |
| `scope_key` の算出 | `extra_labels_filter` を `sort \| join(",")`。空なら `_all` | 順序非依存 |
| 入力の取得方法 | `MemoryService.FindListByCondition` で **週次要約メモリ自身**を絞り込む (`memory_kinds=[MEMORY_KIND_WEEKLY_SUMMARY]` + `roles=[ROLE_ASSISTANT]` + `updated_after/before` + `thread_filter.labels=[weekly_summary]+extra` の AND マッチ) | 週次・日次と同じ「メモリ単位で時間絞り込み」パターン |

| LLM 入力 | `memory.data.content` (週次要約 JSON: overall_purpose / purpose_groups / by_topic / trends / carryover) + `thread_description` の組み合わせ | trends フィールドを使って月内の節目を判定 |
| 月境界 | 暦月 (1 日 00:00 〜 翌月 1 日 00:00 in tz) | 文字列演算で年跨ぎを処理することで、broken-down arithmetic のうるう/DST 問題を回避 |

## 上位の集約（このワークフローの価値）

`weekly-work-summary-single` の出力は週単位での目的整理＋週内動向 (trends) に留まる。本ワークフローではそれらを横断して以下を生成する:

- **`overall_purpose`** — 当月の上位目的を 1〜3 文で言語化
- **`purpose_groups`** — 目的が共通する週次要約をマージし、月単位で再整理
- **`by_topic`** — リポジトリ・技術領域などのトピック軸で再整理
- **`highlights`** — 1 ヶ月で最も重要だった事項を 5 件以内 (title / summary / source_memory_ids)
- **`milestones`** — 月内で resolved に到達した purpose / トピック単位の節目 (title / outcome / completed_in_week / source_memory_ids)
- **`carryover`** — 翌月以降への持ち越し

`purpose_groups.status` は thread-summary の [status](../thread-summary/README_ja.md#status)
と同じ値を使う。`in_review`, `blocked`, `deferred` は `carryover` の対象であり、完了済みの
`milestones` には含めない。

system prompt は `agent-chat-import/workers/monthly-work-summary/prompts/system_prompt.<lang>.txt`、
user prompt 末尾の言語依存指示は `agent-chat-import/workers/monthly-work-summary/prompts/user_tail.<lang>.txt`
に置き、言語別 worker 登録時に `settings.workflow_context` へ焼き込む。

## 使い方

### 言語別 worker の登録（初回 / prompt 変更時）

single は prompt を YAML に埋め込まず、登録時に worker settings へ焼き込む。
batch を動かす前に、対象言語の worker を登録しておく:

```bash
memories-import upsert-generation-workers \
  --feature monthly-work-summary \
  --language all \
  --repo-root /abs/path/to/memories/agent-chat-import
```

`--channel` を省略すると jobworkerp のデフォルト channel が使われる。特定の
channel が必要な場合だけ指定する。

### 単発（1 月分）

1 月だけ生成する場合も batch を `start_month = end_month` で呼ぶ（生の single YAML を
`-w` で直接呼ぶと prompt context が無く失敗するため、登録済みの言語別 worker を
経由する batch を使う）:

```bash
jobworkerp-client job enqueue-workflow \
  -i '{
    "memories_grpc_host": "localhost",
    "memories_grpc_port": 9010,
    "ollama_base_url": "http://192.168.1.2:11434",
    "summary_model": "qwen3.6:27b",
    "start_month": "2026-04",
    "end_month": "2026-04",
    "output_language": "ja"
  }' \
  -w /absolute/path/to/monthly-work-summary-batch.yaml \
  --format json \
  -t 1800
```

`start_month` / `end_month` / `last_n_months` をすべて省略すると「先月」
（`timezone_offset_hours=9` の JST 基準）を自動選択する。cron での月次運用
(毎月 1 日の早朝) はこの形が便利。

### バッチ（月レンジ）

```bash
# 直近 3 ヶ月を一括生成
jobworkerp-client job enqueue-workflow \
  -i '{
    "memories_grpc_host": "localhost",
    "memories_grpc_port": 9010,
    "ollama_base_url": "http://192.168.1.2:11434",
    "summary_model": "qwen3.6:27b",
    "last_n_months": 3
  }' \
  -w /absolute/path/to/monthly-work-summary-batch.yaml \
  --format json \
  -t 86400

# 明示的な月レンジ
jobworkerp-client job enqueue-workflow \
  -i '{
    "memories_grpc_host": "localhost",
    "memories_grpc_port": 9010,
    "ollama_base_url": "http://192.168.1.2:11434",
    "start_month": "2025-11",
    "end_month": "2026-02"
  }' \
  -w /absolute/path/to/monthly-work-summary-batch.yaml \
  --format json \
  -t 86400
```

### ヘルパースクリプト経由（推奨）

```bash
# 直近 3 ヶ月分
agent-chat-import/workflows/monthly-work-summary/run-monthly-summary.sh --last-n-months 3

# 指定月のみ
agent-chat-import/workflows/monthly-work-summary/run-monthly-summary.sh --target-month 2026-04

# プロジェクト絞り込み + 強制再生成
agent-chat-import/workflows/monthly-work-summary/run-monthly-summary.sh \
    --target-month 2026-04 \
    --extra-labels "agent:claude_code,coding_agent" \
    --force-resummarize

# k8s 本番環境向け (port-forward 自動起動)
agent-chat-import/workflows/monthly-work-summary/run-monthly-summary.sh \
    --port-forward \
    --last-n-months 3
```

実行内容を確認したいだけなら `--print-only` を付ける。
全オプションは `run-monthly-summary.sh --help` を参照。

## 入力パラメータ

### 共通

| パラメータ | 必須 | デフォルト | 説明 |
|-----------|------|-----------|------|
| `user_id` | ○ | - | 集約対象。`WEEKLY_SUMMARY` と月次出力を同じ実ユーザー所有にする |
| `memories_grpc_host` / `_port` | ○ | `localhost:9100` | memories gRPC エンドポイント |
| `weekly_label` | - | `weekly_summary` | 入力スレッドのマーカーラベル |
| `monthly_label` | - | `monthly_summary` | 出力スレッドのマーカーラベル |
| `extra_labels_filter` | - | `[]` | 入力に AND マッチする追加ラベル |
| `min_thread_count` | - | `1` | 入力 memory 数がこれ未満なら skip （実態は memory 件数） |
| `max_context_chars` | - | `200000` | 互換のため受け付けるのみ（未使用）。入力上限は `summary_input_byte_budget` と `token_count` で決まる |
| `llm_context_window_tokens` | - | `131072` | モデルのコンテキスト長（ハードリミット） |
| `summary_max_completion_tokens` | - | `32768` | 出力トークン上限 |
| `summary_input_byte_budget` | - | `200000` | token_count 不可時の入力バイト上限 |
| `llm_expected_model` | - | `""` | 指定時は token_count の解決済みモデル名と一致を要求 |
| `extra_labels_match_mode` | - | `LABEL_ALL` | `extra_labels_filter` の照合方法 |
| `summary_model` / `ollama_base_url` | - | daily/weekly と同じ | LLM 設定 |
| `timezone_offset_hours` | - | `9` | 月の境界を切るタイムゾーン（JST 既定） |
| `force_resummarize` | - | `false` | `true` で既存月次要約も再生成 |
| `output_language` | - | `ja` | 生成言語 `ja` / `en`。batch が呼ぶ言語別 worker の選択に使う |

### single worker 固有

batch が単一月へ fan-out する際に渡すフィールド（single worker は直接呼ばず、
batch 経由で `workerName` 指定の言語別 worker に渡る）:

| パラメータ | 必須 | デフォルト | 説明 |
|-----------|------|-----------|------|
| `target_month` | - | 先月 | 対象月 `YYYY-MM` (月は 2 桁ゼロ埋め) |

### batch 固有

| パラメータ | 必須 | デフォルト | 説明 |
|-----------|------|-----------|------|
| `start_month` / `end_month` | - | - | 両方指定で範囲モード（包含） |
| `last_n_months` | - | - | 直近 N ヶ月モード（先月終わり） |
| `target_months` | - | `[]` | 対象期間の明示列挙。指定時は範囲・直近 N より優先 |
| `stop_on_error` | - | `false` | `true` で最初の失敗で停止 |
| `source_scope_digest` | - | 絞り込みなし範囲の digest | path 単位集約（v2）の入力範囲識別子 |
| `generation_id` / `generation_started_at` | - | `""` | 生成世代の識別子（空なら実行時刻）。metadata に記録 |

`(start_month, end_month)` と `last_n_months` の両方が省略されたときは「先月のみ」のフォールバック。両方指定されたときは `(start_month, end_month)` が勝つ。
batch は `output_language` に応じて `memories-monthly-work-summary-single-ja` /
`memories-monthly-work-summary-single-en` を `workerName` で呼ぶ。事前に
`memories-import upsert-generation-workers --feature monthly-work-summary --language all`
などで言語別 single worker を登録しておく。

## パス単位集約と生成時の保護

batch は各月について、まず single を `plan_only: true` で呼び、入力の週次要約メモリを
path bucket（入力スレッドの `path:*` ラベル。0 個なら `unassigned`、複数なら `ambiguous`）に分類する。
次に bucket ごとの path 要約
（`monthly:v2:<user_id>:YYYY-MM:<source_scope_digest>:path:<digest>`、ラベル
`summary_scope:path` / `source_scope:<digest>`）を書き、最後にそれらを入力とした全体要約
（`...:overall`、ラベル `summary_scope:overall`）を書く。計画から消えた bucket の path 要約は
メモリ行だけ削除する（スレッドは残す）。入力が 1 件だけのときは LLM を呼ばずにそのまま転記する。

- `source_scope_digest` は入力ラベル範囲の識別子。既定値は絞り込みなし範囲の digest。
  `extra_labels_match_mode`（`LABEL_ALL` / `LABEL_ANY`）で `extra_labels_filter` の照合方法を選ぶ。
- `target_months` は対象期間を明示列挙するモードで、範囲指定・直近 N 指定より優先される。
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

1. 同 (user_id, month, scope) の `external_id = "monthly:<user_id>:YYYY-MM:<scope_key>"` 集約メモリが存在
2. その集約メモリの `updated_at` が、当月の入力週次要約群の最大 `updated_at` 以上
3. 入力週次要約の ID・`updated_at`・各 metadata の `source_fingerprint` から作る source fingerprint が、保存済み metadata の fingerprint と一致する
4. path 計画の fingerprint と、LLM で生成した要約ではモデル契約の fingerprint も一致する

週次要約が元の時刻を保持したまま再生成された場合も、上流 fingerprint の変化により月次要約を再生成する。

## 出力データの構造

### 集約スレッド (`Thread`)

- `user_id`: 入力 `user_id` と同じ実ユーザー
- `labels`: `["monthly_summary", "month:YYYY-MM", "scope:<scope_key>"]` + sorted `extra_labels_filter`
- `description`: `"YYYY-MM — <overall_purpose>"`

### 集約メモリ (`Memory`, role=ASSISTANT)

- `content`: 下記 JSON 構造を `tojson` した文字列
- `external_id`: `monthly:<user_id>:YYYY-MM:<scope_key>`
- `metadata`: `{month, scope, extra_labels[], source_memory_count, source_snapshot_version, source_max_updated_at, source_fingerprint_algorithm, source_fingerprint, summary_version}`
  - 上記は `v2_rollup_scope` 未指定（直接呼び出し）時の形式。batch が書く path / overall 要約は `schema_version=2` 形式で、`period_kind`・`period_key`・`rollup_scope`・`source_scope_digest`・`path_bucket_kind`・`aggregation_mode`（`llm` / `passthrough`）・`completion_status`・`source_plan_fingerprint`・`model_contract_fingerprint`・`generation_id` などを持つ
  - `source_snapshot_version=2` の fingerprint は **週次要約メモリ** の ID・`updated_at`・上流 fingerprint を含むため、時刻を変えない週次再生成も検知する

```json
{
  "overall_purpose": "...",
  "purpose_groups": [
    {
      "purpose": "...",
      "bullets": ["...", "..."],
      "source_memory_ids": ["..."],
      "status": "resolved"
    }
  ],
  "by_topic": [
    { "topic": "...", "bullets": ["..."] }
  ],
  "highlights": [
    {
      "title": "...",
      "summary": "...",
      "source_memory_ids": ["..."]
    }
  ],
  "milestones": [
    {
      "title": "...",
      "outcome": "...",
      "completed_in_week": "2026-W19",
      "source_memory_ids": ["..."]
    }
  ],
  "carryover": ["..."]
}
```

## 注意事項

- **月跨ぎ ISO 週の月帰属**: 1 つの ISO 週が暦月をまたぐ場合 (例: 2026-W18 = 2026-04-27 〜 2026-05-03)、その週の memory `updated_at` は週内最後の日次活動の時刻になる。本ワークフローの月境界フィルタは `[month_start_ms, month_end_ms)` の範囲で `updated_at` を絞るため、跨る週は **最終日次活動が属する月** に帰属する。たとえば「2026-W18 の最終 daily activity が 2026-05-02」なら、当該週次要約は 2026-05 月の入力に含まれる。これは仕様（運用上のシンプルさを優先）であり、必要なら週月曜の暦月で帰属させる代替実装を別 PR で追加検討する
- **下位層が空の場合**: `weekly_summary` メモリが対象月に 1 件もない場合は `skipped: true, skip_reason: "skipped: not enough source summary memories for the month"` で正常終了する。エラーではない
- **`extra_labels_filter` を変えると別スレッドができる**。daily/weekly と同じ仕様
- **タイムゾーンの取り扱い**。月境界は jq を評価する jobworkerp worker の `TZ` 環境変数（例 `TZ=Asia/Tokyo`）で決まり、夏時間 (DST) と負オフセットに対応する。`TZ` 未設定時のフォールバック `timezone_offset_hours` は時単位（0..23）
- **LLM のコンテキスト長**。`llm_context_window_tokens` を使用モデルのコンテキスト長に合わせること（Ollama では token_count が使えないため `summary_input_byte_budget` で入力量を調整する）。
- **cron 順序**: weekly が完了してから monthly を回すこと。例: weekly を月曜 03:30 JST、monthly を毎月 1 日 04:00 JST

## バッチ結果

`generated_count` / `skipped_count` / `failed_count` と対応する月配列を返す。
既存・最新または入力不足による `skipped` は正常結果として `succeeded_count` に含まれる。
`completed=false, skipped=false` のみ失敗として記録する。
