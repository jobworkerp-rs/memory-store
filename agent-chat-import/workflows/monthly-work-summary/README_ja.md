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

- `weekly-work-summary-single` が `weekly_summary` ラベル付き要約スレッドを `user_id = 100000` 配下に生成済みであること
- 各週次要約スレッドの `ThreadData.description` に `<YYYY-Www> — <overall_purpose>` 形式のテキストが入っていること（weekly-work-summary Step 14 で書き込まれる）
- jobworkerp / memories / Ollama の起動状態は daily/weekly と同じ

## 設計上のキー

| 項目 | 値 | 理由 |
|---|---|---|
| 集約スレッドの所有者 | `user_id = 100000` (= 上層と同じ) | 要約エージェントの出力。ラベルでフィルタ可能なので user 分離は不要 |
| 集約スレッドのラベル | `monthly_summary`, `month:YYYY-MM`, `scope:<scope_key>`, `extra_labels_filter` の各値 (sort 済み) | `monthly_summary` で一覧、`month:` で月絞り込み、`scope:` で同月内の異 scope を分離 |
| 集約メモリの `external_id` | `monthly:YYYY-MM:<scope_key>` | `memory.external_id` は DB 全体で UNIQUE。同月に異なる `extra_labels_filter` で並列実行しても衝突しないよう scope を suffix に含める |
| `scope_key` の算出 | `extra_labels_filter` を `sort \| join(",")`。空なら `_all` | 順序非依存 |
| 入力の取得方法 | `MemoryService.FindListByCondition` で **週次要約メモリ自身**を絞り込む (`external_id_prefix="weekly:"` + `roles=[ROLE_ASSISTANT]` + `updated_after/before` + `thread_filter.labels=[weekly_summary]+extra` の AND マッチ) | 週次・日次と同じ「メモリ単位で時間絞り込み」パターン |
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
  --channel workflow_lang \
  --repo-root /abs/path/to/memories/agent-chat-import
```

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
| `source_user_id` | - | `100000` | 集約対象 |
| `memories_grpc_host` / `_port` | ○ | `localhost:9100` | memories gRPC エンドポイント |
| `weekly_label` | - | `weekly_summary` | 入力スレッドのマーカーラベル |
| `monthly_label` | - | `monthly_summary` | 出力スレッドのマーカーラベル |
| `extra_labels_filter` | - | `[]` | 入力に AND マッチする追加ラベル |
| `min_thread_count` | - | `1` | 入力 memory 数がこれ未満なら skip （実態は memory 件数） |
| `max_context_chars` | - | `200000` | LLM 入力の上限文字数 |
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

`(start_month, end_month)` と `last_n_months` の両方が省略されたときは「先月のみ」のフォールバック。両方指定されたときは `(start_month, end_month)` が勝つ。
batch は `output_language` に応じて `memories-monthly-work-summary-single-ja` /
`memories-monthly-work-summary-single-en` を `workerName` で呼ぶ。事前に
`memories-import upsert-generation-workers --feature monthly-work-summary --language all --channel workflow_lang`
などで言語別 single worker を登録しておく。

## 差分実行

`force_resummarize: false`（既定）のとき、以下を満たす場合にスキップする:

1. 同 (month, scope) の `external_id = "monthly:YYYY-MM:<scope_key>"` 集約メモリが存在
2. その集約メモリの `updated_at` が、当月の入力週次要約群の最大 `updated_at` 以上

## 出力データの構造

### 集約スレッド (`Thread`)

- `user_id`: `source_user_id` (= 100000)
- `labels`: `["monthly_summary", "month:YYYY-MM", "scope:<scope_key>"]` + sorted `extra_labels_filter`
- `description`: `"YYYY-MM — <overall_purpose>"`

### 集約メモリ (`Memory`, role=ASSISTANT)

- `content`: 下記 JSON 構造を `tojson` した文字列
- `external_id`: `monthly:YYYY-MM:<scope_key>`
- `metadata`: `{month, scope, extra_labels[], source_memory_count, source_memory_ids[], source_thread_ids[], summary_version}`
  - `source_memory_ids` は **週次要約メモリ** の id 列

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
- **LLM のコンテキスト長**。月 4-5 週分の週次 JSON が入る。`max_context_chars=200000` は十分
- **cron 順序**: weekly が完了してから monthly を回すこと。例: weekly を月曜 03:30 JST、monthly を毎月 1 日 04:00 JST
