# agent-chat-summary ワークフロー

import 済みのチャットログに対して、スレッド要約 → 日次作業要約 → (任意で) 振り返り (thread-reflection) と、(任意で) personality 抽出を並列に実行する summary 専用ワークフロー。`agent-chat-pipeline.yaml` を import / summary に 2 分割した片割れで、もう一方が `../agent-chat-import/agent-chat-import.yaml`。

```
fork:
  summaryBranch (常時, serial, 致命):
    threadSummaryBatch
      ↓
    dailyWorkSummary
      ↓
    reflectionStage (opt-in)
  personalityBranch (opt-in, 非致命, try/catch):
    threadPersonalityBatch → userPersonalityMerge
```

## いつ使うか

- import と summary を異なるホスト・異なる頻度で回したい場合 (= split-execution)
- `agent-chat-pipeline.yaml` 経由でまとめて回す場合は本ワークフローを直接 enqueue する必要はない (wrapper が内部で呼ぶ)

## 設計上のキー

| 項目 | 値 / 仕様 | 理由 |
|---|---|---|
| 処理窓 | 入力 (`since_date`, `end_date`, `range_start_ms`, `since_ms_utc`) で受け取る | import 側で算出した処理窓と完全一致させるため。summary を単独で起動する split-execution でも、import 側と同じ値を渡すことで挙動を揃えられる |
| stage 間結合 | memories のラベル経由のみ | `daily-work-summary-batch` は `thread-summary-batch` の出力を memory query で読み戻すだけ。workflow output 渡しは無い (= 各 stage を独立に再実行可能) |
| reflection 失敗 | **致命** (try/catch なし) | reflection は summary の最終段。reflector LLM / 設定ミスを silent に握り潰さないため、失敗で workflow 全体を fail させる |
| personality 失敗 | **非致命** (branch 内 try/catch) | personality は補足シグナル。failure で summary 側の結果を巻き戻さない (`personality_error` 出力で検出) |

## 失敗ポリシーの非対称性

reflection と personality はどちらも opt-in な LLM ベース機能だが、失敗時の扱いが対照的。

| | reflection | personality |
|---|---|---|
| 失敗時の workflow 状態 | failed | succeeded (warning) |
| 失敗の検出方法 | jobworkerp UI の task error | `personality_error` 出力フィールド |
| 個別スレッドの失敗 | batch 内 `onError: continue` で握り潰し (← 既存 `thread-reflection-batch` 挙動) | 同じ idiom (既存 `thread-personality-batch` 挙動) |

reflection を致命にしている理由: コーディング作業ログ処理パイプライン全体の **最終段** として位置付けたいため、reflector の設定ミス・モデルの落下・タイムアウトを silent に伝搬させたくない。

## 入力パラメータ

### 必須
| パラメータ | 説明 |
|---|---|
| `user_id` | 対象スレッドの作成者 user id |
| `memories_grpc_url` | memories gRPC エンドポイント URL |
| `since_date` | `YYYY-MM-DD`。処理範囲の起点 |
| `end_date` | `YYYY-MM-DD`。処理範囲の終点 |
| `range_start_ms` | `since_date 00:00 +tz` の epoch ms。`updated_after_ms` の lower bound に使う |
| `since_ms_utc` | UTC `--since` cutoff の epoch ms (day_start mode では 0) |
| `thread_summary_batch_yaml` | `thread-summary-batch.yaml` の絶対パスまたは URL |
| `daily_work_summary_batch_yaml` | `daily-work-summary-batch.yaml` の絶対パスまたは URL |

### 主要な任意パラメータ
| パラメータ | 既定値 | 説明 |
|---|---|---|
| `timezone_offset_hours` | `9` | daily-work-summary-batch に伝播するフォールバック固定オフセット。**worker の `TZ` 環境変数が未設定のときのみ**使われる (DST/負オフセット非対応) |

> **タイムゾーン**: 日界は workflow の jq を評価する **jobworkerp worker プロセスの `TZ` 環境変数** (例 `TZ=Asia/Tokyo`) で決まり、DST・負オフセットに対応する。未設定なら `timezone_offset_hours` にフォールバック。`TZ` は worker のデプロイ環境に設定する (この workflow の入力では渡せない)。
| `memories_grpc_host` | `""` | URL から自動抽出。NAT 越え等で override したい場合のみ指定 |
| `memories_grpc_port` | `0` | 同上 |
| `summary_model` | `qwen3.6:27b` | summary LLM |
| `ollama_base_url` | `http://localhost:11434` | LLM endpoint |
| `memory_thread_label_prefix` | `summary` | thread-summary marker label |
| `daily_summary_label` | `daily_summary` | daily-work-summary marker label |
| `extra_labels_filter` | `[]` | daily-work-summary の scope 絞り込み |
| `force_resummarize` | `false` | thread-summary / daily-summary 両方に伝達 |
| `min_thread_count` | `1` | daily-work-summary のスキップ閾値 |
| `max_context_chars` | `200000` | LLM コンテキスト上限 |

### reflection 段 (任意)

**`thread_reflection_batch_yaml` を指定すると有効化される。** 空なら reflection 段は skip。

| パラメータ | 既定値 | 説明 |
|---|---|---|
| `thread_reflection_batch_yaml` | `""` | `thread-reflection-batch.yaml` の絶対パス / URL。空で reflection 段全体を skip |
| `reflector_model` | `""` (= `summary_model` 流用) | reflector LLM モデル |
| `reflector_base_url` | `""` (= `ollama_base_url` 流用) | reflector LLM endpoint |
| `prompt_version` | `"v1"` | reflection プロンプト リビジョンタグ。プロンプトを変更したら bump して実験コホートを区別できるようにする |
| `reflector_id` | `"self"` | thread-reflection-batch にそのまま渡す |
| `reflection_force` | `false` | thread-reflection-batch の `force` に渡す。`force_resummarize` / `force_reextract` と独立 |
| `context_limit_tokens` | `222000` | reflection チューニング knob |
| `single_pass_threshold_tokens` | `170000` | 同上 |
| `window_size_turns` | `80` | 同上 |
| `window_overlap_turns` | `10` | 同上 |
| `merge_max_input_tokens` | `100000` | 同上 |
| `estimated_tokens_per_turn` | `1500` | 同上 |
| `experiment_id` | (未指定) | 任意の実験コホートタグ。値が無いと thread-reflection-batch にも渡らない |
| `experiment_variant` | (未指定) | 任意のバリアントタグ。同上 |

reflection は summary と同じ `updated_after_ms` 窓 (= `max(since_ms_utc, range_start_ms) - 1`) で動く。**ラベル / 個別スレッド絞り込みが必要な場合は本ワークフロー経由ではなく `thread-reflection-batch.yaml` を直接 enqueue すること。**

### personality 段 (任意)

`thread_personality_batch_yaml` を指定すると 1 層 (per-thread の嗜好シグナル抽出) が有効化される。さらに `user_personality_merge_yaml` も指定すると 2 層 (ユーザ統合プロファイル生成) が有効化される。

| パラメータ | 既定値 | 説明 |
|---|---|---|
| `thread_personality_batch_yaml` | `""` | 空で personality 段全体を skip |
| `user_personality_merge_yaml` | `""` | **2 層 merge の有効化フラグ**。非空のとき 2 層 merge を実行する (batch は登録済み merge worker を呼ぶ。値は有効化判定にのみ使われる) |
| `personality_model` | `""` (= `summary_model` 流用) | personality LLM |
| `min_user_messages` | `2` | ROLE_USER 件数下限 |
| `force_reextract` | `false` | thread-personality-batch に伝達 |
| `force_remerge` | `false` | user-personality-merge に伝達 |
| `max_signals` | `200` | user-personality-merge の入力シグナル上限 |

## 出力

| キー | 説明 |
|---|---|
| `completed` | `daily_summary_succeeded && (reflection 無効 ∨ reflection 成功)` |
| `thread_summary_succeeded` | bool |
| `daily_summary_succeeded` | bool |
| `reflection_enabled` | bool |
| `reflection_succeeded` | bool。reflection 致命扱いのため、これが false で `completed=true` のケースは reflection 無効化時のみ |
| `personality_enabled` | bool |
| `thread_personality_succeeded` | bool。branch ワークフロー自体の完走を表す (= 個別スレッドの失敗は反映されない) |
| `user_personality_merge_succeeded` | bool。同上 |
| `personality_error` | personality branch が raise した時の error メッセージ。それ以外は null |

## 使い方

### `run-summary.sh` (推奨)

```bash
agent-chat-import/workflows/agent-chat-summary/run-summary.sh \
  --memories-grpc-url http://memories.example.com:9100 \
  --user-id 1 \
  --since-date 2026-05-20
```

`--since-date` だけ指定すると、`end_date = 今日 (tz基準)`、`range_start_ms` を `since_date 00:00 +tz` から自動算出、`since_ms_utc = 0` (day_start fallback) で起動する。**import 出力の値をそのまま使う場合は 4 つ全て指定する**:

```bash
agent-chat-import/workflows/agent-chat-summary/run-summary.sh \
  --memories-grpc-url http://memories.example.com:9100 \
  --user-id 1 \
  --since-date 2026-05-20 --end-date 2026-05-21 \
  --range-start-ms 1779170400000 --since-ms-utc 1779256800000
```

### reflection 有効化

```bash
agent-chat-import/workflows/agent-chat-summary/run-summary.sh \
  --memories-grpc-url ... --user-id 1 --since-date 2026-05-20 \
  --enable-reflection \
  --output-language ja \
  --reflector-base-url http://192.168.1.2:11434 \
  --reflector-model qwen3.6:27b
```

reflector_model / reflector_base_url を省略すると summary 側の設定 (`summary_model` / `ollama_base_url`) を流用する。
prompt は `memories-import upsert-generation-workers` で登録する言語別 worker の settings に焼き込まれる。
`run-summary.sh` は `output_language` に応じて batch から呼ぶ言語別 worker 名を選ぶだけで、`--context`
による prompt 注入は行わない。

### personality + reflection を同時に有効化

```bash
agent-chat-import/workflows/agent-chat-summary/run-summary.sh \
  --memories-grpc-url ... --user-id 1 --since-date 2026-05-20 \
  --enable-reflection \
  --enable-personality
```

batch YAML パスは `agent-chat-import/workflows/<feature>/` の既定で補われる。single / merge は batch が登録済み言語別 worker を `workerName` で呼ぶため、個別の YAML パスを渡す経路は無い (`memories-import upsert-generation-workers` で事前登録する)。要約と personality は同じ `user_id` を使い、`memory_kind` によって分離されるため並列実行で衝突しない。

### 直接 enqueue (jobworkerp-client)

```bash
jobworkerp-client -a http://localhost:9000 job enqueue-workflow \
  -i '{
    "user_id": 1,
    "memories_grpc_url": "http://memories.example.com:9100",
    "since_date": "2026-05-20",
    "end_date": "2026-05-21",
    "range_start_ms": 1779170400000,
    "since_ms_utc": 0,
    "thread_summary_batch_yaml":  "/abs/.../agent-chat-import/workflows/thread-summary/thread-summary-batch.yaml",
    "daily_work_summary_batch_yaml":  "/abs/.../agent-chat-import/workflows/daily-work-summary/daily-work-summary-batch.yaml",
    "thread_reflection_batch_yaml":  "/abs/.../agent-chat-import/workflows/thread-reflection/thread-reflection-batch.yaml",
    "summary_model": "qwen3.6:27b",
    "ollama_base_url": "http://192.168.1.2:11434",
    "output_language": "ja"
  }' \
  -w /abs/.../agent-chat-summary.yaml \
  --format json -t 86400
```

## 失敗時の挙動

| 失敗箇所 | jobworkerp 上の表示 | 復旧手順 |
|---|---|---|
| threadSummaryBatch が失敗 | ワークフロー失敗 | thread-summary-batch.yaml を再実行 (差分判定で skip される thread が多い) |
| dailyWorkSummary が失敗 | ワークフロー失敗 (batch 完走前の死亡時のみ。個別日の失敗は `output.failed_dates` で報告され完走扱い) | daily-work-summary-batch.yaml を再実行、または `daily-work-summary-single.yaml` を `target_date` で個別再実行 |
| reflectionStage が失敗 | **ワークフロー失敗 (致命)** | reflector model / base_url を見直して再実行。reflection だけ単独で再実行したい場合は `thread-reflection-batch.yaml` を直接 enqueue |
| threadPersonalityBatch / userPersonalityMerge が失敗 | **ワークフロー成功扱い** (`personality_error` にエラー文字列、`*_succeeded` flag が false) | personality 関連 YAML だけ別途再実行 |
| 個別スレッドの reflection / personality 失敗 | **検出できない** (batch 内 `onError: continue` で握り潰される) | jobworkerp の per-job ログまたは memories DB の件数で確認 |

各タスクは idempotent に設計されているため、本ワークフロー全体を再キューイングしても安全 (force_* flag を立てない限り差分判定で skip される)。

## 注意事項

- **import を先に走らせて完了している必要がある**。さもなくば thread-summary-batch は更新があった thread を 0 件と判定する
- **memories の各種 worker が registered であること**。reflection を有効化する場合は `memories-thread-reflection-single-ja/en` と embedding worker、`MEMORY_REFLECTION_DISPATCH_ENABLED=true` が必要
