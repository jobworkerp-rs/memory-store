# agent-chat-pipeline ワークフロー

> **v1.1.0 から thin wrapper になりました。** import / summary は別ワークフローに分割されています:
> - `agent-chat-import/workflows/agent-chat-import/agent-chat-import.yaml` ([README](../agent-chat-import/README_ja.md))
> - `agent-chat-import/workflows/agent-chat-summary/agent-chat-summary.yaml` ([README](../agent-chat-summary/README_ja.md))
>
> 本ワークフローはその 2 つを順次呼ぶ薄いラッパで、**既存の単発エンキューで全ステージを一括実行する利用形態を維持するため**に残されています。import / summary を別々に運用する場合は対応する sub-workflow を直接呼んでください。

エージェント (Claude Code / Codex / plain) のチャットログを取り込み → スレッド要約 → 日次作業要約まで一気通貫で実行する jobworkerp ワークフロー。任意で personality 抽出 (1層 + 2層) と thread reflection を組み込める。

```
[wrapper] agent-chat-pipeline.yaml
  ├ runImport     → agent-chat-import.yaml
  │                    importChats (COMMAND: memories-import)
  │                    output: since_date, end_date, range_start_ms,
  │                            since_ms_utc, import_succeeded
  │
  └ runSummary    → agent-chat-summary.yaml
                       fork:
                         summaryBranch (serial, 致命):
                           threadSummaryBatch
                             ↓
                           dailyWorkSummary
                             ↓
                           reflectionStage (opt-in)
                         personalityBranch (opt-in, 非致命, try/catch):
                           threadPersonalityBatch → userPersonalityMerge
```

`dailyWorkSummary` は `daily-work-summary-batch.yaml` を呼ぶ。`since_date` (= 処理範囲の起点) から **今日 (tz基準)** までを `[start_date, end_date]` で渡し、batch が日付ごとに `daily-work-summary-single` を順次起動する。`since_date` を指定しない場合は **既存の単一日挙動**を維持するため `start_date = end_date` に潰れる (`since_mode=now_minus` → 今日のみ、`since_mode=day_start` → 昨日のみ)。daily cron で **今日の in-progress daily summary が毎回再生成される副作用を起こさない**ためにこうしている — 今日も処理対象に含めたい場合は `since_date` を明示的に渡す。

summary 系と personality 系は所有者 `user_id` が完全に分離されているため安全に並列実行できる。personality 系は YAML パスを指定したときのみ有効化される。

### reflection と personality の失敗ポリシー (非対称)

| | reflection | personality |
|---|---|---|
| 失敗時のワークフロー状態 | failed (**致命**) | succeeded (warning) |
| 失敗検出 | jobworkerp UI の task error | `personality_error` 出力 |
| 設計理由 | reflection はパイプラインの最終段。reflector 設定ミスを silent に握り潰さない | personality は補足シグナル。失敗で summary 側結果を巻き戻さない |

`thread_personality_succeeded` / `user_personality_merge_succeeded` / `reflection_succeeded` のフラグは **batch / merge ワークフロー自体が完走した** ことしか示さない。`thread-personality-batch` / `thread-reflection-batch` 内部の per-thread fan-out は `onError: continue` で個別失敗を握り潰すため、**個別スレッドの失敗はこれらのフラグに反映されない**。確実に検出したい場合は jobworkerp の per-job ログを確認する。

## 1.0.x → 1.1.0 移行ガイド

**破壊的変更**: 入力スキーマに `agent_chat_import_yaml` と `agent_chat_summary_yaml` の 2 つが **必須項目**として追加された。直接 `jobworkerp-client` を叩く既存ペイロードはこれら 2 行を追加するまで schema validation で reject される。

```diff
 {
   "source": "claude-code",
   "user_id": 1,
   "memories_grpc_url": "http://memories.example.com:9100",
+  "agent_chat_import_yaml":  "/abs/.../agent-chat-import/workflows/agent-chat-import/agent-chat-import.yaml",
+  "agent_chat_summary_yaml": "/abs/.../agent-chat-import/workflows/agent-chat-summary/agent-chat-summary.yaml",
   "thread_summary_batch_yaml":  "...",
   ...
 }
```

`run-pipeline.sh` 経由のユーザは sibling-file 既定で補われるためスクリプト出力に変更はなく、追加引数も不要。

## 想定環境

- **jobworkerp worker は開発マシン上で起動** (チャットログのある `~/.claude` / `~/.codex` にアクセスできる必要があるため)
- **memories はリモートホストで稼働** (LAN 経由で gRPC アクセス)
- **memories-import バイナリは jobworkerp worker と同じホストに配置** (PATH または絶対パスで指定)

## 設計上のキー

| 項目 | 値 / 仕様 | 理由 |
|---|---|---|
| import 範囲 | `--since "<since_date>T00:00:00+<tz>"` のみ (`--until` なし) | memories-import に `--until` がないため広めに取る。daily-work-summary も `[since_date, end_date]` を range mode で処理するので、3 ステージ (import / thread-summary / daily-summary) が同じ窓で揃う (`since_date` 未指定時は `end_date = since_date` の 1 日に潰す) |
| import 失敗の検出 | `treat_nonzero_as_error: true` | memories-import は entry-level エラーがあれば exit 1 で終わる (main.rs L368-370)。COMMAND runner はこれを task 失敗として上位に伝える |
| `--summarize-after-*` 不使用 | 内蔵 dispatch は **ワークフロー失敗を握り潰す** (warn のみで exit 0) ため使わない。代わりにステージ2/3を独立 task として並べる | 各ステージの成否が jobworkerp UI から個別に観察可能 |
| stage timeout | 各 stage 固定値 (import 2h / thread-summary 24h / daily-summary 24h) | DSL `timeout.after.seconds` スキーマが `type: integer` で jq 補間を許さないため。daily-summary は範囲モードで N 日分を直列実行するため 24h に拡張 (個別日の上限は batch 内 `invokeSingle.timeout=30m` で抑制) |
| `since_date` / `end_date` 自動算出 | `since_date` 未指定時は `start = end` で 1 日に潰す (`day_start` → 昨日のみ、`now_minus` → 今日のみ)。`since_date` 指定時のみ `end_date = 今日 (tz基準)` として range mode になる | daily cron で未指定のまま回すと start=end=昨日 で 1 日のみ。`since_date=2026-04-01` 指定 → 4/1〜今日まで全日。今日の未確定 daily を毎回上書きする副作用を避けるため、未指定時は今日を端に含めない |

## 入力パラメータ

### 必須
| パラメータ | 説明 |
|---|---|
| `source` | `claude-code` / `codex` / `plain` |
| `user_id` | importer user id |
| `memories_grpc_url` | memories gRPC エンドポイント URL (例 `http://memories.example.com:9100`)。memories-import の `--server-url` にそのまま渡る。後段ワークフローの GRPC 接続先 (host/port) もこの URL を内部でパースして導出する (1つ指定すれば十分) |
| `thread_summary_batch_yaml` | `thread-summary-batch.yaml` の絶対パスまたは URL |
| `daily_work_summary_batch_yaml` | `daily-work-summary-batch.yaml` の絶対パスまたは URL。pipeline は常にこれを呼び、`since_date 〜 今日` を範囲モードで渡す |

### 主要な任意パラメータ
| パラメータ | 既定値 | 説明 |
|---|---|---|
| `since_date` | `since_mode` 依存 | `YYYY-MM-DD`。**処理範囲の起点**で、`[since_date, 今日 (tz基準)]` の全日が import / thread-summary / daily-summary の対象になる。省略時は range が 1 日に潰れて既存挙動と等価 (`since_mode="day_start"` で `[昨日, 昨日]`、`since_mode="now_minus"` で `[今日, 今日]`)。**今日を範囲端に含めたい場合は明示指定が必要** (省略時は今日の未確定 daily を毎回再生成しないよう端を切り落とす) |
| `end_date` | 自動算出 | `YYYY-MM-DD`。**処理範囲の終点 (inclusive)**。省略時は既存挙動 (`since_date` 指定時 → 今日、未指定時 → `since_mode` で昨日/今日に潰す) を維持。明示指定すると `[since_date, end_date]` の固定窓だけを処理対象にできる。例: `since_date=2026-04-01 end_date=2026-04-30` で 4 月分だけ back-fill (今日の in-progress daily を巻き込まない) |
| `timezone_offset_hours` | `9` | 日界算出用のフォールバック固定オフセット (`+0` 〜 `+23`)。**worker の `TZ` 環境変数が未設定のときのみ**使われる。DST/負オフセット非対応 |

> **タイムゾーン**: 日界は workflow の jq を評価する **jobworkerp worker プロセスの `TZ` 環境変数**で決まる (例 `TZ=Asia/Tokyo`)。`TZ` が設定されていれば `localtime`/`strflocaltime` がそれを反映し、夏時間 (DST) と負オフセット (例 `America/New_York`) に対応する。未設定なら `timezone_offset_hours` にフォールバック。`TZ` は worker のデプロイ環境 (その `.env` や `docker run -e TZ=...`) に設定する必要があり、この workflow の入力では渡せない。
| `since_override` | `""` | RFC 3339 文字列。memories-import `--since` にそのまま渡す。UTC `Z` 形式推奨 (後述) |
| `since_mode` | `"day_start"` | `"day_start"` (現行互換) または `"now_minus"` (短スパン用)。`since_override` 非空時は無視される |
| `since_lookback_seconds` | `0` | `since_mode="now_minus"` 時のみ有効。`now - this value` を `--since` にする。`0` は `day_start` フォールバック |
| `memories_grpc_host` | URL から自動抽出 | 後段ワークフローの GRPC 接続先ホスト override。memories-import (`--server-url`) と後段 WORKFLOW で異なるホスト名を使う必要があるとき (NAT越え等) のみ指定 |
| `memories_grpc_port` | URL から自動抽出 (省略時 80/443) | 同上のポート override |
| `import_command` | `memories-import` | バイナリへの絶対パスを推奨 (worker の PATH 解決に依存しない) |
| `all_projects_or_sessions` | `true` | claude-code → `--all-projects`、codex → `--all-sessions` を付与 |
| `claude_dir` / `codex_dir` | (未指定) | memories-import 既定 (`~/.claude` / `~/.codex`) を上書き |
| `strip_path_prefix` | (未指定) | memories-import の `-P` (CSV) |
| `extra_import_args` | `[]` | 追加引数。例: `["--prune-missing", "--source-name", "obsidian"]` |
| `summary_model` / `ollama_base_url` | `qwen3.6:27b` / `localhost:11434` | LLM 設定 |
| `memory_thread_label_prefix` | `summary` | thread-summary が付与するマーカーラベル |
| `daily_summary_label` | `daily_summary` | daily-work-summary が付与するマーカーラベル |
| `extra_labels_filter` | `[]` | daily-work-summary の scope 絞り込み (順序非依存) |
| `force_resummarize` | `false` | thread-summary と daily-summary 両方に伝達 |
| `min_thread_count` | `1` | daily-work-summary のスキップ閾値 |
| `max_context_chars` | `200000` | LLM コンテキスト上限 |

### personality 段 (任意)

`thread_personality_batch_yaml` を指定すると personality 1層 (スレッド単位の嗜好シグナル抽出) が有効化される。さらに `user_personality_merge_yaml` も指定すると 2層 (ユーザ統合プロファイル生成) が有効化される。

| パラメータ | 既定値 | 説明 |
|---|---|---|
| `thread_personality_batch_yaml` | `""` | `thread-personality-batch.yaml` の絶対パスまたは URL。空で personality 段全体を skip |
| `user_personality_merge_yaml` | `""` | **2層 merge の有効化フラグ**。非空のとき 2層 merge を実行する (batch は登録済み merge worker を呼ぶ。値は有効化判定にのみ使われる) |
| `personality_model` | `""` (= `summary_model` 流用) | personality 抽出/統合用 LLM モデル。空なら `summary_model` をそのまま使う |
| `min_user_messages` | `2` | thread-personality-single の ROLE_USER 件数下限 |
| `force_reextract` | `false` | thread-personality-batch に伝達 |
| `force_remerge` | `false` | user-personality-merge に伝達 |
| `max_signals` | `200` | user-personality-merge の入力シグナル上限 |

## 使い方

### 基本実行 (claude-code, ローカル jobworkerp + リモート memories)

```bash
jobworkerp-client -a http://localhost:9000 job enqueue-workflow \
  -i "$(cat <<'EOF'
{
  "source": "claude-code",
  "user_id": 1,
  "memories_grpc_url": "http://memories.example.com:9100",
  "thread_summary_batch_yaml":  "/abs/path/agent-chat-import/workflows/thread-summary/thread-summary-batch.yaml",
  "daily_work_summary_batch_yaml":  "/abs/path/agent-chat-import/workflows/daily-work-summary/daily-work-summary-batch.yaml",
  "import_command": "/abs/path/target/release/memories-import",
  "strip_path_prefix": "/home/me/works,/home/me/projects",
  "ollama_base_url": "http://192.168.1.2:11434",
  "summary_model": "qwen3.6:27b"
}
EOF
)" \
  -w /abs/path/agent-chat-import/workflows/agent-chat-pipeline/agent-chat-pipeline.yaml \
  --format json -t 93600
```

### 過去全期間を一括 import + 全日 daily summary を再生成 (初期 import / back-fill)

```bash
... -i '{ ..., "since_date": "2024-01-01", "force_resummarize": true, ... }'
```

`since_date=2024-01-01` を指定すると import は `--since 2024-01-01T00:00:00+09:00` で過去全 session を取り込み、thread-summary は 2024-01-01 以降に更新された全 thread を要約、daily-work-summary は 2024-01-01 〜 今日まで **1 日 1 件ずつ** daily summary を生成する。途中の 1 日が失敗しても batch の `onError: continue` で他の日は処理が続き、`output.failed_dates` で再実行対象が分かる。

### 特定 1 日だけを再生成

pipeline 経由で `since_date` を指定すると `[since_date, 今日]` の全日が対象になるため、ピンポイントで 1 日だけ再生成したい場合は `daily-work-summary-single.yaml` を直接 `target_date=YYYY-MM-DD` で enqueue する方が綺麗 (`since_date` 未指定の挙動は昨日 or 当日のみで、任意の過去 1 日をピンポイントに指定する手段が pipeline 側にはないため)。

### スコープを分けた要約 (例: claude-code 経由のコーディング作業のみ)

```bash
... -i '{ ..., "extra_labels_filter": ["agent:claude_code"], ... }'
```

### personality 段を有効化した実行

```bash
... -i '{
  ...,
  "thread_personality_batch_yaml":  "/abs/path/agent-chat-import/workflows/personality/thread-personality-batch.yaml",
  "user_personality_merge_yaml":    "/abs/path/agent-chat-import/workers/personality/user-personality-merge.yaml"
}'
```

summary 段と personality 段は disjoint な所有者 user_id で並列実行されるため、所要時間は (summary 系 + personality 系) ではなく `max(summary 系, personality 系)` になる (Ollama の同時 2 セッションを許容できる前提)。personality 段の **ブランチ全体** が失敗してもパイプライン全体は failure 扱いにならず、`personality_error` 出力のみで通知される (個別スレッドの失敗は出力フラグに反映されない点は上の章を参照)。

### reflection 段を有効化した実行

```bash
run-pipeline.sh --memories-grpc-url ... \
    --enable-reflection \
    --output-language ja \
    --reflector-base-url http://192.168.1.2:11434 \
    --reflector-model qwen3.6:27b
```

または直接 enqueue:

```bash
... -i '{
  ...,
  "thread_reflection_batch_yaml":  "/abs/path/agent-chat-import/workflows/thread-reflection/thread-reflection-batch.yaml",
  "reflector_model": "qwen3.6:27b",
  "reflector_base_url": "http://192.168.1.2:11434",
  "prompt_version": "v1",
  "output_language": "ja"
}'
```

reflection 段は summary branch の **最終段** (`threadSummaryBatch → dailyWorkSummary → reflectionStage`) として直列実行される。**失敗は致命** で、reflection が落ちるとパイプライン全体が fail する (= jobworkerp UI で失敗状態が即時可視化される)。`reflector_model` / `reflector_base_url` を省略すると summary 側の設定 (`summary_model` / `ollama_base_url`) を流用する。

prompt は `memories-import upsert-generation-workers` で登録する言語別 worker の settings に焼き込まれる。
`run-pipeline.sh` は `output_language` に応じて batch から呼ぶ言語別 worker 名を選ぶだけで、`--context`
による prompt 注入は行わない。`jobworkerp-client job enqueue-workflow` を直接使う場合も同じで、
事前に言語別 worker を登録しておく。

reflection のチューニング knob (`context_limit_tokens`, `window_size_turns` 等) は本ワークフローではデフォルト値のみ使う。きめ細かい調整が必要な場合は `thread-reflection-batch.yaml` を直接 enqueue する。

ラベル / スレッド ID で reflection 範囲を絞りたい場合も `thread-reflection-batch.yaml` を直接呼ぶ (本ラッパは summary と同じ `updated_after_ms` 窓を使うため、label_filter / thread_ids を expose していない)。

### 短スパン (1時間おき) 運用

`since_mode="now_minus"` + `since_lookback_seconds=3900` で、`--since = now - 65 分` を毎回算出する。memories-import 側の **session-level mtime filter** (`--since` 指定で自動有効) が組み合わさり、未変更ファイルは parse skip されるため I/O コストはほぼゼロ。thread-summary / daily-work-summary も差分判定で活動のないものは LLM 呼び出しを skip するので、変化のない時間帯では全レイヤでコスト 0 になる。

`since_date` を省略すると `since_mode="now_minus"` 経路では **当日** が起点・終点ともに選ばれ range は当日 1 日のみ (legacy `day_start` モードは引き続き昨日 1 日)。これにより hourly 実行で当日の daily summary が随時更新される。日界をまたぐと起点が新しい日に切り替わるため、前日 23 時台のチャットは「翌日 0 時を超える前の最後の hourly run」で前日 daily に取り込まれる必要がある — `since_lookback_seconds` を cron 間隔より長め (例: 1 時間 cron で 65 分) に取り、最後の run 後に書かれたメモリが次回 run の `--since` 範囲に確実に入るようにする。

```bash
# jobworkerp の periodic worker で 1 時間おきに enqueue する例
jobworkerp-client -a http://localhost:9000 job enqueue-workflow \
  -i '{
    "source": "claude-code",
    "user_id": 1,
    "memories_grpc_url": "http://memories.example.com:9100",
    "thread_summary_batch_yaml":  "/abs/.../thread-summary-batch.yaml",
    "daily_work_summary_batch_yaml": "/abs/.../daily-work-summary-batch.yaml",
    "import_command": "/abs/path/target/release/memories-import",

    "since_mode": "now_minus",
    "since_lookback_seconds": 3900
  }' \
  -w /abs/.../agent-chat-pipeline.yaml --format json -t 93600
```

ポイント:

- `since_lookback_seconds` は **periodic 間隔より長め** に取る (1 時間 cron なら 3900 秒 = 65 分)。NTP / cron jitter / 書き込み中ファイルのマージン (memories-import 側 60 秒既定) をすべて吸収できる
- `range_start_ms` は `since_iso` と独立に **処理範囲の起点 (`since_date` 00:00 +tz)** に固定されるので、`since_override` / `now_minus` で `--since` を前に進めても thread-summary-batch の `updated_after_ms` は range 全体をカバーし続ける (取りこぼし防止)。**これは「単一日の開始」ではなく「範囲の起点」**。range mode (`since_date` 指定時) では `range_start_ms` から `end_date` 24:00 in tz までが処理範囲
- `since_override` を使う場合は **UTC 整数秒 ISO 8601 (`Z` 形式、`2026-05-08T08:00:00Z`)** のみ受理。`+HH:MM` オフセット付き / `.123Z` 小数秒付きは workflow 入力 schema (`pattern`) と runtime の両方で reject されるので、誤った形式が silent に range_start_ms へフォールバックすることはない
→ daily-work-summary は `scope=agent:claude_code` の独立した集約スレッド/メモリを生成。空配列で起動した「全 project 横断」版とは衝突しない。

## 失敗時の挙動

| 失敗箇所 | jobworkerp 上の表示 | 復旧手順 |
|---|---|---|
| importChats が exit 1 | パイプライン全体が失敗。後段は走らない | memories-import のログを確認し、エラーを修正後に再実行 (idempotent) |
| threadSummaryBatch が失敗 | パイプラインは失敗だが import は成功している | thread-summary-batch.yaml を直接再実行するか、本ワークフローを再実行 (差分判定でスキップされる thread が多いため安価) |
| dailyWorkSummary が失敗 | パイプラインは失敗だが import + thread-summary は成功。batch が完走しなかった (= 全 N 日 try する前に外形的に死んだ) 場合のみここに該当。個別日の失敗は `output.failed_dates` で報告され完走扱いになる | `daily-work-summary-batch.yaml` を同じ `start_date` / `end_date` で再実行するか、特定日のみ `daily-work-summary-single.yaml` を `target_date=...` で再実行 |
| reflectionStage が失敗 | **パイプライン全体が失敗 (致命)**。import / thread-summary / daily-summary は成功している。reflection は summary branch の最終段なので、ここまでは memories に正しく書き込まれている | reflector_model / reflector_base_url を見直して再実行。reflection 段のみ単独で再走したい場合は `thread-reflection-batch.yaml` を直接 enqueue |
| threadPersonalityBatch / userPersonalityMerge ワークフロー自体が失敗 | **パイプラインは成功扱い**。`personality_error` 出力にエラー文字列、`thread_personality_succeeded` / `user_personality_merge_succeeded` が `false` | personality 段のみ別途 `thread-personality-batch.yaml` / `user-personality-merge.yaml` で再実行。summary 系には影響しない |
| thread-personality-batch / thread-reflection-batch の **個別スレッド** が失敗 | **検出できない** (batch ワークフロー自体は `onError: continue` で完走) | jobworkerp の per-job ログまたは memories DB の personality / reflection memory 件数で確認 |

各 task は idempotent に設計されているため、本ワークフロー自体を再キューイングしても安全。

## 注意事項

- **memories-import バイナリは事前にビルド済みであること** (`cargo build --release -p agent-chat-import`)。`import_command` を絶対パスで指定推奨
- **`memories_grpc_url` だけ指定すれば host/port は自動抽出される**。memories-import の `--server-url` (URL 形式必須) と後段 WORKFLOW runner の GRPC 呼び出し (host/port 分離形式) を1つの URL でまかなう。`memories_grpc_host`/`_port` は両者が異なる必要がある特殊環境用の override
- **フォールバックの `timezone_offset_hours` は負オフセット非対応** (`buildSinceWindow` が `+HH:00` を素直に組み立てるため)。負オフセットや夏時間が必要なら worker の `TZ` 環境変数を設定する (例 `TZ=America/New_York`)。`TZ` 設定時は `strflocaltime("%:z")` がその日の実オフセット (`-04:00` 等) を算出するため制約は解消される
- **range が長い (= 過去全期間 back-fill) と LLM 呼び出し回数が線形に増える**。daily-work-summary-batch は per-day で直列に `daily-work-summary-single` を起動するので、`since_date=2024-01-01` のような指定は数百回の LLM 呼び出しを発生させうる。force_resummarize 無し時は差分判定で活動のない日はスキップされるため、初期 import の 2 周目以降は安価
- **memories-import は `external_id` でデデュプリケートされる**ため、`since_date` を広めに取って同じ範囲を複数回流しても二重登録にはならない
