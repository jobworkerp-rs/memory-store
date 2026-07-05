# agent-chat-import ワークフロー

エージェント (Claude Code / Codex / plain) のチャットログを memories に取り込むだけの、import 専用ワークフロー。`agent-chat-pipeline.yaml` を import / summary に 2 分割した片割れで、もう一方が `../agent-chat-summary/agent-chat-summary.yaml`。

```
[1] importChats (COMMAND: memories-import)
        ↓ exit 0 で完了 (treat_nonzero_as_error: true で失敗を伝搬)
output: { since_date, end_date, since_iso, range_start_ms, since_ms_utc, import_succeeded }
```

## いつ使うか

- **複数ホスト・複数プロセスから並列に import を流したい**運用 (host-A は claude-code、host-B は codex を別 cron で回す、など)
- import と summary を異なる頻度で回したい (例: import は 10 分おき、summary は時間ごと)
- `agent-chat-pipeline.yaml` 経由でまとめて回す場合は本ワークフローを直接 enqueue する必要はない (wrapper が内部で呼ぶ)

## 想定環境

- **jobworkerp worker は import ホスト側で起動** (`~/.claude` / `~/.codex` にアクセスできる必要がある)
- **memories はリモートホストで稼働** (LAN 経由で gRPC アクセス)
- **memories-import バイナリは jobworkerp worker と同じホストに配置**

## 設計上のキー

| 項目 | 値 / 仕様 | 理由 |
|---|---|---|
| import 範囲 | `--since "<since_date>T00:00:00+<tz>"` のみ (`--until` なし) | memories-import 側に `--until` が無いため広めに取る。summary 側が `[since_date, end_date]` を範囲モードで処理する |
| import 失敗の検出 | `treat_nonzero_as_error: true` | memories-import は entry-level エラーがあれば exit 1 で終わる。COMMAND runner はこれを task 失敗として上位に伝える |
| 冪等性 | memories-import が `external_id` でデデュプリケート | 同じ範囲を複数回流しても二重登録にはならない |
| output | since_date / end_date / range_start_ms / since_ms_utc | summary 側がこの 4 値を消費して同じ処理窓を再現する |

## 入力パラメータ

### 必須
| パラメータ | 説明 |
|---|---|
| `source` | `claude-code` / `codex` / `plain` |
| `user_id` | importer user id |
| `memories_grpc_url` | memories gRPC エンドポイント URL (例 `http://memories.example.com:9100`)。memories-import の `--server-url` にそのまま渡る |

### 主要な任意パラメータ
| パラメータ | 既定値 | 説明 |
|---|---|---|
| `since_date` | `since_mode` 依存 | `YYYY-MM-DD`。**処理範囲の起点**。省略時は range が 1 日に潰れる (`day_start` → `[昨日, 昨日]`、`now_minus` → `[今日, 今日]`) |
| `end_date` | 自動算出 | `YYYY-MM-DD`。**処理範囲の終点 (inclusive)**。省略時は `since_date` 設定時 → 今日 (tz基準)、未設定時は `since_mode` で `[昨日, 昨日]` か `[今日, 今日]` に潰す既存挙動を維持。明示指定すると履歴 back-fill (`since_date=2026-04-01`, `end_date=2026-04-30`) ができ、今日の in-progress summary を巻き込まない |
| `timezone_offset_hours` | `9` | 日界算出用のフォールバック固定オフセット (`+0` 〜 `+23`)。**worker の `TZ` 環境変数が未設定のときのみ**使われる。DST/負オフセットは worker の `TZ` (例 `TZ=Asia/Tokyo`) で対応 |
| `since_override` | `""` | UTC 整数秒 ISO 8601 (`2026-05-08T08:00:00Z`)。memories-import `--since` にそのまま渡す。`+HH:MM` オフセットや小数秒は受理しない |
| `since_mode` | `"day_start"` | `"day_start"` (互換) または `"now_minus"` (短スパン用)。`since_override` 非空時は無視される |
| `since_lookback_seconds` | `0` | `since_mode="now_minus"` 時のみ有効。`now - this value` を `--since` にする |
| `import_command` | `memories-import` | バイナリへの絶対パスを推奨 |
| `all_projects_or_sessions` | `true` | claude-code → `--all-projects`、codex → `--all-sessions` を付与 |
| `claude_dir` / `codex_dir` | (未指定) | memories-import 既定 (`~/.claude` / `~/.codex`) を上書き |
| `strip_path_prefix` | (未指定) | memories-import の `-P` (CSV) |
| `extra_import_args` | `[]` | 追加引数 (例: `["--prune-missing"]`) |
| `memories_grpc_host` | `""` | summary 側との対称性のために受け取るが、import 自体では使わない |
| `memories_grpc_port` | `0` | 同上 |

## 出力

| キー | 説明 |
|---|---|
| `completed` | `import_succeeded` と同義 |
| `since_date` | `YYYY-MM-DD`。処理範囲の起点 (入力時の `since_date` を `since_mode` で fallback 解決した値) |
| `end_date` | `YYYY-MM-DD`。処理範囲の終点 (`since_date` が空のときは `since_date` と同じ日に潰れる) |
| `since_iso` | memories-import に渡した `--since` 文字列そのもの |
| `range_start_ms` | `since_date 00:00 +tz` を epoch ms にしたもの。summary 側の `range_start_ms` 入力にそのまま使う |
| `since_ms_utc` | UTC `--since` cutoff を epoch ms にしたもの (day_start mode では 0)。summary 側の `since_ms_utc` 入力にそのまま使う |
| `import_succeeded` | bool |

## 使い方

### `run-import.sh` (推奨)

```bash
agent-chat-import/workflows/agent-chat-import/run-import.sh \
  --memories-grpc-url http://memories.example.com:9100 \
  --import-command /abs/path/target/release/memories-import \
  --strip-path-prefix /home/me/works,/home/me/projects
```

`--print-only` をつけると enqueue せずに JSON ペイロードと jobworkerp-client コマンドを表示する。

### 直接 enqueue

```bash
jobworkerp-client -a http://localhost:9000 job enqueue-workflow \
  -i '{
    "source": "claude-code",
    "user_id": 1,
    "memories_grpc_url": "http://memories.example.com:9100",
    "import_command": "/abs/path/target/release/memories-import",
    "strip_path_prefix": "/home/me/works,/home/me/projects"
  }' \
  -w /abs/path/agent-chat-import/workflows/agent-chat-import/agent-chat-import.yaml \
  --format json -t 7200
```

### 短スパン (1 時間おき) 運用

```bash
# crontab
0 * * * * /abs/path/agent-chat-import/workflows/agent-chat-import/run-import.sh \
    --memories-grpc-url http://memories.example.com:9100 \
    --import-command /abs/path/target/release/memories-import \
    --since-mode now_minus --since-lookback-seconds 3900 \
    --jobworkerp-addr http://localhost:9000
```

`--since-lookback-seconds` は periodic 間隔より長め (1 時間 cron なら 3900 秒 = 65 分) を推奨。NTP / cron jitter / 書き込み中ファイルの margin (memories-import 側 60 秒既定) を吸収できる。

### 過去全期間を一括 import (back-fill)

```bash
agent-chat-import/workflows/agent-chat-import/run-import.sh \
  --memories-grpc-url http://memories.example.com:9100 \
  --since-date 2024-01-01
```

`--since-date 2024-01-01` を指定すると `--since 2024-01-01T00:00:00+09:00` で過去全 session を取り込む。`external_id` ベースの冪等性があるので、何度流しても二重登録にはならない。

### 固定の歴史窓だけを back-fill (`end_date` 指定)

```bash
agent-chat-import/workflows/agent-chat-import/run-import.sh \
  --memories-grpc-url http://memories.example.com:9100 \
  --since-date 2026-04-01 \
  --end-date 2026-04-30
```

`--end-date` を併用すると `[since_date, end_date]` の固定窓だけを処理対象にできる。`--end-date` 省略時の「today を取り込む」既定挙動をスキップしたいケース (4 月分だけ後追いで集計したい等) で使う。

## 失敗時の挙動

| 失敗箇所 | jobworkerp 上の表示 | 復旧手順 |
|---|---|---|
| importChats が exit 1 | ワークフロー全体が失敗 | memories-import のログ確認 → エラー修正後に再実行 (idempotent) |

タスク自体は idempotent に設計されているため、再キューイング自体は常に安全。

## 注意事項

- **memories-import バイナリは事前にビルド済みであること** (`cargo build --release -p agent-chat-import`)
- **フォールバックの `timezone_offset_hours` は負オフセット・夏時間非対応** (`+0` 〜 `+23`)。負オフセットや DST が必要なら jobworkerp worker の `TZ` 環境変数を設定する (例 `TZ=America/New_York`)
- **本ワークフロー単体は summary を起動しない**。summary 側を別途 enqueue するか、`agent-chat-pipeline.yaml` の wrapper を使う
