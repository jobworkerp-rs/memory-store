# スレッドグループ要約ワークフロー

English: [README.md](README.md)

他の生成系と同じく batch / single の組で構成する。`thread-group-summary-batch.yaml` が
対象グループの選択とメンバー要約の更新を行い、言語別 worker
`memories-thread-group-summary-single-<lang>`
（`workers/thread-group-summary/thread-group-summary-single.yaml`）に fan-out する。single は、有効な ThreadGroup ごとにメンバースレッドの
永続化済みスレッド要約を集約し、1 件のグループ要約メモリ
（`MEMORY_KIND_DERIVED_SUMMARY`、`external_id = thread-group-summary:<group_id>`）として保存する。
保存したメモリは purpose `summary` でグループに関連付ける。

## 処理の流れ

batch は次を行う。

1. 全対象グループの lineage を取得し、有効なメンバースレッドを所有者ごとにまとめる。
2. メンバースレッドごとに `memories-thread-summary-single-<lang>` を 1 件ずつ呼び、各件の前に
   `progress_processed` / `progress_total` を出す。複数グループに属するスレッドも 1 回だけ更新される。
   single はメッセージを読む前に最新判定してスキップするので、最新のメンバーは短時間で終わり、
   古いメンバー要約のまま集約することはない。更新に失敗したメンバーを含むグループは集約せず、
   `thread-refresh-failed`（`reason_code: incomplete_input`）として報告する。
3. グループごとに、スレッド要約 single がメッセージ不足でスキップしたメンバーを渡して single worker を呼ぶ。

single worker は 1 グループだけを処理する。

1. lineage を再取得し（`expected_membership_snapshot_digest` が指定され、現在値と異なれば中止）、
   各メンバーの要約を external ID `summary:<thread_id>` で取得する。
2. `min_message_count` 未満でスキップされ要約も持たないメンバーは集約入力から除外する。
   それ以外の理由で要約が欠けている場合は `outcome: incomplete_input` を返し、batch がそのグループを
   `thread-summary-missing`（`reason_code: incomplete_input`）として失敗させる。
   入力が残らないグループはスキップする。
3. メンバー要約が 1 件ならそのまま使い（passthrough）、複数なら LLM で統合する。
   LLM 出力が JSON として解釈できない、または title / summary が空の場合は最大 2 回
   再生成し、それでも満たさなければ保存しない。
4. 保存済みの `summary_fingerprint`（メンバー要約 ID と本文、membership snapshot digest、
   言語、集約方式）が変わっていなければ書き込まずスキップする。
5. 書き込み直前に lineage を再取得し、その間に membership snapshot が変わっていれば
   `publication_state: stale` として保存し、`stale_groups` に入れる。

## 前提条件

- memories サーバが所有者ごとのグループ分離に対応していること
  （`ThreadGroupService/GetThreadGroupCapabilities` が
  `supports_independent_group_owner_scope = true` を返す）。未対応なら即座に失敗する。
- 言語別の `memories-thread-summary-single-<lang>` と `memories-thread-group-summary-single-<lang>`
  worker が登録済みであること（`memories-import upsert-generation-workers --feature thread-summary` と
  `--feature thread-group-summary`、または `--feature all`）。prompt は
  `workers/thread-group-summary/prompts/` にあり、登録時に worker へ焼き込まれる。
- 本 batch は、呼び出すスレッド要約・グループ要約の single worker の登録 channel とは別の channel で実行する
  （[channel 構成](../../README_ja.md#channel-構成) 参照）。

## 実行例

```bash
jobworkerp-client job enqueue-workflow \
  -i '{
    "group_owner_user_id": 1,
    "memories_grpc_host": "localhost",
    "memories_grpc_port": 9010,
    "ollama_base_url": "http://localhost:11434",
    "summary_model": "qwen3.6:27b",
    "output_language": "ja",
    "activity_after_ms": 1767225600000,
    "activity_before_ms": 1767312000000
  }' \
  -w /abs/path/to/thread-group-summary-batch.yaml
```

## 入力パラメータ

| パラメータ | 必須 | デフォルト | 説明 |
|---|---|---|---|
| `group_owner_user_id` | ○ | - | グループの選択とグループ要約の所有に使うユーザ ID |
| `memories_grpc_host` / `memories_grpc_port` / `memories_grpc_tls` | ○ | `localhost` / `9100` / `false` | memories の gRPC 接続先 |
| `target_group_ids` | - | 有効な全グループ | 指定したグループのみ要約 |
| `target_group_id` | - | - | このグループだけを要約。存在しないか無効なら失敗 |
| `expected_membership_snapshot_digest` | - | - | 現在の snapshot digest と異なればそのグループを中止 |
| `activity_after_ms` / `activity_before_ms` | - | - | 排他的な epoch ms の期間。両方指定（かつ `target_group_id` 未指定）のとき、会話がこの期間に重なるメンバーを持つグループだけを要約する。要約自体は全メンバーから集約する |
| `output_language` | - | `ja` | `ja` / `en` |
| `summary_model` / `ollama_base_url` | - | `qwen3.6:27b` / `http://localhost:11434` | 統合に使う LLM。スレッド単位 batch にも渡す |
| `min_message_count` | - | `4` | スレッド単位 batch に渡す |
| `max_context_chars` | - | `200000` | スレッド単位 batch に渡す |
| `force_resummarize` | - | `false` | 最新でもメンバー要約・グループ要約を再生成する |
| `memory_thread_label_prefix` | - | `"summary"` | per-thread batch に渡す要約ラベル。thread-summary と同じ値にする |

1 グループだけを要約するときは、batch に `target_group_id`（必要なら
`expected_membership_snapshot_digest` も）を渡す。メンバー要約の更新を経由するためである。
single worker を直接呼ぶと、保存済みのメンバー要約をそのまま集約する。

## 出力

`completed` は失敗グループがなく、かつ全対象グループに結果があるときだけ true。
ほかに `processed_groups`、`expected_group_count`、`generated_count`、`skipped_count`、
`stale_count`、`failed_count`、グループ ID 配列 `generated_groups` / `skipped_groups` /
`stale_groups` / `incomplete_groups`、`failed_groups`（`group_id`、
`expected_membership_snapshot_digest`、`reason_code`、`detail` を持つオブジェクト。
呼び出し側が同じグループを再試行できる）、`last_error` を返す。
結果を返さずに終わった（timeout・中断）グループは `reason_code: workflow_failed` の失敗として数える。
