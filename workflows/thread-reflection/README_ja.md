# thread-reflection embedding ワークフロー

thread reflection の intent embedding を投入する据え置きワークフロー群です。reflection search document は generic memory auto-embedding (`../auto-embedding.yaml`) で投入します。reflection 生成用の single / batch / prompt は `../../agent-chat-import/workflows/thread-reflection/README_ja.md` を参照してください。

## ファイル構成

| ファイル | 役割 |
|---|---|
| `auto-reflection-summary-embedding.yaml` | 旧 summary status 更新用の legacy compatibility workflow |
| `auto-reflection-summary-embedding-workers.yaml` | `ReflectionSummaryDispatcher` 用の legacy worker 定義 |
| `auto-reflection-intent-embedding.yaml` | reflection intent を `reflection_intent_vector` テーブルへ upsert し、`MarkReflectionEmbeddingStatus(INTENT)` を更新する |
| `auto-reflection-intent-embedding-workers.yaml` | `ReflectionIntentDispatcher` が起動時に自動登録する worker 定義 |
| `log/` | jobworkerp-client 実行ログ |

## 前提条件

- memories service が起動していること。
- `MEMORY_REFLECTION_DISPATCH_ENABLED=true` を設定していること。
- `JOBWORKERP_ADDR` を設定していること。
- `MEMORY_GRPC_HOST` / `MEMORY_GRPC_PORT` が jobworkerp 側で解決できること。
- `auto-embedding-workers.yaml` が事前に登録済みで、`memories-mm-embedding` / `memories-upsert-embedding` を使えること。
- `REFLECTION_WORKERS_YAML` / `REFLECTION_INTENT_WORKERS_YAML` を変更する場合は、このディレクトリの auto-reflection worker YAML を指すこと。
- workflow 内で GRPC runner worker を呼ぶ場合は `using: unary` を明示すること。未指定だと jobworkerp が `run` method として解決し、GRPC runner の `unary` / `streaming` method map に一致せず失敗します。

## 自動登録

memories 起動時に `ReflectionIntentDispatcher` が次の worker を登録します。

- `memories-auto-reflection-intent-embedding`
- `memories-mark-reflection-embedding-status`

reflection search document は generic memory embedding dispatcher の `memories-auto-embedding` で投入します。kill switch が false の間は reflection intent dispatcher と自動登録はスキップされます。YAML は memories プロセスがローカルで読み込み、`$file:` を展開してから jobworkerp に upsert します。jobworkerp プロセスからこの YAML パスが見える必要はありません。

## モデル切替時の注意

`memories-mm-embedding` の model_id を変更した場合、過去に蓄積した intent vector との距離は無効になります。切替後は速やかに intent index を再構築します。

```bash
grpcurl -plaintext -d '{}' localhost:9010 \
  llm_memory.service.ReflectionVectorService/RebuildIntentIndex
```

加えて、既存 reflection intent の embedding を再投入します。

```bash
grpcurl -plaintext -d '{
  "kind": "EMBEDDING_KIND_INTENT"
}' localhost:9010 \
  llm_memory.service.ReflectionVectorService/RedispatchReflectionEmbeddings
```

reflection search document は次で再投入します。

```bash
grpcurl -plaintext -d '{
  "user_id": 300000,
  "kinds": ["DISPATCH_KIND_TEXT"]
}' localhost:9010 \
  llm_memory.service.MemoryVectorService/RedispatchEmbeddings
```

## 観測と復旧

- intent embedding workflow の失敗は `intent_embedding_status=FAILED` と `intent_embedding_error=<reason>` に反映されます。
- search document の復旧は `MemoryVectorService.RedispatchEmbeddings(user_id=300000, kinds=[TEXT])`、intent の復旧は `RedispatchReflectionEmbeddings(kind=INTENT)` で再投入します。
- `MatchFailureSignatures` の cap 到達時は `is_truncated` / `scanned_count` を見て filter を絞るか `REFLECTION_FS_MATCH_SCAN_CAP` を引き上げます。

## 関連ドキュメント

- generation workflow: `../../agent-chat-import/workflows/thread-reflection/README_ja.md`
- API guide: `../../docs/thread-reflection-guide_ja.md`
- embedding pipeline: `../auto-embedding.yaml` / `../auto-embedding-workers.yaml`
