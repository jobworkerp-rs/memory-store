# thread-reflection embedding ワークフロー

thread reflection 生成後の summary / intent embedding を投入する据え置きワークフロー群です。reflection 生成用の single / batch / prompt は `../../agent-chat-import/workflows/thread-reflection/README.md` を参照してください。

## ファイル構成

| ファイル | 役割 |
|---|---|
| `auto-reflection-summary-embedding.yaml` | reflection summary を既存 `memory_vector` テーブルへ upsert し、`MarkReflectionEmbeddingStatus(SUMMARY)` を更新する |
| `auto-reflection-summary-embedding-workers.yaml` | `ReflectionSummaryDispatcher` が起動時に自動登録する worker 定義 |
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

## 自動登録

memories 起動時に `ReflectionSummaryDispatcher` と `ReflectionIntentDispatcher` が次の worker を登録します。

- `memories-auto-reflection-summary-embedding`
- `memories-auto-reflection-intent-embedding`
- `memories-mark-reflection-embedding-status`

kill switch が false の間は dispatcher と自動登録はスキップされます。YAML は memories プロセスがローカルで読み込み、`$file:` を展開してから jobworkerp に upsert します。jobworkerp プロセスからこの YAML パスが見える必要はありません。

## モデル切替時の注意

`memories-mm-embedding` の model_id を変更した場合、過去に蓄積した intent vector との距離は無効になります。切替後は速やかに intent index を再構築します。

```bash
grpcurl -plaintext -d '{}' localhost:9010 \
  llm_memory.service.ReflectionVectorService/RebuildIntentIndex
```

加えて、既存 reflection の embedding を再投入します。

```bash
grpcurl -plaintext -d '{
  "kind": "EMBEDDING_KIND_BOTH"
}' localhost:9010 \
  llm_memory.service.ReflectionVectorService/RedispatchReflectionEmbeddings
```

## 観測と復旧

- embedding workflow の失敗は `*_embedding_status=FAILED` と `*_embedding_error=<reason>` に反映されます。
- 一時障害からの復旧は `RedispatchReflectionEmbeddings` で再投入します。
- `MatchFailureSignatures` の cap 到達時は `is_truncated` / `scanned_count` を見て filter を絞るか `REFLECTION_FS_MATCH_SCAN_CAP` を引き上げます。

## 関連ドキュメント

- generation workflow: `../../agent-chat-import/workflows/thread-reflection/README.md`
- API guide: `../../docs/thread-reflection-guide.md`
- embedding pipeline: `../auto-embedding.yaml` / `../auto-embedding-workers.yaml`
