# thread-reflection generation ワークフロー

memories のチャットスレッドから LLM reflection を生成する generation 系ワークフローです。embedding 再投入と dispatcher 自動登録は `../../../workflows/thread-reflection/README_ja.md` が担当します。

## ファイル構成

| ファイル | 役割 |
|---|---|
| `thread-reflection-batch.yaml` | 複数スレッドへの一括 reflection。`output_language` に応じて言語別 worker を `workerName` で呼ぶ |
| `../../workers/thread-reflection/thread-reflection-single.yaml` | 単一スレッドの reflection 生成 |
| `../../workers/thread-reflection/prompts/` | 言語別 prompt。`upsert-generation-workers` が worker settings へ焼き込む |

## 前提条件

- jobworkerp server が起動していること。
- memories service が起動していること。
- `MEMORY_GRPC_HOST` / `MEMORY_GRPC_PORT` が jobworkerp 側で解決できること。
- `MEMORY_REFLECTION_REFLECTOR_MODEL` / `MEMORY_REFLECTION_REFLECTOR_BASE_URL` を設定していること。
- `memories-import upsert-generation-workers` で言語別 worker を登録していること。

## 初回セットアップ

```bash
memories-import upsert-generation-workers \
  --feature reflection \
  --language all \
  --channel workflow_lang \
  --repo-root /abs/path/to/memories/agent-chat-import
```

prompt を変更した場合は、`upsert-generation-workers` を再実行して worker settings を更新します。

## バッチ実行

```bash
jobworkerp-client job enqueue-workflow \
  -i '{
    "user_id": 1,
    "memories_grpc_host": "localhost",
    "memories_grpc_port": 9010,
    "reflector_model": "qwen3.6:27b",
    "reflector_base_url": "http://192.168.1.2:11434",
    "prompt_version": "20260511-baseline",
    "output_language": "ja",
    "updated_within_hours": 24
  }' \
  -w /abs/path/to/memories/agent-chat-import/workflows/thread-reflection/thread-reflection-batch.yaml \
  --format json \
  -t 86400
```

`thread-reflection-batch.yaml` は `memories-thread-reflection-single-ja` / `memories-thread-reflection-single-en` を呼び分けます。prompt context を `--context` で渡す必要はありません。

## 関連ドキュメント

- embedding / model 切替運用: `../../../workflows/thread-reflection/README_ja.md`
- API guide: `../../../docs/thread-reflection-guide_ja.md`
