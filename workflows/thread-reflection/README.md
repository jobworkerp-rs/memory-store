# Thread Reflection Embedding Workflows

Japanese: [README_ja.md](README_ja.md)

These workflows maintain the reflection intent embedding path. Reflection
search-document embeddings now use the generic memory auto-embedding path
(`workflows/auto-embedding.yaml`). Reflection generation workflows live under
[../../agent-chat-import/workflows/thread-reflection/README.md](../../agent-chat-import/workflows/thread-reflection/README.md).

## Files

| File | Role |
|---|---|
| `auto-reflection-summary-embedding.yaml` | Legacy compatibility workflow for old summary status updates |
| `auto-reflection-summary-embedding-workers.yaml` | Legacy worker definitions for `ReflectionSummaryDispatcher` |
| `auto-reflection-intent-embedding.yaml` | Upserts reflection intent into `reflection_intent_vector` and marks INTENT status |
| `auto-reflection-intent-embedding-workers.yaml` | Worker definitions auto-registered by `ReflectionIntentDispatcher` |

## Prerequisites

- memories service is running.
- `MEMORY_REFLECTION_DISPATCH_ENABLED=true`.
- `JOBWORKERP_ADDR` is configured.
- `MEMORY_GRPC_HOST` and `MEMORY_GRPC_PORT` are routable from jobworkerp.
- `auto-embedding-workers.yaml` has registered `memories-mm-embedding` and
  `memories-upsert-embedding`.

## Auto-Registration

At memories startup, the reflection intent dispatcher registers:

- `memories-auto-reflection-intent-embedding`
- `memories-mark-reflection-embedding-status`

The generic memory embedding dispatcher registers `memories-auto-embedding` for
reflection search documents. When the kill switch is false, reflection intent
dispatch and auto-registration are skipped.
YAML is read and `$file:` includes are expanded by the memories process; the
jobworkerp process does not need filesystem access to these YAML paths.

## Model Changes

When the `memories-mm-embedding` model changes, distances to historical intent
vectors are invalid. Rebuild the intent index and redispatch intent embeddings.
Redispatch reflection search documents through `MemoryVectorService` with
`user_id=300000`.

```bash
grpcurl -plaintext -d '{}' localhost:9010 \
  llm_memory.service.ReflectionVectorService/RebuildIntentIndex

grpcurl -plaintext -d '{"kind":"EMBEDDING_KIND_INTENT"}' localhost:9010 \
  llm_memory.service.ReflectionVectorService/RedispatchReflectionEmbeddings
```
