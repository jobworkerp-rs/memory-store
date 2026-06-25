# Thread Reflection Embedding Workflows

Japanese: [README_ja.md](README_ja.md)

These workflows enqueue summary and intent embeddings after a thread reflection
has been generated. Reflection generation workflows live under
[../../agent-chat-import/workflows/thread-reflection/README.md](../../agent-chat-import/workflows/thread-reflection/README.md).

## Files

| File | Role |
|---|---|
| `auto-reflection-summary-embedding.yaml` | Upserts reflection summary into `memory_vector` and marks SUMMARY status |
| `auto-reflection-summary-embedding-workers.yaml` | Worker definitions auto-registered by `ReflectionSummaryDispatcher` |
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

At memories startup, the reflection summary and intent dispatchers register:

- `memories-auto-reflection-summary-embedding`
- `memories-auto-reflection-intent-embedding`
- `memories-mark-reflection-embedding-status`

When the kill switch is false, dispatchers and auto-registration are skipped.
YAML is read and `$file:` includes are expanded by the memories process; the
jobworkerp process does not need filesystem access to these YAML paths.

## Model Changes

When the `memories-mm-embedding` model changes, distances to historical intent
vectors are invalid. Rebuild the intent index and redispatch embeddings.

```bash
grpcurl -plaintext -d '{}' localhost:9010 \
  llm_memory.service.ReflectionVectorService/RebuildIntentIndex

grpcurl -plaintext -d '{"kind":"DISPATCH_KIND_BOTH"}' localhost:9010 \
  llm_memory.service.ReflectionVectorService/RedispatchReflectionEmbeddings
```
