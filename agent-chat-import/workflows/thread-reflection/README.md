# Thread Reflection Generation Workflow

Japanese: [README_ja.md](README_ja.md)

This directory contains generation workflows that create LLM reflections from
memories chat threads. Embedding redispatch and dispatcher auto-registration are
documented in [../../../workflows/thread-reflection/README.md](../../../workflows/thread-reflection/README.md).

## Files

| File | Role |
|---|---|
| `thread-reflection-batch.yaml` | Batch reflection generation for multiple threads |
| `../../workers/thread-reflection/thread-reflection-single.yaml` | Single-thread reflection generation |
| `../../workers/thread-reflection/prompts/` | Language-specific prompts embedded during worker registration |

## Prerequisites

- jobworkerp server is running.
- memories service is running.
- `MEMORY_GRPC_HOST` and `MEMORY_GRPC_PORT` are routable from jobworkerp.
- `MEMORY_REFLECTION_REFLECTOR_MODEL` and
  `MEMORY_REFLECTION_REFLECTOR_BASE_URL` are configured.
- Language-specific generation workers have been registered.

## Setup

```bash
memories-import upsert-generation-workers \
  --feature reflection \
  --language all \
  --channel workflow_lang \
  --repo-root /abs/path/to/memories/agent-chat-import
```

Re-run this after prompt changes.

## Batch Run

```bash
jobworkerp-client job enqueue-workflow \
  -i '{
    "user_id": 1,
    "memories_grpc_host": "localhost",
    "memories_grpc_port": 9010,
    "reflector_model": "qwen3.6:27b",
    "reflector_base_url": "http://localhost:11434",
    "prompt_version": "20260511-baseline",
    "output_language": "en",
    "updated_within_hours": 24
  }' \
  -w /abs/path/to/memories/agent-chat-import/workflows/thread-reflection/thread-reflection-batch.yaml
```

`thread-reflection-batch.yaml` dispatches
`memories-thread-reflection-single-ja/en` according to `output_language`. Prompt
context is already embedded in worker settings.

## Related

- embedding/model operations: [../../../workflows/thread-reflection/README.md](../../../workflows/thread-reflection/README.md)
- API guide: [../../../docs/thread-reflection-guide.md](../../../docs/thread-reflection-guide.md)
