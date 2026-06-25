# Thread Summary Workflow

Japanese: [README_ja.md](README_ja.md)

This workflow summarizes memories chat threads with an LLM and stores the result
in `ThreadData.description` and a dedicated summary memory thread.

## Files

| File | Description |
|---|---|
| `thread-summary-batch.yaml` | Batch workflow that processes threads sequentially |
| `../../workers/thread-summary/thread-summary-single.yaml` | Single-thread summary workflow; prompts are embedded into worker settings at registration time |

The batch calls language-specific workers by `workerName`:
`memories-thread-summary-single-ja` or `memories-thread-summary-single-en`.
Register them with `memories-import upsert-generation-workers` before running
the batch.

## Prerequisites

- jobworkerp server is running.
- memories service is running and reachable by gRPC.
- Ollama-compatible LLM endpoint is available.
- Language-specific generation workers have been registered.

## Register Workers

```bash
memories-import upsert-generation-workers \
  --feature thread-summary \
  --language all \
  --channel workflow_lang \
  --repo-root /abs/path/to/memories/agent-chat-import
```

Re-run registration after changing prompts.

## Run One Thread

Use the batch with one `thread_id`; do not call the raw single YAML directly,
because the raw YAML does not contain embedded prompt context.

```bash
jobworkerp-client job enqueue-workflow \
  -i '{
    "user_id": 1,
    "memories_grpc_host": "localhost",
    "memories_grpc_port": 9010,
    "ollama_base_url": "http://localhost:11434",
    "summary_model": "qwen3.6:27b",
    "summary_user_id": 100000,
    "thread_ids": ["7453040111820003484"],
    "output_language": "en"
  }' \
  -w /absolute/path/to/thread-summary-batch.yaml
```

## Run a Batch

Filter by user, labels, explicit thread IDs, or updated time. The workflow
stores summary outputs under `summary_user_id` and labels them for downstream
daily/weekly/monthly aggregation.

`onError: continue` isolates per-thread failures, so inspect jobworkerp per-job
logs for individual thread errors.
