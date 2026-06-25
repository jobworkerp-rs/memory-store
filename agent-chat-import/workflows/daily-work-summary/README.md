# Daily Work Summary Workflow

Japanese: [README_ja.md](README_ja.md)

Aggregates per-thread summaries into one daily work summary. This is the third
summary layer.

```text
agent-chat-import -> thread-summary-single -> daily-work-summary-single
```

## Files

| File | Description |
|---|---|
| `daily-work-summary-batch.yaml` | Runs daily summaries for a date range |
| `../../workers/daily-work-summary/daily-work-summary-single.yaml` | Single-day aggregation workflow |
| `run-daily-summary.sh` | Helper script using `jobworkerp-client` |

The batch dispatches `memories-daily-work-summary-single-ja/en` based on
`output_language`. Register workers with `memories-import
upsert-generation-workers` before running.

## Prerequisites

- Thread summaries exist under `summary_user_id = 100000`.
- Summary threads have `summary` labels.
- `ThreadData.description` contains the thread title/summary text written by
  `thread-summary-single`.

## Design

| Item | Behavior |
|---|---|
| Owner | `user_id = 100000`, same as thread summaries |
| Labels | `daily_summary`, `date:YYYY-MM-DD`, `scope:<scope_key>`, plus extra labels |
| External ID | `daily:YYYY-MM-DD:<scope_key>` |
| Scope key | Sorted `extra_labels_filter` joined by comma, or `_all` |
| Input query | Finds summary memories by `external_id_prefix="summary:"`, role, updated window, and labels |

Filtering by memory `updated_at` preserves the original conversation date,
unlike filtering by summary-thread `updated_at`, which is bumped when summary
memories are attached.

## Example

```bash
jobworkerp-client job enqueue-workflow \
  -i '{
    "user_id": 1,
    "summary_user_id": 100000,
    "target_date": "2026-05-01",
    "memories_grpc_host": "localhost",
    "memories_grpc_port": 9010,
    "ollama_base_url": "http://localhost:11434",
    "summary_model": "qwen3.6:27b",
    "output_language": "en"
  }' \
  -w /absolute/path/to/daily-work-summary-batch.yaml
```
