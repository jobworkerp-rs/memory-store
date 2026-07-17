# Monthly Work Summary Workflow

Japanese: [README_ja.md](README_ja.md)

Aggregates weekly work summaries into one calendar-month summary, extracting
monthly highlights and milestones. This is the fifth summary layer.

## Files

| File | Description |
|---|---|
| `monthly-work-summary-batch.yaml` | Runs monthly summaries for a month range |
| `../../workers/monthly-work-summary/monthly-work-summary-single.yaml` | Single-month aggregation workflow |
| `run-monthly-summary.sh` | Helper script using `jobworkerp-client` |

The batch dispatches `memories-monthly-work-summary-single-ja/en` according to
`output_language`.

## Prerequisites

- Weekly summaries exist under `summary_user_id = 100000`.
- Weekly summary threads have `weekly_summary` labels.
- Weekly summary descriptions use the expected `<YYYY-Www> - <purpose>` style.

## Design

| Item | Behavior |
|---|---|
| Owner | `user_id = 100000` |
| Labels | `monthly_summary`, `month:YYYY-MM`, `scope:<scope_key>`, plus extra labels |
| External ID | `monthly:YYYY-MM:<scope_key>` |
| Input query | Finds weekly summary memories by `external_id_prefix="weekly:"`, role, updated window, and labels |

Monthly `purpose_groups.status` preserves the [thread-summary status
vocabulary](../thread-summary/README.md#status-values). `in_review`, `blocked`,
and `deferred` groups remain in `carryover` and do not qualify as completed milestones.

## Example

```bash
jobworkerp-client job enqueue-workflow \
  -i '{
    "user_id": 1,
    "summary_user_id": 100000,
    "target_month": "2026-05",
    "memories_grpc_host": "localhost",
    "memories_grpc_port": 9010,
    "ollama_base_url": "http://localhost:11434",
    "summary_model": "qwen3.6:27b",
    "output_language": "en"
  }' \
  -w /absolute/path/to/monthly-work-summary-batch.yaml
```
