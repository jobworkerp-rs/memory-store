# Weekly Work Summary Workflow

Japanese: [README_ja.md](README_ja.md)

Aggregates daily work summaries into one ISO-week summary, extracting purpose
groups, topics, and trends. This is the fourth summary layer.

```text
thread summaries -> daily summaries -> weekly summaries -> monthly summaries
```

## Files

| File | Description |
|---|---|
| `weekly-work-summary-batch.yaml` | Runs weekly summaries for a week range |
| `../../workers/weekly-work-summary/weekly-work-summary-single.yaml` | Single-week aggregation workflow |
| `run-weekly-summary.sh` | Helper script using `jobworkerp-client` |

The batch dispatches `memories-weekly-work-summary-single-ja/en` according to
`output_language`.

## Prerequisites

- Daily summaries exist under the requested `user_id` with kind `DAILY_SUMMARY`.
- Daily summary threads have `daily_summary` labels.
- The workflow engine uses jaq 3.x or compatible ISO-week parsing.

## Design

| Item | Behavior |
|---|---|
| Thread creator | Requested `user_id`, same as daily summaries |
| Labels | `weekly_summary`, `iso_week:YYYY-Www`, `scope:<scope_key>`, plus extra labels |
| External ID | `weekly:<user_id>:YYYY-Www:<scope_key>` |
| Input query | Finds daily summary memories by `external_id_prefix="daily:"`, role, updated window, and labels |

Weekly `purpose_groups.status` preserves the [thread-summary status
vocabulary](../thread-summary/README.md#status-values). `in_review`, `blocked`,
and `deferred` groups remain `continued` and appear in `carryover`; they are not `completed`.

## Example

```bash
jobworkerp-client job enqueue-workflow \
  -i '{
    "user_id": 1,
    "target_iso_week": "2026-W18",
    "memories_grpc_host": "localhost",
    "memories_grpc_port": 9010,
    "ollama_base_url": "http://localhost:11434",
    "summary_model": "qwen3.6:27b",
    "output_language": "en"
  }' \
  -w /absolute/path/to/weekly-work-summary-batch.yaml
```
