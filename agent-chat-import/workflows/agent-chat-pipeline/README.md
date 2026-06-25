# agent-chat-pipeline Workflow

Japanese: [README_ja.md](README_ja.md)

Thin wrapper that runs the split import and summary workflows in sequence. It is
kept for deployments that still want one enqueue operation for the full pipeline.
For split execution, enqueue the sub-workflows directly:

- [../agent-chat-import/README.md](../agent-chat-import/README.md)
- [../agent-chat-summary/README.md](../agent-chat-summary/README.md)

```text
agent-chat-pipeline.yaml
  -> runImport  -> agent-chat-import.yaml
  -> runSummary -> agent-chat-summary.yaml
```

The pipeline imports agent chat logs, then runs thread summary and daily work
summary. It can optionally run personality extraction and thread reflection.

## Failure Policy

| Stage | Failure behavior | Reason |
|---|---|---|
| import | Fatal | Summary should not run on partial import failure |
| thread/daily summary | Fatal | These are the main pipeline output |
| reflection | Fatal when enabled | Reflector configuration failures should be visible |
| personality | Non-fatal warning | Supplemental signal; should not roll back summary outputs |

The success flags only indicate that the batch/merge workflow itself completed.
Per-thread failures inside fan-out batches are logged by jobworkerp and are not
reflected in those flags.

## Main Inputs

| Input | Description |
|---|---|
| `agent_chat_import_yaml` | Import sub-workflow path/URL |
| `agent_chat_summary_yaml` | Summary sub-workflow path/URL |
| `source` | `claude-code`, `codex`, or `plain` |
| `user_id` | Conversation owner |
| `memories_grpc_url` | Importer server URL |
| `memories_grpc_host` / `memories_grpc_port` | Callback endpoint for summary workflows |
| `thread_summary_batch_yaml` | Thread summary batch YAML |
| `daily_work_summary_batch_yaml` | Daily summary batch YAML |

Optional paths enable reflection and personality.

## Example

```bash
jobworkerp-client job enqueue-workflow \
  -i '{
    "source": "claude-code",
    "user_id": 1,
    "memories_grpc_url": "http://localhost:9010",
    "memories_grpc_host": "localhost",
    "memories_grpc_port": 9010,
    "thread_summary_batch_yaml": "/abs/path/thread-summary-batch.yaml",
    "daily_work_summary_batch_yaml": "/abs/path/daily-work-summary-batch.yaml"
  }' \
  -w /abs/path/agent-chat-pipeline.yaml
```

When `since_date` is specified, daily summaries run for the inclusive range
`[since_date, today]` unless `end_date` is also specified. Without `since_date`,
the workflow preserves the single-day behavior of the selected `since_mode`.
