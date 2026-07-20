# agent-chat-summary Workflow

Japanese: [README_ja.md](README_ja.md)

Summary-only workflow for imported chat logs. It runs thread summary, daily work
summary, and optionally thread reflection and personality extraction. It is the
summary half of the split pipeline; the import half is
`../agent-chat-import/agent-chat-import.yaml`.

```text
fork:
  summaryBranch (fatal, serial):
    threadSummaryBatch -> dailyWorkSummary -> reflectionStage (optional)
  personalityBranch (optional, non-fatal):
    threadPersonalityBatch -> userPersonalityMerge
```

## When to Use

- Import and summary run on different hosts or cadences.
- The full pipeline wrapper is not needed.

## Design Keys

| Item | Behavior |
|---|---|
| Processing window | Received as inputs from the import workflow |
| Stage coupling | Via memories labels only; each stage can be rerun independently |
| Reflection failure | Fatal; workflow fails to avoid silent reflector misconfiguration |
| Personality failure | Non-fatal; exposed through `personality_error` |

## Required Inputs

| Input | Description |
|---|---|
| `user_id` | Original conversation thread creator |
| `memories_grpc_host` / `memories_grpc_port` | memories callback endpoint |
| `thread_summary_batch_yaml` | Thread summary batch YAML path/URL |
| `daily_work_summary_batch_yaml` | Daily work summary batch YAML path/URL |
| `since_date`, `end_date`, `range_start_ms`, `since_ms_utc` | Processing window values |

Optional inputs enable reflection and personality:

- `thread_reflection_batch_yaml`
- `thread_personality_batch_yaml`
- `user_personality_merge_yaml`

## Example

```bash
jobworkerp-client job enqueue-workflow \
  -i '{
    "user_id": 1,
    "memories_grpc_host": "localhost",
    "memories_grpc_port": 9010,
    "since_date": "2026-04-01",
    "end_date": "2026-04-30",
    "range_start_ms": 1775001600000,
    "since_ms_utc": 1775001600000,
    "thread_summary_batch_yaml": "/abs/.../thread-summary-batch.yaml",
    "daily_work_summary_batch_yaml": "/abs/.../daily-work-summary-batch.yaml"
  }' \
  -w /abs/path/agent-chat-summary.yaml
```

Per-thread fan-out inside summary/reflection/personality batches uses
`onError: continue`. Inspect jobworkerp per-job logs for individual thread
failures.
