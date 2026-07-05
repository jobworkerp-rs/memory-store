# agent-chat-import Workflow

Japanese: [README_ja.md](README_ja.md)

Import-only jobworkerp workflow for Claude Code, Codex, and plain-text chat
logs. It is the import half of `agent-chat-pipeline.yaml`; the summary half is
`../agent-chat-summary/agent-chat-summary.yaml`.

```text
importChats (COMMAND: memories-import)
  -> output: since_date, end_date, since_iso, range_start_ms, since_ms_utc,
             import_succeeded
```

## When to Use

- Run imports from multiple hosts or processes in parallel.
- Run import and summary at different cadences.
- Use this workflow directly only for split execution. The pipeline wrapper
  calls it internally.

## Assumed Environment

- jobworkerp worker runs on the import host so it can access `~/.claude` or
  `~/.codex`.
- memories runs remotely and is reachable through gRPC.
- `memories-import` is installed on the same host as the jobworkerp worker.

## Design Keys

| Item | Behavior |
|---|---|
| Import window | Uses `--since "<since_date>T00:00:00+<tz>"`; no `--until` because the CLI does not expose it |
| Failure propagation | `treat_nonzero_as_error: true`; importer exit 1 fails the task |
| Idempotency | `memories-import` deduplicates by `external_id` |
| Output | Summary workflows consume the computed date/window values |

## Required Inputs

| Input | Description |
|---|---|
| `source` | `claude-code`, `codex`, or `plain` |
| `user_id` | Import owner user ID |
| `memories_grpc_url` | Passed to `memories-import --server-url` |

Common optional inputs include `since_date`, `end_date`,
`timezone_offset_hours`, `since_mode`, `source_args`, `labels`, and
source-specific path options. Day boundaries follow the jobworkerp
worker's `TZ` environment variable (DST-aware) when set, falling back to
`timezone_offset_hours` otherwise.

## Example

```bash
jobworkerp-client job enqueue-workflow \
  -i '{
    "source": "claude-code",
    "user_id": 1,
    "memories_grpc_url": "http://memories.example.com:9100",
    "since_date": "2026-04-01",
    "end_date": "2026-04-30"
  }' \
  -w /abs/path/agent-chat-import/workflows/agent-chat-import/agent-chat-import.yaml
```

The output values should be passed unchanged to `agent-chat-summary.yaml` when
running the two workflows separately.
