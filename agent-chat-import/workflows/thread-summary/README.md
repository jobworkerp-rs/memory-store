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
  --repo-root /abs/path/to/memories/agent-chat-import
```

Omit `--channel` to use jobworkerp's default channel; specify it only when a
custom channel is required.

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
    "thread_ids": ["7453040111820003484"],
    "output_language": "en"
  }' \
  -w /absolute/path/to/thread-summary-batch.yaml
```

## Run a Batch

Filter by user, labels, explicit thread IDs, or conversation time. The workflow
stores summary outputs under the source `user_id` with `THREAD_SUMMARY` and
labels them for downstream daily/weekly/monthly aggregation.

`onError: continue` isolates per-thread failures, so inspect jobworkerp per-job
logs for individual thread errors.

Batch-only inputs:

| Input | Default | Description |
|---|---|---|
| `thread_ids` | all | Summarize only these thread IDs. |
| `target_thread_ids` | unset | Authoritative target set (string IDs). Takes precedence over `thread_ids`; an empty array targets zero threads. |
| `labels_filter` | none | Only threads with these labels. |
| `labels_filter_match_mode` | `LABEL_ALL` | `LABEL_ALL` or `LABEL_ANY` for `labels_filter`. |
| `last_message_within_hours` | unset | Only threads whose last message is within the past N hours of workflow start. |
| `last_message_after_ms` | unset | Absolute lower bound (epoch ms, `>`) on `last_message_at`; wins over `last_message_within_hours`. |
| `last_message_before_ms` | unset | Absolute upper bound (epoch ms, `<=`) on `last_message_at`. |
| `thread_created_after_ms` / `thread_created_before_ms` | unset | Thread creation-time bounds (epoch ms, `>` / `<=`). |

The former `updated_within_hours` / `updated_after_ms` / `updated_before_ms`
inputs were renamed to the `last_message_*` names above.

Summary memories carry the source conversation's `first_message_at` /
`last_message_at` as `created_at` / `updated_at` (thread audit times are not
used); daily summaries bucket by that `updated_at`, and re-summarization runs
only when `last_message_at` is newer. Malformed or blank LLM output is
regenerated up to twice and is never persisted.

The YAML sets no document/loop/try timeouts. To cut off a hung LLM or single
call, configure `WORKFLOW_TASK_DEFAULT_TIMEOUT_SEC` (applies to run/call tasks
only, e.g. `3600`) on the jobworkerp server.

## Status Values

The structured summary `status` uses these values:

| Value | Meaning |
|---|---|
| `ongoing` | The main work is still in progress. |
| `deferred` | Work is intentionally postponed. |
| `resolved` | The main goal and all follow-up work are complete. |
| `abandoned` | The work was explicitly discontinued. |

`in_review`, `blocked`, and `deferred` are carryover states for downstream summaries.

## Batch outcome contract

The batch returns `generated_count`, `skipped_count`, and `failed_count` plus
the corresponding thread ID arrays. An up-to-date summary is a successful
skip and remains part of `succeeded_count`, allowing a serial parent workflow
to continue to daily aggregation. Only `completed=false, skipped=false` is a
logical failure. Up-to-date summaries are detected in one preflight query;
only missing, stale, relabeled, forced, or description-less threads dispatch
the single worker. Items that end without an outcome (timeout or abort) are
counted in `failed_threads`. `insufficient_message_threads` lists skipped
threads below `min_message_count`, which the
[thread-group summary](../thread-group-summary/README.md) uses to tell "too
short to summarize" apart from a missing summary. Each iteration publishes
`progress_processed` / `progress_total` in its streamed output context.
