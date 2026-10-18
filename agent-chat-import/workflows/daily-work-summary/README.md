# Daily Work Summary Workflow

Japanese: [README_ja.md](README_ja.md)

Aggregates per-thread summaries into one daily work summary. This is the third
summary layer.

```text
agent-chat-import -> thread-summary-single -> daily-work-summary-single
```

## Batch outcome contract

The batch exposes generated, skipped, and failed counts and date arrays.
`skipped=true` is a successful outcome and is included in `succeeded_count`,
so a serial parent can continue to weekly aggregation. A child result with
both `completed=false` and `skipped=false` is recorded as a failure.

## Files

| File | Description |
|---|---|
| `daily-work-summary-batch.yaml` | Runs daily summaries for a date range |
| `../../workers/daily-work-summary/daily-work-summary-single.yaml` | Single-day aggregation workflow |
| `run-daily-summary.sh` | Helper script using `jobworkerp-client` |

The batch dispatches `memories-daily-work-summary-single-ja/en` based on
`output_language`. Register workers with `memories-import
upsert-generation-workers` before running.

## Register Generation Workers

Register the language-specific single workers before running the batch:

```bash
memories-import upsert-generation-workers \
  --feature daily-work-summary \
  --language all \
  --repo-root /absolute/path/to/memories/agent-chat-import
```

Omit `--channel` to use jobworkerp's default channel. Specify a channel only
when a custom channel is required.

## Prerequisites

- Thread summaries exist under the requested `user_id` with kind `THREAD_SUMMARY`.
- Summary threads have `summary` labels.
- `ThreadData.description` contains the thread title/summary text written by
  `thread-summary-single`.

## Design

| Item | Behavior |
|---|---|
| Thread creator | Requested `user_id`, same as thread summaries |
| Labels | `daily_summary`, `date:YYYY-MM-DD`, `scope:<scope_key>`, plus extra labels |
| External ID | `daily:<user_id>:YYYY-MM-DD:<scope_key>` |
| Scope key | Sorted `extra_labels_filter` joined by comma, or `_all` |
| Input query | Finds `THREAD_SUMMARY` memories by `memory_kinds`, role, updated window, and `summary` + extra labels (path-scoped v2 rollups: see below) |

Thread summary memories store the conversation's last message time
(`last_message_at`) as `updated_at`, so filtering by memory `updated_at` selects
the day the conversation happened. Filtering by summary-thread `updated_at`
would not, because it is bumped when summary memories are attached.

Daily `purpose_groups.status` preserves the thread-summary status vocabulary.
`in_review`, `blocked`, and `deferred` groups remain in `carryover`; see the [thread-summary
status definitions](../thread-summary/README.md#status-values).

## Path-scoped rollups and generation safeguards

For each date the batch first runs the single workflow with
`plan_only: true` to group the source thread summary memories into path buckets
(the `path:*` label of each source thread; `unassigned` / `ambiguous` when a
thread has zero or several). It then writes one path summary per bucket
(`daily:v2:<user_id>:YYYY-MM-DD:<source_scope_digest>:path:<digest>`,
labels `summary_scope:path`, `source_scope:<digest>`) and finally one overall
summary built from those path summaries
(`...:overall`, label `summary_scope:overall`). Path summaries whose bucket
disappeared from the plan are deleted (memory row only). A source set of
exactly one memory is copied through without an LLM call.

- `source_scope_digest` identifies the source label scope; its default is the
  digest of the unfiltered scope. `extra_labels_match_mode` (`LABEL_ALL` /
  `LABEL_ANY`) selects how `extra_labels_filter` is matched.
- `target_dates` selects an exact sorted set of periods and takes precedence over
  range and last-N modes.
- The LLM request is first measured with the LLM runner's `token_count`
  method against `llm_context_window_tokens` and
  `summary_max_completion_tokens`, dropping the oldest sources until it fits.
  Providers that cannot count exactly (e.g. Ollama) fall back to
  `summary_input_byte_budget`. `llm_expected_model`, when set, must match the
  resolved model. `max_context_chars` is accepted for compatibility only.
- LLM output that is malformed or blank is rejected with
  `.../invalid-output` and regenerated up to twice; it is never saved.
- Re-generation is triggered by a change of the source fingerprint, the
  path-plan fingerprint, or (for LLM-built summaries) the model-contract
  fingerprint (prompt, model, budgets).
- `stop_on_error: true` stops at the first failed path or period; the default
  is best-effort. The batch output `outcome` is `succeeded`,
  `completed_with_errors` (an overall was written although a path failed;
  see `failed_path_buckets`), or `failed`.
- The batch document and its period loop are bounded by 31 days. Items
  that end without an outcome (timeout or abort) are counted as failed.
  Individual run/call tasks follow jobworkerp's
  `WORKFLOW_TASK_DEFAULT_TIMEOUT_SEC` when the worker sets it.

## Example

```bash
jobworkerp-client job enqueue-workflow \
  -i '{
    "user_id": 1,
    "target_dates": ["2026-05-01"],
    "memories_grpc_host": "localhost",
    "memories_grpc_port": 9010,
    "ollama_base_url": "http://localhost:11434",
    "summary_model": "qwen3.6:27b",
    "llm_context_window_tokens": 131072,
    "output_language": "en"
  }' \
  -w /absolute/path/to/daily-work-summary-batch.yaml
```
