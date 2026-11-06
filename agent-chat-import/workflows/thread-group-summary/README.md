# Thread Group Summary Workflow

Japanese: [README_ja.md](README_ja.md)

Like the other generation features, this is a batch/single pair.
`thread-group-summary-batch.yaml` selects the groups and refreshes member
summaries, then fans out to the language worker
`memories-thread-group-summary-single-<lang>`
(`workers/thread-group-summary/thread-group-summary-single.yaml`), which
aggregates the persisted per-thread
summaries of each active ThreadGroup's members into one group summary memory
(`MEMORY_KIND_DERIVED_SUMMARY`, `external_id = thread-group-summary:<group_id>`)
linked to the group with purpose `summary`.

## Flow

The batch:

1. Reads the lineage of every target group and collects active member threads
   per owner.
2. Calls `memories-thread-summary-single-<lang>` once per member thread,
   publishing `progress_processed` / `progress_total` before each call. A
   thread shared by several groups is refreshed once, and the single skips an
   up-to-date thread before reading its messages, so no group is aggregated
   from a stale member summary. A group containing a member whose refresh
   failed is reported as `thread-refresh-failed`
   (`reason_code: incomplete_input`) without being aggregated.
3. Calls the single worker for each group, passing the members the
   thread-summary single skipped as too short.

The single worker processes exactly one group:

1. Re-reads the lineage (and rejects the run if
   `expected_membership_snapshot_digest` is set and differs), then looks up each
   member's summary by its `summary:<thread_id>` external ID.
2. Excludes members skipped for having fewer than `min_message_count` messages
   that have no summary. Any other missing member summary is returned as
   `outcome: incomplete_input`, which the batch reports as
   `thread-summary-missing` (`reason_code: incomplete_input`). A group with no
   remaining source is skipped.
3. Uses the single member summary as-is (passthrough) or asks the LLM to merge
   several summaries. Malformed or blank LLM output is regenerated up to twice
   and never persisted.
4. Skips the write when the stored `summary_fingerprint` (member summary IDs and
   bodies, membership snapshot digest, language, mode) is unchanged.
5. Re-reads the lineage just before writing. If the membership snapshot changed
   meanwhile, the summary is written with `publication_state: stale` and the
   group is reported in `stale_groups`.

## Prerequisites

- The memories server supports independent group ownership
  (`ThreadGroupService/GetThreadGroupCapabilities` returns
  `supports_independent_group_owner_scope = true`); otherwise the batch fails
  fast.
- The `memories-thread-summary-single-<lang>` and
  `memories-thread-group-summary-single-<lang>` workers are registered:
  `memories-import upsert-generation-workers --feature thread-summary` and
  `--feature thread-group-summary` (or `--feature all`). Prompts live in
  `workers/thread-group-summary/prompts/` and are baked in at registration.
- Run this batch on a channel other than the ones its thread-summary and
  group single workers are registered on (see [Channels](../../README.md#channels)).

## Run

```bash
jobworkerp-client job enqueue-workflow \
  -i '{
    "group_owner_user_id": 1,
    "memories_grpc_host": "localhost",
    "memories_grpc_port": 9010,
    "ollama_base_url": "http://localhost:11434",
    "summary_model": "qwen3.6:27b",
    "output_language": "en",
    "activity_after_ms": 1767225600000,
    "activity_before_ms": 1767312000000
  }' \
  -w /abs/path/to/thread-group-summary-batch.yaml
```

## Inputs

| Input | Required | Default | Description |
|---|---|---|---|
| `group_owner_user_id` | yes | - | Owner used to select groups and to own the group summary. |
| `memories_grpc_host` / `memories_grpc_port` / `memories_grpc_tls` | yes | `localhost` / `9100` / `false` | memories gRPC endpoint. |
| `target_group_ids` | no | all active | Summarize only these groups. |
| `target_group_id` | no | - | Summarize exactly this group; fails if it is missing or inactive. |
| `expected_membership_snapshot_digest` | no | - | Abort the group if its current snapshot digest differs. |
| `activity_after_ms` / `activity_before_ms` | no | - | Exclusive epoch-ms window. When both are set (and `target_group_id` is not), only groups with a member whose conversation overlaps the window are summarized; each such group is still aggregated from all of its members. |
| `output_language` | no | `ja` | `ja` or `en`. |
| `summary_model` / `ollama_base_url` | no | `qwen3.6:27b` / `http://localhost:11434` | LLM used for merging and forwarded to the per-thread batch. |
| `min_message_count` | no | `4` | Forwarded to the per-thread batch. |
| `max_context_chars` | no | `200000` | Forwarded to the per-thread batch. |
| `force_resummarize` | no | `false` | Regenerate member and group summaries even when current. |
| `memory_thread_label_prefix` | no | `"summary"` | Summary label forwarded to the per-thread batch; set it to the same value used for thread summaries. |

To summarize one group, pass `target_group_id` (optionally with
`expected_membership_snapshot_digest`) to the batch so member summaries are
refreshed first. Calling the single worker directly aggregates whatever member
summaries are already stored.

## Output

`completed` is true only when no group failed and every target group has an
outcome. The batch also returns `processed_groups`, `expected_group_count`,
`generated_count`, `skipped_count`, `stale_count`, `failed_count`, the group ID
arrays `generated_groups` / `skipped_groups` / `stale_groups` /
`incomplete_groups`, `failed_groups` (objects with `group_id`,
`expected_membership_snapshot_digest`, `reason_code`, `detail`, so a caller can
retry the same group), and `last_error`. Groups that end without an outcome
(timeout or abort) are reported as failed with `reason_code: workflow_failed`.
