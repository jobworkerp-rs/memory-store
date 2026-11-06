# Personality Workflows

Japanese: [README_ja.md](README_ja.md)

Two-layer workflow set for extracting a user's preferences, interests,
decision-making style, and communication style.

```text
agent-chat-import
  -> thread-personality-single    layer 1: per-thread signals
  -> user-personality-merge       layer 2: consolidated user profile
```

This path is separate from thread-summary outputs. It uses different labels,
threads, and owner user IDs.

## Files

| File | Role |
|---|---|
| `thread-personality-batch.yaml` | Layer-1 batch over a user's threads |
| `../../workers/personality/thread-personality-single.yaml` | Extracts signals from one thread |
| `../../workers/personality/user-personality-merge.yaml` | Merges layer-1 signals into one profile |
| `../../workers/personality/prompts/*.{ja,en}.txt` | Language-specific prompts |

The batch calls language-specific workers by name. Register them first:

```bash
memories-import upsert-generation-workers \
  --feature personality \
  --language all \
  --repo-root /abs/path/to/memories/agent-chat-import
```

Omit `--channel` to use jobworkerp's default channel; specify it only when a
custom channel is required.

## Invariants

- Layer-1 threads are identified by labels:
  `personality`, `user:<source_user_id>`, `thread:<source_thread_id>`.
- Layer-1 metadata includes `source_user_id`, `source_thread_id`,
  `signal_version`, `no_signal`, and `truncation_level`.
- Layer-2 profile threads are identified by
  `personality_profile`, `user:<source_user_id>`.
- Layer-2 profile memories use
  `external_id = "personality_profile:<source_user_id>"`.
- Personality threads and memories use the source `user_id` with
  `PERSONALITY`; no separate owner fields are accepted.

## Batch Example

```bash
jobworkerp-client job enqueue-workflow \
  -i '{
    "user_id": 1,
    "memories_grpc_host": "localhost",
    "memories_grpc_port": 9010,
    "ollama_base_url": "http://localhost:11434",
    "personality_model": "qwen3.6:27b",
    "merge_enabled": true,
    "output_language": "en"
  }' \
  -w /abs/path/agent-chat-import/workflows/personality/thread-personality-batch.yaml
```

Thread selection inputs match the reflection batch: `thread_ids`,
`target_thread_ids` (authoritative; an explicit empty array targets zero
threads), `labels_filter` + `labels_filter_match_mode`,
`last_message_within_hours` / `last_message_after_ms` (conversation last-message
time), and `thread_created_after_ms` / `thread_created_before_ms`. `exclude_thread_labels` (default `["subagent"]`) leaves threads with those labels out of period / label selection; explicit IDs are not filtered. Threads are
processed newest first; once `target_signal_count` (default 100) valid signals
exist, only re-extraction of already processed threads is skipped (new threads
always run; `force_reextract=true` ignores the cap).

Layer-1 per-thread failures are isolated with `onError: continue`. The output
reports `completed` (no thread failed), `processed_threads`, `succeeded_count`,
`failed_count`, `last_error`, `budget_skipped_count`, and the signal counts;
`merged` is true only when the layer-2 merge ran without error (`merge_error`
otherwise).

## Loading the Profile

Personalized agents should load the consolidated profile with
`MemoryService/FindListByCondition`, using the deterministic external ID for the
source user, and inject only the useful subset into prompts.
