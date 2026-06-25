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
  --channel workflow_lang \
  --repo-root /abs/path/to/memories/agent-chat-import
```

## Invariants

- Layer-1 threads are identified by labels:
  `personality`, `user:<source_user_id>`, `thread:<source_thread_id>`.
- Layer-1 metadata includes `source_user_id`, `source_thread_id`,
  `signal_version`, `no_signal`, and `truncation_level`.
- Layer-2 profile threads are identified by
  `personality_profile`, `user:<source_user_id>`.
- Layer-2 profile memories use
  `external_id = "personality_profile:<source_user_id>"`.
- `personality_user_id` must differ from the source `user_id` and
  `summary_user_id`.

## Batch Example

```bash
jobworkerp-client job enqueue-workflow \
  -i '{
    "user_id": 1,
    "personality_user_id": 200000,
    "summary_user_id": 100000,
    "memories_grpc_host": "localhost",
    "memories_grpc_port": 9010,
    "ollama_base_url": "http://localhost:11434",
    "personality_model": "qwen3.6:27b",
    "merge_enabled": true,
    "output_language": "en"
  }' \
  -w /abs/path/agent-chat-import/workflows/personality/thread-personality-batch.yaml
```

Layer-1 per-thread failures are isolated with `onError: continue`. Check
jobworkerp per-job logs or personality memory counts for detailed success/fail
state.

## Loading the Profile

Personalized agents should load the consolidated profile with
`MemoryService/FindListByCondition`, using the deterministic external ID for the
source user, and inject only the useful subset into prompts.
