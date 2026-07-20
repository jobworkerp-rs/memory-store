# agent-chat-import (`memories-import`)

Standalone CLI for importing agent conversation logs into memories.

Japanese: [README_ja.md](README_ja.md)

Supported source subcommands:

- `claude-code`: Claude Code JSONL transcripts under
  `~/.claude/projects/<hash>/<session>.jsonl`.
- `codex`: OpenAI Codex CLI rollout JSONL under
  `~/.codex/sessions/<yyyy>/<mm>/<dd>/rollout-*.jsonl`.
- `plain`: plain-text trees such as Obsidian vaults (`.md` / `.txt`).

## Canonical Schema

The importer normalizes source-specific events so display layers, RAG tools, and
summary workflows can read memory metadata without knowing the source format.

- `metadata.tool`: common metadata for tool calls and tool outputs. It includes
  `name`, `source`, `category`, `call_id`, `arguments`, `output`, `status`, and
  source event information.
- Attachments: image/audio/video/url/ref payloads are stored or registered in
  server-side media storage such as S3, file, or url storage. Memories refer to
  them through `media_object_id`.
- `metadata.raw.<source>`: provider-specific fields that do not map into the
  canonical shape.
- `metadata.claude_code.*`: Claude Code-only top-level scalar fields copied
  into a stable metadata object. Large payloads and fields already consumed by
  canonical/block/attachment paths are excluded.

Default size guards:

- `MEMORY_TOOL_OUTPUT_FULL_BYTES = 65536`
- `MEMORY_TOOL_OUTPUT_PREVIEW_BYTES = 4096`
- `MEMORY_TOOL_ARG_PREVIEW_BYTES = 512`
- `MEMORY_ATTACHMENT_INLINE_MAX_BYTES = 1048576`

If media registration fails or an input cannot be converted, the batch keeps
running and leaves enough metadata for later recovery.

Block decomposition creates these memories:

- `tool_use` block: one `kind=tool_call`, `role=assistant`,
  `content_type=tool` memory.
- `tool_result` block: one `kind=tool_output`, `role=tool`,
  `content_type=tool` memory, plus attachment sub-block memories for embedded
  images.
- Direct `image` block: one `kind=attachment` memory.
- `type=attachment` JSONL event: one `kind=attachment`, `role=meta` memory.

Global options such as `-u`, `-s`, `-l`, `-n`, `-v`, `-b`, `--server-url`, and
post-import generation options can be placed before or after the subcommand.

```bash
memories-import claude-code -u 1 --all-projects
memories-import -u 1 claude-code --all-projects
```

## Build

```bash
cargo build --release -p agent-chat-import
cargo build --release -p agent-chat-import --no-default-features --features personality-after
cargo build --release -p agent-chat-import --no-default-features --features summarize-after
```

The `summarize-after` and `personality-after` features are independent CLI
wrappers that dispatch the corresponding jobworkerp workflows after import.

The release binary is `target/release/memories-import`.

## Required Runtime Inputs

| Input | Required when | Example |
|---|---|---|
| `--server-url` | Real import, except `--dry-run` | `http://localhost:9010` |
| `JOBWORKERP_ADDR` | `--summarize-after-*` or `--extract-personality-after-*` | `http://localhost:9000` |

## `claude-code`

Import one session:

```bash
memories-import --user-id 1 --server-url http://localhost:9010 claude-code \
  --session-file ~/.claude/projects/-home-me-app/abc123.jsonl
```

Import a project directory:

```bash
memories-import --user-id 1 --server-url http://localhost:9010 claude-code \
  --project-dir ~/.claude/projects/-home-me-app
```

Import all projects since a timestamp:

```bash
memories-import --user-id 1 \
  --server-url http://localhost:9010 \
  --since 2026-04-29T00:00:00Z \
  --labels coding_agent,claude_code \
  claude-code --all-projects
```

`--labels` can be specified multiple times. Each occurrence is parsed as CSV,
then flattened and deduplicated. Labels longer than 512 bytes are rejected at
CLI parse time.

Use `--strip-path-prefix` to remove environment-specific path prefixes from
`path:` labels:

```bash
memories-import --user-id 1 claude-code \
  --server-url http://localhost:9010 \
  --all-projects \
  --strip-path-prefix /home/me,/usr/share
```

`cwd=/home/me/work/foo` becomes `path:work/foo`. `dir:` labels and
`metadata.project_path` remain absolute.

Dry-run counts inputs without connecting to the server:

```bash
memories-import --user-id 1 \
  --dry-run \
  --since 2026-04-29T00:00:00Z \
  claude-code --all-projects
```

## Register Generation Workers

`upsert-generation-workers` registers language-specific workflow workers for
reflection, summary, work-summary, and personality generation. Prompts are
embedded into worker settings at registration time; re-run registration after
changing prompts.

```bash
JOBWORKERP_ADDR=http://localhost:9000 \
memories-import upsert-generation-workers \
  --feature all \
  --language all \
  --channel workflow_lang
```

This subcommand does not import data and does not require `--user-id`.

The command resolves the `agent-chat-import` crate directory in this order:

1. `--repo-root <PATH>`
2. `MEMORY_REPO_ROOT`
3. build-time `CARGO_MANIFEST_DIR`

When distributing the binary outside the build tree, set `--repo-root` or
`MEMORY_REPO_ROOT` to the `agent-chat-import` crate directory, not the workspace
root.

## Import and Then Summarize

With `--summarize-after-file` or `--summarize-after-json`, the CLI dispatches
`agent-chat-import/workflows/thread-summary/thread-summary-batch.yaml` after a
successful import. The `summarize-after` feature and `JOBWORKERP_ADDR` are
required.

```bash
cat > /tmp/summarize.json <<'EOF'
{
  "memories_grpc_host": "localhost",
  "memories_grpc_port": 9010,
  "ollama_base_url": "http://192.168.1.2:11434",
  "summary_model": "qwen3.6:27b"
}
EOF

JOBWORKERP_ADDR=http://localhost:9000 \
memories-import --user-id 1 \
  --server-url http://localhost:9010 \
  --since 2026-04-29T00:00:00Z \
  --summarize-after-file /tmp/summarize.json \
  --summarize-workflow /abs/path/to/memories/agent-chat-import/workflows/thread-summary/thread-summary-batch.yaml \
  claude-code --all-projects
```

`--since` is converted to absolute epoch milliseconds and passed to the workflow
as `updated_after_ms`. If import reports errors, summary dispatch is skipped to
avoid summarizing partial thread state.

For frequent imports, `--since` also enables a session-file mtime filter.
`--mtime-margin-seconds` defaults to `60`; use `--no-mtime-filter` when mtime is
unreliable, such as on some NFS or cloud-synced filesystems.

## Import and Then Extract Personality

With `--extract-personality-after-file` or
`--extract-personality-after-json`, the CLI dispatches
`agent-chat-import/workflows/personality/thread-personality-batch.yaml` after a
successful import. This path is independent from summary dispatch; when both are
specified, they are dispatched in parallel.

```bash
cat > /tmp/personality.json <<'EOF'
{
  "memories_grpc_host": "localhost",
  "memories_grpc_port": 9010,
  "ollama_base_url": "http://192.168.1.2:11434",
  "personality_model": "qwen3.6:27b",
  "min_user_messages": 2,
  "merge_enabled": true
}
EOF

JOBWORKERP_ADDR=http://localhost:9000 \
memories-import --user-id 1 \
  --server-url http://localhost:9010 \
  --output-language ja \
  --since 2026-04-29T00:00:00Z \
  --extract-personality-after-file /tmp/personality.json \
  --personality-workflow /abs/path/to/memories/agent-chat-import/workflows/personality/thread-personality-batch.yaml \
  claude-code --all-projects
```

Personality workflows use the imported `user_id` and `PERSONALITY` memory kind,
so they do not need a separate owner value. When `merge_enabled: true`, the
batch also runs the second-layer user profile merge workflow.

## `codex`

Import OpenAI Codex CLI rollout JSONL. One rollout becomes one thread. The
`session_meta.payload.id` UUID is used as `session_id`, so re-importing the same
rollout is idempotent.

```bash
memories-import --user-id 1 --server-url http://localhost:9010 codex \
  --session-file ~/.codex/sessions/2026/05/02/rollout-2026-05-02T10-22-47-...-.jsonl

memories-import --user-id 1 --server-url http://localhost:9010 codex \
  --day-dir ~/.codex/sessions/2026/05/02

memories-import --user-id 1 --dry-run codex --all-sessions
```

Kinds are stored in `metadata.kind` and can be filtered by `--include-types`.
The default imports `user`, `assistant`, `tool_call`, `tool_output`, `system`,
and `reasoning`.

Encrypted reasoning content is not stored by default. Instead, the importer
stores `encrypted_content_sha256` and `encrypted_content_size`. Use
`--include-encrypted-reasoning` only when storing the encrypted body is
intentional.

Tool outputs are linked to matching tool calls through `parent_ids` by default.
Disable this with `--no-link-tool-calls`.

## `plain`

Import a plain-text tree such as an Obsidian vault. The importer respects
`.gitignore`, supports extra glob exclusions, and parses YAML frontmatter.

Thread strategies:

- `per-file`: default; one file becomes one thread.
- `per-dir`: files directly under the same parent directory share one thread.
- `single`: the entire root becomes one thread.

`plain` source identity is built from `--source-name` and `--root`. Keep the
same root path for the same source name. Use distinct source names such as
`obsidian-private` and `notes-archive` for different vaults.

By default, import is add-only. Deleted, renamed, or moved files do not remove
previously imported memories. Use `--prune-missing` to delete server-side
memories whose `metadata.path` no longer exists on disk. Prune runs only after
an import finishes without session errors. It considers only the current
creator's scoped IDs. Migrate legacy plain IDs during the maintenance window
before using prune with this release.

```bash
memories-import --user-id 1 --server-url http://localhost:9010 plain \
  --root ~/Obsidian/Private \
  --source-name obsidian-private

memories-import --user-id 1 --server-url http://localhost:9010 plain \
  --root ~/Obsidian/Private \
  --thread-strategy per-dir \
  --source-name obsidian-private

memories-import --user-id 1 --server-url http://localhost:9010 plain \
  --root ~/notes \
  --exclude-glob '**/*.bak' \
  --exclude-glob '.obsidian/**' \
  --source-name notes
```

Important `plain` options:

| Option | Default | Description |
|---|---|---|
| `--source-name` | `plain` | Vault identifier; must match `^[a-z0-9_-]{1,32}$` |
| `--ext` | `md,txt` | Imported extensions |
| `--thread-strategy` | `per-file` | `per-file`, `per-dir`, or `single` |
| `--frontmatter` / `--no-frontmatter` | on | Parse leading YAML frontmatter |
| `--label-from-frontmatter` | none | Add labels from selected frontmatter keys |
| `--max-file-size-bytes` | `1048576` | Skip larger files |
| `--respect-gitignore` / `--no-respect-gitignore` | on | Respect `.gitignore` |
| `--prune-missing` | off | Delete memories for missing files |
| `--no-interactive` | off | Disable confirmation prompts; required for cron/CI prune |

## Global Options

| Option | Default | Description |
|---|---|---|
| `-u, --user-id` | required for import | Creator user ID for imported threads |
| `-s, --since` | none | ISO 8601 lower bound; passed to generation workflows as epoch ms |
| `--mtime-margin-seconds` | `60` | Conservative session-file mtime margin for `--since` |
| `--no-mtime-filter` | `false` | Disable session-level mtime filtering |
| `-l, --labels` | none | Additional labels; comma-separated and repeatable |
| `-n, --dry-run` | `false` | Count without server writes |
| `-v, --verbose` | `false` | Equivalent to `RUST_LOG=debug` |
| `-b, --batch-size` | `100` | Progress logging interval |
| `--summarize-after-file` / `--summarize-after-json` | none | Mutually exclusive; requires `--summarize-workflow` |
| `--summarize-workflow` | none | Absolute path to `thread-summary-batch.yaml` |
| `--extract-personality-after-file` / `--extract-personality-after-json` | none | Mutually exclusive; requires `--personality-workflow` |
| `--personality-workflow` | none | Absolute path to `thread-personality-batch.yaml` |
| `--server-retry-max` | `3` | Max RPC attempts including the first |
| `--server-retry-base-ms` | `1000` | Exponential backoff base |
| `--server-retry-cap-ms` | `30000` | Backoff cap |
| `--server-retry-jitter-ratio` | `0.25` | Jitter ratio |
| `--no-retry` | `false` | Disable RPC retries |
| `--chunk-max-entries` | `200` | Max memories per `AddMemoriesBatch` request |
| `--chunk-max-bytes` | `4194304` | Max prost-encoded bytes per request |

The client naturally applies back-pressure by awaiting each chunk. Retryable
gRPC errors and PostgreSQL serialization/deadlock SQLSTATEs are retried with
exponential backoff. `AddMemoriesBatch` is idempotent by external ID, so retries
do not create duplicates.

## External IDs

Sessions use `source:<thread-creator-id>:<source-specific-id>`. If that form
would exceed the database's 512-byte limit, it deterministically becomes
`source:<thread-creator-id>:~<sha256>`. Migrate existing data with
`migrate-memory-kind apply` and `verify` during the maintenance window before
deploying this importer; it does not query or reuse legacy external IDs.

For long-running imports, glibc arena fragmentation can accumulate RSS. Consider
setting:

```bash
MALLOC_ARENA_MAX=2 memories-import claude-code --all-projects ...
```

## Related Documentation

- [workflows/thread-summary/README.md](workflows/thread-summary/README.md)
- [workflows/personality/README.md](workflows/personality/README.md)
- [workflows/agent-chat-pipeline/README.md](workflows/agent-chat-pipeline/README.md)
