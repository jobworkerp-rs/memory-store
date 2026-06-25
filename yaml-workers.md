# memories Auto-Embedding Worker YAML

Japanese: [yaml-workers_ja.md](yaml-workers_ja.md)

This document covers memories-specific jobworkerp worker YAML. For the generic
YAML format, environment interpolation, and `$file:` include behavior, see
[modules/jobworkerp-client/docs/worker-yaml.md](modules/jobworkerp-client/docs/worker-yaml.md).

## Files and Responsibilities

| File | Registered workers | Role |
|---|---|---|
| `workflows/auto-embedding-workers.yaml` | `memories-mm-embedding`, `memories-upsert-embedding`, `memories-auto-embedding` | Memory vector dispatcher workers and the source of truth for the shared multimodal embedding worker |
| `workflows/auto-thread-embedding-workers.yaml` | `memories-upsert-thread-embedding`, `memories-auto-thread-embedding` | Thread vector dispatcher workers |

| Worker | Runner | Role |
|---|---|---|
| `memories-mm-embedding` | `MultimodalEmbeddingRunner` | Embeds text and images into a shared vector space; shared by memory, thread, reflection, and image paths |
| `memories-upsert-embedding` | `GRPC` | Calls `MemoryVectorService/BatchUpsertEmbeddings` |
| `memories-upsert-thread-embedding` | `GRPC` | Calls `ThreadVectorService/UpsertEmbedding` |
| `memories-auto-embedding` / `memories-auto-thread-embedding` | `WORKFLOW` | Entry points enqueued by dispatchers |

Workflow bodies are included from `auto-embedding.yaml` and
`auto-thread-embedding.yaml` with `$file:`. The workflow copies
`.model_info.model_name` from runner output into vector metadata as
`embedding_model`, falling back to `"unknown"`.

## Shared Embedding Worker

`memories-mm-embedding` is defined only in memory worker YAML. Thread,
reflection, and image dispatchers load memory worker YAML as a prerequisite
before registering their own workers. This keeps model settings in one place and
prevents last-write-wins drift from duplicate worker definitions.

Repeated prerequisite registration is idempotent because the YAML content is the
same.

## memories-Specific Environment Variables

| Variable | Default | Description |
|---|---|---|
| `MEMORY_WORKERS_YAML` | `<infra crate>/../workflows/auto-embedding-workers.yaml` | Memory worker YAML path |
| `MEMORY_THREAD_WORKERS_YAML` | `<infra crate>/../workflows/auto-thread-embedding-workers.yaml` | Thread worker YAML path |
| `MEMORY_GRPC_HOST` | required | Routable host that embedding workflows use to call back into memories |
| `MEMORY_GRPC_PORT` | required | Callback port |
| `MEMORY_GRPC_ADDR` | unsupported | Old `host:port` form; startup errors and asks for host/port split |
| `MEMORY_EMBEDDING_TIMEOUT_SEC` | `120` | jobworkerp job timeout |
| `MEMORY_EMBEDDING_MAX_CONTENT_LEN` | `8192` | Input text truncation limit |
| `MEMORY_MM_EMBEDDING_WORKER` | `memories-mm-embedding` | Shared worker name used by registration and query paths |

Embedding model selection is done by editing YAML. There is no
`MEMORY_EMBEDDING_MODEL` environment variable. Only the worker name is
centralized through `MEMORY_MM_EMBEDDING_WORKER`.

## `GRPC_ADDR` vs Callback Host/Port

`GRPC_ADDR` is the listen address for this process. `MEMORY_GRPC_HOST` and
`MEMORY_GRPC_PORT` are the callback endpoint jobworkerp can reach. They must be
configured independently because `0.0.0.0` is valid for listening but invalid as
a remote target, and jobworkerp may run in another host or namespace.

## Changing the Model

1. Edit `memories-mm-embedding.settings` in
   `workflows/auto-embedding-workers.yaml`.
2. Update `model_id`, and `tokenizer_model_id` if needed.
3. Adjust device, dtype, sequence length, and other runner settings.
4. Set `MEMORY_VECTOR_SIZE` to the model-reported embedding dimension.
5. Restart memories so worker YAML is reloaded and upserted.
6. Reindex existing vectors when the embedding space changes.

## Related Files

- `workflows/auto-embedding-workers.yaml`
- `workflows/auto-thread-embedding-workers.yaml`
- `workflows/auto-embedding.yaml`
- `workflows/auto-thread-embedding.yaml`
- `infra/src/infra/embedding_dispatch.rs`
- `infra/src/infra/memory_vector/dispatcher.rs`
- `infra/src/infra/thread_vector/dispatcher.rs`
- `modules/jobworkerp-client/docs/worker-yaml.md`
