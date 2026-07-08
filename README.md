# memory-store

Memory management service for LLM applications. It provides persistent
conversation context, vector semantic search, thread-based conversation
management, media storage, and reflection search through gRPC APIs.

Japanese: [README_ja.md](README_ja.md)

## Features

- **Memory CRUD**: store, search, and manage conversation messages with roles,
  content types, parent links, metadata, and `media_object` references.
- **Thread management**: conversation threads, thread-memory relations, labels,
  ancestor resolution, and batch import RPCs.
- **System prompts**: represented as `Memory` records with
  `role = ROLE_SYSTEM`; there is no dedicated system-prompt table/API.
- **Vector and full-text search**: LanceDB semantic search, BM25 full-text
  search, and hybrid search. The LanceDB stack is always included.
- **Thread and reflection search**: vector/FTS search over thread descriptions
  and reflection intent.
- **Media management**: images and other media are represented by
  `media_object` records and stored through file/S3/url/inline backends.
- **Automatic embedding generation**: jobworkerp dispatch for memory, thread,
  image, and reflection embeddings.
- **RAG tools**: search workers and function sets can be registered in
  jobworkerp at startup.

## Architecture

```text
Client (gRPC / gRPC-Web)
    |
    v
grpc-admin (tonic gRPC server, binary: front)
    |
    v
app (business logic + Stretto cache)
    |
    +-- infra (RDB: SQLite/PostgreSQL)
    +-- infra::memory_vector / thread_vector / reflection_vector (LanceDB)
    +-- jobworkerp-client (workflow dispatch / RAG tool registration)
```

## Workspace

| Crate | Role |
|---|---|
| `protobuf` | gRPC and Protocol Buffer definitions |
| `infra` | Data access layer for SQLx and LanceDB |
| `app` | Business logic and cache management |
| `grpc-admin` | gRPC server `front` and operation batches |
| `agent-chat-import` | CLI for importing agent conversation logs |
| `modules/command-utils` | Shared CLI utilities |
| `modules/infra-utils` | Database and infrastructure utilities |
| `modules/memory-utils` | Memory cache utilities based on Stretto |
| `modules/jobworkerp-client` | jobworkerp gRPC client, path dependency for the workspace crates |

## gRPC Services

| Service | Main RPCs |
|---|---|
| **MemoryService** | Create, Update, Delete, Find, FindList, FindListByCondition, Count, CountByCondition, UpdateContentNoDispatch |
| **ThreadService** | Create, Update, Delete, AddMemory, AddMemoriesBatch, FindMemoriesByThreadId, ResolveAncestorClosure, label RPCs |
| **MemoryRatingService** | Create, Upsert, Update, Delete, Find, FindByMemoryId, FindByUserId |
| **MemoryVectorService** | SearchByVector, SearchByText, HybridSearch, SearchSemantic, SearchByMedia, GetSurroundingMemories, BatchUpsertEmbeddings, RedispatchEmbeddings, CountSearchMatches |
| **ThreadVectorService** | SearchByVector, SearchByText, HybridSearch, BatchUpsertEmbeddingsRows, RedispatchEmbeddings, GetIndexStats, CountSearchMatches |
| **MediaService** | Upload, Register, Find, Resolve, Delete |
| **ReflectionService** | Generate, FinalizeReflection, Search, FindSimilarTrajectories, MatchFailureSignatures, aggregate/stat RPCs |
| **ReflectionVectorService** | BatchUpsertIntentEmbeddings, RedispatchReflectionEmbeddings, RebuildIntentIndex, GetIntentIndexStats |

Proto definitions live under `protobuf/protobuf/llm_memory/`.

## Build

```bash
# Standard build with SQLite and LanceDB vector search
cargo build --release

# PostgreSQL support
cargo build --release --features postgres --no-default-features

# Lindera tokenizer support; dictionaries must be supplied at runtime
cargo build --release --features lindera
```

The LanceDB vector/FTS stack is a required dependency. The `lindera` feature
enables morphological tokenization for Japanese and Korean.

## Setup and Run

```bash
cp dot.env .env
# Edit .env: GRPC_ADDR, database settings, vector settings, and so on.

cargo run --release --bin front

# Run with the Japanese FTS tokenizer enabled.
cargo run --release --features lindera --bin front
```

`GRPC_ADDR` is required and has no code default. `dot.env` provides
`0.0.0.0:9000` as a local example.

## Configuration

### Database

| Variable | Default | Description |
|---|---|---|
| `SQLITE_URL` | `sqlite://test.sqlite3` | SQLite connection URL |
| `SQLITE_MAX_CONNECTIONS` | `20` | Maximum connection count |
| `SQLITE_DISABLE_WAL` | `false` | Set to `true` to disable SQLite WAL |

### gRPC Server

| Variable | Default | Description |
|---|---|---|
| `GRPC_ADDR` | none, required | gRPC listen address. `dot.env` uses `0.0.0.0:9000` |
| `USE_GRPC_WEB` | `false` in code, `true` in `dot.env` | Enable gRPC-Web |
| `MAX_FRAME_SIZE` | none | Maximum frame size. `dot.env` uses `16777215` |

### Vector Search

| Variable | Default | Description |
|---|---|---|
| `MEMORY_VECTOR_ENABLED` | `false` | Enable vector search |
| `MEMORY_LANCEDB_URI` | `data/lancedb/memories.lancedb` | LanceDB storage path |
| `MEMORY_LANCEDB_TABLE` | `memories` | Table name |
| `MEMORY_VECTOR_SIZE` | none | Embedding dimension, required when vectors are enabled |
| `MEMORY_DISTANCE_TYPE` | `cosine` | `cosine`, `l2`, or `dot` |
| `MEMORY_OPTIMIZE_COMPACT_INTERVAL` | `1000` | Compact + index optimization interval; `0` disables it |
| `MEMORY_OPTIMIZE_PRUNE_INTERVAL` | `100` | LanceDB manifest prune interval; `0` disables it |
| `MEMORY_OPTIMIZE_PRUNE_OLDER_THAN_SECS` | `300` | Manifest history retention in seconds |
| `MEMORY_OPTIMIZE_PRUNE_ON_STARTUP` | `true` | Run prune once at startup |
| `MEMORY_VECTOR_INDEX_ENABLED` | `true` | Enable ANN index creation |
| `MEMORY_VECTOR_INDEX_MIN_ROWS` | `256` | Minimum row count before ANN index creation |
| `MEMORY_VECTOR_INDEX_NPROBES` | `20` | IVF probe count |

`MEMORY_AUTO_OPTIMIZE_INTERVAL` is obsolete. If it is set, it is ignored and a
migration warning is logged at startup.

### Thread Vector Search

| Variable | Default | Description |
|---|---|---|
| `THREAD_VECTOR_ENABLED` | `false` | Enable real search RPCs for `ThreadVectorService` |
| `THREAD_VECTOR_SIZE` | `MEMORY_VECTOR_SIZE` | Thread embedding dimension |
| `THREAD_LANCEDB_URI` | `MEMORY_LANCEDB_URI` | LanceDB URI for thread vectors |
| `THREAD_LANCEDB_TABLE` | `threads` | Thread vector table |
| `THREAD_DISTANCE_TYPE` | `MEMORY_DISTANCE_TYPE` | Distance function |
| `THREAD_OPTIMIZE_*` / `THREAD_VECTOR_INDEX_*` | `MEMORY_*` | Maintenance and index settings for thread vectors |

### FTS Tokenizer

| Variable | Default | Description |
|---|---|---|
| `MEMORY_FTS_TOKENIZER` | build-dependent | `lindera/ipadic` with the `lindera` feature, otherwise `ngram` |
| `MEMORY_FTS_NGRAM_MIN` / `MEMORY_FTS_NGRAM_MAX` | `2` / `3` | ngram tokenizer sizes |
| `MEMORY_FTS_FORCE_REBUILD` | `false` | Force FTS index rebuild |
| `LANCE_LANGUAGE_MODEL_HOME` | none | Lindera dictionary root |

### Automatic Embedding Generation

Worker definitions are managed in YAML. Memories-specific workers are described
in [yaml-workers.md](yaml-workers.md), and the general YAML format is described
in [modules/jobworkerp-client/docs/worker-yaml.md](modules/jobworkerp-client/docs/worker-yaml.md).
To change embedding models, edit `workflows/auto-embedding-workers.yaml`
directly. The actual model name used to generate vectors is read from the
runner's `model_info.model_name` and recorded in vector metadata.

| Variable | Default | Description |
|---|---|---|
| `MEMORY_AUTO_EMBEDDING_ENABLED` | `false` | Enable automatic embedding generation |
| `JOBWORKERP_ADDR` | none | jobworkerp gRPC address |
| `MEMORY_EMBEDDING_TIMEOUT_SEC` | `120` | Job timeout in seconds |
| `MEMORY_EMBEDDING_MAX_CONTENT_LEN` | `8192` | Maximum input text length; longer text is truncated |
| `MEMORY_WORKERS_YAML` | `<infra crate>/../workflows/auto-embedding-workers.yaml` | Memory worker YAML |
| `MEMORY_THREAD_WORKERS_YAML` | `<infra crate>/../workflows/auto-thread-embedding-workers.yaml` | Thread worker YAML |
| `MEMORY_GRPC_HOST` | required | Host that embedding workflows use to call back into this server |
| `MEMORY_GRPC_PORT` | required | Callback port |
| `MEMORY_MM_EMBEDDING_WORKER` | `memories-mm-embedding` | Shared jobworkerp worker for text, image, and query embeddings |
| `MEMORY_IMAGE_WORKERS_YAML` | `workflows/auto-image-embedding-workers.yaml` | Image embedding worker YAML |

`GRPC_ADDR` is the listen address for this process. `MEMORY_GRPC_HOST` and
`MEMORY_GRPC_PORT` are the routable callback endpoint used by jobworkerp
workflows. They often differ; for example, `0.0.0.0` is valid for listening but
is not a valid remote callback target.

### RAG Tools

| Variable | Default | Description |
|---|---|---|
| `MEMORY_RAG_TOOLS_ENABLED` | `false` | Register search workers/function sets in jobworkerp at startup |
| `MEMORY_RAG_MANIFEST_YAML` | `workflows/rag-tools-manifest.yaml` | RAG tool manifest |
| `MEMORY_RAG_CHANNEL` | `rag` | jobworkerp channel for RAG workers |

`MEMORY_RAG_TOOLS_ENABLED=true` requires `MEMORY_AUTO_EMBEDDING_ENABLED=true`,
`JOBWORKERP_ADDR`, `MEMORY_GRPC_HOST`, and `MEMORY_GRPC_PORT`.

### Media and Image Memory

| Variable | Default | Description |
|---|---|---|
| `MEDIA_STORAGE_BACKEND` | `file` | `file`, `s3`, `url`, or `inline` |
| `MEDIA_STORAGE_LOCAL_DIR` | `./media` | Root directory for the file/url backend |
| `MEDIA_STORAGE_S3_*` | none | S3/minio backend settings |
| `MEDIA_PRESIGN_TTL_SEC` | `900` | Presigned GET TTL for Resolve/Find |
| `MEDIA_UPLOAD_MAX_BYTES` | `20971520` | Upload size limit in bytes |
| `MEMORY_IMAGE_SEARCH_MODE` | `none` | `none`, `multimodal`, `vlm_caption`, or `both` |
| `MEDIA_GC_GRACE_SEC` | `3600` | Grace period for orphan media GC |

`MEDIA_STORAGE_BACKEND=inline` is for tests only. The process fails fast when it
is combined with `MEMORY_IMAGE_SEARCH_MODE != none`.

### Reflection

| Variable | Default | Description |
|---|---|---|
| `REFLECTION_INTENT_VECTOR_ENABLED` | `false` | Enable the reflection intent vector store |
| `REFLECTION_FS_MATCH_SCAN_CAP` | `1000` | RDB scan cap for `MatchFailureSignatures` |
| `REFLECTION_USER_ID` | `300000` | Owner user ID for reflection memories |
| `REFLECTION_LANCEDB_URI` | `${MEMORY_LANCEDB_URI}/reflection_intent` | Reflection intent vector store URI |
| `REFLECTION_VECTOR_SIZE` | `MEMORY_VECTOR_SIZE` | Intent embedding dimension |
| `MEMORY_REFLECTION_DISPATCH_ENABLED` | `false` | Enable reflection generation and intent embedding dispatch |
| `MEMORY_REFLECTION_REFLECTOR_MODEL` | none | Model used to generate reflections |
| `MEMORY_REFLECTION_REFLECTOR_BASE_URL` | none | LLM endpoint for reflection generation |
| `REFLECTION_DEFAULT_LANGUAGE` | `ja` | Default reflection language |

See `dot.env` for the full configuration surface.

## Vector Search

Vector search supports three modes:

- **Vector search**: cosine similarity for embeddings, with L2 and dot-product
  options. Multi-vector aggregation supports Sum, Average, Max,
  WeightedByPosition, and RankFusion.
- **Full-text search**: LanceDB BM25 FTS. The default tokenizer is
  `lindera/ipadic` with the `lindera` feature and `ngram` otherwise.
- **Hybrid search**: vector + text search with RRF, weighted score blending,
  vector-then-FTS reranking, or FTS-then-vector reranking.

Memory, thread, and reflection intent vector stores use an N-row chunk schema:
one logical entity can produce multiple rows keyed by `vector_kind` and
`chunk_index`. A schema fingerprint mismatch fails at startup.

Operational guides:

- [docs/vectordb-rebuild-runbook.md](docs/vectordb-rebuild-runbook.md)
- [docs/vectordb-optimize-tuning.md](docs/vectordb-optimize-tuning.md)

## Tests

```bash
cargo test --workspace --all-targets -- --test-threads=1
cargo test -p app -- --test-threads=1
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy --workspace --all-targets --features postgres --no-default-features -- -D warnings
```

Keep `--test-threads=1` to avoid contention on SQLite and other shared
resources.

## Database Schema

| Table | Description |
|---|---|
| `memory` | Memory records: content, role, content type, parents, metadata, `media_object_id`, and so on |
| `thread` | Conversation threads: user, title, description, channel, metadata, default system memory |
| `memory_rating` | Memory ratings; unique by `(memory_id, user_id)` |
| `thread_memory` | Many-to-many thread-memory relation with position |
| `thread_label` | Thread labels |
| `media_object` | Media metadata, storage references, and GC state |
| `thread_reflection_index` and related tables | Sidecar tables for reflection search and aggregation |

`memory.thread_id` and the `system_prompt` table have been removed. Thread
membership is represented only by `thread_memory`.

Schema files:

- `infra/sql/sqlite/001_schema.sql`
- `infra/sql/sqlite/003_reflection_schema.sql`
- `infra/sql/sqlite/004_media_object.sql`
- PostgreSQL mirrors under `infra/sql/postgres/`

## Operation Batches

`grpc-admin` provides operation binaries in addition to the gRPC server.

```bash
cargo run --release --bin cleanup-orphan-media -- --help
```

## Documentation

- [yaml-workers.md](yaml-workers.md): memories-specific jobworkerp worker YAML
- [agent-chat-import/README.md](agent-chat-import/README.md): import CLI and generation worker registration
- [docs/image-memory-operations-guide.md](docs/image-memory-operations-guide.md): image memory operations

## License

MIT
