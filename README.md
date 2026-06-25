# llm-memories

LLMアプリケーション向けのメモリ管理システム。会話コンテキストの永続化、ベクトルセマンティック検索、スレッドベースの会話管理をgRPC APIで提供します。

## 特徴

- **Memory CRUD** — 会話メッセージの保存・検索・管理（role、content_type、thread紐付け対応）
- **Thread管理** — 会話スレッドの作成・管理、スレッド削除時の子Memoryカスケード削除
- **SystemPrompt管理** — ユーザー単位のシステムプロンプトテンプレート
- **ベクトル検索** — LanceDBによるセマンティック検索、BM25全文検索、ハイブリッド検索 (常時有効)
- **自動Embedding生成** — jobworkerp連携によるMemory作成時の自動ベクトル化
- **プラグイン** — jobworkerpのプラグインランナーとして動作する `llm-memory-plugin`

## アーキテクチャ

```
Client (gRPC)
    │
    ▼
grpc-admin (gRPCサーバー, port 9010)
    │
    ▼
app (ビジネスロジック + キャッシュ)
    │
    ├── infra (RDB: SQLite/PostgreSQL)
    │
    └── infra/memory_vector (LanceDB ベクトルストア)

llm-memory-plugin (jobworkerpプラグイン, .so/.dylib)
    │
    └── gRPC経由で grpc-admin に接続
```

## ワークスペース構成

| クレート | 役割 |
|---------|------|
| `protobuf` | gRPC/Protocol Buffer定義 |
| `infra` | データアクセス層（SQLx, LanceDB） |
| `app` | ビジネスロジック、キャッシュ管理 |
| `grpc-admin` | gRPCサーバーエントリポイント |
| `llm-memory-plugin` | jobworkerpプラグイン（cdylib） |
| `modules/command-utils` | CLI共通ユーティリティ |
| `modules/infra-utils` | DB・インフラユーティリティ |
| `modules/memory-utils` | メモリキャッシュ（Stretto） |

## gRPCサービス

| サービス | 主なRPC |
|---------|--------|
| **MemoryService** | Create, Update, Delete, Find, FindList, Count |
| **ThreadService** | Create, Update, Delete, AddMemory, FindMemoriesByThreadId |
| **SystemPromptService** | Create, Update, Delete, Find, FindList |
| **MemoryRatingService** | Create, Update, Delete, Find（memory_id + user_idでユニーク） |
| **MemoryVectorService** | SearchByVector, SearchByText, HybridSearch, UpsertEmbedding, RebuildIndex |

Proto定義: `protobuf/protobuf/llm_memory/`

## ビルド

```bash
# 標準ビルド（SQLite + LanceDB ベクトル検索を含む）
cargo build --release

# PostgreSQL対応
cargo build --release --features postgres --no-default-features

# プラグインビルド（.so生成）
cargo build --release -p llm-memory-plugin
```

LanceDB によるベクトル/FTS スタックは必須依存です。

## セットアップと起動

```bash
# 設定ファイルを用意
cp dot.env .env
# .env を編集（GRPC_ADDR、DB設定、ベクトル検索設定など）

# gRPCサーバー起動
cargo run --release --bin front
```

## 設定（環境変数）

### データベース

| 変数 | デフォルト | 説明 |
|------|----------|------|
| `SQLITE_URL` | `sqlite://test.sqlite3` | SQLite接続URL |
| `SQLITE_MAX_CONNECTIONS` | `20` | 最大接続数 |

### gRPCサーバー

| 変数 | デフォルト | 説明 |
|------|----------|------|
| `GRPC_ADDR` | `0.0.0.0:9000` | gRPCリッスンアドレス |
| `USE_GRPC_WEB` | `true` | gRPC-Web有効化 |
| `MAX_FRAME_SIZE` | `16777215` | 最大フレームサイズ (16MB-1) |

### ベクトル検索

| 変数 | デフォルト | 説明 |
|------|----------|------|
| `MEMORY_VECTOR_ENABLED` | `false` | ベクトル検索の有効化 |
| `MEMORY_LANCEDB_URI` | — | LanceDBストレージパス |
| `MEMORY_LANCEDB_TABLE` | `memories` | テーブル名 |
| `MEMORY_VECTOR_SIZE` | — | Embedding次元数（必須） |
| `MEMORY_DISTANCE_TYPE` | `cosine` | 距離関数: `cosine`, `l2`, `dot` |
| `MEMORY_AUTO_OPTIMIZE_INTERVAL` | `50` | 自動最適化間隔 |

### 自動Embedding生成

ワーカ定義（runner_settings、retry_policy、response_type など）は YAML で管理します。memories 固有の構成は [`yaml-workers.md`](yaml-workers.md)、YAML フォーマットの一般仕様は `modules/jobworkerp-client/docs/worker-yaml.md` を参照してください。Embeddingモデル自体（`model_id` / `tokenizer_model_id` / `model_files`）の切り替えは `workflows/auto-embedding-workers.yaml` を直接編集します。実際にベクトルを生成したモデル名は runner の `model_info.model_name` から取得され、ベクトルレコードの `embedding_model` メタデータに自動的に記録されるため、別途 env 変数で指定する必要はありません。

| 変数 | デフォルト | 説明 |
|------|----------|------|
| `MEMORY_AUTO_EMBEDDING_ENABLED` | `false` | 自動Embedding生成の有効化 |
| `JOBWORKERP_ADDR` | — | jobworkerp gRPCアドレス |
| `MEMORY_EMBEDDING_TIMEOUT_SEC` | `120` | ジョブタイムアウト（秒） |
| `MEMORY_EMBEDDING_MAX_CONTENT_LEN` | `8192` | 入力テキストの最大文字数（超過分は切り詰め） |
| `MEMORY_WORKERS_YAML` | `<infra crate>/../workflows/auto-embedding-workers.yaml` | memory 側ワーカ定義 YAML |
| `MEMORY_THREAD_WORKERS_YAML` | `<infra crate>/../workflows/auto-thread-embedding-workers.yaml` | thread 側ワーカ定義 YAML |
| `MEMORY_GRPC_HOST` | **必須**（デフォルトなし） | embedding ジョブが UpsertEmbedding を呼び戻すホスト。`MEMORY_AUTO_EMBEDDING_ENABLED=true` のとき未指定だと起動失敗。silent な loopback fallback は廃止 |
| `MEMORY_GRPC_PORT` | **必須**（デフォルトなし） | 同上のポート |

#### `GRPC_ADDR` と `MEMORY_GRPC_HOST` / `MEMORY_GRPC_PORT` の使い分け

両者は紛らわしいが役割が異なるため、別個に設定する必要があります。

| 変数 | 役割 | 値の例 |
|------|------|--------|
| `GRPC_ADDR` | **listen 側**：本プロセス（grpc-admin / `front` バイナリ）が gRPC をバインドするアドレス | `0.0.0.0:9000`（全インターフェイスで listen） |
| `MEMORY_GRPC_HOST` / `MEMORY_GRPC_PORT` | **callback 側**：jobworkerp 上で実行される embedding workflow が、本サーバの `MemoryVectorService` / `ThreadVectorService` を呼び戻すために使う routable な host/port | `127.0.0.1` + `9000`（同一ホスト）／コンテナ名 + 内部 port／k8s Service 名 + Service port |

両者がしばしば異なる必要がある理由:

- `0.0.0.0` のような unspecified address は listen には使えるが、リモートからの接続先としては不正なので callback には使えない
- jobworkerp が本サーバと別ホスト・別 namespace で動く場合、callback は到達可能なホスト名/IP を別途指定する必要がある
- `MEMORY_GRPC_PORT` は listen 側の port と一致するのが通常だが、port-forward / service mesh / sidecar proxy が間に挟まる場合は別 port になり得る

設定の決定手順: まず jobworkerp プロセスから本サーバ (`grpc-admin`) へ届く host/port を運用環境の構成図上で特定 → その値を `MEMORY_GRPC_HOST` / `MEMORY_GRPC_PORT` に設定。`GRPC_ADDR` は本プロセスの listen 側だけを表すので、それとは独立に決めます。

全設定項目は `dot.env` を参照してください。

## ベクトル検索

ベクトル検索は3つのモードをサポートします:

- **ベクトル検索** — Embeddingのコサイン類似度（L2/Dot対応）。マルチベクトル集約対応（Sum, Average, Max, WeightedByPosition, RankFusion）
- **全文検索** — LanceDBのBM25全文検索
- **ハイブリッド検索** — ベクトル＋テキストの組み合わせ。4つの融合戦略:
  - **RRF** — Reciprocal Rank Fusion（デフォルト）
  - **Weighted** — 重み付きスコアブレンド
  - **VectorThenFTS** — ベクトル検索→テキスト再ランク
  - **FTSThenVector** — テキスト検索→ベクトル再ランク

詳細: `docs/hybrid-search-spec.md`, `docs/memories-vectordb-spec.md`

## プラグイン（llm-memory-plugin）

jobworkerpの `MultiMethodPluginRunner` として動作し、以下のメソッドを提供:

| メソッド | 説明 |
|---------|------|
| `store` | Memoryの保存 |
| `find_all` | Memory一覧取得 |
| `search` | ベクトル/テキスト/ハイブリッド検索 |

### CLI（テスト用）

```bash
cargo run -p llm-memory-plugin -- --server-url http://localhost:9010 store --prompt "Hello" --user-id 1
cargo run -p llm-memory-plugin -- find-all --limit 10
cargo run -p llm-memory-plugin -- search --query "topic"
```

## テスト

```bash
# 全テスト
cargo test --workspace --all-targets -- --test-threads=1

# 特定クレート
cargo test -p app -- --test-threads=1

# lint
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
```

`--test-threads=1` はSQLite等の共有リソースの競合回避のため必要です。

## データベーススキーマ

| テーブル | 説明 |
|---------|------|
| `memory` | メモリレコード（content, role, content_type, thread_id等）。role=ROLE_SYSTEM の Memory がシステムプロンプトを表現する |
| `thread` | 会話スレッド（default_system_memory_id, user_id, embedding等）。`default_system_memory_id` は AddMemory 時に parent_ids に自動注入される ROLE_SYSTEM Memory の ID（optional） |
| `system_prompt` | (legacy) システムプロンプトテンプレート。Phase 3 で廃止予定 |
| `memory_rating` | メモリ評価（memory_id + user_idでユニーク） |
| `thread_memory` | スレッドとメモリの多対多関連テーブル（thread_id, memory_id, position） |

スキーマ: `infra/sql/sqlite/001_schema.sql`

## ドキュメント

- `docs/auto-embedding-spec.md` — 自動Embedding生成の設計仕様
- `docs/hybrid-search-spec.md` — ハイブリッド検索戦略の仕様
- `docs/memories-vectordb-spec.md` — ベクトルDB設計仕様
- `docs/memories-vectordb-impl-spec.md` — ベクトルDB実装仕様
- `docs/vectordb-open-issues.md` — 既知の課題

## ライセンス

MIT
