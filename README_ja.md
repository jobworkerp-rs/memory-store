# memory-store

LLM アプリケーション向けのメモリ管理サービスです。gRPC API を通じて、永続的な
会話コンテキスト、ベクトル意味検索、スレッド単位の会話管理、メディア保存、
およびリフレクション検索を提供します。

English: [README.md](README.md)

## 機能

- **メモリ CRUD**: ロール、コンテンツ種別、親リンク、メタデータ、`media_object`
  参照を含む会話メッセージの保存、検索、管理を行います。
- **スレッド管理**: 会話スレッド、スレッドとメモリの関連、ラベル、祖先解決、
  一括インポート RPC を提供します。
- **システムプロンプト**: `role = ROLE_SYSTEM` を持つ `Memory` レコードとして表現します。
  専用のシステムプロンプト用テーブルや API はありません。
- **ベクトル・全文検索**: LanceDB の意味検索、BM25 全文検索、ハイブリッド検索を
  提供します。LanceDB スタックは常に含まれます。
- **スレッド・リフレクション検索**: スレッドの説明およびリフレクションの意図に対する
  ベクトル検索・FTS を提供します。
- **メディア管理**: 画像などのメディアを `media_object` レコードとして表現し、
  file/S3/url/inline バックエンドで保存します。
- **埋め込みの自動生成**: メモリ、スレッド、画像、リフレクションの埋め込み生成を
  jobworkerp へディスパッチします。
- **RAG ツール**: 検索ワーカーと関数セットを、起動時に jobworkerp へ登録できます。

## アーキテクチャ

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

## ワークスペース

| クレート | 役割 |
|---|---|
| `protobuf` | gRPC および Protocol Buffer 定義 |
| `infra` | SQLx と LanceDB のデータアクセス層 |
| `app` | 業務ロジックおよびキャッシュ管理 |
| `grpc-admin` | gRPC サーバー `front` と操作バッチ |
| `agent-chat-import` | エージェント会話ログをインポートする CLI |
| `modules/command-utils` | 共通 CLI ユーティリティ |
| `modules/infra-utils` | データベースおよびインフラストラクチャ用ユーティリティ |
| `modules/memory-utils` | Stretto ベースのメモリキャッシュ用ユーティリティ |
| `modules/jobworkerp-client` | jobworkerp gRPC クライアント。ワークスペース内クレートのパス依存関係 |

## gRPC サービス

| サービス | 主な RPC |
|---|---|
| **MemoryService** | Create, Update, Delete, Find, FindList, FindListByCondition, Count, CountByCondition, UpdateContentNoDispatch |
| **ThreadService** | Create, Update, Delete, AddMemory, AddMemoriesBatch, FindMemoriesByThreadId, ResolveAncestorClosure、ラベル RPC |
| **MemoryRatingService** | Create, Upsert, Update, Delete, Find, FindByMemoryId, FindByUserId |
| **MemoryVectorService** | SearchByVector, SearchByText, HybridSearch, SearchSemantic, SearchByMedia, GetSurroundingMemories, BatchUpsertEmbeddings, RedispatchEmbeddings, CountSearchMatches |
| **ThreadVectorService** | SearchByVector, SearchByText, HybridSearch, BatchUpsertEmbeddingsRows, RedispatchEmbeddings, GetIndexStats, CountSearchMatches |
| **MediaService** | Upload, Register, Find, Resolve, Delete |
| **ReflectionService** | Generate, FinalizeReflection, Search, FindSimilarTrajectories, MatchFailureSignatures、集約・統計 RPC |
| **ReflectionVectorService** | BatchUpsertIntentEmbeddings, RedispatchReflectionEmbeddings, RebuildIntentIndex, GetIntentIndexStats |

Proto 定義は `protobuf/protobuf/llm_memory/` にあります。

## ビルド

```bash
# SQLite と LanceDB ベクトル検索を含む標準ビルド
cargo build --release

# PostgreSQL サポート
cargo build --release --features postgres --no-default-features

# Lindera トークナイザーのサポート。実行時に辞書を用意する必要があります
cargo build --release --features lindera
```

LanceDB のベクトル/FTS スタックは必須依存関係です。`lindera` 機能を有効にすると、
日本語および韓国語の形態素トークナイズが有効になります。

### SQLite マイグレーションバンドル

ローカルの Memories SQLite データベースを管理するデスクトップアプリケーションの開発者は、
Memories と同じソースリビジョンからネイティブのマイグレーションバンドルをビルドします。

```bash
CARGO_BUILD_JOBS=1 scripts/build-memories-db-migrate-sqlite.sh /path/to/memories-db-migrate
```

このビルドは Linux/x86_64 および macOS/Apple Silicon をサポートします。ネイティブの
`memories-db-migrate` バイナリと、対応する SHA-256 固定済み Atlas バイナリを
パッケージ化します。データベースを開く前に、すべてのデータベース書き込み処理を停止し、
SQLite/LanceDB の一体化したバックアップを利用可能な状態にしてからバンドルを実行してください。
アプリケーションは SQL を適用したり Atlas を直接実行したりせず、バンドル内の
`memories-db-migrate` コマンドを呼び出す必要があります。

## セットアップと実行

```bash
cp dot.env .env
# .env を編集します: GRPC_ADDR、データベース設定、ベクトル設定など。

cargo run --release --bin front

# 日本語 FTS トークナイザーを有効にして実行します。
cargo run --release --features lindera --bin front
```

`GRPC_ADDR` は必須であり、コード上のデフォルト値はありません。`dot.env` にはローカル用の
例として `0.0.0.0:9000` が設定されています。

## 設定

### データベース

| 変数 | デフォルト | 説明 |
|---|---|---|
| `SQLITE_URL` | `sqlite://test.sqlite3` | SQLite 接続 URL |
| `SQLITE_MAX_CONNECTIONS` | `20` | 最大接続数 |
| `SQLITE_DISABLE_WAL` | `false` | `true` に設定すると SQLite WAL を無効化 |

### gRPC サーバー

| 変数 | デフォルト | 説明 |
|---|---|---|
| `GRPC_ADDR` | なし、必須 | gRPC のリッスンアドレス。`dot.env` では `0.0.0.0:9000` |
| `SEARCH_INDEX_MAINTENANCE_GRPC_ADDR` | なし、必須 | メンテナンス用 gRPC のリッスンアドレス。`GRPC_ADDR` と同じ場合、メンテナンスサービスは通常のリスナーを共有します。ネットワーク分離が必要な場合は別アドレスを使用してください。`dot.env` では `0.0.0.0:9001` |
| `SEARCH_INDEX_MAINTENANCE_TASK_HISTORY_LIMIT` | `256` | `task_id` によるステータス照会用に保持する、完了済みメンテナンスタスクレコードの最大数 |
| `SEARCH_INDEX_MAINTENANCE_CHECK_DEADLINE_SECS` | なし、必須 | 読み取り専用インデックスチェックの正の期限（秒） |
| `SEARCH_INDEX_MAINTENANCE_BACKOFF_INITIAL_SECS` / `SEARCH_INDEX_MAINTENANCE_BACKOFF_MULTIPLIER` / `SEARCH_INDEX_MAINTENANCE_BACKOFF_MAX_SECS` | なし、必須 | 再試行バックオフ。期間は正、倍率は 1 以上、最大値は初期値以上である必要があります |
| `USE_GRPC_WEB` | コードでは `false`、`dot.env` では `true` | gRPC-Web を有効化 |
| `MAX_FRAME_SIZE` | なし | 最大フレームサイズ。`dot.env` では `16777215` |

### ベクトル検索

| 変数 | デフォルト | 説明 |
|---|---|---|
| `MEMORY_VECTOR_ENABLED` | `false` | ベクトル検索を有効化 |
| `MEMORY_LANCEDB_URI` | `data/lancedb/memories.lancedb` | LanceDB の保存パス |
| `MEMORY_LANCEDB_TABLE` | `memories` | テーブル名 |
| `MEMORY_VECTOR_SIZE` | なし | 埋め込み次元数。ベクトルを有効にする場合は必須 |
| `MEMORY_DISTANCE_TYPE` | `cosine` | `cosine`、`l2`、または `dot` |
| `MEMORY_INDEX_UPDATE_INTERVAL_SECS` / `MEMORY_COMPACTION_INTERVAL_SECS` / `MEMORY_PRUNE_INTERVAL_SECS` | なし、必須 | 自動メンテナンス間隔（秒）。`0` は対応する処理を無効化 |
| `MEMORY_PRUNE_OLDER_THAN_SECS` | なし、必須 | 古いマニフェストの保持期間（秒）。`0` では即時に対象となることを許可 |
| `MEMORY_INDEX_UPDATE_UNINDEXED_ROWS` | なし、必須 | インデックス更新の行数しきい値。`0` はこの候補を無効化 |
| `MEMORY_VECTOR_INDEX_ENABLED` | `true` | ANN インデックス作成を有効化 |
| `MEMORY_VECTOR_INDEX_MIN_ROWS` | `256` | ANN インデックス作成前の最小行数 |
| `MEMORY_VECTOR_INDEX_NPROBES` | `20` | IVF プローブ数 |

`*_AUTO_OPTIMIZE_INTERVAL` および `*_OPTIMIZE_*` はサポートされなくなりました。
これらの操作回数および起動時 prune のセマンティクスは安全に変換できないため、値を設定すると
設定エラーとして起動を停止します。

### スレッドベクトル検索

| 変数 | デフォルト | 説明 |
|---|---|---|
| `THREAD_VECTOR_ENABLED` | `false` | `ThreadVectorService` の実際の検索 RPC を有効化 |
| `THREAD_VECTOR_SIZE` | `MEMORY_VECTOR_SIZE` | スレッド埋め込みの次元数 |
| `THREAD_LANCEDB_URI` | `MEMORY_LANCEDB_URI` | スレッドベクトル用 LanceDB URI |
| `THREAD_LANCEDB_TABLE` | `threads` | スレッドベクトルテーブル |
| `THREAD_DISTANCE_TYPE` | `MEMORY_DISTANCE_TYPE` | 距離関数 |
| `THREAD_INDEX_UPDATE_INTERVAL_SECS` / `THREAD_COMPACTION_INTERVAL_SECS` / `THREAD_PRUNE_INTERVAL_SECS` / `THREAD_PRUNE_OLDER_THAN_SECS` / `THREAD_INDEX_UPDATE_UNINDEXED_ROWS` | なし、必須 | スレッドテーブルのメンテナンスポリシー。`MEMORY_*` と同じ `0` のセマンティクスを使用 |

### FTS トークナイザー

| 変数 | デフォルト | 説明 |
|---|---|---|
| `MEMORY_FTS_TOKENIZER` | ビルド依存 | `lindera` 機能ありでは `lindera/ipadic`、それ以外では `ngram` |
| `MEMORY_FTS_NGRAM_MIN` / `MEMORY_FTS_NGRAM_MAX` | `2` / `3` | ngram トークナイザーのサイズ |
| `MEMORY_FTS_FORCE_REBUILD` | `false` | 次回の maintenance FTS build で既存 index を強制再構築する。完了後に必ず `false` へ戻す |
| `LANCE_LANGUAGE_MODEL_HOME` | なし | Lindera 辞書のルート |

### 埋め込みの自動生成

ワーカー定義は YAML で管理されます。Memories 固有のワーカーは
[yaml-workers.md](yaml-workers.md)、一般的な YAML 形式は
[modules/jobworkerp-client/docs/worker-yaml.md](modules/jobworkerp-client/docs/worker-yaml.md)
で説明しています。埋め込みモデルを変更するには、
`workflows/auto-embedding-workers.yaml` を直接編集してください。ベクトル生成で実際に
使用されるモデル名はランナーの `model_info.model_name` から読み取り、ベクトルメタデータに
記録されます。

| 変数 | デフォルト | 説明 |
|---|---|---|
| `MEMORY_AUTO_EMBEDDING_ENABLED` | `false` | 埋め込みの自動生成を有効化 |
| `JOBWORKERP_ADDR` | なし | jobworkerp gRPC アドレス |
| `MEMORY_EMBEDDING_TIMEOUT_SEC` | `120` | ジョブのタイムアウト（秒） |
| `MEMORY_EMBEDDING_MAX_CONTENT_LEN` | `8192` | 入力テキストの最大長。より長いテキストは切り詰め |
| `MEMORY_WORKERS_YAML` | `<infra crate>/../workflows/auto-embedding-workers.yaml` | メモリワーカー YAML |
| `MEMORY_THREAD_WORKERS_YAML` | `<infra crate>/../workflows/auto-thread-embedding-workers.yaml` | スレッドワーカー YAML |
| `MEMORY_GRPC_HOST` | 必須 | 埋め込みワークフローがこのサーバーへコールバックするために使うホスト |
| `MEMORY_GRPC_PORT` | 必須 | コールバックポート |
| `MEMORY_MM_EMBEDDING_WORKER` | `memories-mm-embedding` | テキスト・画像・クエリの埋め込みで共有する jobworkerp ワーカー |
| `MEMORY_EMBEDDING_DOCUMENT_PREFIX` | 未設定 | 非対応: 現行ランナーではソースオフセットを維持しつつ各チャンクへ適用できないため、空でない値は起動時に拒否されます |
| `MEMORY_EMBEDDING_QUERY_PREFIX` | 未設定 | 埋め込み前に意味検索、ハイブリッド検索、RAG、意図クエリのテキストへ付加する接頭辞。RAG は登録時に埋め込むため、jobworkerp 側での定義は不要 |
| `MEMORY_IMAGE_WORKERS_YAML` | `workflows/auto-image-embedding-workers.yaml` | 画像埋め込みワーカー YAML |

`GRPC_ADDR` はこのプロセスのリッスンアドレスです。`MEMORY_GRPC_HOST` と
`MEMORY_GRPC_PORT` は jobworkerp ワークフローが使用する到達可能なコールバックエンドポイントです。
両者は異なることが多く、たとえば `0.0.0.0` はリッスンには有効ですが、リモートコールバックの
宛先としては有効ではありません。

### RAG ツール

| 変数 | デフォルト | 説明 |
|---|---|---|
| `MEMORY_RAG_TOOLS_ENABLED` | `false` | 起動時に検索ワーカー/関数セットを jobworkerp に登録 |
| `MEMORY_RAG_MANIFEST_YAML` | `workflows/rag-tools-manifest.yaml` | RAG ツールマニフェスト |
| `MEMORY_RAG_CHANNEL` | `rag` | RAG ワーカー用 jobworkerp チャネル |

`MEMORY_RAG_TOOLS_ENABLED=true` には `MEMORY_AUTO_EMBEDDING_ENABLED=true`、
`JOBWORKERP_ADDR`、`MEMORY_GRPC_HOST`、`MEMORY_GRPC_PORT` が必要です。

### メディアと画像メモリ

| 変数 | デフォルト | 説明 |
|---|---|---|
| `MEDIA_STORAGE_BACKEND` | `file` | `file`、`s3`、`url`、または `inline` |
| `MEDIA_STORAGE_LOCAL_DIR` | `./media` | file/url バックエンドのルートディレクトリ |
| `MEDIA_STORAGE_S3_*` | なし | S3/minio バックエンド設定 |
| `MEDIA_PRESIGN_TTL_SEC` | `900` | Resolve/Find 用の事前署名 GET の TTL |
| `MEDIA_UPLOAD_MAX_BYTES` | `20971520` | アップロードサイズ上限（バイト） |
| `MEMORY_IMAGE_SEARCH_MODE` | `none` | `none`、`multimodal`、`vlm_caption`、または `both` |
| `MEDIA_GC_GRACE_SEC` | `3600` | 孤立メディア GC の猶予期間 |

`MEDIA_STORAGE_BACKEND=inline` はテスト専用です。
`MEMORY_IMAGE_SEARCH_MODE != none` と組み合わせると、プロセスは即座に失敗します。

### リフレクション

| 変数 | デフォルト | 説明 |
|---|---|---|
| `REFLECTION_INTENT_VECTOR_ENABLED` | `false` | リフレクション意図ベクトルストアを有効化 |
| `REFLECTION_FS_MATCH_SCAN_CAP` | `1000` | `MatchFailureSignatures` の RDB スキャン上限 |
| `REFLECTION_LANCEDB_URI` | `${MEMORY_LANCEDB_URI}/reflection_intent` | リフレクション意図ベクトルストア URI |
| `REFLECTION_VECTOR_SIZE` | `MEMORY_VECTOR_SIZE` | 意図埋め込みの次元数 |
| `MEMORY_REFLECTION_DISPATCH_ENABLED` | `false` | リフレクション生成および意図埋め込みディスパッチを有効化 |
| `MEMORY_REFLECTION_REFLECTOR_MODEL` | なし | リフレクション生成に使用するモデル |
| `MEMORY_REFLECTION_REFLECTOR_BASE_URL` | なし | リフレクション生成用 LLM エンドポイント |
| `REFLECTION_DEFAULT_LANGUAGE` | `ja` | デフォルトのリフレクション言語 |

設定項目の全体は `dot.env` を参照してください。

## ベクトル検索

ベクトル検索は 3 つのモードをサポートします。

- **ベクトル検索**: 埋め込みのコサイン類似度を使用し、L2 および内積も選択できます。
  複数ベクトルの集約では、Sum、Average、Max、WeightedByPosition、RankFusion をサポートします。
- **全文検索**: LanceDB BM25 FTS。デフォルトのトークナイザーは、`lindera` 機能ありでは
  `lindera/ipadic`、それ以外では `ngram` です。
- **ハイブリッド検索**: RRF、重み付きスコアブレンド、ベクトル検索後の FTS 再ランキング、
  または FTS 後のベクトル再ランキングによるベクトル + テキスト検索です。

メモリ、スレッド、リフレクション意図のベクトルストアでは N 行チャンクスキーマを使用します。
1 つの論理エンティティから `vector_kind` と `chunk_index` をキーとする複数行が生成されます。
スキーマフィンガープリントが一致しない場合、起動時に失敗します。

運用ガイド:

- [docs/vectordb-rebuild-runbook.md](docs/vectordb-rebuild-runbook.md)
- [docs/lancedb-search-index-maintenance-runbook_ja.md](docs/lancedb-search-index-maintenance-runbook_ja.md)

## テスト

```bash
cargo test --workspace --all-targets -- --test-threads=1
cargo test -p app -- --test-threads=1
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy --workspace --all-targets --features postgres --no-default-features -- -D warnings
```

SQLite などの共有リソースに対する競合を避けるため、`--test-threads=1` を維持してください。

## データベーススキーマ

| テーブル | 説明 |
|---|---|
| `memory` | メモリレコード: コンテンツ、ロール、コンテンツ種別、親、メタデータ、`media_object_id` など |
| `thread` | 会話スレッド: ユーザー、タイトル、説明、チャネル、メタデータ、デフォルトシステムメモリ |
| `memory_rating` | メモリ評価。`(memory_id, user_id)` で一意 |
| `thread_memory` | 位置を持つ、スレッドとメモリの多対多関連 |
| `thread_label` | スレッドラベル |
| `media_object` | メディアメタデータ、ストレージ参照、GC 状態 |
| `thread_reflection_index` および関連テーブル | リフレクション検索および集約用のサイドカーテーブル |

`memory.thread_id` および `system_prompt` テーブルは削除されました。スレッドへの所属は
`thread_memory` のみで表現されます。

スキーマファイル:

- `infra/sql/sqlite/001_schema.sql`
- `infra/sql/sqlite/003_reflection_schema.sql`
- `infra/sql/sqlite/004_media_object.sql`
- `infra/sql/postgres/` 配下の PostgreSQL 対応ファイル

## 操作バッチ

`grpc-admin` は gRPC サーバーに加えて、操作用バイナリを提供します。

```bash
cargo run --release --bin cleanup-orphan-media -- --help
```

## ドキュメント

- [yaml-workers.md](yaml-workers.md): memories 固有の jobworkerp ワーカー YAML
- [agent-chat-import/README.md](agent-chat-import/README.md): インポート CLI と生成ワーカー登録
- [docs/image-memory-operations-guide.md](docs/image-memory-operations-guide.md): 画像メモリの運用

## ライセンス

MIT
