# memories — auto-embedding worker YAML 構成

`memories` の auto-embedding パイプラインで使う jobworkerp worker は、起動時に YAML から読み込まれて jobworkerp サーバへ upsert される。**YAML フォーマット仕様 / API / env interpolation / `$file:` include の挙動などの一般仕様は `modules/jobworkerp-client/docs/worker-yaml.md` を参照**。本ドキュメントは memories 固有の構成のみを扱う。

## ファイル配置と worker の責務

| ファイル | 登録される worker | 役割 |
|---|---|---|
| `workflows/auto-embedding-workers.yaml` | `memories-mm-embedding`<br>`memories-upsert-embedding`<br>`memories-auto-embedding` | memory_vector dispatcher 用。共有 worker `memories-mm-embedding` の唯一の source of truth |
| `workflows/auto-thread-embedding-workers.yaml` | `memories-upsert-thread-embedding`<br>`memories-auto-thread-embedding` | thread_vector dispatcher 用。`memories-mm-embedding` はここでは定義しない |

| worker | runner | 役割 |
|---|---|---|
| `memories-mm-embedding` | `MultimodalEmbeddingRunner` | テキスト / 画像を共有ベクトル空間に embedding する。`embed_text` は長文を chunk 分割し chunk ごとに 1 embedding + 文字オフセットを返す。`model_info.model_name` を結果に乗せる。memory / thread / reflection 全 dispatcher で共有。worker 名は `%{MEMORY_MM_EMBEDDING_WORKER:-memories-mm-embedding}` で展開され、登録・参照・Rust query 経路すべてが同 env を見る |
| `memories-upsert-embedding` | `GRPC` | `MemoryVectorService/BatchUpsertEmbeddings` を呼んで vector を永続化（N 行 rows パス対応） |
| `memories-upsert-thread-embedding` | `GRPC` | `ThreadVectorService/UpsertEmbedding` を呼んで thread vector を永続化（1 thread 1 ベクトル） |
| `memories-auto-embedding` / `memories-auto-thread-embedding` | `WORKFLOW` | 上記 2 step を順に実行。dispatcher が enqueue する入口 |

ワークフロー本体（`memories-auto-embedding` / `memories-auto-thread-embedding`）は worker YAML から `$file: auto-embedding.yaml` / `$file: auto-thread-embedding.yaml` で取り込む。ワークフロー側で `embedding_model` メタデータを runner 出力の `.model_info.model_name` から取得し、`upsertEmbedding` step の引数に渡す（fallback: `"unknown"`）。

## thread dispatcher の prerequisite ロード

`memories-mm-embedding` は memory 側 YAML だけが定義する。thread dispatcher を初期化すると、`infra/src/infra/thread_vector/dispatcher.rs::ThreadEmbeddingJobDispatcher::from_config` が memory 側 YAML を `prerequisite_yaml_paths` に積む。`EmbeddingDispatcherCore::get_or_init` は prerequisite を先に `register_workers_from_yaml` で登録してから自身の YAML を登録するため:

- thread dispatcher 単独（memory dispatcher が未初期化）でも `memories-mm-embedding` が必ず存在する
- thread 側 YAML で `memories-mm-embedding` を再定義しないので、UpsertByName の last-write-wins による設定ドリフトが発生しない（model 設定の運用変更は memory 側 YAML 1 か所で完結する）

同様に reflection summary / intent dispatcher も memory 側 YAML を prerequisite に積み、共有 `memories-mm-embedding` を使う（query ベクトルが格納済みベクトルと同一空間に乗る）。

両 dispatcher を順次初期化した場合、memory 側 YAML は 2 回 upsert されるが内容は同じなので冪等。

## 環境変数（memories 固有）

| 変数 | デフォルト | 説明 |
|---|---|---|
| `MEMORY_WORKERS_YAML` | `<infra crate>/../workflows/auto-embedding-workers.yaml` | memory 側 worker YAML のパス |
| `MEMORY_THREAD_WORKERS_YAML` | `<infra crate>/../workflows/auto-thread-embedding-workers.yaml` | thread 側 worker YAML のパス |
| `MEMORY_GRPC_HOST` | **必須**（デフォルトなし） | embedding ジョブが UpsertEmbedding を呼び戻すホスト。`MEMORY_AUTO_EMBEDDING_ENABLED=true` のとき未指定だと起動失敗する。接続先を暗黙にループバックへ落とすと cross-host デプロイで embedding 書き込みが silent loss するため fallback を設けない |
| `MEMORY_GRPC_PORT` | **必須**（デフォルトなし） | 同上のポート。同じ理由で fallback なし |
| `MEMORY_GRPC_ADDR` | （非対応） | 旧 `host:port` 形式。設定されていると起動時に明示エラーで `MEMORY_GRPC_HOST` / `MEMORY_GRPC_PORT` への分割を要求する（silent break 防止） |
| `MEMORY_EMBEDDING_TIMEOUT_SEC` | `120` | dispatcher が enqueue する `JobRequest.timeout`（秒） |
| `MEMORY_EMBEDDING_MAX_CONTENT_LEN` | `8192` | 入力テキストの最大文字数（超過分は truncate） |
| `MEMORY_MM_EMBEDDING_WORKER` | `memories-mm-embedding` | MultimodalEmbeddingRunner worker 名の唯一のソース。全 workflow YAML が `%{MEMORY_MM_EMBEDDING_WORKER:-memories-mm-embedding}` で参照し、Rust の query 経路（SearchSemantic / SearchByMedia / F-S8）も同 env を解決するため、保存と query の worker 名が常に一致する |

embedding モデルの選定は YAML を直接編集する（`MEMORY_EMBEDDING_MODEL` のような env はない）。worker **名**だけは `MEMORY_MM_EMBEDDING_WORKER` で一元的に切り替えられる（同一モデルを別 worker 名で登録した環境への対応）。

### `GRPC_ADDR` と `MEMORY_GRPC_HOST` / `MEMORY_GRPC_PORT` は別物

| 変数 | 方向 | 値の典型 |
|---|---|---|
| `GRPC_ADDR`（共通設定） | **listen 側**：本プロセスがバインドするアドレス | `0.0.0.0:9000` |
| `MEMORY_GRPC_HOST` / `MEMORY_GRPC_PORT` | **callback 側**：jobworkerp 上の embedding workflow が `memories-upsert-embedding` 経由で本サーバを呼び戻すための routable な host / port | `127.0.0.1` + `9000` ／ コンテナ名 + 内部 port ／ k8s Service 名 + Service port |

YAML 側（`workflows/auto-*-workers.yaml`）の GRPC worker 定義は `${MEMORY_GRPC_HOST}` / `${MEMORY_GRPC_PORT}` のみを参照し、`GRPC_ADDR` を直接読み出すことはありません。両者を独立に設定する必要があるのは、unspecified address（`0.0.0.0`）は listen 用には正当でも callback 先としては到達不能なため、また jobworkerp と memories が別ホスト・別 namespace で動く場合に callback 側のホスト名／port が listen 側と一致しないためです。listen と callback で host/port が偶然同じになる単一ホスト構成でも、明示的に両方設定する運用としています（silent な fallback 廃止）。

## モデル変更の手順

`workflows/auto-embedding-workers.yaml` の `memories-mm-embedding.settings` を編集する:

1. `model_id` を新モデルに更新（必要なら `tokenizer_model_id` も）
2. `device` / `dtype` / `max_sequence_length` など runner 固有 settings を新モデルに合わせる
3. `MEMORY_VECTOR_SIZE`（`dot.env` 側）を新モデルの次元数に合わせて更新（runner が報告する `model_info.embedding_dimension` と一致しないと起動時に fail-fast）
4. memories プロセスを再起動。既存の vector レコードはモデル変更前のもののため、必要なら別途 reindex する（text/image/caption の全 vector_kind が同一空間のため全再構築が原則）

YAML 変更は memories プロセスの再起動でのみ反映される（jobworkerp 側に upsert 済みの `WorkerData` は再読み込みされない）。

## 関連ファイル

- `workflows/auto-embedding-workers.yaml` / `workflows/auto-thread-embedding-workers.yaml` — worker 定義
- `workflows/auto-embedding.yaml` / `workflows/auto-thread-embedding.yaml` — workflow 定義（`$file:` で取り込み）
- `infra/src/infra/embedding_dispatch.rs` — dispatcher 共通コア（`prerequisite_yaml_paths` を含む）
- `infra/src/infra/memory_vector/dispatcher.rs` / `infra/src/infra/thread_vector/dispatcher.rs` — 各 dispatcher
- `infra/src/infra.rs::require_grpc_callback_env` — `MEMORY_GRPC_HOST` / `MEMORY_GRPC_PORT` の必須チェック（旧 `MEMORY_GRPC_ADDR` 検知も含む）。auto-embedding 有効時に `AppModule::new_by_env` から呼ばれる
- `modules/jobworkerp-client/docs/worker-yaml.md` — 一般 YAML 仕様（defaults / RetryPolicy / env interpolation / `$file:` include）
