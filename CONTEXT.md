# Memories

会話、メモリ、およびそれらを検索・要約するための thread を管理するコンテキストです。監査上の変更時刻と会話活動の時刻は別の概念として扱います。

## Language

**Thread 監査時刻**:
thread レコード自身の作成または変更をサーバーが記録した時刻。
_Avoid_: 会話更新時刻、最新メッセージ日時

**会話開始日時**:
現在 thread に所属する memory の中で最も早いメッセージ日時。
_Avoid_: thread 作成日時

**会話最新日時**:
現在 thread に所属する memory の中で最も遅いメッセージ日時。
_Avoid_: thread 更新日時

**派生 memory の業務時刻**:
要約などの派生 memory が、派生元の会話または対象期間を表すために保持する時刻。派生 memory 自身の監査時刻ではない。
_Avoid_: 派生 memory の生成日時、thread 監査時刻

**ThreadVector 投影**:
thread の検索に使う LanceDB 上の派生表現。RDB の thread 時刻と同じ意味を持つ。
_Avoid_: thread の正本

**既存 thread の legacy 監査時刻**:
時刻管理の正規化前から存在する thread の `created_at` / `updated_at`。真の監査時刻は復元できないため値を保持し、新規作成または実更新後から新しい監査時刻規則を適用する。
_Avoid_: 正規化済み監査時刻、メッセージ日時

**スキーマ移行**:
RDB の表、列、索引、制約を、順序付けられた版に従って変更する作業。レコード内容の変換や派生値の再計算は含まない。
_Avoid_: データ移行、バックフィル

**データ移行**:
既存レコードの内容、参照、または派生値を、再開可能なバッチ処理で補完・変換・検証する保守作業。
_Avoid_: スキーマ移行、DDL

**baseline**:
移行ツール導入前から存在する DB が、指定スキーマ版の要件を満たすことを検証して、以後のスキーマ移行管理へ取り込む操作。
_Avoid_: SQL 適用、レコード加工

**schema contract**:
アプリケーションが起動時の RDB スキーマ互換性を判定するための、移行カタログで管理された版情報。
_Avoid_: migration executor の履歴、schema の同一性検証結果

**schema verification**:
特定時点の RDB が migration directory、実 schema、静的 seed の要件を満たすかを確認する運用上の検証結果。DB に保持する永続的な状態ではない。
_Avoid_: schema contract、migration history

**静的 seed expectation**:
全環境で同じ値であるべき参照データの、版に対応付いた行・列・件数の期待値。利用者ごとの業務レコードは含めない。
_Avoid_: データ移行、業務データの完全性検査

**検証用 DB**:
スキーマ移行の期待状態を評価するためだけに用いる、対象 DB から分離された一時的な空の DB。
_Avoid_: 対象 DB、業務データの保管先

**検証 lease**:
検証用 DB の一時的な所有と回収状態を表す運用上の記録。対象 DB のスキーマや業務データには含めない。
_Avoid_: schema contract、migration history

**後続データ移行タスク**:
特定のスキーマ版を導入した後に選択され、独自の再開状態と検証を持つデータ移行の実装単位。
_Avoid_: スキーマ移行、Atlas revision history

**後続データ移行タスク世代**:
同じタスク ID における、処理契約と checkpoint の解釈が不変な版。タスク ID と世代の組が実行・完了の単位となる。
_Avoid_: 表示用の説明文の改訂、スキーマ移行版

**タスク実行 lease**:
一つの後続データ移行タスク世代を一つの executor だけが処理するための、一時的な排他所有権。
_Avoid_: 検証 lease、スキーマ移行 lock

**移行 run**:
一つの対象 DB とリリース成果物に対する、一回限りの migration container の実行単位。
_Avoid_: 通常のアプリケーション起動、schema contract

**release control store**:
移行 run と検証 lease を管理する deployment control-plane の永続ストア。対象 RDB には含めない。
_Avoid_: target RDB、migration history

## ThreadGroup 用語

**ThreadGroup**:
複数の Thread を一つの作業・調査・変更単位としてまとめる primary な論理コンテナ。一つの active Thread は最大一つの primary ThreadGroup に所属する。

**group owner**:
ThreadGroup の所有者。member Thread 個々の所有者とは独立し、認可された group owner は異なる所有者の member を含む group 全体を参照できる。表示 root や操作主体を意味しない。

**membership**:
Thread と primary ThreadGroup の所属を表す link metadata。Thread やチャット本文のコピーではない。active な所属のほか、merge または split 前の所属を示す `redirected`、Thread 削除後の `deleted` placeholder を履歴として保持できる。

**Thread relation**:
二つの Thread 間で成立した方向付きの事実。membership や group 間参照ではなく、`delegated`、`fork`、`continuation` のいずれかで表す。manual split 後も relation 自体は保持できる。

**relation kind**:
Thread 間の関係の種別。`delegated` は委譲、`fork` は分岐、`continuation` は別 native session による継続を表す。

**observation / evidence level**:
関係候補を支持する観測と、その保証レベル。`exact` は明示的根拠、`strong` は複数根拠による強い裏付け、`heuristic` は候補に留まる推測、`unsupported` は安全に識別できないため正規 relation に使用しない状態を表す。

**identity key parts**:
Thread の source identity を構成する `(user_id, source, identity_scope, native_id)` の四つ組。ここでの user は Thread の所有者であり、group owner とは区別する。

**source token**:
source identity の `source` に使う、importer の canonical な識別子。Codex は `codex`、OpenCode は `opencode`、Claude Code は `claude_code` とする。

**trusted internal caller**:
前段で認証・認可済みの内部呼出し元。memories は caller の認可を再判定せず、受け取った操作主体を監査用 `actor_id` として記録する。

**grouping authority**:
primary ThreadGroup の配置を決めた主体。手動指定された所属は固定され、自動追加された member の所属は最も近い手動所属済み祖先の group に従う。

**group redirect**:
merge で吸収された旧 ThreadGroup ID から target group への alias。target が後に分割された場合は一つの group ではなく分割結果を指す。

**manual collection**:
primary ThreadGroup と独立した、作成者の user_id に属する任意分類。同じ Thread を複数 collection に所属させられ、canonical relation、root、primary membership を変更しない。Thread データを複製せず参照だけを保持し、link の削除または Thread 削除で所属は失われ、通常または明示 override 付き reimport で復元されない。可視性は前段の client / proxy が制御する。

**placeholder member**:
削除された Thread の canonical key、所属、削除時刻を保持する系譜上の node。source の有無や再 import 許可にかかわらず保持し、本文・概要は含まない。

**revival**:
再 import を許可された Thread が同じ source identity から再作成され、現在の所属と relation endpoint が再接続されること。relation の採用撤回を取り消す操作は含まない。

**suppression marker**:
source-backed Thread の削除時に作成する、content を含まない deletion marker。`forbid_reimport` の値を保持し、`true` の場合に通常再 import を防ぐ。Thread やチャット本文のコピーではない。

**ThreadGroup latest activity**:
current membership のうち現存 Thread が持つ会話最新日時の最大値。deleted placeholder は本文を持たないため値に寄与しない。対象となる会話最新日時がなければ値はない。

**ThreadGroup policy version**:
canonical relation の evidence 組合せ、relation type、選択優先順位を固定する不変の判定契約の識別子。runtime flag や利用者設定ではない。採用済み relation が exact / strong のどちらだったかを監査可能にする。

**inactive history purge**:
ThreadGroup の non-active membership、group、relation、対応する observation / audit、deletion marker を operator の明示確認後に物理削除する操作。active membership / relation と Thread / Memory content は対象外であり、deletion marker を削除すると `forbid_reimport` の禁止も失われる。placeholder-only group は membership purge 後に空になった場合だけ削除される。
