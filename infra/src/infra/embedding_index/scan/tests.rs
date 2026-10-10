use super::*;
use crate::infra::embedding_index::table::EntryOutcome;
use crate::infra::memory::rdb::{MemoryRepository, MemoryRepositoryImpl};
use crate::infra::vector_table::open_or_create;
use arrow_array::{RecordBatch, RecordBatchIterator};
use arrow_schema::{DataType, Field, Schema};
use infra_utils::infra::rdb::UseRdbPool;
use protobuf::llm_memory::data::{ContentType, MemoryData, MessageRole, UserId};
use std::sync::Arc;

async fn setup_pool() -> &'static RdbPool {
    use infra_utils::infra::test::setup_test_rdb_from;
    let (dir, reset) = if cfg!(feature = "postgres") {
        ("sql/postgres", "TRUNCATE TABLE memory, thread CASCADE;")
    } else {
        (
            "sql/sqlite",
            "DELETE FROM thread_memory; DELETE FROM memory; DELETE FROM thread;",
        )
    };
    let pool = setup_test_rdb_from(dir).await;
    for stmt in reset.split(';').filter(|s| !s.trim().is_empty()) {
        sqlx::query(stmt).execute(pool).await.unwrap();
    }
    pool
}

async fn insert(
    repo: &MemoryRepositoryImpl,
    content: &str,
    role: MessageRole,
    ct: ContentType,
    metadata: Option<&str>,
) -> i64 {
    let data = MemoryData {
        user_id: Some(UserId { value: 1 }),
        content: content.to_string(),
        content_type: ct as i32,
        role: role as i32,
        metadata: metadata.map(str::to_string),
        ..Default::default()
    };
    let mut tx = repo.db_pool().begin().await.unwrap();
    let id = repo.create(&mut *tx, &data).await.unwrap();
    tx.commit().await.unwrap();
    id.value
}

fn rows_schema(id: &str) -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new(id, DataType::Int64, false),
        Field::new("vector_kind", DataType::Utf8, false),
        Field::new("chunk_index", DataType::Int32, false),
    ]))
}

async fn vector_table(uri: &str, name: &str, id: &str, rows: &[(i64, &str, i32)]) -> Table {
    let schema = rows_schema(id);
    let table = open_or_create(uri, name, &schema).await.unwrap().table;
    if !rows.is_empty() {
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.0))),
                Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.1))),
                Arc::new(Int32Array::from_iter_values(rows.iter().map(|r| r.2))),
            ],
        )
        .unwrap();
        let reader: Box<dyn arrow_array::RecordBatchReader + Send> = Box::new(
            RecordBatchIterator::new(vec![Ok::<_, arrow_schema::ArrowError>(batch)], schema),
        );
        table.add(reader).execute().await.unwrap();
    }
    table
}

fn entry(table: TableLabel, id: i64, version: &SourceVersion, chunks: u32) -> IndexEntry {
    IndexEntry {
        table,
        entity_id: id,
        vector_kind: "text".into(),
        space_id: SpaceId("space".into()),
        source_version: version.clone(),
        generation_id: "g".into(),
        outcome: EntryOutcome::Success {
            chunk_count: chunks,
        },
        media_digest: None,
        recorded_at: 0,
    }
}

async fn scan_all(scanner: &mut Scanner<'_>) -> Vec<ScanItem> {
    let mut all = Vec::new();
    while let Some(page) = scanner.next_page().await.unwrap() {
        all.extend(page);
    }
    all
}

fn summary(items: &[ScanItem]) -> Vec<String> {
    let mut out: Vec<String> = items
        .iter()
        .map(|i| match i {
            ScanItem::Target {
                table,
                entity_id,
                vector_kind,
                state,
                ..
            } => format!(
                "{}:{entity_id}:{vector_kind}:{}",
                table.as_str(),
                state.as_str()
            ),
            ScanItem::Orphan {
                table,
                entity_id,
                vector_kind,
                ..
            } => format!("{}:{entity_id}:{vector_kind}:orphan", table.as_str()),
            ScanItem::CaptionMissing { entity_id, .. } => format!("caption:{entity_id}"),
        })
        .collect();
    out.sort();
    out
}

/// Every state, orphans past the last entity, intents, and threads, with
/// a page size that splits the entities across pages.
#[test]
fn classifies_targets_and_orphans_across_pages() {
    infra_utils::infra::test::TEST_RUNTIME.block_on(async {
        let pool = setup_pool().await;
        let idg = crate::test_helper::shared_id_generator();
        let memory = MemoryRepositoryImpl::new(idg.clone(), pool);
        let media = MediaObjectRepositoryImpl::new(idg.clone(), pool);
        let user = MessageRole::RoleUser;
        let text = ContentType::Text;
        let complete = insert(&memory, "complete", user, text, None).await;
        let stale = insert(&memory, "edited", user, text, None).await;
        let unverified = insert(&memory, "legacy", user, text, None).await;
        let missing = insert(&memory, "new", user, text, None).await;
        let tool = insert(&memory, "Read()", user, ContentType::Tool, None).await;
        let reflection = insert(
            &memory,
            "summary",
            MessageRole::RoleReflection,
            text,
            Some(r#"{"eval":{"task_intent":"intent"}}"#),
        )
        .await;
        let gone = reflection + 1_000_000;

        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_string_lossy().to_string();
        let memory_table = vector_table(
            &uri,
            "memories",
            "memory_id",
            &[
                (complete, "text", 0),
                (stale, "text", 0),
                (unverified, "text", 0),
                (tool, "text", 0),
                (gone, "text", 0),
            ],
        )
        .await;
        let intent_table = vector_table(&uri, "intents", "memory_id", &[]).await;
        let index = EmbeddingIndex::open(&uri).await.unwrap();
        let v = |s: &str| {
            crate::infra::embedding_index::source_version::text(
                crate::infra::embedding_index::source_version::TextSource::MemoryText,
                s,
            )
        };
        index
            .put(&[
                entry(TableLabel::Memory, complete, &v("complete"), 1),
                entry(TableLabel::Memory, stale, &v("before edit"), 1),
            ])
            .await
            .unwrap();

        let config = ScanConfig {
            space: SpaceId("space".into()),
            image_search_mode: ImageSearchMode::None,
            max_content_len: 100,
            page_size: 2,
        };
        let mut scanner = Scanner::new(
            pool,
            &media,
            config,
            vec![
                ScanTable {
                    label: TableLabel::Memory,
                    table: Some(memory_table),
                    index: Some(index.clone()),
                },
                ScanTable {
                    label: TableLabel::ReflectionIntent,
                    table: Some(intent_table),
                    index: Some(index.clone()),
                },
            ],
        );
        let items = scan_all(&mut scanner).await;
        let mut expected = vec![
            format!("memory:{complete}:text:complete"),
            format!("memory:{stale}:text:stale"),
            format!("memory:{unverified}:text:unverified"),
            format!("memory:{missing}:text:missing"),
            format!("memory:{tool}:text:orphan"),
            format!("memory:{reflection}:text:missing"),
            format!("memory:{gone}:text:orphan"),
            format!("reflection_intent:{reflection}:text:missing"),
        ];
        expected.sort();
        assert_eq!(summary(&items), expected);
    });
}

#[test]
fn empty_rdb_reports_every_row_as_orphan() {
    infra_utils::infra::test::TEST_RUNTIME.block_on(async {
        let pool = setup_pool().await;
        let idg = crate::test_helper::shared_id_generator();
        let media = MediaObjectRepositoryImpl::new(idg, pool);
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_string_lossy().to_string();
        let threads = vector_table(&uri, "threads", "thread_id", &[(7, "text", 0)]).await;
        let index = EmbeddingIndex::open(&uri).await.unwrap();
        let mut scanner = Scanner::new(
            pool,
            &media,
            ScanConfig {
                space: SpaceId("space".into()),
                image_search_mode: ImageSearchMode::None,
                max_content_len: 100,
                page_size: 10,
            },
            vec![ScanTable {
                label: TableLabel::Thread,
                table: Some(threads),
                index: Some(index),
            }],
        );
        assert_eq!(
            summary(&scan_all(&mut scanner).await),
            vec!["thread:7:text:orphan"]
        );
    });
}

#[test]
fn delete_orphans_removes_rows_and_entries_of_non_targets_only() {
    infra_utils::infra::test::TEST_RUNTIME.block_on(async {
        let pool = setup_pool().await;
        let idg = crate::test_helper::shared_id_generator();
        let memory = MemoryRepositoryImpl::new(idg.clone(), pool);
        let media = MediaObjectRepositoryImpl::new(idg.clone(), pool);
        let kept = insert(
            &memory,
            "kept",
            MessageRole::RoleUser,
            ContentType::Text,
            None,
        )
        .await;
        let gone = kept + 1_000_000;
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_string_lossy().to_string();
        let table = vector_table(
            &uri,
            "memories",
            "memory_id",
            &[(kept, "text", 0), (gone, "text", 0), (gone, "text", 1)],
        )
        .await;
        let index = EmbeddingIndex::open(&uri).await.unwrap();
        let v = crate::infra::embedding_index::source_version::text(
            crate::infra::embedding_index::source_version::TextSource::MemoryText,
            "x",
        );
        index
            .put(&[
                entry(TableLabel::Memory, kept, &v, 1),
                entry(TableLabel::Memory, gone + 1, &v, 1),
            ])
            .await
            .unwrap();
        let config = ScanConfig {
            space: SpaceId("space".into()),
            image_search_mode: ImageSearchMode::None,
            max_content_len: 100,
            page_size: 2,
        };
        let tables = vec![ScanTable {
            label: TableLabel::Memory,
            table: Some(table.clone()),
            index: Some(index.clone()),
        }];
        // A kind that is not a target of an existing memory is kept when
        // only deleted entities are cleaned.
        table
            .add({
                let schema = rows_schema("memory_id");
                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(Int64Array::from_iter_values([kept])),
                        Arc::new(StringArray::from_iter_values(["image"])),
                        Arc::new(Int32Array::from_iter_values([0])),
                    ],
                )
                .unwrap();
                Box::new(RecordBatchIterator::new(
                    vec![Ok::<_, arrow_schema::ArrowError>(batch)],
                    schema,
                )) as Box<dyn arrow_array::RecordBatchReader + Send>
            })
            .execute()
            .await
            .unwrap();
        let removed = delete_orphans(pool, true, &media, config.clone(), tables.clone())
            .await
            .unwrap();
        assert_eq!(
            removed, 2,
            "only `gone` and `gone + 1`, not the image of `kept`"
        );
        assert_eq!(table.count_rows(None).await.unwrap(), 2);
        let removed = delete_orphans(pool, false, &media, config.clone(), tables.clone())
            .await
            .unwrap();
        assert_eq!(removed, 1, "the image kind of `kept` is not a target");
        assert_eq!(table.count_rows(None).await.unwrap(), 1);
        let mut scanner = Scanner::new(pool, &media, config, tables);
        let after = summary(&scan_all(&mut scanner).await);
        assert!(after.iter().all(|s| !s.ends_with(":orphan")), "{after:?}");
        assert!(
            after.contains(&format!("memory:{kept}:text:stale")),
            "{after:?}"
        );
    });
}
