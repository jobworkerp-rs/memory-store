//! `inspect` / `plan` against a real RDB and LanceDB, observing that they
//! classify correctly and change nothing.

use super::counts::CountKind;
use super::inspect::derive as inspect;
use super::observe::{EmbeddingEnv, StoreSpec, observe};
use super::output::{Decision, ErrorCode, SpaceValue, State};
use super::plan::derive as plan;
use infra::infra::embedding_dispatch::ImageSearchMode;
use infra::infra::embedding_index::source_version::{self, TextSource};
use infra::infra::embedding_index::{EmbeddingIndex, EntryOutcome, IndexEntry, TableLabel};
use infra::infra::embedding_space::SpaceComponents;
use infra::infra::embedding_space::record::{
    MarkerState, MigrationMarker, SpaceRecord, write_marker, write_space_record,
};
use infra::infra::memory::rdb::{MemoryRepository, MemoryRepositoryImpl};
use infra::infra::memory_vector::config::{
    DistanceType, FtsConfig, VectorDBConfig, VectorIndexConfig,
};
use infra::infra::memory_vector::record::MemoryVectorRecord;
use infra::infra::memory_vector::repository::MemoryVectorRepositoryImpl;
use infra_utils::infra::rdb::{RdbPool, UseRdbPool};
use infra_utils::infra::test::{TEST_RUNTIME, setup_test_rdb_from};
use protobuf::llm_memory::data::{ContentType, MemoryData, MessageRole, UserId};

const DIM: usize = 4;

fn space(model: &str) -> SpaceComponents {
    SpaceComponents {
        model_id: model.into(),
        tokenizer_model_id: String::new(),
        revision: "unversioned".into(),
        dimension: DIM as u32,
        distance: "cosine".into(),
    }
}

async fn pool() -> &'static RdbPool {
    if cfg!(feature = "postgres") {
        let pool = setup_test_rdb_from("../infra/sql/postgres").await;
        sqlx::query("TRUNCATE TABLE memory, thread CASCADE;")
            .execute(pool)
            .await
            .unwrap();
        pool
    } else {
        let pool = setup_test_rdb_from("../infra/sql/sqlite").await;
        for t in ["thread_memory", "memory", "thread"] {
            sqlx::query(sqlx::AssertSqlSafe(format!("DELETE FROM {t}")))
                .execute(pool)
                .await
                .unwrap();
        }
        pool
    }
}

struct Fixture {
    _dir: tempfile::TempDir,
    uri: String,
    env: EmbeddingEnv,
    pool: &'static RdbPool,
    memory: MemoryRepositoryImpl,
}

async fn fixture() -> Fixture {
    let pool = pool().await;
    let dir = tempfile::tempdir().unwrap();
    let uri = dir.path().join("lance").to_string_lossy().to_string();
    let env = EmbeddingEnv {
        stores: vec![StoreSpec {
            label: TableLabel::Memory,
            uri: uri.clone(),
            table_name: "memories".into(),
        }],
        current: Some(space("m")),
        state_dir: dir.path().join("state"),
        image_search_mode: ImageSearchMode::None,
        max_content_len: 100,
    };
    Fixture {
        memory: MemoryRepositoryImpl::new(infra::infra::IdGeneratorWrapper::new(), pool),
        _dir: dir,
        uri,
        env,
        pool,
    }
}

impl Fixture {
    async fn add_memory(&self, content: &str) -> (i64, MemoryData) {
        let data = MemoryData {
            user_id: Some(UserId { value: 1 }),
            content: content.into(),
            content_type: ContentType::Text as i32,
            role: MessageRole::RoleUser as i32,
            ..Default::default()
        };
        let mut tx = self.memory.db_pool().begin().await.unwrap();
        let id = self.memory.create(&mut *tx, &data).await.unwrap();
        tx.commit().await.unwrap();
        (id.value, data)
    }

    async fn repo(&self) -> MemoryVectorRepositoryImpl {
        MemoryVectorRepositoryImpl::new(VectorDBConfig {
            uri: self.uri.clone(),
            table_name: "memories".into(),
            vector_size: DIM,
            distance_type: DistanceType::Cosine,
            fts: FtsConfig::default(),
            vector_index: VectorIndexConfig::default(),
        })
        .await
        .unwrap()
    }

    async fn add_rows(&self, id: i64, data: &MemoryData, chunks: i32) {
        let records = (0..chunks)
            .map(|c| {
                MemoryVectorRecord::from_chunk_with_content(
                    id,
                    data,
                    &[0.5; DIM],
                    Some("runner-model"),
                    "text",
                    c,
                    0,
                    1,
                    "x".into(),
                )
            })
            .collect();
        self.repo()
            .await
            .replace_kinds_upsert(id, &["text"], records)
            .await
            .unwrap();
    }

    async fn index_success(&self, id: i64, text: &str, chunks: u32) {
        EmbeddingIndex::open(&self.uri)
            .await
            .unwrap()
            .put(&[IndexEntry {
                table: TableLabel::Memory,
                entity_id: id,
                vector_kind: "text".into(),
                space_id: space("m").space_id(),
                source_version: source_version::text(TextSource::MemoryText, text),
                generation_id: "g".into(),
                outcome: EntryOutcome::Success {
                    chunk_count: chunks,
                },
                media_digest: None,
                recorded_at: 0,
            }])
            .await
            .unwrap();
    }

    async fn record_space(&self) {
        let repo = self.repo().await;
        write_space_record(&repo.table_handle(), &SpaceRecord::new(&space("m"), false))
            .await
            .unwrap();
    }
}

#[test]
fn empty_everything_is_consistent_and_adopted_on_start() {
    TEST_RUNTIME.block_on(async {
        let f = fixture().await;
        let obs = observe(f.pool, &f.env).await;
        assert_eq!(obs.space, SpaceValue::None);
        let line = inspect(&obs);
        assert_eq!(line.state, State::Consistent);
        let p = plan(&obs, line.state, space("other").space_id().as_str());
        assert_eq!(p.decision, Decision::AdoptOnStart);
        assert!(
            !std::path::Path::new(&f.uri).exists(),
            "inspect created nothing"
        );
        assert!(!f.env.state_dir.exists());
    });
}

#[test]
fn rdb_targets_without_vectors_need_reembedding_unless_recorded_in_the_target() {
    TEST_RUNTIME.block_on(async {
        let f = fixture().await;
        f.add_memory("hello").await;
        let obs = observe(f.pool, &f.env).await;
        let line = inspect(&obs);
        assert_eq!(
            (line.state, line.missing.to_string()),
            (State::Incomplete, "1".into())
        );
        assert_eq!(
            plan(&obs, line.state, space("m").space_id().as_str()).decision,
            Decision::ReembedRequired
        );

        // Recorded in the target space: reconciling is enough.
        f.record_space().await;
        let obs = observe(f.pool, &f.env).await;
        let line = inspect(&obs);
        assert_eq!(line.state, State::Incomplete);
        assert_eq!(
            plan(&obs, line.state, space("m").space_id().as_str()).decision,
            Decision::ReconcileRequired
        );
    });
}

#[test]
fn counts_distinguish_states_per_entity_not_per_chunk() {
    TEST_RUNTIME.block_on(async {
        let f = fixture().await;
        f.record_space().await;
        let (complete, d1) = f.add_memory("complete").await;
        let (stale, d2) = f.add_memory("edited").await;
        let (unverified, d3) = f.add_memory("legacy").await;
        f.add_rows(complete, &d1, 3).await;
        f.index_success(complete, "complete", 3).await;
        f.add_rows(stale, &d2, 2).await;
        f.index_success(stale, "before edit", 2).await;
        f.add_rows(unverified, &d3, 2).await;
        f.add_rows(complete + 1_000_000, &d1, 1).await;

        let obs = observe(f.pool, &f.env).await;
        let c = *obs.counts.as_ref().unwrap().kind(CountKind::MemoryText);
        assert_eq!(
            (c.required, c.complete, c.stale, c.unverified),
            (3, 1, 1, 1)
        );
        assert_eq!(obs.counts.as_ref().unwrap().orphan, 1);
        assert_eq!(inspect(&obs).state, State::Incomplete);
    });
}

#[test]
fn only_unverified_rows_are_not_consistent() {
    TEST_RUNTIME.block_on(async {
        let f = fixture().await;
        let (id, data) = f.add_memory("old").await;
        f.add_rows(id, &data, 1).await;
        let obs = observe(f.pool, &f.env).await;
        assert_eq!(obs.space, SpaceValue::Id(space("m").space_id().to_string()));
        assert_eq!(inspect(&obs).state, State::Unverified);
    });
}

#[test]
fn markers_block_and_nothing_is_written() {
    TEST_RUNTIME.block_on(async {
        let f = fixture().await;
        let (id, data) = f.add_memory("x").await;
        f.add_rows(id, &data, 1).await;
        let repo = f.repo().await;
        write_marker(
            &repo.table_handle(),
            Some(&MigrationMarker {
                state: MarkerState::Pending,
                attempt_id: "a".into(),
            }),
        )
        .await
        .unwrap();
        let version = repo.table_handle().version().await.unwrap();
        let obs = observe(f.pool, &f.env).await;
        assert_eq!(inspect(&obs).state, State::RebuildInconsistent);
        let p = plan(
            &obs,
            State::RebuildInconsistent,
            space("n").space_id().as_str(),
        );
        assert_eq!(p.reason, Some(ErrorCode::EmbeddingSwitchInProgress));
        assert!(!p.executable);
        assert_eq!(
            f.repo().await.table_handle().version().await.unwrap(),
            version,
            "inspect and plan do not write to LanceDB"
        );
        assert!(
            EmbeddingIndex::open_existing(&f.uri)
                .await
                .unwrap()
                .is_none()
        );
    });
}

#[test]
fn mixed_unrecorded_models_are_unknown_and_need_reembedding() {
    TEST_RUNTIME.block_on(async {
        let f = fixture().await;
        let (id, data) = f.add_memory("a").await;
        f.add_rows(id, &data, 1).await;
        let repo = f.repo().await;
        let other = MemoryVectorRecord::from_chunk_with_content(
            id + 1,
            &data,
            &[0.5; DIM],
            Some("another-model"),
            "text",
            0,
            0,
            1,
            "x".into(),
        );
        repo.replace_kinds_upsert(id + 1, &["text"], vec![other])
            .await
            .unwrap();
        let obs = observe(f.pool, &f.env).await;
        assert_eq!(obs.space, SpaceValue::Unknown);
        let line = inspect(&obs);
        assert_eq!(line.state, State::Unknown);
        assert_eq!(line.required.to_string(), "unknown");
        let p = plan(&obs, line.state, space("m").space_id().as_str());
        assert_eq!(p.decision, Decision::ReembedRequired);
        assert_eq!(
            p.memory_text, 1,
            "targets are counted even in an unknown space"
        );
        assert_eq!(p.failed_permanent.to_string(), "unknown");
    });
}

#[test]
fn unavailable_database_reports_unknown_counts() {
    TEST_RUNTIME.block_on(async {
        let f = fixture().await;
        #[cfg(feature = "postgres")]
        let url = "postgres://nobody:nothing@127.0.0.1:1/absent";
        #[cfg(not(feature = "postgres"))]
        let url = "sqlite:///nonexistent-directory-for-inspect/absent.sqlite3";
        let unreachable: &'static RdbPool = Box::leak(Box::new(
            sqlx::pool::PoolOptions::new()
                .acquire_timeout(std::time::Duration::from_secs(2))
                .connect_lazy(url)
                .unwrap(),
        ));
        let obs = observe(unreachable, &f.env).await;
        let line = inspect(&obs);
        assert_eq!(line.state, State::Unavailable);
        assert_eq!(line.required.to_string(), "unknown");
    });
}
