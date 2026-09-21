#![cfg(not(feature = "postgres"))]

use anyhow::Result;
use app::app::thread_group::{
    GroupSearchMode, GroupSearchTarget, ThreadGroupMemberSearchHit,
    ThreadGroupMemberSearchProvider, ThreadGroupMemberSearchQuery, ThreadGroupReadService,
};
use async_trait::async_trait;
use infra::infra::thread_group::group::{ThreadGroupRepository, ThreadGroupRepositoryImpl};
use infra::infra::thread_group::member::{
    ThreadGroupMemberRepository, ThreadGroupMemberRepositoryImpl,
};
use infra::infra::thread_group::test_support::{
    T0, insert_thread, key, new_group, new_member, setup_thread_group_pool,
};
use infra::infra::thread_label::rdb::{ThreadLabelRepository, ThreadLabelRepositoryImpl};
use protobuf::llm_memory::data::{MemoryKind, ThreadSearchFilter};
use std::collections::HashMap;
use std::sync::Mutex;

static TEST_DATABASE_LOCK: Mutex<()> = Mutex::new(());

fn lock_test_database() -> std::sync::MutexGuard<'static, ()> {
    // The fixture intentionally shares one temporary database within this test binary.
    TEST_DATABASE_LOCK.lock().expect("test database lock")
}

#[derive(Default)]
struct FakeSearchProvider {
    scores: HashMap<(String, i64), f32>,
}

#[async_trait]
impl ThreadGroupMemberSearchProvider for FakeSearchProvider {
    async fn search_member(
        &self,
        query: &ThreadGroupMemberSearchQuery,
        _group_id: i64,
        thread_id: i64,
    ) -> Result<Option<ThreadGroupMemberSearchHit>> {
        Ok(self
            .scores
            .get(&(query.query_text.clone(), thread_id))
            .copied()
            .map(|score| ThreadGroupMemberSearchHit { thread_id, score }))
    }
}

fn query(target: GroupSearchTarget, text: &str) -> ThreadGroupMemberSearchQuery {
    ThreadGroupMemberSearchQuery {
        target,
        mode: GroupSearchMode::Keyword,
        query_text: text.to_owned(),
        query_vectors: Vec::new(),
        thread_filter: None,
        memory_filter: None,
        hybrid_options: None,
    }
}

async fn add_group(group_key: u64, thread_ids: &[i64]) -> i64 {
    let pool = setup_thread_group_pool().await;
    let groups = ThreadGroupRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool);
    let group_id = groups
        .create_tx(pool, &new_group(group_key))
        .await
        .expect("group");
    let members = ThreadGroupMemberRepositoryImpl::new(pool);
    for thread_id in thread_ids {
        insert_thread(pool, *thread_id, Some(*thread_id)).await;
        members
            .insert_tx(
                pool,
                &new_member(group_id, Some(*thread_id), key(*thread_id as u64)),
            )
            .await
            .expect("member");
    }
    group_id
}

#[test]
fn all_predicates_default_to_one_member_and_cross_member_is_explicit() {
    let _database_lock = lock_test_database();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let group_id = add_group(700_001, &[700_001, 700_002]).await;
        let mut provider = FakeSearchProvider::default();
        provider.scores.insert(("thread".into(), 700_001), 0.8);
        provider.scores.insert(("memory".into(), 700_002), 0.9);
        let read = ThreadGroupReadService::new(setup_thread_group_pool().await);
        let predicates = vec![
            query(GroupSearchTarget::Thread, "thread"),
            query(GroupSearchTarget::Memory, "memory"),
        ];

        let same_member = read
            .search_groups(&provider, &predicates, false, 10, None, None)
            .await
            .expect("same-member search");
        assert!(same_member.results.is_empty());

        let cross_member = read
            .search_groups(&provider, &predicates, true, 10, None, None)
            .await
            .expect("cross-member search");
        assert_eq!(cross_member.results.len(), 1);
        assert_eq!(cross_member.results[0].group.id, group_id);
        assert_eq!(cross_member.results[0].witnesses.len(), 2);
    });
}

#[test]
fn group_search_uses_max_member_score_then_latest_and_canonical_ties() {
    let _database_lock = lock_test_database();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let first = add_group(700_010, &[700_010]).await;
        let second = add_group(700_011, &[700_011]).await;
        let third = add_group(700_012, &[700_012]).await;
        let mut provider = FakeSearchProvider::default();
        provider.scores.insert(("q".into(), 700_010), 0.7);
        provider.scores.insert(("q".into(), 700_011), 0.9);
        provider.scores.insert(("q".into(), 700_012), 0.9);
        let read = ThreadGroupReadService::new(setup_thread_group_pool().await);
        let page = read
            .search_groups(
                &provider,
                &[query(GroupSearchTarget::Thread, "q")],
                false,
                10,
                None,
                None,
            )
            .await
            .expect("search");
        assert_eq!(page.results[0].group.id, third);
        assert_eq!(page.results[1].group.id, second);
        assert_eq!(page.results[2].group.id, first);
        assert_eq!(page.results[0].relevance_score, 0.9);
    });
}

#[test]
fn keyset_cursor_is_opaque_and_rejects_a_changed_snapshot() {
    let _database_lock = lock_test_database();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        add_group(700_020, &[700_020]).await;
        add_group(700_021, &[700_021]).await;
        let pool = setup_thread_group_pool().await;
        let read = ThreadGroupReadService::new(pool);
        let mut provider = FakeSearchProvider::default();
        provider.scores.insert(("q".into(), 700_020), 0.9);
        provider.scores.insert(("q".into(), 700_021), 0.8);
        let predicates = [query(GroupSearchTarget::Thread, "q")];

        let first = read
            .search_groups(&provider, &predicates, false, 1, None, None)
            .await
            .expect("first page");
        let cursor = first.next_page_token.expect("next page");
        assert!(!cursor.contains("offset"));
        assert!(!cursor.contains("group_canonical_key"));

        let second = read
            .search_groups(&provider, &predicates, false, 1, Some(&cursor), None)
            .await
            .expect("second page");
        assert_eq!(second.results.len(), 1);
        assert_ne!(second.results[0].group.id, first.results[0].group.id);

        let groups =
            ThreadGroupRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool);
        groups
            .update_title_tx(pool, first.results[0].group.id, Some("changed"), 9_999_999)
            .await
            .expect("update group");
        let error = read
            .search_groups(&provider, &predicates, false, 1, Some(&cursor), None)
            .await
            .expect_err("changed snapshot must reject cursor");
        assert!(error.to_string().contains("snapshot_changed"));
    });
}

#[test]
fn browse_cursor_search_supports_label_only_filters() {
    let _database_lock = lock_test_database();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(async {
        let matching = add_group(700_030, &[700_030]).await;
        let other = add_group(700_031, &[700_031]).await;
        let pool = setup_thread_group_pool().await;
        sqlx::query("UPDATE thread SET memory_kind = 0 WHERE id = ?")
            .bind(700_030_i64)
            .execute(pool)
            .await
            .expect("legacy unspecified memory kind");
        let labels = ThreadLabelRepositoryImpl::new(pool);
        labels
            .add_labels(700_030, &["project:lookback".into()], T0)
            .await
            .expect("label");

        let read = ThreadGroupReadService::new(pool);
        let filter = ThreadSearchFilter {
            user_id: Some(1),
            labels: vec!["project:lookback".into()],
            memory_kinds: vec![MemoryKind::Raw as i32],
            ..Default::default()
        };
        let page = read
            .search_groups(
                &FakeSearchProvider::default(),
                &[],
                false,
                10,
                None,
                Some(&filter),
            )
            .await
            .expect("browse page");
        assert_eq!(
            page.results
                .iter()
                .map(|result| result.group.id)
                .collect::<Vec<_>>(),
            vec![matching]
        );
        assert_ne!(
            page.results
                .iter()
                .map(|result| result.group.id)
                .collect::<Vec<_>>(),
            vec![other]
        );
        assert!(page.results[0].witnesses.is_empty());
    });
}
