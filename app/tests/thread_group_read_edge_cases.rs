//! Read-model edge cases that the golden characterization fixture does not
//! reach: owner scoping when a group's member thread belongs to another
//! user, and inputs larger than one `IN (...)` chunk.
//! Each test builds its own rows in this binary's private database.
#![cfg(not(feature = "postgres"))]

use anyhow::Result;
use app::app::thread_group::{
    ThreadGroupMemberSearchHit, ThreadGroupMemberSearchProvider, ThreadGroupMemberSearchQuery,
    ThreadGroupReadService,
};
use async_trait::async_trait;
use infra::infra::thread_group::candidate::{
    ThreadGroupCandidateAssociationRepository, ThreadGroupCandidateAssociationRepositoryImpl,
};
use infra::infra::thread_group::group::{ThreadGroupRepository, ThreadGroupRepositoryImpl};
use infra::infra::thread_group::member::{
    ThreadGroupMemberRepository, ThreadGroupMemberRepositoryImpl,
};
use infra::infra::thread_group::rows::values;
use infra::infra::thread_group::test_support::{
    IN_LIST_CHUNK_SIZE, T0, insert_thread_for_user, key, new_candidate, new_group, new_member,
    setup_thread_group_pool,
};
use infra_utils::infra::rdb::RdbPool;
use infra_utils::infra::test::TEST_RUNTIME;
use protobuf::llm_memory::data::ThreadSearchFilter;
use std::sync::Mutex;

static TEST_DATABASE_LOCK: Mutex<()> = Mutex::new(());

fn lock_test_database() -> std::sync::MutexGuard<'static, ()> {
    // Tests share one temporary database within this binary.
    TEST_DATABASE_LOCK.lock().expect("test database lock")
}

/// Browse-only searches never consult member search.
struct NoSearch;

#[async_trait]
impl ThreadGroupMemberSearchProvider for NoSearch {
    async fn search_member(
        &self,
        _query: &ThreadGroupMemberSearchQuery,
        _group_id: i64,
        _thread_id: i64,
    ) -> Result<Option<ThreadGroupMemberSearchHit>> {
        Ok(None)
    }
}

/// One group owned by `group_owner` whose single member thread belongs to
/// `thread_owner`. Takes a connection so bulk fixtures can share a
/// transaction.
async fn add_group(
    pool: &'static RdbPool,
    conn: &mut sqlx::SqliteConnection,
    n: u64,
    group_owner: i64,
    thread_owner: i64,
) -> i64 {
    let groups = ThreadGroupRepositoryImpl::new(infra::test_helper::shared_id_generator(), pool);
    let mut group = new_group(n);
    group.user_id = group_owner;
    let group_id = groups.create_tx(&mut *conn, &group).await.expect("group");
    let thread_id = n as i64;
    insert_thread_for_user(&mut *conn, thread_id, thread_owner, Some(T0 + thread_id)).await;
    ThreadGroupMemberRepositoryImpl::new(pool)
        .insert_tx(&mut *conn, &new_member(group_id, Some(thread_id), key(n)))
        .await
        .expect("member");
    group_id
}

async fn browse(
    read: &ThreadGroupReadService,
    thread_user_id: i64,
    owner: Option<i64>,
) -> Vec<i64> {
    let filter = ThreadSearchFilter {
        user_id: Some(thread_user_id),
        ..Default::default()
    };
    read.search_groups(&NoSearch, &[], false, 100, None, Some(&filter), owner)
        .await
        .expect("browse")
        .results
        .into_iter()
        .map(|result| result.group.id)
        .collect()
}

#[test]
fn browse_filters_member_threads_while_owner_scopes_groups() {
    let _lock = lock_test_database();
    TEST_RUNTIME
        .block_on(async {
            let pool = setup_thread_group_pool().await;
            let read = ThreadGroupReadService::new(pool);
            // Owner 11's group holds a thread of user 12; owner 12's group
            // holds its own thread.
            let mut conn = pool.acquire().await?;
            let foreign_member = add_group(pool, &mut conn, 910_001, 11, 12).await;
            let own_member = add_group(pool, &mut conn, 910_002, 12, 12).await;
            drop(conn);

            // The browse filter matches member threads regardless of group owner.
            let unscoped = browse(&read, 12, None).await;
            assert!(unscoped.contains(&foreign_member) && unscoped.contains(&own_member));
            // The owner scope applies to the group, so another owner's group is
            // never returned even when its member thread matches.
            assert_eq!(browse(&read, 12, Some(12)).await, vec![own_member]);
            assert_eq!(browse(&read, 12, Some(11)).await, vec![foreign_member]);
            // A group whose only member belongs to someone else does not match
            // its own owner's thread filter.
            assert!(browse(&read, 11, Some(11)).await.is_empty());

            assert!(read.get_lineage(foreign_member, Some(12)).await?.is_none());
            assert!(read.get_lineage(foreign_member, Some(11)).await?.is_some());
            Ok::<_, anyhow::Error>(())
        })
        .expect("owner scoping");
}

#[test]
fn reads_cover_groups_beyond_one_in_list_chunk() {
    let _lock = lock_test_database();
    TEST_RUNTIME
        .block_on(async {
            let pool = setup_thread_group_pool().await;
            let read = ThreadGroupReadService::new(pool);
            let owner = 31;
            let total = IN_LIST_CHUNK_SIZE + 1;
            // One transaction: SQLite would otherwise commit (and sync) per row.
            let mut tx = pool.begin().await?;
            let mut group_ids = Vec::new();
            for index in 0..total {
                group_ids
                    .push(add_group(pool, &mut tx, 930_000 + index as u64, owner, owner).await);
            }
            tx.commit().await?;
            // Unresolved rows that only the last chunk can match: one naming the
            // group and one naming its member thread.
            let last_group = *group_ids.last().unwrap();
            let last_thread = (930_000 + total - 1) as i64;
            let candidates = ThreadGroupCandidateAssociationRepositoryImpl::new(
                infra::test_helper::shared_id_generator(),
                pool,
            );
            for (n, group, thread) in [
                (930_901, Some(last_group), None),
                (930_902, None, Some(last_thread)),
            ] {
                let mut candidate = new_candidate(n);
                candidate.state = values::candidate_state::PENDING.to_string();
                candidate.candidate_group_id = group;
                candidate.subject_thread_id = thread;
                candidates.insert_tx(pool, &candidate).await?;
            }

            let views = read.list_groups(false, None, None, Some(owner)).await?;
            assert_eq!(views.len(), total);
            assert_eq!(read.count_groups(Some(owner)).await?, total as i64);
            let last = views
                .iter()
                .find(|view| view.id == last_group)
                .expect("last group listed");
            assert_eq!(last.unresolved_count, 2);
            assert_eq!(last.active_member_count, 1);

            // Paging clamps to 100 per page and still visits every group once.
            let filter = ThreadSearchFilter {
                user_id: Some(owner),
                ..Default::default()
            };
            let mut seen = Vec::new();
            let mut token = None;
            loop {
                let page = read
                    .search_groups(
                        &NoSearch,
                        &[],
                        false,
                        1_000,
                        token.as_deref(),
                        Some(&filter),
                        Some(owner),
                    )
                    .await?;
                assert!(page.results.len() <= 100);
                seen.extend(page.results.into_iter().map(|result| result.group.id));
                token = page.next_page_token;
                if token.is_none() {
                    break;
                }
            }
            seen.sort_unstable();
            group_ids.sort_unstable();
            assert_eq!(seen, group_ids);
            Ok::<_, anyhow::Error>(())
        })
        .expect("chunk boundary");
}
